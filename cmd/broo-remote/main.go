package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultWorkdir          = "/home/curcio/polybot"
	defaultStatePath        = "logs/broo-remote_state.json"
	defaultLogPath          = "logs/broo-remote.out"
	defaultHeartbeatPath    = "logs/broo-remote.heartbeat"
	defaultTescesDBPath     = "tasks.db"
	defaultAIDADBPath       = "idle.db"
	defaultTaskLockPath     = "logs/broo-remote.task.lock"
	defaultTradesDBPath     = "logs/trades.db"
	backgroundTickInterval  = 1 * time.Minute
	tescesPollInterval      = 1 * time.Minute
	defaultAIDAPollInterval = 7 * time.Minute
	defaultTaskPollTimeout  = 15 * time.Second
	maxTelegramChars        = 4096
	codexAutoCompactFlag    = "enable_request_compression"
	defaultProvider         = "codex"
)

type Config struct {
	BotToken         string
	Workdir          string
	StatePath        string
	HeartbeatPath    string
	Provider         string
	RunnerBin        string
	Model            string
	HistoryPath      string
	AudioModel       string
	TescesPath       string
	AIDAPath         string
	AIDAPollInterval time.Duration
	TaskLockPath     string
}

type State struct {
	OwnerChatID   int64                `json:"owner_chat_id"`
	OwnerUserID   int64                `json:"owner_user_id"`
	LastUpdateID  int64                `json:"last_update_id"`
	Sessions      map[string]Session   `json:"sessions"`
	Queued        map[string][]Message `json:"queued"`
	BothEndsWatch *BothEndsWatch       `json:"both_ends_watch,omitempty"`
}

type Session struct {
	ThreadID  string `json:"thread_id"`
	UpdatedAt string `json:"updated_at"`
}

type BothEndsWatch struct {
	IDs         []int64 `json:"ids"`
	RequestedAt string  `json:"requested_at"`
}

type HistoryEntry struct {
	SessionID string `json:"session_id"`
	TS        int64  `json:"ts"`
	Text      string `json:"text"`
}

type SessionSummary struct {
	ThreadID  string
	LastTS    int64
	UpdatedAt string
	Preview   string
}

type Store struct {
	path  string
	mu    sync.Mutex
	state State
}

type TelegramClient struct {
	baseURL     string
	fileBaseURL string
	client      *http.Client
}

type SendMessageResponse struct {
	OK          bool          `json:"ok"`
	Result      TelegramReply `json:"result"`
	Description string        `json:"description"`
}

type UpdateResponse struct {
	OK          bool     `json:"ok"`
	Result      []Update `json:"result"`
	Description string   `json:"description"`
}

type GetFileResponse struct {
	OK          bool   `json:"ok"`
	Result      TGFile `json:"result"`
	Description string `json:"description"`
}

type UserResponse struct {
	OK          bool   `json:"ok"`
	Result      TGUser `json:"result"`
	Description string `json:"description"`
}

type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

type Message struct {
	MessageID int64         `json:"message_id"`
	Text      string        `json:"text"`
	Caption   string        `json:"caption"`
	Chat      Chat          `json:"chat"`
	From      *TGUser       `json:"from"`
	Voice     *TGMediaFile  `json:"voice"`
	Audio     *TGMediaFile  `json:"audio"`
	Video     *TGMediaFile  `json:"video"`
	Document  *TGMediaFile  `json:"document"`
	Photo     []TGPhotoSize `json:"photo"`
}

type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type TGUser struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
}

type TGMediaFile struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
	FileSize int64  `json:"file_size"`
}

type TGPhotoSize struct {
	FileID   string `json:"file_id"`
	FileSize int64  `json:"file_size"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
}

type TGFile struct {
	FileID   string `json:"file_id"`
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size"`
}

type TelegramReply struct {
	MessageID int64 `json:"message_id"`
}

type RemoteRunner struct {
	Provider string
	Binary   string
	Workdir  string
	Model    string
}

type TaskStore struct {
	name    string
	path    string
	db      *sql.DB
	dueOnly bool
}

type TaskRecord struct {
	ID           int64
	Prompt       string
	ScheduledFor string
}

type BothEndsReportRow struct {
	ID          int64
	Symbol      string
	Timeframe   string
	Status      string
	ResolvedAt  string
	SoldSide    string
	WinnerSide  string
	WindowStart int64
	WindowEnd   int64
	TotalCost   float64
	PnL         float64
}

type Bot struct {
	cfg      Config
	log      *log.Logger
	store    *Store
	telegram *TelegramClient
	runner   *RemoteRunner
	tesces   *TaskStore
	aida     *TaskStore

	mu      sync.Mutex
	busy    map[int64]bool
	pending map[int64][]*Message
	active  map[int64]context.CancelFunc
}

type ProgressReporter struct {
	tg        *TelegramClient
	chatID    int64
	messageID int64
	line      string
	lastText  string
	mu        sync.Mutex
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(cfg.StatePath), 0o755); err != nil {
		log.Fatalf("mkdir state dir: %v", err)
	}

	store, err := loadStore(cfg.StatePath)
	if err != nil {
		log.Fatalf("load state: %v", err)
	}

	tesces, err := openTaskStore("TASKS", cfg.TescesPath, true)
	if err != nil {
		log.Fatalf("open TASKS: %v", err)
	}
	defer tesces.db.Close()

	aida, err := openTaskStore("IDLE", cfg.AIDAPath, false)
	if err != nil {
		log.Fatalf("open IDLE: %v", err)
	}
	defer aida.db.Close()

	tg := &TelegramClient{
		baseURL:     fmt.Sprintf("https://api.telegram.org/bot%s", cfg.BotToken),
		fileBaseURL: fmt.Sprintf("https://api.telegram.org/file/bot%s", cfg.BotToken),
		client:      &http.Client{Timeout: 40 * time.Second},
	}

	me, err := tg.GetMe()
	if err != nil {
		log.Fatalf("telegram getMe: %v", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Printf("broo-remote online as @%s", me.Username)
	if store.OwnerChatID() != 0 {
		logger.Printf("owner chat locked to %d", store.OwnerChatID())
	} else {
		logger.Printf("owner chat not claimed yet; first /start will claim it")
	}

	bot := &Bot{
		cfg:      cfg,
		log:      logger,
		store:    store,
		telegram: tg,
		runner: &RemoteRunner{
			Provider: cfg.Provider,
			Binary:   cfg.RunnerBin,
			Workdir:  cfg.Workdir,
			Model:    cfg.Model,
		},
		tesces:  tesces,
		aida:    aida,
		busy:    make(map[int64]bool),
		pending: make(map[int64][]*Message),
		active:  make(map[int64]context.CancelFunc),
	}

	if err := bot.Run(context.Background()); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func loadConfig() (Config, error) {
	cfg := Config{
		BotToken:      strings.TrimSpace(os.Getenv("BROO_REMOTE_BOT_TOKEN")),
		Workdir:       strings.TrimSpace(os.Getenv("BROO_REMOTE_WORKDIR")),
		StatePath:     strings.TrimSpace(os.Getenv("BROO_REMOTE_STATE_PATH")),
		HeartbeatPath: strings.TrimSpace(os.Getenv("BROO_REMOTE_HEARTBEAT_PATH")),
		Provider:      strings.TrimSpace(os.Getenv("BROO_REMOTE_PROVIDER")),
		RunnerBin:     firstNonEmptyEnv("BROO_REMOTE_RUNNER_BIN", "BROO_REMOTE_CODEX_BIN"),
		Model:         strings.TrimSpace(os.Getenv("BROO_REMOTE_MODEL")),
		HistoryPath:   strings.TrimSpace(os.Getenv("BROO_REMOTE_HISTORY_PATH")),
		AudioModel:    strings.TrimSpace(os.Getenv("BROO_REMOTE_AUDIO_MODEL")),
		TescesPath:    firstNonEmptyEnv("BROO_REMOTE_TASKS_DB_PATH", "BROO_REMOTE_TESCES_DB_PATH"),
		AIDAPath:      firstNonEmptyEnv("BROO_REMOTE_IDLE_DB_PATH", "BROO_REMOTE_AIDA_DB_PATH"),
		AIDAPollInterval: firstPositiveDurationMinutesEnv(
			"BROO_REMOTE_IDLE_POLL_INTERVAL_MIN",
			"BROO_REMOTE_AIDA_POLL_INTERVAL_MIN",
		),
		TaskLockPath: strings.TrimSpace(os.Getenv("BROO_REMOTE_TASK_LOCK_PATH")),
	}
	if cfg.BotToken == "" {
		return Config{}, errors.New("BROO_REMOTE_BOT_TOKEN is required")
	}
	if cfg.Workdir == "" {
		cfg.Workdir = defaultWorkdir
	}
	if cfg.StatePath == "" {
		cfg.StatePath = defaultStatePath
	}
	if cfg.Provider == "" {
		cfg.Provider = defaultProvider
	}
	if cfg.RunnerBin == "" {
		cfg.RunnerBin = "codex"
	}
	if cfg.HeartbeatPath == "" {
		cfg.HeartbeatPath = filepath.Join(cfg.Workdir, defaultHeartbeatPath)
	}
	if cfg.HistoryPath == "" {
		cfg.HistoryPath = "/home/curcio/.codex/history.jsonl"
	}
	if cfg.AudioModel == "" {
		cfg.AudioModel = "base"
	}
	if cfg.TescesPath == "" {
		cfg.TescesPath = filepath.Join(cfg.Workdir, defaultTescesDBPath)
	}
	if cfg.AIDAPath == "" {
		cfg.AIDAPath = filepath.Join(cfg.Workdir, defaultAIDADBPath)
	}
	if cfg.AIDAPollInterval <= 0 {
		cfg.AIDAPollInterval = defaultAIDAPollInterval
	}
	if cfg.TaskLockPath == "" {
		cfg.TaskLockPath = filepath.Join(cfg.Workdir, defaultTaskLockPath)
	}
	if _, err := os.Stat(cfg.Workdir); err != nil {
		return Config{}, fmt.Errorf("workdir %s: %w", cfg.Workdir, err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.HeartbeatPath), 0o755); err != nil {
		return Config{}, fmt.Errorf("heartbeat dir: %w", err)
	}
	return cfg, nil
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func firstPositiveDurationMinutesEnv(keys ...string) time.Duration {
	for _, key := range keys {
		raw := strings.TrimSpace(os.Getenv(key))
		if raw == "" {
			continue
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			continue
		}
		return time.Duration(value) * time.Minute
	}
	return 0
}

func openTaskStore(name, path string, dueOnly bool) (*TaskStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("%s mkdir: %w", name, err)
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("%s open: %w", name, err)
	}

	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		db.Close()
		return nil, fmt.Errorf("%s pragma journal_mode: %w", name, err)
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		db.Close()
		return nil, fmt.Errorf("%s pragma busy_timeout: %w", name, err)
	}

	schema := `
CREATE TABLE IF NOT EXISTS tasks (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	prompt TEXT NOT NULL,
	scheduled_for TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'pending',
	created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
	claimed_at TEXT NOT NULL DEFAULT '',
	completed_at TEXT NOT NULL DEFAULT '',
	last_error TEXT NOT NULL DEFAULT '',
	last_result TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_tasks_status_schedule ON tasks(status, scheduled_for, id);
`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("%s schema: %w", name, err)
	}

	if _, err := db.Exec(`
UPDATE tasks
SET status='pending',
	claimed_at='',
	last_error=CASE
		WHEN TRIM(last_error) = '' THEN 'requeued after broo-remote restart'
		ELSE last_error || char(10) || 'requeued after broo-remote restart'
	END
WHERE status='running'
`); err != nil {
		db.Close()
		return nil, fmt.Errorf("%s requeue running tasks: %w", name, err)
	}

	return &TaskStore{
		name:    name,
		path:    path,
		db:      db,
		dueOnly: dueOnly,
	}, nil
}

func (t *TaskStore) ClaimTask(ctx context.Context, now time.Time) (*TaskRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTaskPollTimeout)
	defer cancel()

	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var (
		query string
		args  []any
	)
	nowText := now.UTC().Format(time.RFC3339)
	if t.dueOnly {
		query = `
SELECT id, prompt, scheduled_for
FROM tasks
WHERE status='pending'
  AND TRIM(scheduled_for) <> ''
  AND scheduled_for <= ?
ORDER BY scheduled_for, id
LIMIT 1
`
		args = []any{nowText}
	} else {
		query = `
SELECT id, prompt, scheduled_for
FROM tasks
WHERE status='pending'
ORDER BY created_at, id
LIMIT 1
`
	}

	task := &TaskRecord{}
	err = tx.QueryRowContext(ctx, query, args...).Scan(&task.ID, &task.Prompt, &task.ScheduledFor)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	res, err := tx.ExecContext(ctx, `
UPDATE tasks
SET status='running',
	claimed_at=?,
	completed_at='',
	last_error='',
	last_result=''
WHERE id=? AND status='pending'
`, nowText, task.ID)
	if err != nil {
		return nil, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return task, nil
}

func (t *TaskStore) CompleteTask(id int64, result string) error {
	_, err := t.db.Exec(`
UPDATE tasks
SET status='done',
	completed_at=?,
	last_error='',
	last_result=?
WHERE id=?
`, time.Now().UTC().Format(time.RFC3339), result, id)
	return err
}

func (t *TaskStore) FailTask(id int64, taskErr error) error {
	msg := ""
	if taskErr != nil {
		msg = taskErr.Error()
	}
	_, err := t.db.Exec(`
UPDATE tasks
SET status='failed',
	completed_at=?,
	last_error=?,
	last_result=''
WHERE id=?
`, time.Now().UTC().Format(time.RFC3339), msg, id)
	return err
}

var errTaskLocked = errors.New("another task is already running")

func (b *Bot) tryAcquireTaskLock() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(b.cfg.TaskLockPath), 0o755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(b.cfg.TaskLockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errTaskLocked
		}
		return nil, err
	}

	if err := f.Truncate(0); err == nil {
		if _, err := f.Seek(0, 0); err == nil {
			_, _ = fmt.Fprintf(f, "pid=%d\nstarted_at=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
		}
	}
	return f, nil
}

func releaseTaskLock(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}

func loadStore(path string) (*Store, error) {
	s := &Store{
		path: path,
		state: State{
			Sessions: make(map[string]Session),
			Queued:   make(map[string][]Message),
		},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return nil, fmt.Errorf("parse state: %w", err)
	}
	if s.state.Sessions == nil {
		s.state.Sessions = make(map[string]Session)
	}
	if s.state.Queued == nil {
		s.state.Queued = make(map[string][]Message)
	}
	return s, nil
}

func (s *Store) OwnerChatID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.OwnerChatID
}

func (s *Store) OwnerUserID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.OwnerUserID
}

func (s *Store) ClaimOwner(chatID, userID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.OwnerChatID != 0 {
		if s.state.OwnerChatID != chatID {
			return false, nil
		}
		if s.state.OwnerUserID == 0 && userID != 0 {
			s.state.OwnerUserID = userID
			return true, s.saveLocked()
		}
		return s.state.OwnerUserID == 0 || s.state.OwnerUserID == userID, nil
	}
	s.state.OwnerChatID = chatID
	s.state.OwnerUserID = userID
	return true, s.saveLocked()
}

func (s *Store) EnsureOwnerUser(chatID, userID int64) error {
	if userID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.OwnerChatID != chatID || s.state.OwnerUserID != 0 {
		return nil
	}
	s.state.OwnerUserID = userID
	return s.saveLocked()
}

func (s *Store) LastUpdateID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.LastUpdateID
}

func (s *Store) SetLastUpdateID(updateID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if updateID <= s.state.LastUpdateID {
		return nil
	}
	s.state.LastUpdateID = updateID
	return s.saveLocked()
}

func (s *Store) GetSession(chatID int64) (Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.state.Sessions[strconv.FormatInt(chatID, 10)]
	return session, ok
}

func (s *Store) PutSession(chatID int64, threadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Sessions[strconv.FormatInt(chatID, 10)] = Session{
		ThreadID:  threadID,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	return s.saveLocked()
}

func (s *Store) EnqueueMessage(chatID int64, msg *Message) error {
	if msg == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state.Queued == nil {
		s.state.Queued = make(map[string][]Message)
	}
	clone := cloneMessage(msg)
	s.state.Queued[strconv.FormatInt(chatID, 10)] = append(s.state.Queued[strconv.FormatInt(chatID, 10)], *clone)
	return s.saveLocked()
}

func (s *Store) PrependMessage(chatID int64, msg *Message) error {
	if msg == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state.Queued == nil {
		s.state.Queued = make(map[string][]Message)
	}
	key := strconv.FormatInt(chatID, 10)
	clone := cloneMessage(msg)
	queue := s.state.Queued[key]
	queue = append([]Message{*clone}, queue...)
	s.state.Queued[key] = queue
	return s.saveLocked()
}

func (s *Store) ShiftQueued(chatID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strconv.FormatInt(chatID, 10)
	queue := s.state.Queued[key]
	if len(queue) == 0 {
		delete(s.state.Queued, key)
		return s.saveLocked()
	}

	if len(queue) == 1 {
		delete(s.state.Queued, key)
	} else {
		s.state.Queued[key] = queue[1:]
	}
	return s.saveLocked()
}

func (s *Store) SnapshotQueued() map[int64][]*Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[int64][]*Message, len(s.state.Queued))
	for key, queue := range s.state.Queued {
		chatID, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			continue
		}
		items := make([]*Message, 0, len(queue))
		for i := range queue {
			msg := queue[i]
			msgCopy := msg
			items = append(items, cloneMessage(&msgCopy))
		}
		if len(items) > 0 {
			out[chatID] = items
		}
	}
	return out
}

func (s *Store) GetBothEndsWatch() *BothEndsWatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneBothEndsWatch(s.state.BothEndsWatch)
}

func (s *Store) PutBothEndsWatch(watch *BothEndsWatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.BothEndsWatch = cloneBothEndsWatch(watch)
	return s.saveLocked()
}

func (s *Store) ClearBothEndsWatch() error {
	return s.PutBothEndsWatch(nil)
}

func (s *Store) DeleteSession(chatID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.Sessions, strconv.FormatInt(chatID, 10))
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (t *TelegramClient) GetMe() (TGUser, error) {
	var resp UserResponse
	if err := t.getJSON("/getMe", url.Values{}, &resp); err != nil {
		return TGUser{}, err
	}
	if !resp.OK {
		return TGUser{}, fmt.Errorf("telegram getMe failed: %s", resp.Description)
	}
	return resp.Result, nil
}

func (t *TelegramClient) GetUpdates(offset int64) ([]Update, error) {
	return t.getUpdates(offset, 30)
}

func (t *TelegramClient) GetUpdatesNow(offset int64) ([]Update, error) {
	return t.getUpdates(offset, 1)
}

func (t *TelegramClient) getUpdates(offset int64, timeoutSeconds int) ([]Update, error) {
	form := url.Values{}
	form.Set("timeout", strconv.Itoa(timeoutSeconds))
	if offset > 0 {
		form.Set("offset", strconv.FormatInt(offset, 10))
	}
	var resp UpdateResponse
	if err := t.getJSON("/getUpdates", form, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("telegram getUpdates failed: %s", resp.Description)
	}
	return resp.Result, nil
}

func (t *TelegramClient) GetFile(fileID string) (TGFile, error) {
	form := url.Values{}
	form.Set("file_id", fileID)
	var resp GetFileResponse
	if err := t.getJSON("/getFile", form, &resp); err != nil {
		return TGFile{}, err
	}
	if !resp.OK {
		return TGFile{}, fmt.Errorf("telegram getFile failed: %s", resp.Description)
	}
	return resp.Result, nil
}

func (t *TelegramClient) DownloadFile(filePath string) ([]byte, error) {
	resp, err := t.client.Get(t.fileBaseURL + "/" + strings.TrimLeft(filePath, "/"))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("telegram file %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return io.ReadAll(resp.Body)
}

func (t *TelegramClient) SendMessage(chatID int64, text string) error {
	_, err := t.SendMessageWithID(chatID, text)
	return err
}

func (t *TelegramClient) SendMessageWithID(chatID int64, text string) (int64, error) {
	for _, chunk := range splitTelegramText(text) {
		form := url.Values{}
		form.Set("chat_id", strconv.FormatInt(chatID, 10))
		form.Set("text", chunk)
		var resp SendMessageResponse
		if err := t.postFormJSON("/sendMessage", form, &resp); err != nil {
			return 0, err
		}
		if !resp.OK {
			return 0, fmt.Errorf("telegram sendMessage failed: %s", resp.Description)
		}
		if resp.Result.MessageID != 0 {
			return resp.Result.MessageID, nil
		}
	}
	return 0, nil
}

func (t *TelegramClient) EditMessage(chatID, messageID int64, text string) error {
	form := url.Values{}
	form.Set("chat_id", strconv.FormatInt(chatID, 10))
	form.Set("message_id", strconv.FormatInt(messageID, 10))
	form.Set("text", truncateTelegramText(text))
	return t.postForm("/editMessageText", form)
}

func (t *TelegramClient) SendAction(chatID int64, action string) error {
	form := url.Values{}
	form.Set("chat_id", strconv.FormatInt(chatID, 10))
	form.Set("action", action)
	return t.postForm("/sendChatAction", form)
}

func (t *TelegramClient) getJSON(path string, form url.Values, out any) error {
	resp, err := t.client.PostForm(t.baseURL+path, form)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("telegram %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (t *TelegramClient) postForm(path string, form url.Values) error {
	return t.postFormJSON(path, form, nil)
}

func (t *TelegramClient) postFormJSON(path string, form url.Values, out any) error {
	resp, err := t.client.PostForm(t.baseURL+path, form)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("telegram %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (b *Bot) Run(ctx context.Context) error {
	b.writeHeartbeat("starting")
	b.restorePersistedQueue(ctx)

	offset := b.store.LastUpdateID() + 1
	updates, err := b.telegram.GetUpdatesNow(offset)
	if err != nil {
		b.log.Printf("initial getUpdates error: %v", err)
		b.writeHeartbeat("initial_getupdates_error")
	} else {
		b.writeHeartbeat("initial_updates_ok")
		offset = b.dispatchUpdates(ctx, offset, updates)
	}

	go b.backgroundLoop(ctx)

	for {
		b.writeHeartbeat("polling")
		updates, err := b.telegram.GetUpdates(offset)
		if err != nil {
			b.log.Printf("getUpdates error: %v", err)
			b.writeHeartbeat("getupdates_error")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(3 * time.Second):
			}
			continue
		}

		b.writeHeartbeat("updates_ok")
		offset = b.dispatchUpdates(ctx, offset, updates)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (b *Bot) writeHeartbeat(status string) {
	payload := map[string]any{
		"ts":     time.Now().UTC().Format(time.RFC3339),
		"status": status,
		"pid":    os.Getpid(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	tmp := b.cfg.HeartbeatPath + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, b.cfg.HeartbeatPath)
}

func (b *Bot) heartbeatLoop(ctx context.Context, status string) {
	b.writeHeartbeat(status)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			b.writeHeartbeat(status + "_done")
			return
		case <-ticker.C:
			b.writeHeartbeat(status)
		}
	}
}

func (b *Bot) dispatchUpdates(ctx context.Context, offset int64, updates []Update) int64 {
	for _, update := range updates {
		offset = update.UpdateID + 1
		if err := b.store.SetLastUpdateID(update.UpdateID); err != nil {
			b.log.Printf("save offset error: %v", err)
		}
		if update.Message == nil {
			continue
		}
		go b.handleMessage(ctx, update.Message)
	}
	return offset
}

func (b *Bot) handleMessage(ctx context.Context, msg *Message) {
	if msg == nil || msg.Chat.ID == 0 {
		return
	}
	if msg.From != nil && msg.From.IsBot {
		return
	}
	text := strings.TrimSpace(msg.Text)
	if text == "" && msg.Caption == "" && msg.Voice == nil && msg.Audio == nil && msg.Video == nil && msg.Document == nil && len(msg.Photo) == 0 {
		return
	}

	if !b.isAuthorized(msg, text) {
		_ = b.telegram.SendMessage(msg.Chat.ID, "Esse bot ainda não está liberado para este chat. Envia /start no chat correto para fazer o claim.")
		return
	}
	if msg.From != nil {
		_ = b.store.EnsureOwnerUser(msg.Chat.ID, msg.From.ID)
	}

	if handled := b.handleCommand(ctx, msg, text); handled {
		return
	}

	msg = cloneMessage(msg)

	if !b.tryBegin(msg.Chat.ID) {
		position := b.prepend(msg.Chat.ID, msg)
		if b.interruptActiveRun(msg.Chat.ID) {
			ack := "Interrompendo a execução atual para aplicar teu steer."
			if position > 1 {
				ack = fmt.Sprintf("Interrompendo a execução atual para aplicar teu steer. Itens pendentes: %d.", position-1)
			}
			_ = b.telegram.SendMessage(msg.Chat.ID, ack)
			return
		}
		ack := "Ainda estou processando a anterior. Enfileirei esta para rodar depois."
		if position > 1 {
			ack = fmt.Sprintf("Ainda estou processando a anterior. Enfileirei esta para rodar depois. Fila: %d.", position)
		}
		_ = b.telegram.SendMessage(msg.Chat.ID, ack)
		return
	}
	go b.processQueue(ctx, msg)
}

func (b *Bot) handleCommand(ctx context.Context, msg *Message, text string) bool {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return false
	}
	cmd := strings.ToLower(fields[0])

	switch cmd {
	case "/start":
		if msg.Chat.Type != "private" {
			_ = b.telegram.SendMessage(msg.Chat.ID, "Faz o claim só em chat privado comigo.")
			return true
		}
		userID := int64(0)
		if msg.From != nil {
			userID = msg.From.ID
		}
		claimed, err := b.store.ClaimOwner(msg.Chat.ID, userID)
		if err != nil {
			_ = b.telegram.SendMessage(msg.Chat.ID, "Falhou ao registrar o owner deste bot.")
			return true
		}
		var sb strings.Builder
		if claimed && b.store.OwnerChatID() == msg.Chat.ID {
			sb.WriteString("Chat registrado como owner deste bot.\n\n")
		}
		sb.WriteString(helpText())
		_ = b.telegram.SendMessage(msg.Chat.ID, sb.String())
		return true
	case "/help":
		_ = b.telegram.SendMessage(msg.Chat.ID, helpText())
		return true
	case "/status":
		session, ok := b.store.GetSession(msg.Chat.ID)
		status := fmt.Sprintf("owner_chat_id=%d\nowner_user_id=%d\nprovider=%s\nworkdir=%s\n", b.store.OwnerChatID(), b.store.OwnerUserID(), b.cfg.Provider, b.cfg.Workdir)
		if ok {
			status += fmt.Sprintf("thread_id=%s\nupdated_at=%s", session.ThreadID, session.UpdatedAt)
		} else {
			status += "thread_id=(none)"
		}
		_ = b.telegram.SendMessage(msg.Chat.ID, status)
		return true
	case "/new", "/reset":
		if err := b.store.DeleteSession(msg.Chat.ID); err != nil {
			_ = b.telegram.SendMessage(msg.Chat.ID, "Falhou ao limpar a sessão.")
			return true
		}
		rest := strings.TrimSpace(strings.TrimPrefix(text, fields[0]))
		if rest == "" {
			_ = b.telegram.SendMessage(msg.Chat.ID, "Sessão limpa. A próxima mensagem começa uma conversa nova.")
			return true
		}
		_ = b.telegram.SendMessage(msg.Chat.ID, "Sessão limpa. Processando tua nova mensagem.")
		go b.handleMessage(ctx, &Message{
			Chat: msg.Chat,
			From: msg.From,
			Text: rest,
		})
		return true
	case "/resume":
		return b.handleResumeCommand(msg, fields)
	default:
		return false
	}
}

func (b *Bot) handleResumeCommand(msg *Message, fields []string) bool {
	recent, err := loadRecentSessions(b.cfg.HistoryPath, 12)
	if err != nil {
		_ = b.telegram.SendMessage(msg.Chat.ID, "Falhou ao ler o histórico local do Codex.")
		return true
	}

	if len(fields) == 1 {
		if len(recent) == 0 {
			_ = b.telegram.SendMessage(msg.Chat.ID, "Não achei sessões recentes no histórico do Codex.")
			return true
		}
		current, _ := b.store.GetSession(msg.Chat.ID)
		var sb strings.Builder
		sb.WriteString("Sessões recentes:\n")
		for i, item := range recent {
			marker := ""
			if current.ThreadID == item.ThreadID {
				marker = " [atual]"
			}
			sb.WriteString(fmt.Sprintf(
				"%d. %s%s\n%s\n%s\n\n",
				i+1,
				item.ThreadID,
				marker,
				item.UpdatedAt,
				item.Preview,
			))
		}
		sb.WriteString("Usa /resume <número> ou /resume <thread_id>.")
		_ = b.telegram.SendMessage(msg.Chat.ID, sb.String())
		return true
	}

	target := strings.TrimSpace(fields[1])
	if target == "" {
		_ = b.telegram.SendMessage(msg.Chat.ID, "Usa /resume <número> ou /resume <thread_id>.")
		return true
	}

	threadID := target
	if idx, err := strconv.Atoi(target); err == nil {
		if idx < 1 || idx > len(recent) {
			_ = b.telegram.SendMessage(msg.Chat.ID, "Número de sessão fora da lista.")
			return true
		}
		threadID = recent[idx-1].ThreadID
	}

	if err := b.store.PutSession(msg.Chat.ID, threadID); err != nil {
		_ = b.telegram.SendMessage(msg.Chat.ID, "Falhou ao trocar a sessão atual.")
		return true
	}
	_ = b.telegram.SendMessage(msg.Chat.ID, fmt.Sprintf("Sessão atual trocada para:\n%s", threadID))
	return true
}

func loadRecentSessions(path string, limit int) ([]SessionSummary, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	latest := make(map[string]SessionSummary)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var item HistoryEntry
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			continue
		}
		if item.SessionID == "" {
			continue
		}
		if prev, ok := latest[item.SessionID]; ok && prev.LastTS >= item.TS {
			continue
		}
		latest[item.SessionID] = SessionSummary{
			ThreadID:  item.SessionID,
			LastTS:    item.TS,
			UpdatedAt: time.Unix(item.TS, 0).In(time.Local).Format("2006-01-02 15:04:05 MST"),
			Preview:   previewText(item.Text),
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	out := make([]SessionSummary, 0, len(latest))
	for _, item := range latest {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastTS > out[j].LastTS
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func previewText(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	if s == "" {
		return "(sem preview)"
	}
	return s
}

func (b *Bot) isAuthorized(msg *Message, text string) bool {
	if msg == nil {
		return false
	}
	if msg.Chat.Type != "private" {
		return false
	}
	owner := b.store.OwnerChatID()
	if owner == 0 {
		return strings.HasPrefix(strings.ToLower(strings.TrimSpace(text)), "/start")
	}
	if owner != msg.Chat.ID {
		return false
	}
	ownerUser := b.store.OwnerUserID()
	if ownerUser == 0 {
		return true
	}
	return msg.From != nil && msg.From.ID == ownerUser
}

func (b *Bot) tryBegin(chatID int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.busy[chatID] {
		return false
	}
	b.busy[chatID] = true
	return true
}

func (b *Bot) setActiveRun(chatID int64, cancel context.CancelFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cancel == nil {
		delete(b.active, chatID)
		return
	}
	b.active[chatID] = cancel
}

func (b *Bot) clearActiveRun(chatID int64, cancel context.CancelFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.active[chatID]
	if current == nil {
		return
	}
	if fmt.Sprintf("%p", current) != fmt.Sprintf("%p", cancel) {
		return
	}
	delete(b.active, chatID)
}

func (b *Bot) interruptActiveRun(chatID int64) bool {
	b.mu.Lock()
	cancel := b.active[chatID]
	b.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (b *Bot) enqueue(chatID int64, msg *Message) int {
	b.mu.Lock()
	b.pending[chatID] = append(b.pending[chatID], msg)
	size := len(b.pending[chatID])
	b.mu.Unlock()

	if err := b.store.EnqueueMessage(chatID, msg); err != nil {
		b.log.Printf("persist queue error chat=%d: %v", chatID, err)
	}
	return size
}

func (b *Bot) prepend(chatID int64, msg *Message) int {
	b.mu.Lock()
	queue := b.pending[chatID]
	queue = append([]*Message{msg}, queue...)
	b.pending[chatID] = queue
	size := len(queue)
	b.mu.Unlock()

	if err := b.store.PrependMessage(chatID, msg); err != nil {
		b.log.Printf("persist queue prepend error chat=%d: %v", chatID, err)
	}
	return size
}

func (b *Bot) nextQueued(chatID int64) *Message {
	b.mu.Lock()
	queue := b.pending[chatID]
	if len(queue) == 0 {
		delete(b.busy, chatID)
		delete(b.pending, chatID)
		b.mu.Unlock()
		return nil
	}
	next := queue[0]
	rest := queue[1:]
	if len(rest) == 0 {
		delete(b.pending, chatID)
	} else {
		b.pending[chatID] = rest
	}
	b.mu.Unlock()

	if err := b.store.ShiftQueued(chatID); err != nil {
		b.log.Printf("persist queue shift error chat=%d: %v", chatID, err)
	}
	return next
}

func (b *Bot) restorePersistedQueue(ctx context.Context) {
	for chatID, queue := range b.store.SnapshotQueued() {
		if len(queue) == 0 {
			continue
		}

		first := queue[0]
		rest := queue[1:]

		b.mu.Lock()
		b.busy[chatID] = true
		if len(rest) > 0 {
			b.pending[chatID] = rest
		} else {
			delete(b.pending, chatID)
		}
		b.mu.Unlock()

		if err := b.store.ShiftQueued(chatID); err != nil {
			b.log.Printf("persist queue restore shift error chat=%d: %v", chatID, err)
		}
		go b.processQueue(ctx, first)
	}
}

func (b *Bot) finishActiveRun(ctx context.Context, chatID int64) {
	if next := b.nextQueued(chatID); next != nil {
		go b.processQueue(ctx, next)
	}
}

func (b *Bot) backgroundLoop(ctx context.Context) {
	ticker := time.NewTicker(backgroundTickInterval)
	defer ticker.Stop()

	lastTescesCheck := time.Time{}
	lastAidaCheck := time.Now().UTC()
	b.backgroundTick(ctx, &lastTescesCheck, &lastAidaCheck)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.backgroundTick(ctx, &lastTescesCheck, &lastAidaCheck)
		}
	}
}

func (b *Bot) backgroundTick(ctx context.Context, lastTescesCheck, lastAidaCheck *time.Time) {
	ownerChatID := b.store.OwnerChatID()
	if ownerChatID == 0 {
		return
	}

	now := time.Now().UTC()
	if lastTescesCheck.IsZero() || now.Sub(*lastTescesCheck) >= tescesPollInterval {
		*lastTescesCheck = now
		started, err := b.tryLaunchBackgroundTask(ctx, ownerChatID, b.tesces, now)
		if err != nil {
			b.log.Printf("background %s error: %v", b.tesces.name, err)
		}
		if started {
			return
		}
	}

	started, err := b.tryLaunchBothEndsWatchReport(ctx, ownerChatID)
	if err != nil {
		b.log.Printf("background both_ends watch error: %v", err)
	}
	if started {
		return
	}

	if !lastAidaCheck.IsZero() && now.Sub(*lastAidaCheck) < b.cfg.AIDAPollInterval {
		return
	}
	*lastAidaCheck = now

	started, err = b.tryLaunchBackgroundTask(ctx, ownerChatID, b.aida, now)
	if err != nil {
		b.log.Printf("background %s error: %v", b.aida.name, err)
	}
	if started {
		return
	}
}

func (b *Bot) tryLaunchBackgroundTask(ctx context.Context, chatID int64, store *TaskStore, now time.Time) (bool, error) {
	if store == nil {
		return false, nil
	}
	if !b.tryBegin(chatID) {
		return false, nil
	}

	lockFile, err := b.tryAcquireTaskLock()
	if err != nil {
		b.finishActiveRun(ctx, chatID)
		if errors.Is(err, errTaskLocked) {
			return false, nil
		}
		return false, err
	}

	task, err := store.ClaimTask(ctx, now)
	if err != nil {
		releaseTaskLock(lockFile)
		b.finishActiveRun(ctx, chatID)
		return false, err
	}
	if task == nil {
		releaseTaskLock(lockFile)
		b.finishActiveRun(ctx, chatID)
		return false, nil
	}

	go func() {
		b.processBackgroundTask(ctx, chatID, store, task, lockFile)
		b.finishActiveRun(ctx, chatID)
	}()
	return true, nil
}

func (b *Bot) tryLaunchBothEndsWatchReport(ctx context.Context, chatID int64) (bool, error) {
	watch := b.store.GetBothEndsWatch()
	if watch == nil || len(watch.IDs) == 0 {
		return false, nil
	}

	pendingIDs, err := pendingBothEndsIDs(filepath.Join(b.cfg.Workdir, defaultTradesDBPath), watch.IDs)
	if err != nil {
		return false, err
	}
	if len(pendingIDs) > 0 {
		return false, nil
	}

	if !b.tryBegin(chatID) {
		return false, nil
	}

	go func() {
		defer b.finishActiveRun(ctx, chatID)
		if err := b.processBothEndsWatchReport(chatID, watch); err != nil {
			b.log.Printf("both_ends watch report error: %v", err)
		}
	}()
	return true, nil
}

func (b *Bot) processBothEndsWatchReport(chatID int64, watch *BothEndsWatch) error {
	report, err := buildBothEndsWatchReport(filepath.Join(b.cfg.Workdir, defaultTradesDBPath), watch.IDs)
	if err != nil {
		return err
	}
	if err := b.telegram.SendMessage(chatID, report); err != nil {
		return err
	}
	if err := b.store.ClearBothEndsWatch(); err != nil {
		return err
	}
	return nil
}

func (b *Bot) processBackgroundTask(ctx context.Context, chatID int64, store *TaskStore, task *TaskRecord, lockFile *os.File) {
	defer releaseTaskLock(lockFile)

	typingCtx, cancelTyping := context.WithCancel(ctx)
	defer cancelTyping()
	go b.typingLoop(typingCtx, chatID)
	go b.heartbeatLoop(typingCtx, "background_task")

	progress := NewProgressReporter(b.telegram, chatID)
	progress.Add(fmt.Sprintf("%s #%d em execução.", store.name, task.ID))

	prompt := task.Prompt
	if store.dueOnly {
		prompt = fmt.Sprintf("Task automática de %s #%d agendada para %s.\n\n%s", store.name, task.ID, task.ScheduledFor, task.Prompt)
	} else {
		prompt = fmt.Sprintf("Task automática de %s #%d para rodar quando estiver ocioso.\n\n%s", store.name, task.ID, task.Prompt)
	}

	reply, err := b.runPreparedTextLocked(ctx, chatID, prompt, nil, progress.Add, lockFile)
	if err != nil {
		b.log.Printf("background task error %s#%d: %v", store.name, task.ID, err)
		progress.Add(fmt.Sprintf("%s #%d falhou.", store.name, task.ID))
		if saveErr := store.FailTask(task.ID, err); saveErr != nil {
			b.log.Printf("background task save fail %s#%d: %v", store.name, task.ID, saveErr)
		}
		_ = b.telegram.SendMessage(chatID, fmt.Sprintf("[%s #%d] Falhou:\n%s", store.name, task.ID, sanitizeForTelegram(err.Error())))
		return
	}

	progress.Add(fmt.Sprintf("%s #%d concluído.", store.name, task.ID))
	if strings.TrimSpace(reply) == "" {
		reply = "Codex terminou sem resposta textual."
	}
	if saveErr := store.CompleteTask(task.ID, reply); saveErr != nil {
		b.log.Printf("background task save complete %s#%d: %v", store.name, task.ID, saveErr)
	}
	_ = b.telegram.SendMessage(chatID, fmt.Sprintf("[%s #%d] %s", store.name, task.ID, reply))
}

func pendingBothEndsIDs(dbPath string, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}

	query := fmt.Sprintf(`
SELECT id
FROM both_ends
WHERE id IN (%s)
  AND (COALESCE(status, '') = 'pending' OR TRIM(COALESCE(resolved_at, '')) = '')
ORDER BY id
`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pending []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		pending = append(pending, id)
	}
	return pending, rows.Err()
}

func buildBothEndsWatchReport(dbPath string, ids []int64) (string, error) {
	rows, err := loadBothEndsReportRows(dbPath, ids)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "Lote monitorado do both buy fechou, mas não achei rows no trades.db.", nil
	}

	var (
		totalCost  float64
		totalPnL   float64
		soldWinner int
		soldLoser  int
		noSell     int
		best       = rows[0]
		worst      = rows[0]
		firstID    = rows[0].ID
		lastID     = rows[0].ID
	)

	for _, row := range rows {
		totalCost += row.TotalCost
		totalPnL += row.PnL
		if row.ID < firstID {
			firstID = row.ID
		}
		if row.ID > lastID {
			lastID = row.ID
		}
		if row.PnL > best.PnL {
			best = row
		}
		if row.PnL < worst.PnL {
			worst = row
		}
		switch {
		case row.SoldSide == "":
			noSell++
		case row.WinnerSide != "" && row.SoldSide == row.WinnerSide:
			soldWinner++
		default:
			soldLoser++
		}
	}

	roi := 0.0
	if totalCost > 0 {
		roi = (totalPnL / totalCost) * 100
	}

	return strings.Join([]string{
		formatBothEndsWindowLine(rows, firstID, lastID),
		fmt.Sprintf("Posições: %d | custo %s | pnl %s (%+.2f%%).", len(rows), formatUSD(totalCost), formatSignedUSD(totalPnL), roi),
		fmt.Sprintf("Sell certo: %d | sell errado: %d | sem sell: %d.", soldLoser, soldWinner, noSell),
		fmt.Sprintf("Melhor: #%d %s %s %s.", best.ID, strings.ToUpper(best.Symbol), best.Timeframe, formatSignedUSD(best.PnL)),
		fmt.Sprintf("Pior: #%d %s %s %s.", worst.ID, strings.ToUpper(worst.Symbol), worst.Timeframe, formatSignedUSD(worst.PnL)),
	}, "\n"), nil
}

func loadBothEndsReportRows(dbPath string, ids []int64) ([]BothEndsReportRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}

	query := fmt.Sprintf(`
SELECT id,
       COALESCE(symbol, ''),
       COALESCE(timeframe, ''),
       COALESCE(status, ''),
       COALESCE(resolved_at, ''),
       COALESCE(sold_side, ''),
       COALESCE(winner_side, ''),
       COALESCE(window_start, 0),
       COALESCE(window_end, 0),
       COALESCE(total_cost, 0),
       COALESCE(pnl, 0)
FROM both_ends
WHERE id IN (%s)
ORDER BY id
`, strings.Join(placeholders, ","))

	sqlRows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	var rows []BothEndsReportRow
	for sqlRows.Next() {
		var row BothEndsReportRow
		if err := sqlRows.Scan(
			&row.ID,
			&row.Symbol,
			&row.Timeframe,
			&row.Status,
			&row.ResolvedAt,
			&row.SoldSide,
			&row.WinnerSide,
			&row.WindowStart,
			&row.WindowEnd,
			&row.TotalCost,
			&row.PnL,
		); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, sqlRows.Err()
}

func formatUSD(v float64) string {
	return fmt.Sprintf("$%.2f", v)
}

func formatSignedUSD(v float64) string {
	if v >= 0 {
		return fmt.Sprintf("+$%.2f", v)
	}
	return fmt.Sprintf("-$%.2f", -v)
}

func formatBothEndsWindowLine(rows []BothEndsReportRow, firstID, lastID int64) string {
	labels := bothEndsWindowLabels(rows)
	if len(labels) == 0 {
		return fmt.Sprintf("Lote both buy fechado. IDs %d-%d.", firstID, lastID)
	}
	return "Lote both buy fechado. Horários Brasília: " + strings.Join(labels, ", ") + "."
}

func bothEndsWindowLabels(rows []BothEndsReportRow) []string {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		loc = time.FixedZone("BRT", -3*60*60)
	}

	seen := make(map[string]struct{})
	var labels []string
	for _, row := range rows {
		if row.WindowStart == 0 || row.WindowEnd == 0 {
			continue
		}
		start := time.Unix(row.WindowStart, 0).In(loc)
		end := time.Unix(row.WindowEnd, 0).In(loc)
		label := fmt.Sprintf("%02d:%02d-%02d:%02d", start.Hour(), start.Minute(), end.Hour(), end.Minute())
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels
}

func (b *Bot) typingLoop(ctx context.Context, chatID int64) {
	_ = b.telegram.SendAction(chatID, "typing")
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = b.telegram.SendAction(chatID, "typing")
		}
	}
}

func NewProgressReporter(tg *TelegramClient, chatID int64) *ProgressReporter {
	return &ProgressReporter{
		tg:     tg,
		chatID: chatID,
	}
}

func (p *ProgressReporter) Add(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.line == line {
		return
	}
	p.line = line

	text := "Codex trabalhando..."
	if p.line != "" {
		text += "\n\n" + p.line
	}
	text = truncateTelegramText(text)
	if text == p.lastText {
		return
	}

	if p.messageID == 0 {
		msgID, err := p.tg.SendMessageWithID(p.chatID, text)
		if err == nil && msgID != 0 {
			p.messageID = msgID
			p.lastText = text
		}
		return
	}

	if err := p.tg.EditMessage(p.chatID, p.messageID, text); err == nil {
		p.lastText = text
	}
}

func summarizeCodexEvent(ev map[string]any) string {
	t, _ := ev["type"].(string)
	switch t {
	case "thread.started":
		if id, _ := ev["thread_id"].(string); id != "" {
			return "Sessão iniciada: " + id
		}
		return "Sessão iniciada."
	case "turn.started":
		return "Analisando."
	case "error":
		if msg, _ := ev["message"].(string); msg != "" {
			return "Aviso do stream: " + previewText(msg)
		}
		return "Aviso do stream."
	case "item.started", "item.completed":
		item, ok := ev["item"].(map[string]any)
		if !ok {
			return ""
		}
		itemType, _ := item["type"].(string)
		switch itemType {
		case "agent_message":
			if text, _ := item["text"].(string); text != "" {
				return "Agente: " + previewText(text)
			}
		case "command_execution":
			command, _ := item["command"].(string)
			if t == "item.started" {
				if command != "" {
					return "Executando comando: " + previewText(command)
				}
				return "Executando comando."
			}
			exitCode := ""
			if raw, ok := item["exit_code"]; ok && raw != nil {
				exitCode = fmt.Sprintf(" (exit %v)", raw)
			}
			output, _ := item["aggregated_output"].(string)
			if output != "" {
				return fmt.Sprintf("Comando concluído%s: %s", exitCode, previewText(output))
			}
			if command != "" {
				return fmt.Sprintf("Comando concluído%s: %s", exitCode, previewText(command))
			}
			return "Comando concluído" + exitCode
		case "error":
			if msg, _ := item["message"].(string); msg != "" {
				return "Erro: " + previewText(msg)
			}
			return "Erro."
		}
	}
	return ""
}

func cloneMessage(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	clone := *msg
	if msg.From != nil {
		from := *msg.From
		clone.From = &from
	}
	if msg.Voice != nil {
		voice := *msg.Voice
		clone.Voice = &voice
	}
	if msg.Audio != nil {
		audio := *msg.Audio
		clone.Audio = &audio
	}
	if msg.Video != nil {
		video := *msg.Video
		clone.Video = &video
	}
	if msg.Document != nil {
		document := *msg.Document
		clone.Document = &document
	}
	if len(msg.Photo) > 0 {
		clone.Photo = append([]TGPhotoSize(nil), msg.Photo...)
	}
	return &clone
}

func cloneBothEndsWatch(watch *BothEndsWatch) *BothEndsWatch {
	if watch == nil {
		return nil
	}
	ids := make([]int64, len(watch.IDs))
	copy(ids, watch.IDs)
	return &BothEndsWatch{
		IDs:         ids,
		RequestedAt: watch.RequestedAt,
	}
}

func (b *Bot) processQueue(ctx context.Context, msg *Message) {
	for current := msg; current != nil; current = b.nextQueued(current.Chat.ID) {
		b.processMessage(ctx, current)
	}
}

func (b *Bot) processMessage(ctx context.Context, msg *Message) {
	runCtx, cancelRun := context.WithCancel(ctx)
	b.setActiveRun(msg.Chat.ID, cancelRun)
	defer func() {
		b.clearActiveRun(msg.Chat.ID, cancelRun)
		cancelRun()
	}()

	typingCtx, cancelTyping := context.WithCancel(runCtx)
	defer cancelTyping()
	go b.typingLoop(typingCtx, msg.Chat.ID)
	go b.heartbeatLoop(typingCtx, "chat_task")
	progress := NewProgressReporter(b.telegram, msg.Chat.ID)
	progress.Add("Preparando mensagem.")

	preparedText, imagePaths, cleanup, err := b.prepareMessageInput(runCtx, msg, progress.Add)
	if err != nil {
		b.log.Printf("prepare message error chat=%d: %v", msg.Chat.ID, err)
		progress.Add("Falha ao preparar a mensagem.")
		_ = b.telegram.SendMessage(msg.Chat.ID, sanitizeForTelegram(err.Error()))
		return
	}
	defer cleanup()
	progress.Add("Recebi tua mensagem.")

	reply, err := b.runPreparedText(runCtx, msg.Chat.ID, preparedText, imagePaths, progress.Add)
	if err != nil {
		if errors.Is(runCtx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
			progress.Add("Interrompido para seguir teu steer.")
			return
		}
		b.log.Printf("codex run error chat=%d: %v", msg.Chat.ID, err)
		if errors.Is(err, errTaskLocked) {
			progress.Add("Já tem outra task rodando.")
			_ = b.telegram.SendMessage(msg.Chat.ID, "Ainda estou ocupado com outra task. Tenta de novo em seguida.")
			return
		}
		progress.Add("Falha ao falar com o Codex.")
		_ = b.telegram.SendMessage(msg.Chat.ID, "Falhou ao falar com o Codex:\n"+sanitizeForTelegram(err.Error()))
		return
	}

	if strings.TrimSpace(reply) == "" {
		reply = "Codex terminou sem resposta textual."
	}
	progress.Add("Concluído.")
	if err := b.telegram.SendMessage(msg.Chat.ID, reply); err != nil {
		b.log.Printf("send reply error chat=%d: %v", msg.Chat.ID, err)
	}
}

func (b *Bot) runPreparedText(ctx context.Context, chatID int64, preparedText string, imagePaths []string, onProgress func(string)) (string, error) {
	lockFile, err := b.tryAcquireTaskLock()
	if err != nil {
		return "", err
	}
	defer releaseTaskLock(lockFile)

	return b.runPreparedTextLocked(ctx, chatID, preparedText, imagePaths, onProgress, lockFile)
}

func (b *Bot) runPreparedTextLocked(ctx context.Context, chatID int64, preparedText string, imagePaths []string, onProgress func(string), lockFile *os.File) (string, error) {
	_ = lockFile

	session, ok := b.store.GetSession(chatID)
	threadID := ""
	if ok {
		threadID = session.ThreadID
	}
	if onProgress != nil {
		if threadID != "" {
			onProgress("Continuando na sessão atual.")
		} else {
			onProgress("Abrindo uma sessão nova.")
		}
	}

	reply, newThreadID, err := b.runner.Run(ctx, threadID, preparedText, imagePaths, onProgress)
	if err != nil {
		return "", err
	}

	if newThreadID != "" && newThreadID != threadID {
		if err := b.store.PutSession(chatID, newThreadID); err != nil {
			b.log.Printf("save session error chat=%d: %v", chatID, err)
		}
	} else if threadID != "" {
		if err := b.store.PutSession(chatID, threadID); err != nil {
			b.log.Printf("touch session error chat=%d: %v", chatID, err)
		}
	}

	return reply, nil
}

func (b *Bot) prepareMessageInput(ctx context.Context, msg *Message, onProgress func(string)) (string, []string, func(), error) {
	if text := strings.TrimSpace(msg.Text); text != "" {
		return text, nil, func() {}, nil
	}

	imageFileID, imageExt := selectImageFile(msg)
	if imageFileID != "" {
		if onProgress != nil {
			onProgress("Baixando imagem do Telegram.")
		}
		file, err := b.telegram.GetFile(imageFileID)
		if err != nil {
			return "", nil, func() {}, fmt.Errorf("Falhou ao pegar a imagem do Telegram: %w", err)
		}
		data, err := b.telegram.DownloadFile(file.FilePath)
		if err != nil {
			return "", nil, func() {}, fmt.Errorf("Falhou ao baixar a imagem do Telegram: %w", err)
		}

		ext := inferImageExtension(imageExt, file)
		tmpFile, err := os.CreateTemp("", "broo-remote-image-*"+ext)
		if err != nil {
			return "", nil, func() {}, fmt.Errorf("Falhou ao criar arquivo temporário para a imagem: %w", err)
		}
		tmpPath := tmpFile.Name()
		if _, err := tmpFile.Write(data); err != nil {
			tmpFile.Close()
			_ = os.Remove(tmpPath)
			return "", nil, func() {}, fmt.Errorf("Falhou ao salvar a imagem temporária: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return "", nil, func() {}, fmt.Errorf("Falhou ao fechar a imagem temporária: %w", err)
		}

		prompt := strings.TrimSpace(msg.Caption)
		if prompt == "" {
			prompt = "Analisa esta imagem enviada pelo Telegram."
		}
		cleanup := func() {
			_ = os.Remove(tmpPath)
		}
		return prompt, []string{tmpPath}, cleanup, nil
	}

	videoMedia, videoExt := selectVideoFile(msg)
	if videoMedia != nil {
		if onProgress != nil {
			onProgress("Baixando vídeo do Telegram.")
		}
		file, err := b.telegram.GetFile(videoMedia.FileID)
		if err != nil {
			return "", nil, func() {}, fmt.Errorf("Falhou ao pegar o vídeo do Telegram: %w", err)
		}
		data, err := b.telegram.DownloadFile(file.FilePath)
		if err != nil {
			return "", nil, func() {}, fmt.Errorf("Falhou ao baixar o vídeo do Telegram: %w", err)
		}

		tmpDir, err := os.MkdirTemp("", "broo-remote-video-*")
		if err != nil {
			return "", nil, func() {}, fmt.Errorf("Falhou ao criar pasta temporária para o vídeo: %w", err)
		}
		videoPath := filepath.Join(tmpDir, "input"+inferVideoExtension(videoMedia, videoExt, file))
		if err := os.WriteFile(videoPath, data, 0o600); err != nil {
			_ = os.RemoveAll(tmpDir)
			return "", nil, func() {}, fmt.Errorf("Falhou ao salvar o vídeo temporário: %w", err)
		}
		if onProgress != nil {
			onProgress("Extraindo frames do vídeo.")
		}
		framePaths, err := extractVideoFrames(ctx, videoPath, tmpDir)
		if err != nil {
			_ = os.RemoveAll(tmpDir)
			return "", nil, func() {}, fmt.Errorf("Falhou ao extrair frames do vídeo: %w", err)
		}

		prompt := strings.TrimSpace(msg.Caption)
		if prompt == "" {
			prompt = "Analisa este vídeo enviado pelo Telegram usando os frames extraídos."
		} else {
			prompt = prompt + "\n\nAnalisa este vídeo enviado pelo Telegram usando os frames extraídos."
		}
		cleanup := func() {
			_ = os.RemoveAll(tmpDir)
		}
		return prompt, framePaths, cleanup, nil
	}

	media := msg.Voice
	if media == nil {
		media = msg.Audio
	}
	if media == nil {
		return "", nil, func() {}, errors.New("Não achei texto, áudio, imagem ou vídeo utilizável nesta mensagem.")
	}

	if onProgress != nil {
		onProgress("Baixando áudio do Telegram.")
	}
	file, err := b.telegram.GetFile(media.FileID)
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("Falhou ao pegar o arquivo do Telegram: %w", err)
	}
	data, err := b.telegram.DownloadFile(file.FilePath)
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("Falhou ao baixar o arquivo do Telegram: %w", err)
	}

	ext := inferAudioExtension(media, file)
	tmpFile, err := os.CreateTemp("", "broo-remote-audio-*"+ext)
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("Falhou ao criar arquivo temporário para o áudio: %w", err)
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", nil, func() {}, fmt.Errorf("Falhou ao salvar o áudio temporário: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", nil, func() {}, fmt.Errorf("Falhou ao fechar o áudio temporário: %w", err)
	}
	defer os.Remove(tmpPath)

	if onProgress != nil {
		onProgress("Transcrevendo áudio offline.")
	}
	transcript, err := transcribeAudioOffline(ctx, b.cfg.Workdir, b.cfg.AudioModel, tmpPath)
	if err != nil {
		return "", nil, func() {}, fmt.Errorf("Falhou ao transcrever o áudio: %w", err)
	}

	caption := strings.TrimSpace(msg.Caption)
	if caption == "" {
		return transcript, nil, func() {}, nil
	}
	return fmt.Sprintf("Caption do áudio:\n%s\n\nTranscrição:\n%s", caption, transcript), nil, func() {}, nil
}

func selectImageFile(msg *Message) (fileID string, extHint string) {
	if msg == nil {
		return "", ""
	}
	if msg.Document != nil {
		mime := strings.TrimSpace(strings.ToLower(msg.Document.MimeType))
		name := strings.TrimSpace(msg.Document.FileName)
		if strings.HasPrefix(mime, "image/") || isImageFilename(name) {
			return msg.Document.FileID, filepath.Ext(name)
		}
	}
	if len(msg.Photo) == 0 {
		return "", ""
	}
	best := msg.Photo[0]
	for _, item := range msg.Photo[1:] {
		if item.FileSize >= best.FileSize {
			best = item
		}
	}
	return best.FileID, ".jpg"
}

func selectVideoFile(msg *Message) (*TGMediaFile, string) {
	if msg == nil {
		return nil, ""
	}
	if msg.Video != nil {
		return msg.Video, filepath.Ext(strings.TrimSpace(msg.Video.FileName))
	}
	if msg.Document != nil {
		mime := strings.TrimSpace(strings.ToLower(msg.Document.MimeType))
		name := strings.TrimSpace(msg.Document.FileName)
		if strings.HasPrefix(mime, "video/") || isVideoFilename(name) {
			return msg.Document, filepath.Ext(name)
		}
	}
	return nil, ""
}

func inferImageExtension(extHint string, file TGFile) string {
	if ext := strings.TrimSpace(extHint); ext != "" {
		return ext
	}
	if ext := filepath.Ext(strings.TrimSpace(file.FilePath)); ext != "" {
		return ext
	}
	return ".jpg"
}

func isImageFilename(name string) bool {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(name)))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp":
		return true
	default:
		return false
	}
}

func isVideoFilename(name string) bool {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(name)))
	switch ext {
	case ".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi":
		return true
	default:
		return false
	}
}

func inferVideoExtension(media *TGMediaFile, extHint string, file TGFile) string {
	if ext := strings.TrimSpace(extHint); ext != "" {
		return ext
	}
	if media != nil {
		if ext := filepath.Ext(strings.TrimSpace(media.FileName)); ext != "" {
			return ext
		}
		switch {
		case strings.Contains(media.MimeType, "mp4"):
			return ".mp4"
		case strings.Contains(media.MimeType, "quicktime"):
			return ".mov"
		case strings.Contains(media.MimeType, "webm"):
			return ".webm"
		}
	}
	if ext := filepath.Ext(strings.TrimSpace(file.FilePath)); ext != "" {
		return ext
	}
	return ".mp4"
}

func inferAudioExtension(media *TGMediaFile, file TGFile) string {
	if media != nil {
		if ext := filepath.Ext(strings.TrimSpace(media.FileName)); ext != "" {
			return ext
		}
		switch {
		case strings.Contains(media.MimeType, "ogg"):
			return ".ogg"
		case strings.Contains(media.MimeType, "mpeg"), strings.Contains(media.MimeType, "mp3"):
			return ".mp3"
		case strings.Contains(media.MimeType, "mp4"):
			return ".mp4"
		case strings.Contains(media.MimeType, "wav"):
			return ".wav"
		case strings.Contains(media.MimeType, "webm"):
			return ".webm"
		}
	}
	if ext := filepath.Ext(strings.TrimSpace(file.FilePath)); ext != "" {
		return ext
	}
	return ".ogg"
}

func transcribeAudioOffline(ctx context.Context, workdir, model, audioPath string) (string, error) {
	scriptPath := filepath.Join(workdir, "scripts", "transcribe_audio.py")
	if _, err := os.Stat(scriptPath); err != nil {
		return "", fmt.Errorf("script de transcrição offline ausente em %s: %w", scriptPath, err)
	}

	args := []string{scriptPath}
	if strings.TrimSpace(model) != "" {
		args = append(args, "--model", strings.TrimSpace(model))
	}
	args = append(args, audioPath)

	cmd := exec.CommandContext(ctx, "python3", args...)
	cmd.Dir = workdir
	output, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(sanitizeForTelegram(string(output)))
		if msg == "" {
			return "", err
		}
		return "", fmt.Errorf("%v: %s", err, msg)
	}

	text := strings.TrimSpace(sanitizeForTelegram(string(output)))
	if text == "" {
		return "", errors.New("a transcrição voltou vazia")
	}
	return text, nil
}

func extractVideoFrames(ctx context.Context, videoPath, tmpDir string) ([]string, error) {
	pattern := filepath.Join(tmpDir, "frame-%02d.jpg")
	args := []string{
		"-y",
		"-i", videoPath,
		"-vf", "fps=1,scale='min(1280,iw)':-2",
		"-frames:v", "6",
		pattern,
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(sanitizeForTelegram(string(output)))
		if msg == "" {
			return nil, err
		}
		return nil, fmt.Errorf("%v: %s", err, msg)
	}
	framePaths, err := filepath.Glob(filepath.Join(tmpDir, "frame-*.jpg"))
	if err != nil {
		return nil, err
	}
	sort.Strings(framePaths)
	if len(framePaths) == 0 {
		return nil, errors.New("nenhum frame foi extraído do vídeo")
	}
	return framePaths, nil
}

func (r *RemoteRunner) Run(ctx context.Context, threadID, prompt string, imagePaths []string, onProgress func(string)) (reply string, newThreadID string, err error) {
	switch strings.ToLower(strings.TrimSpace(r.Provider)) {
	case "", "codex":
	case "claude-code", "broo":
		return "", "", fmt.Errorf("provider %q is not supported yet; only codex is implemented right now", r.Provider)
	default:
		return "", "", fmt.Errorf("unknown provider %q", r.Provider)
	}

	outFile, err := os.CreateTemp("", "broo-remote-last-*.txt")
	if err != nil {
		return "", "", err
	}
	outPath := outFile.Name()
	outFile.Close()
	defer os.Remove(outPath)

	var args []string
	if threadID == "" {
		args = []string{
			"exec",
			"--json",
			"--enable", codexAutoCompactFlag,
			"--skip-git-repo-check",
			"--dangerously-bypass-approvals-and-sandbox",
			"-C", r.Workdir,
			"-o", outPath,
		}
		if r.Model != "" {
			args = append(args, "--model", r.Model)
		}
		for _, imagePath := range imagePaths {
			if strings.TrimSpace(imagePath) != "" {
				args = append(args, "--image", imagePath)
			}
		}
		args = append(args, "-")
		prompt = initialPrompt(prompt, r.Workdir)
	} else {
		args = []string{
			"exec", "resume",
			"--json",
			"--enable", codexAutoCompactFlag,
			"--skip-git-repo-check",
			"--dangerously-bypass-approvals-and-sandbox",
			"-o", outPath,
		}
		if r.Model != "" {
			args = append(args, "--model", r.Model)
		}
		for _, imagePath := range imagePaths {
			if strings.TrimSpace(imagePath) != "" {
				args = append(args, "--image", imagePath)
			}
		}
		args = append(args, threadID, "-")
	}

	cmd := exec.CommandContext(ctx, r.Binary, args...)
	cmd.Dir = r.Workdir
	cmd.Stdin = strings.NewReader(prompt)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", "", err
	}

	var stderrBuf bytes.Buffer
	if err := cmd.Start(); err != nil {
		return "", "", err
	}

	go func() {
		_, _ = io.Copy(&stderrBuf, stderr)
	}()

	lastMessage := ""
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		if onProgress != nil {
			if summary := summarizeCodexEvent(ev); summary != "" {
				onProgress(summary)
			}
		}
		if t, _ := ev["type"].(string); t == "thread.started" {
			if id, _ := ev["thread_id"].(string); id != "" {
				newThreadID = id
			}
		}
		if t, _ := ev["type"].(string); t == "item.completed" {
			if item, ok := ev["item"].(map[string]any); ok {
				if itemType, _ := item["type"].(string); itemType == "agent_message" {
					if text, _ := item["text"].(string); text != "" {
						lastMessage = text
					}
				}
			}
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return "", newThreadID, scanErr
	}

	waitErr := cmd.Wait()

	data, readErr := os.ReadFile(outPath)
	if readErr == nil {
		reply = strings.TrimSpace(string(data))
	}
	if reply == "" {
		reply = strings.TrimSpace(lastMessage)
	}

	if waitErr != nil {
		msg := strings.TrimSpace(stderrBuf.String())
		if msg == "" {
			msg = waitErr.Error()
		}
		if reply != "" {
			return reply, newThreadID, fmt.Errorf("%s", msg)
		}
		return "", newThreadID, fmt.Errorf("%s", msg)
	}

	return reply, newThreadID, nil
}

func initialPrompt(userText, workdir string) string {
	return strings.TrimSpace(fmt.Sprintf(`
You are talking to the repository owner through a Telegram relay.
Keep replies concise and plain text unless formatting clearly helps.
Work in %s by default unless the user says otherwise.
Do the work when asked; do not stop at analysis unless the user asked for analysis only.
For time references, use Brazil/Brasilia time (America/Sao_Paulo) unless the user says otherwise.
There is a knowledge.md file in the repo root with persistent repo-owner notes; consult it when useful.

User message:
%s
`, workdir, userText))
}

func helpText() string {
	return strings.TrimSpace(`
Codex via Telegram está pronto.

Comandos:
/help   mostra esta ajuda
/status mostra chat owner, workdir e thread atual
/new    limpa a sessão atual
/reset  alias de /new
/resume lista sessões recentes ou troca para outra sessão

Qualquer outra mensagem vai para o Codex mantendo a sessão deste chat.`)
}

func splitTelegramText(text string) []string {
	text = sanitizeForTelegram(text)
	if len(text) <= maxTelegramChars {
		return []string{text}
	}
	var parts []string
	for len(text) > maxTelegramChars {
		cut := maxTelegramChars
		for cut > maxTelegramChars-500 && cut > 0 && text[cut] != '\n' {
			cut--
		}
		if cut <= 0 || cut < maxTelegramChars-500 {
			cut = maxTelegramChars
		}
		parts = append(parts, strings.TrimSpace(text[:cut]))
		text = strings.TrimSpace(text[cut:])
	}
	if text != "" {
		parts = append(parts, text)
	}
	return parts
}

func truncateTelegramText(text string) string {
	if len(text) <= maxTelegramChars {
		return text
	}
	return text[:maxTelegramChars-3] + "..."
}

func sanitizeForTelegram(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	return strings.TrimSpace(s)
}
