# Broo Remote

This is a separate bot from the Poly notifications bot.

It uses Telegram **long polling**, so you do **not** need to open any public port or bind anything on Ghost. The process only makes outbound HTTPS requests to Telegram and runs a local CLI provider.

## Files

- `cmd/broo-remote/main.go` — Telegram relay service
- `start_broo_remote.sh` — build + restart helper for this bot only
- `watch_broo_remote.sh` — watchdog loop
- `start_broo_remote_watchdog.sh` — starts the watchdog in background
- `.env.broo-remote` — private env file for this bot
- `logs/broo-remote_state.json` — owner chat + session persistence
- `logs/broo-remote.out` — runtime log
- `logs/broo-remote.heartbeat` — health heartbeat
- `logs/broo-remote_watchdog.out` — watchdog log

## Setup

1. Create `.env.broo-remote` from `.env.broo-remote.example`
2. Put the BotFather token in `BROO_REMOTE_BOT_TOKEN`
3. Optional for audio transcription: install Whisper locally
4. Pick the provider in `BROO_REMOTE_PROVIDER` (`codex`, `claude-code`, `broo`)
5. Start with:

```bash
cd /home/curcio/broo-remote
./start_broo_remote.sh
./start_broo_remote_watchdog.sh
```

## Owner Claim

The bot starts with **no owner chat**.

The **first `/start`** it receives claims the owner chat and locks the bot to that chat only.

After that:

- `/help` shows commands
- `/status` shows current thread
- `/new` resets the chat session
- any other message is sent to the configured local provider

Provider status today:

- `codex`: supported
- `claude-code`: config stub only, not implemented yet
- `broo`: config stub only, not implemented yet

## Audio Notes

The relay can also process Telegram `voice` / `audio` messages.

Behavior:

- the bot downloads the Telegram audio file
- it transcribes the audio with local Whisper in Python
- the transcript is then sent into the normal Codex flow like a text message

Requirements:

- `ffmpeg` installed on the host
- `python3 -m pip install --user openai-whisper`
- optional model override: `BROO_REMOTE_AUDIO_MODEL`
- default model: `base`
- first use downloads the selected Whisper model locally

## Autonomous Tasks

The relay now also keeps two local SQLite queues in the repo root:

- `tasks.db` for scheduled work
- `idle.db` for idle-time work

Polling behavior:

- at boot it checks Telegram backlog first before starting background work
- every `1` minute the bot checks `tasks.db` for due tasks
- if no due scheduled task starts, it checks `idle.db` every `7` minutes by default
- both queues only run when the owner chat is idle; if Telegram is busy, they wait
- runtime work is also guarded by a task lock file so idle work does not start while another task is already executing

Both databases use the same `tasks` table:

- `id` integer primary key
- `prompt` text, required
- `scheduled_for` text in UTC RFC3339, required only for `tasks.db`
- `status` text: `pending`, `running`, `done`, `failed`
- `created_at`, `claimed_at`, `completed_at`
- `last_error`, `last_result`

Optional env override:

- `BROO_REMOTE_TASKS_DB_PATH`
- `BROO_REMOTE_IDLE_DB_PATH`
- `BROO_REMOTE_PROVIDER`
- `BROO_REMOTE_RUNNER_BIN`
- `BROO_REMOTE_IDLE_POLL_INTERVAL_MIN`
- `BROO_REMOTE_TASK_LOCK_PATH`

Example inserts:

```sql
INSERT INTO tasks (prompt, scheduled_for) VALUES (
  'Review open strategy risks and summarize.',
  '2026-03-26T18:30:00Z'
);
```

```sql
INSERT INTO tasks (prompt) VALUES (
  'Look for small cleanup opportunities in the repo and document them.'
);
```
