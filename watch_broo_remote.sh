#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env.broo-remote ]]; then
  echo "Missing .env.broo-remote"
  exit 1
fi

set -a
source .env.broo-remote
set +a

HEARTBEAT_PATH="${BROO_REMOTE_HEARTBEAT_PATH:-/home/curcio/broo-remote/logs/broo-remote.heartbeat}"
PIDFILE="logs/broo-remote.pid"
WATCHDOG_PIDFILE="logs/broo-remote_watchdog.pid"
WATCHDOG_LOG="logs/broo-remote_watchdog.out"
CHECK_INTERVAL="${BROO_REMOTE_WATCHDOG_INTERVAL_SEC:-20}"
STALE_AFTER="${BROO_REMOTE_WATCHDOG_STALE_SEC:-120}"

mkdir -p logs
echo "$$" > "$WATCHDOG_PIDFILE"

log_line() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$WATCHDOG_LOG"
}

find_broo_remote_pid() {
  local pid=""
  if [[ -f "$PIDFILE" ]]; then
    pid="$(cat "$PIDFILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      printf '%s\n' "$pid"
      return 0
    fi
  fi

  pid="$(pgrep -f '^(\./broo-remote|/home/curcio/broo-remote/broo-remote)$' | head -n1 || true)"
  if [[ -n "$pid" ]]; then
    printf '%s\n' "$pid"
    return 0
  fi
  return 1
}

heartbeat_age() {
  if [[ ! -f "$HEARTBEAT_PATH" ]]; then
    echo 999999
    return
  fi
  now="$(date +%s)"
  hb="$(stat -c %Y "$HEARTBEAT_PATH" 2>/dev/null || echo 0)"
  echo $(( now - hb ))
}

restart_broo_remote() {
  log_line "restart requested"
  tail -n 40 logs/broo-remote.out >> "$WATCHDOG_LOG" 2>/dev/null || true
  if ./start_broo_remote.sh >> "$WATCHDOG_LOG" 2>&1; then
    log_line "restart ok"
  else
    log_line "restart failed"
  fi
}

while true; do
  pid="$(find_broo_remote_pid || true)"
  age="$(heartbeat_age)"

  if [[ -z "$pid" ]]; then
    log_line "process missing; heartbeat_age=${age}s"
    restart_broo_remote
    sleep 10
    continue
  fi

  if [[ "$age" -gt "$STALE_AFTER" ]]; then
    log_line "heartbeat stale pid=$pid age=${age}s"
    kill "$pid" 2>/dev/null || true
    sleep 2
    restart_broo_remote
    sleep 10
    continue
  fi

  sleep "$CHECK_INTERVAL"
done
