#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p logs

find_watchdog_pid() {
  pgrep -f '(^| )(bash ./watch_broo_remote.sh|./watch_broo_remote.sh|/home/curcio/broo-remote/watch_broo_remote.sh)( |$)' | head -n1 || true
}

if [[ -f logs/broo-remote_watchdog.pid ]]; then
  pid="$(cat logs/broo-remote_watchdog.pid 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    echo "Stopped broo-remote watchdog (pid $pid)"
    sleep 1
  fi
fi

existing="$(pgrep -f '(^| )(bash ./watch_broo_remote.sh|./watch_broo_remote.sh|/home/curcio/broo-remote/watch_broo_remote.sh)( |$)' || true)"
if [[ -n "$existing" ]]; then
  while read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" 2>/dev/null || true
    echo "Stopped broo-remote watchdog (pid $pid)"
  done <<< "$existing"
  sleep 1
fi

rm -f logs/broo-remote_watchdog.pid

if command -v setsid >/dev/null 2>&1; then
  nohup setsid -f ./watch_broo_remote.sh >> logs/broo-remote_watchdog.out 2>&1 < /dev/null
else
  nohup ./watch_broo_remote.sh >> logs/broo-remote_watchdog.out 2>&1 < /dev/null &
fi

sleep 1
pid=""
for _ in 1 2 3 4 5; do
  pid="$(find_watchdog_pid)"
  [[ -n "$pid" ]] && break
  sleep 1
done
if [[ -z "$pid" ]]; then
  echo "Failed to start broo-remote watchdog"
  exit 1
fi

echo "$pid" > logs/broo-remote_watchdog.pid
echo "broo-remote watchdog running (pid $pid)"
