#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
export PATH="$(pwd)/bin:$PATH"

if [[ ! -f .env.broo-remote ]]; then
  echo "Missing .env.broo-remote"
  exit 1
fi

set -a
source .env.broo-remote
set +a

if env | grep -qi 'litellm'; then
  echo "Refusing to start broo-remote: litellm is blocked in this repo"
  env | grep -i 'litellm' | sed 's/=.*//'
  exit 1
fi

mkdir -p logs

find_broo_remote_pid() {
  pgrep -f '(^| )(./broo-remote|/home/curcio/broo-remote/broo-remote)( |$)' | head -n1 || true
}

echo "Building broo-remote..."
go build -o broo-remote ./cmd/broo-remote/

if [[ -f logs/broo-remote.pid ]]; then
  pid="$(cat logs/broo-remote.pid 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    echo "Stopped broo-remote (pid $pid)"
    sleep 1
  fi
fi

existing="$(pgrep -f '(^| )(./broo-remote|/home/curcio/broo-remote/broo-remote)( |$)' || true)"
if [[ -n "$existing" ]]; then
  while read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" 2>/dev/null || true
    echo "Stopped broo-remote (pid $pid)"
  done <<< "$existing"
  sleep 1
fi

rm -f logs/broo-remote.pid

if command -v setsid >/dev/null 2>&1; then
  nohup setsid -f ./broo-remote >> logs/broo-remote.out 2>&1 < /dev/null
else
  nohup ./broo-remote >> logs/broo-remote.out 2>&1 < /dev/null &
fi

sleep 1
pid=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  pid="$(find_broo_remote_pid)"
  [[ -n "$pid" ]] && break
  sleep 1
done
if [[ -z "$pid" ]]; then
  echo "Failed to start broo-remote"
  exit 1
fi

echo "$pid" > logs/broo-remote.pid
echo "broo-remote running (pid $pid)"
