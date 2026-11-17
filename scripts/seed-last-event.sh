#!/usr/bin/env bash
# Seed Redis with the current per-shard snapchain event ids so fresh shuttle
# subscribers start from "now" instead of replaying the events the node still
# retains (~3 days).
#
# Run this once, RIGHT BEFORE `docker compose up -d`.
# Re-running it later moves the cursors forward (events in between are skipped),
# so only use it when you actually want to reset them.
#
# Cursor keys match what the pinned shuttle release reads:
#   hub:<HUB_HOST>:<shard_index>:last-hub-event-id   e.g. "hub:<snapchain-private-ip>:3383:1:last-hub-event-id"
#
# Usage:
#   /opt/shuttle/scripts/seed-last-event.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/shuttle}"

if [ ! -f "$APP_DIR/.env" ]; then
  echo "missing $APP_DIR/.env" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. "$APP_DIR/.env"
set +a

SHARDS="${SHARDS:-2}"
HUB_HTTP_HOST="${HUB_HOST%%:*}"
HUB_HUB="${HUB_HOST}"

# TencentDB for Redis rejects AUTH with an empty username, and `redis-cli -u
# redis://:pass@host` sends AUTH "" <pass>, which fails with WRONGPASS.
# Parse the URL and pass the password via REDISCLI_AUTH instead.
redis_url="${REDIS_URL#*://}"
redis_auth=""
redis_hostport="$redis_url"
if [[ "$redis_url" == *"@"* ]]; then
  redis_auth="${redis_url%@*}"
  redis_hostport="${redis_url##*@}"
fi
redis_pass="${redis_auth#*:}"
redis_host="${redis_hostport%:*}"
redis_port="${redis_hostport##*:}"
if [ -n "$redis_pass" ]; then
  export REDISCLI_AUTH="$redis_pass"
fi

rcli() {
  redis-cli --no-auth-warning -h "$redis_host" -p "$redis_port" "$@"
}

for shard in $(seq 1 "$SHARDS"); do
  latest_id="$(
    curl -fsS "http://${HUB_HTTP_HOST}:3381/v1/events?from_event_id=0&reverse=true&pageSize=1&shard_index=${shard}" \
      | jq -r '.events[0].id'
  )"

  if [ -z "$latest_id" ] || [ "$latest_id" = "null" ]; then
    echo "could not determine latest snapchain event id for shard ${shard}" >&2
    exit 1
  fi

  cursor_key="hub:${HUB_HUB}:${shard}:last-hub-event-id"
  rcli SET "$cursor_key" "$latest_id" >/dev/null
  current="$(rcli GET "$cursor_key")"
  if [ "$current" != "$latest_id" ]; then
    echo "failed to persist cursor ${cursor_key} (redis returned: ${current:-<empty>})" >&2
    exit 1
  fi
  echo "shard ${shard}: cursor=${latest_id} (${cursor_key})"

  # Drop the plain "<HUB_HOST>:<shard>" key written by older versions.
  rcli DEL "${HUB_HUB}:${shard}" >/dev/null || true
done

# Cursor key used by older versions of this script; migrating it would mix up
# shard cursors, so make sure it is gone.
rcli DEL hub:shuttle:last-hub-event-id >/dev/null || true
