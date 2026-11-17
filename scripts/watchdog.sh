#!/usr/bin/env bash
# Watchdog for the shuttle subscribers.
#
# Upstream shuttle has a 30s idle watchdog that cancels the hub event stream and
# then aborts the retry loop, leaving the container up but dead. We raised
# SUBSCRIBE_RPC_TIMEOUT in .env, and this timer is the second line of defence:
# if the node has new events but our Redis cursor has not moved for one interval
# (~2 min), or recent logs show "aborting retry", recreate the subscribers.
#
# Runs from shuttle-watchdog.timer (every 2 minutes) as root.
set -u

APP_DIR=/opt/shuttle
STATE_DIR=/run/shuttle-watchdog
MIN_RESTART_INTERVAL=300  # seconds between restarts
mkdir -p "$STATE_DIR"
log() { logger -t shuttle-watchdog "$*"; echo "[$(date -Is)] $*"; }

if [ ! -f "$APP_DIR/.env" ]; then
  log "missing $APP_DIR/.env, nothing to do"
  exit 0
fi

set -a
# shellcheck disable=SC1091
. "$APP_DIR/.env"
set +a

HUB_HTTP="${HUB_HOST%%:*}"

# Parse REDIS_URL (redis://:pass@host:port) without exposing the password.
redis_rest="${REDIS_URL#*://}"
redis_auth="${redis_rest%@*}"
redis_hostport="${redis_rest##*@}"
REDIS_PASS="${redis_auth#*:}"
REDIS_HOST="${redis_hostport%:*}"
REDIS_PORT="${redis_hostport##*:}"
export REDISCLI_AUTH="$REDIS_PASS"
rcli() { redis-cli --no-auth-warning -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"; }

restart_needed=0
for shard in 1 2; do
  latest=$(curl -fsS --max-time 5 "http://${HUB_HTTP}:3381/v1/events?from_event_id=0&reverse=true&pageSize=1&shard_index=${shard}" 2>/dev/null | jq -r '.events[0].id // empty' 2>/dev/null)
  if [ -z "$latest" ]; then
    log "shard${shard}: cannot read latest event id from node, skipping"
    continue
  fi
  cur=$(rcli GET "hub:${HUB_HOST}:${shard}:last-hub-event-id" 2>/dev/null || true)
  prev_file="${STATE_DIR}/shard${shard}"
  prev=""
  [ -f "$prev_file" ] && prev="$(cat "$prev_file")"
  if [ -n "$cur" ] && [ "$latest" -gt "$cur" ] && [ "$prev" = "shard${shard}:${cur}" ]; then
    log "shard${shard} stalled: cursor=${cur} latest=${latest} unchanged for one interval"
    restart_needed=1
  fi
  printf 'shard%s:%s' "$shard" "$cur" > "$prev_file"
done

# A dead subscriber logs "aborting retry" and then goes silent.
if cd "$APP_DIR" && docker compose logs --since 3m shuttle-1 shuttle-2 2>/dev/null | grep -q "aborting retry"; then
  log "recent 'aborting retry' found in subscriber logs"
  restart_needed=1
fi

if [ "$restart_needed" = 1 ]; then
  last="$(cat "${STATE_DIR}/last_restart" 2>/dev/null || echo 0)"
  now="$(date +%s)"
  if [ $((now - last)) -ge "$MIN_RESTART_INTERVAL" ]; then
    log "restarting shuttle subscribers"
    ( cd "$APP_DIR" && docker compose up -d --force-recreate shuttle-1 shuttle-2 ) >>"${STATE_DIR}/restart.log" 2>&1
    echo "$now" > "${STATE_DIR}/last_restart"
  else
    log "restart skipped (restarted $((now - last))s ago)"
  fi
fi
