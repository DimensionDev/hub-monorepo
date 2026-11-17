#!/usr/bin/env bash
# redis-cli wrapper for the managed TencentDB Redis instance.
#
# `redis-cli -u "$REDIS_URL"` sends AUTH with an empty username, which TencentDB
# for Redis rejects (WRONGPASS). This wrapper parses /opt/shuttle/.env and lets
# redis-cli use REDISCLI_AUTH instead.
#
# Usage:
#   /opt/shuttle/scripts/redis-cli.sh PING
#   /opt/shuttle/scripts/redis-cli.sh GET hub:shuttle:last-hub-event-id
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

exec redis-cli --no-auth-warning -h "$redis_host" -p "$redis_port" "$@"
