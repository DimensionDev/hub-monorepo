#!/usr/bin/env bash
# Deploy shuttle to the server without CI or a registry.
#
#   scripts/deploy.sh deploy [ref]           # default HEAD: upload, build on server, switch
#   scripts/deploy.sh rollback <sha> [--force]
#   scripts/deploy.sh status
#
# deploy uploads `git archive <ref>` to $APP_DIR/releases/<sha>, builds `shuttle:<sha>` on the
# server (native amd64, `nice` so the running shards keep their CPU), then switches:
#   1. migration guard: refuse an image that knows fewer migrations than the running one
#   2. sync docker-compose.yml and scripts/*.sh from the image into $APP_DIR (old copies backed up)
#   3. set IMAGE_TAG in $APP_DIR/.env
#   4. recreate shuttle-1, wait for "Starting stream consumer" (migrations done), then shuttle-2
#   5. keep the newest $KEEP_RELEASES releases/images plus the current and previous tag
#
# Shards are stopped then started, never overlapped (one consumer per shard). This script
# never touches Redis cursors; do not run seed-last-event.sh for a normal deploy.
#
# Environment: SHUTTLE_SSH_HOST, SHUTTLE_SSH_IDENTITY (same as dev-tunnel.py),
#              SHUTTLE_APP_DIR (default /opt/shuttle), KEEP_RELEASES (default 3),
#              READY_TIMEOUT (seconds, default 180).
set -euo pipefail

SSH_HOST="${SHUTTLE_SSH_HOST:-ubuntu@43.130.67.184}"
SSH_IDENTITY="${SHUTTLE_SSH_IDENTITY:-$HOME/Project/ssh/tencent-shuttle}"
APP_DIR="${SHUTTLE_APP_DIR:-/opt/shuttle}"
KEEP_RELEASES="${KEEP_RELEASES:-3}"
READY_TIMEOUT="${READY_TIMEOUT:-180}"
SELF="${BASH_SOURCE[0]}"

die() { echo "deploy: $*" >&2; exit 1; }
usage() { sed -n '2,6p' "$SELF" | sed 's/^# \{0,1\}//'; exit 1; }

ssh_cmd() { ssh -i "$SSH_IDENTITY" -o BatchMode=yes -o ServerAliveInterval=30 "$SSH_HOST" "$@"; }

# Run the remote half of this file (below the __REMOTE__ marker) as root on the server.
remote() {
  local args
  printf -v args ' %q' "$@"
  sed -n '/^# __REMOTE__$/,$p' "$SELF" |
    ssh_cmd "sudo APP_DIR=$(printf %q "$APP_DIR") KEEP_RELEASES=$KEEP_RELEASES READY_TIMEOUT=$READY_TIMEOUT bash -s --$args"
}

cmd_deploy() {
  local ref="${1:-HEAD}" sha
  cd "$(git -C "$(dirname "$SELF")" rev-parse --show-toplevel)"
  sha="$(git rev-parse --short=12 "$ref^{commit}" 2>/dev/null)" || die "unknown ref: $ref"
  if [ "$ref" = "HEAD" ] && ! git diff --quiet HEAD --; then
    die "working tree has uncommitted changes; commit them or pass an explicit ref"
  fi
  echo "deploy: $(git log -1 --format='%h %s' "$sha")"

  if remote has-release "$sha"; then
    echo "deploy: release $sha already uploaded"
  else
    echo "deploy: uploading source"
    git archive --format=tar "$sha" | gzip -9 | ssh_cmd "sudo bash -c '
      set -e; d=$APP_DIR/releases/$sha
      rm -rf \$d.tmp && mkdir -p \$d.tmp && tar -xz -C \$d.tmp && mv \$d.tmp \$d'"
  fi
  remote build "$sha"
  remote switch "$sha" ""
  remote prune
}

cmd_rollback() {
  local sha="${1:-}" force="${2:-}"
  [[ "$sha" =~ ^[0-9a-f]{7,40}$ ]] || die "rollback needs a sha (see: scripts/deploy.sh status)"
  [ -z "$force" ] || [ "$force" = "--force" ] || usage
  remote switch "$sha" "$force"
}

case "${1:-}" in
  deploy) shift; cmd_deploy "$@" ;;
  rollback) shift; cmd_rollback "$@" ;;
  status) remote status ;;
  *) usage ;;
esac
exit 0

# __REMOTE__
# Braced so bash parses the whole body before running it: the script arrives on stdin,
# and a command that reads stdin must not swallow the rest of it.
{
set -euo pipefail
cd "$APP_DIR"
RELEASES="$APP_DIR/releases"
mkdir -p "$RELEASES"

log() { echo "[server] $*"; }
fail() { echo "[server] ERROR: $*" >&2; exit 1; }
current_tag() { sed -n 's/^IMAGE_TAG=//p' .env | tail -1; }
image_exists() { docker image inspect "shuttle:$1" >/dev/null 2>&1; }
# Migration names registered in the image's migrator (db.ts getMigrations).
migrations_of() {
  docker run --rm --entrypoint sh "shuttle:$1" -c \
    "grep -oE '\"[0-9]{3}_[a-z0-9_]+\"' src/example-app/db.ts | tr -d '\"' | sort -u"
}

wait_ready() {
  local svc=$1 since=$2 deadline=$((SECONDS + READY_TIMEOUT)) out cid
  log "waiting for $svc (up to ${READY_TIMEOUT}s)"
  while [ $SECONDS -lt $deadline ]; do
    out="$(docker compose logs --no-color --since "$since" "$svc" 2>&1 || true)"
    if grep -q "Starting stream consumer" <<<"$out"; then
      grep -E 'Migration ".*" was executed successfully' <<<"$out" || true
      log "$svc is consuming"
      return 0
    fi
    cid="$(docker compose ps -q "$svc")"
    if grep -qE "Failed to apply all database migrations|Failed to migrate database" <<<"$out" ||
       [ "$(docker inspect -f '{{.RestartCount}}' "$cid" 2>/dev/null || echo 0)" != "0" ]; then
      echo "$out" | tail -40 >&2
      fail "$svc failed to start; the other shard was not touched"
    fi
    sleep 3
  done
  echo "$out" | tail -40 >&2
  fail "$svc not ready after ${READY_TIMEOUT}s"
}

case "$1" in
  has-release)
    [ -d "$RELEASES/$2" ] ;;

  build)
    sha=$2
    if image_exists "$sha"; then log "image shuttle:$sha exists, skipping build"; exit 0; fi
    [ -d "$RELEASES/$sha" ] || fail "release $sha not uploaded"
    log "building shuttle:$sha"
    nice -n 10 docker build -t "shuttle:$sha" --build-arg SHUTTLE_REVISION="$sha" "$RELEASES/$sha"
    ;;

  switch)
    sha=$2 force=$3 cur="$(current_tag)"
    image_exists "$sha" || fail "image shuttle:$sha not found on server"
    if [ "$cur" = "$sha" ]; then log "already on $sha, recreating anyway"; fi

    if [ -n "$cur" ] && [ "$cur" != "$sha" ] && image_exists "$cur"; then
      missing="$(comm -23 <(migrations_of "$cur") <(migrations_of "$sha"))"
      if [ -n "$missing" ]; then
        [ "$force" = "--force" ] || fail "shuttle:$sha does not know migrations applied by $cur: $missing
  migrations are forward-only; rolling back past them is unsafe (use --force only if you know the schema is compatible)"
        log "WARNING: --force past migrations: $missing"
      fi
    fi

    backup="$APP_DIR/backups/$(date +%Y%m%d%H%M%S)"
    mkdir -p "$backup/scripts"
    cp -p .env "$backup/.env"
    [ -f docker-compose.yml ] && cp -p docker-compose.yml "$backup/"
    cp -p scripts/*.sh "$backup/scripts/" 2>/dev/null || true
    cid="$(docker create "shuttle:$sha")"
    docker cp "$cid:/src/docker-compose.yml" docker-compose.yml
    mkdir -p scripts && docker cp "$cid:/src/scripts/." scripts/
    docker rm "$cid" >/dev/null
    chmod +x scripts/*.sh
    if grep -q '^IMAGE_TAG=' .env; then sed -i "s/^IMAGE_TAG=.*/IMAGE_TAG=$sha/" .env; else echo "IMAGE_TAG=$sha" >>.env; fi
    log "IMAGE_TAG ${cur:-<unset>} -> $sha (backup: $backup)"

    for svc in shuttle-1 shuttle-2; do
      since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      docker compose up -d --no-deps --force-recreate "$svc"
      wait_ready "$svc" "$since"
    done
    log "deployed $sha; roll back with: scripts/deploy.sh rollback ${cur:-<previous sha>}"
    ;;

  prune)
    cur="$(current_tag)"
    prev="$(ls -1t backups 2>/dev/null | head -1)"
    prev="$( [ -n "$prev" ] && sed -n 's/^IMAGE_TAG=//p' "backups/$prev/.env" | tail -1 || true)"
    keep="$(ls -1t "$RELEASES" | grep -E '^[0-9a-f]{7,40}$' | head -n "$KEEP_RELEASES"; echo "$cur"; echo "$prev")"
    for d in $(ls -1 "$RELEASES" | grep -E '^[0-9a-f]{7,40}$'); do
      grep -qx "$d" <<<"$keep" || { log "removing release $d"; rm -rf "${RELEASES:?}/$d"; }
    done
    for t in $(docker images shuttle --format '{{.Tag}}' | grep -E '^[0-9a-f]{7,40}$'); do
      grep -qx "$t" <<<"$keep" || { log "removing image shuttle:$t"; docker rmi "shuttle:$t" >/dev/null || true; }
    done
    ls -1t backups 2>/dev/null | tail -n +11 | while read -r b; do rm -rf "backups/$b"; done
    docker image prune -f >/dev/null
    ;;

  status)
    echo "IMAGE_TAG=$(current_tag)"
    docker compose ps
    echo; echo "images:"; docker images shuttle --format '  {{.Tag}}  {{.CreatedSince}}  {{.Size}}'
    echo "releases:"; ls -1t "$RELEASES" | sed 's/^/  /'
    ;;

  *) fail "unknown remote command: $1" ;;
esac
exit 0
}
