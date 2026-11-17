#!/usr/bin/env bash
# Apply idempotent read-only grants for the shuttle PostgreSQL database.
#
# The read-only *account* is created by Terraform (tencentcloud_postgresql_account.readonly,
# outputs postgres_readonly_user / postgres_readonly_url). Its object-level grants are
# applied by this script because the Tencent Cloud API only supports per-object grants
# (which go stale as shuttle migrations create new tables).
#
# We connect as the shuttle app account (database owner, credentials are in
# /opt/shuttle/.env) and grant:
#   - CONNECT on the database
#   - USAGE on schema public
#   - SELECT on all existing tables / sequences
#   - ALTER DEFAULT PRIVILEGES so tables created by future shuttle migrations are readable
#     automatically (no re-run needed for new tables).
#
# Run on the shuttle VM (Docker + /opt/shuttle/.env required):
#   sudo bash /opt/shuttle/scripts/pg-readonly-grants.sh
# Optional: PG_READONLY_USER=other_ro SHUTTLE_IMAGE=... sudo -E bash ...
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/shuttle}"
IMAGE="${SHUTTLE_IMAGE:-ghcr.io/dimensiondev/hub-monorepo/shuttle:sha-e5fbaa4}"
RO_USER="${PG_READONLY_USER:-shuttle_ro}"

cd "$APP_DIR"
exec docker run --rm --network host --env-file .env \
  -e READONLY_USER="$RO_USER" \
  --entrypoint node "$IMAGE" -e '
const { Client } = require("/src/node_modules/pg");
const ro = process.env.READONLY_USER;
const ident = (s) => "\"" + String(s).replace(/"/g, "\"\"") + "\"";
(async () => {
  const c = new Client({ connectionString: process.env.POSTGRES_URL });
  await c.connect();
  const db = (await c.query("select current_database() as d")).rows[0].d;
  const role = (await c.query("select current_user as u")).rows[0].u;
  const stmts = [
    `GRANT CONNECT ON DATABASE ${ident(db)} TO ${ident(ro)}`,
    `GRANT USAGE ON SCHEMA public TO ${ident(ro)}`,
    `GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${ident(ro)}`,
    `GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO ${ident(ro)}`,
    `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${ident(ro)}`,
    `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO ${ident(ro)}`,
  ];
  for (const sql of stmts) {
    await c.query(sql);
    console.log("OK  " + sql);
  }
  console.log(`Grants applied: database=${db} owner=${role} read_only_role=${ro}`);
  await c.end();
})().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
'
