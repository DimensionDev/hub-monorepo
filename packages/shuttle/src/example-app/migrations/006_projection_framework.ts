import { Kysely, sql } from "kysely";
import { HubError } from "@dimensiondev/hub-nodejs";
import type { HubTables } from "../../shuttle/db";

/**
 * Generic projection framework state:
 * - `shuttle_block_message_state` becomes `shuttle_message_state`, shared by every projection.
 *   `event_id` / `shard_index` are nullable so reconcile (backfill) writes can record a row
 *   without a hub event.
 * - `shuttle_projection_issues` records expected data gaps instead of failing the event.
 * - `onchain_events` gets an idempotency key so replays and backfills never duplicate rows.
 */
export const up = async (db: Kysely<HubTables>, schema: string) => {
  await sql`ALTER TABLE ${sql.id(schema, "shuttle_block_message_state")} RENAME TO shuttle_message_state`.execute(db);
  await sql`ALTER TABLE ${sql.id(schema, "shuttle_message_state")}
    RENAME CONSTRAINT shuttle_block_message_state_pkey TO shuttle_message_state_pkey`.execute(db);
  await sql`ALTER TABLE ${sql.id(schema, "shuttle_message_state")}
    ALTER COLUMN event_id DROP NOT NULL,
    ALTER COLUMN shard_index DROP NOT NULL`.execute(db);

  await db.schema
    .createTable("shuttleProjectionIssues")
    .ifNotExists()
    .addColumn("id", "uuid", (c) => c.primaryKey().defaultTo(sql`gen_random_uuid()`))
    .addColumn("projection", "text", (c) => c.notNull())
    .addColumn("fid", "bigint")
    .addColumn("hash", "bytea", (c) => c.notNull())
    .addColumn("reason", "text", (c) => c.notNull())
    .addColumn("payload", "jsonb")
    .addColumn("createdAt", "timestamptz", (c) => c.notNull().defaultTo(sql`now()`))
    .addUniqueConstraint("shuttle_projection_issues_key", ["projection", "hash", "reason"])
    .execute();

  // Duplicates that differ in content are a data error, not something to resolve silently.
  const conflicts = await sql<{ chainId: string; blockNumber: string; logIndex: number }>`
    SELECT chain_id, block_number, log_index FROM ${sql.id(schema, "onchain_events")}
    GROUP BY chain_id, block_number, log_index
    HAVING count(DISTINCT (type, fid, tx_hash, body::text)) > 1
    LIMIT 20
  `.execute(db);
  if (conflicts.rows.length) {
    const keys = conflicts.rows.map((r) => `${r.chainId}:${r.blockNumber}:${r.logIndex}`).join(", ");
    throw new HubError(
      "bad_request.conflict",
      `onchain_events has conflicting duplicates (chain:block:log): ${keys}; resolve them before migrating`,
    );
  }
  await sql`
    DELETE FROM ${sql.id(schema, "onchain_events")} e
    USING (
      SELECT id, row_number() OVER (PARTITION BY chain_id, block_number, log_index ORDER BY id) AS n
      FROM ${sql.id(schema, "onchain_events")}
    ) d
    WHERE e.id = d.id AND d.n > 1
  `.execute(db);
  await db.schema
    .createIndex("onchain_events_chain_block_log_unique")
    .ifNotExists()
    .on("onchain_events")
    .columns(["chainId", "blockNumber", "logIndex"])
    .unique()
    .execute();
};
