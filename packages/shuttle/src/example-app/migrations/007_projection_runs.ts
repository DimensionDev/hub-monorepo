import { Kysely, sql } from "kysely";
import type { HubTables } from "../../shuttle/db";

/**
 * Run history for operations that (re)populate projections: `rebuild` (from raw rows already
 * in the DB) and `backfill` (reconcile against the hub). Written by the CLI commands, the
 * backfill worker and the local dashboard; `updated_at` doubles as a heartbeat.
 */
export const up = async (db: Kysely<HubTables>, _schema: string) => {
  await db.schema
    .createTable("shuttleProjectionRuns")
    .ifNotExists()
    .addColumn("id", "uuid", (c) => c.primaryKey().defaultTo(sql`gen_random_uuid()`))
    .addColumn("kind", "text", (c) => c.notNull().check(sql`kind IN ('rebuild', 'backfill')`))
    .addColumn("projections", sql`text[]`, (c) => c.notNull())
    .addColumn("fids", sql`bigint[]`)
    .addColumn("trigger", "text", (c) => c.notNull())
    .addColumn("status", "text", (c) => c.notNull().check(sql`status IN ('running', 'succeeded', 'failed')`))
    .addColumn("total", "integer")
    .addColumn("done", "integer", (c) => c.notNull().defaultTo(0))
    .addColumn("failedCount", "integer", (c) => c.notNull().defaultTo(0))
    .addColumn("lastFid", "bigint")
    .addColumn("details", "jsonb")
    .addColumn("error", "text")
    .addColumn("startedAt", "timestamptz", (c) => c.notNull().defaultTo(sql`now()`))
    .addColumn("updatedAt", "timestamptz", (c) => c.notNull().defaultTo(sql`now()`))
    .addColumn("finishedAt", "timestamptz")
    .execute();
  await db.schema
    .createIndex("shuttle_projection_runs_started_at_idx")
    .ifNotExists()
    .on("shuttleProjectionRuns")
    .column("startedAt")
    .execute();
};
