import { Kysely, sql } from "kysely";
import { HubError } from "@dimensiondev/hub-nodejs";
import type { HubTables } from "../../shuttle/db";
import { adoptReferenceTable } from "../reference/adopt";
import { referenceTables } from "../reference/schema";

/**
 * Creates every business table the projections will write (all but blocks, which 005 owns), exactly
 * as the reference database defines them, so later projections only add logic. Existing tables are
 * validated and adopted; missing indexes are added. Nothing existing is dropped or rewritten.
 */
export const up = async (db: Kysely<HubTables>, schema: string) => {
  // casts.text uses gin_trgm_ops. pg_trgm is a trusted extension, so a role with CREATE on the
  // database can install it; keep it where it already is, otherwise put it in public.
  await sql`CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public`.execute(db);
  const trgm = await sql<{ schema: string }>`
    SELECT n.nspname AS schema FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = 'pg_trgm'
  `.execute(db);
  const trgmSchema = trgm.rows[0]?.schema;
  if (!trgmSchema) throw new HubError("unavailable", "pg_trgm extension is not installed");

  for (const spec of referenceTables) await adoptReferenceTable(db, schema, spec, trgmSchema);
};
