import { Kysely, sql } from "kysely";
import { HubError } from "@dimensiondev/hub-nodejs";
import type { HubTables } from "../../shuttle/db";

const columns: Record<string, [string, boolean]> = {
  id: ["uuid", true],
  created_at: ["timestamp without time zone", true],
  updated_at: ["timestamp without time zone", true],
  timestamp: ["timestamp without time zone", true],
  deleted_at: ["timestamp without time zone", false],
  blocker_fid: ["bigint", true],
  blocked_fid: ["bigint", true],
};

const indexes: Record<string, { columns: string[]; predicate: string | null; primary: boolean }> = {
  blocks_pkey: { columns: ["id"], predicate: null, primary: true },
  blocks_blocked_fid_deleted_at: {
    columns: ["blocked_fid", "deleted_at"],
    predicate: "(deleted_at IS NULL)",
    primary: false,
  },
  blocks_blocker_fid_deleted_at: {
    columns: ["blocker_fid", "deleted_at"],
    predicate: "(deleted_at IS NULL)",
    primary: false,
  },
  blocks_updated_at_idx: { columns: ["updated_at"], predicate: null, primary: false },
};

/** Validate an existing business table before adopting it; never replace data or constraints. */
export const up = async (db: Kysely<HubTables>, schema: string) => {
  const relation = await sql<{ kind: string }>`
    SELECT c.relkind AS kind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ${schema} AND c.relname = 'blocks'
  `.execute(db);
  if (!relation.rows.length) {
    await db.schema
      .createTable("blocks")
      .addColumn("id", "uuid", (c) => c.notNull())
      .addColumn("createdAt", "timestamp", (c) => c.notNull())
      .addColumn("updatedAt", "timestamp", (c) => c.notNull())
      .addColumn("timestamp", "timestamp", (c) => c.notNull())
      .addColumn("deletedAt", "timestamp")
      .addColumn("blockerFid", "bigint", (c) => c.notNull())
      .addColumn("blockedFid", "bigint", (c) => c.notNull())
      .addPrimaryKeyConstraint("blocks_pkey", ["id"])
      .execute();
  } else if (relation.rows[0]?.kind !== "r") {
    throw new HubError("bad_request.validation_failure", "Incompatible blocks relation: expected an ordinary table");
  }

  const actualColumns = await sql<{
    name: string;
    type: string;
    required: boolean;
    defaultValue: string | null;
    identity: string;
    generated: string;
  }>`
    SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type,
      a.attnotnull AS required, pg_get_expr(d.adbin, d.adrelid) AS default_value,
      a.attidentity AS identity, a.attgenerated AS generated
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE n.nspname = ${schema} AND c.relname = 'blocks' AND a.attnum > 0 AND NOT a.attisdropped
  `.execute(db);
  if (
    actualColumns.rows.length !== Object.keys(columns).length ||
    actualColumns.rows.some((c) => {
      const expected = columns[c.name];
      return (
        !expected ||
        c.type !== expected[0] ||
        c.required !== expected[1] ||
        c.defaultValue !== null ||
        c.identity !== "" ||
        c.generated !== ""
      );
    })
  ) {
    throw new HubError(
      "bad_request.validation_failure",
      "Incompatible blocks columns; expected the reference's seven columns without defaults",
    );
  }
  const constraints = await sql<{ name: string; definition: string }>`
    SELECT con.conname AS name, pg_get_constraintdef(con.oid) AS definition
    FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ${schema} AND c.relname = 'blocks' AND con.contype != 'n'
  `.execute(db);
  if (
    constraints.rows.length !== 1 ||
    constraints.rows[0]?.name !== "blocks_pkey" ||
    constraints.rows[0]?.definition !== "PRIMARY KEY (id)"
  ) {
    throw new HubError(
      "bad_request.validation_failure",
      "Incompatible blocks constraints; expected only blocks_pkey on id",
    );
  }
  const actualIndexes = await sql<{
    name: string;
    method: string;
    keys: string[];
    predicate: string | null;
    primary: boolean;
    unique: boolean;
    valid: boolean;
    included: number;
  }>`
    SELECT ic.relname AS name, am.amname AS method,
      ARRAY(SELECT pg_get_indexdef(i.indexrelid, k, true)
        FROM generate_series(1, i.indnkeyatts) k ORDER BY k) AS keys,
      pg_get_expr(i.indpred, i.indrelid) AS predicate,
      i.indisprimary AS primary, i.indisunique AS unique, i.indisvalid AS valid,
      i.indnatts - i.indnkeyatts AS included
    FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_am am ON am.oid = ic.relam
    WHERE n.nspname = ${schema} AND c.relname = 'blocks'
  `.execute(db);
  for (const index of actualIndexes.rows) {
    const expected = indexes[index.name];
    if (
      !expected ||
      index.method !== "btree" ||
      index.keys.join(",") !== expected.columns.join(",") ||
      index.predicate !== expected.predicate ||
      index.primary !== expected.primary ||
      index.unique !== expected.primary ||
      !index.valid ||
      index.included !== 0
    ) {
      throw new HubError("bad_request.validation_failure", `Incompatible blocks index: ${index.name}`);
    }
  }
  for (const [name, index] of Object.entries(indexes)) {
    if (actualIndexes.rows.some((existing) => existing.name === name)) continue;
    await sql`CREATE INDEX ${sql.id(name)} ON ${sql.id(schema, "blocks")}
      (${sql.join(index.columns.map((column) => sql.id(column)))})
      WITH (fillfactor=100, deduplicate_items=true)
      ${index.predicate ? sql`WHERE deleted_at IS NULL` : sql``}`.execute(db);
  }

  await db.schema
    .createTable("shuttleBlockMessageState")
    .addColumn("fid", "bigint", (c) => c.notNull())
    .addColumn("hash", "bytea", (c) => c.notNull())
    .addColumn("shardIndex", "integer", (c) => c.notNull())
    .addColumn("eventId", "bigint", (c) => c.notNull())
    .addColumn("operation", "text", (c) => c.notNull())
    .addColumn("eventTimestamp", "timestamptz", (c) => c.notNull())
    .addPrimaryKeyConstraint("shuttle_block_message_state_pkey", ["fid", "hash"])
    .execute();
};
