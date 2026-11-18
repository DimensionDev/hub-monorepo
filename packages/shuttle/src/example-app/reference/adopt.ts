import { HubError } from "@dimensiondev/hub-nodejs";
import { Kysely, sql } from "kysely";
import type { ColumnSpec, TableSpec } from "./schema";

/**
 * Normalises an index definition for comparison: collapses whitespace, writes the table schema as
 * `{schema}` and drops the schema qualifier pg_get_indexdef adds to an operator class that is not
 * on the search path (e.g. `public.gin_trgm_ops` when the tables live in another schema).
 */
export function normalizeIndexDef(def: string, quotedSchema: string, extensionSchemas: string[] = []): string {
  let result = def.replace(/\s+/g, " ").trim();
  result = result.replace(` ON ${quotedSchema}.`, " ON {schema}.");
  for (const ext of extensionSchemas) result = result.replaceAll(` ${ext}.gin_trgm_ops`, " gin_trgm_ops");
  return result;
}

type ActualColumn = {
  name: string;
  type: string;
  required: boolean;
  defaultValue: string | null;
  identity: string;
  generated: string;
};

function sameColumn(actual: ActualColumn, [name, type, notNull, defaultValue]: ColumnSpec): boolean {
  return (
    actual.name === name &&
    actual.type === type &&
    actual.required === notNull &&
    actual.defaultValue === defaultValue &&
    actual.identity === "" &&
    actual.generated === ""
  );
}

function describe([name, type, notNull, defaultValue]: ColumnSpec): string {
  return `${name} ${type}${notNull ? " NOT NULL" : ""}${defaultValue === null ? "" : ` DEFAULT ${defaultValue}`}`;
}

/**
 * Creates a business table exactly as the reference defines it, or validates an existing one and
 * adds any missing indexes. Never drops, rewrites or relaxes anything that already exists: a
 * mismatch is an error to resolve by hand.
 *
 * `trgmSchema` is where pg_trgm is installed; `gin_trgm_ops` is qualified with it when creating.
 */
export async function adoptReferenceTable(
  // biome-ignore lint/suspicious/noExplicitAny: migrations run against an untyped Kysely
  db: Kysely<any>,
  schema: string,
  spec: TableSpec,
  trgmSchema: string,
): Promise<"created" | "adopted"> {
  const quoted = (await sql<{ q: string }>`SELECT quote_ident(${schema}) AS q`.execute(db)).rows[0]?.q ?? schema;
  const quotedTrgm =
    (await sql<{ q: string }>`SELECT quote_ident(${trgmSchema}) AS q`.execute(db)).rows[0]?.q ?? trgmSchema;
  const relation = await sql<{ kind: string }>`
    SELECT c.relkind AS kind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ${schema} AND c.relname = ${spec.name}
  `.execute(db);

  const pkey = `${spec.name}_pkey`;
  let outcome: "created" | "adopted" = "adopted";
  if (!relation.rows.length) {
    const columns = spec.columns.map(
      ([name, type, notNull, defaultValue]) =>
        sql`${sql.id(name)} ${sql.raw(type)}${sql.raw(notNull ? " NOT NULL" : "")}${
          defaultValue === null ? sql`` : sql` DEFAULT ${sql.raw(defaultValue)}`
        }`,
    );
    await sql`CREATE TABLE ${sql.id(schema, spec.name)} (
      ${sql.join(columns)},
      CONSTRAINT ${sql.id(pkey)} PRIMARY KEY (id)
    )`.execute(db);
    outcome = "created";
  } else if (relation.rows[0]?.kind !== "r") {
    throw new HubError(
      "bad_request.validation_failure",
      `Incompatible ${spec.name} relation: expected an ordinary table`,
    );
  }

  const actualColumns = await sql<ActualColumn>`
    SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type,
      a.attnotnull AS required, pg_get_expr(d.adbin, d.adrelid) AS default_value,
      a.attidentity AS identity, a.attgenerated AS generated
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE n.nspname = ${schema} AND c.relname = ${spec.name} AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
  `.execute(db);
  const problems: string[] = [];
  const byName = new Map(actualColumns.rows.map((c) => [c.name, c]));
  for (const column of spec.columns) {
    const actual = byName.get(column[0]);
    if (!actual) problems.push(`missing column ${describe(column)}`);
    else if (!sameColumn(actual, column)) problems.push(`column ${column[0]} differs; expected ${describe(column)}`);
  }
  for (const actual of actualColumns.rows) {
    if (spec.columns.some(([name]) => name === actual.name)) continue;
    const extra = spec.extraColumns?.find(([name]) => name === actual.name);
    if (!extra) problems.push(`unexpected column ${actual.name}`);
    else if (!sameColumn(actual, extra)) problems.push(`column ${actual.name} differs; expected ${describe(extra)}`);
  }

  const constraints = await sql<{ name: string; definition: string }>`
    SELECT con.conname AS name, pg_get_constraintdef(con.oid) AS definition
    FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ${schema} AND c.relname = ${spec.name} AND con.contype != 'n'
  `.execute(db);
  if (
    constraints.rows.length !== 1 ||
    constraints.rows[0]?.name !== pkey ||
    constraints.rows[0]?.definition !== "PRIMARY KEY (id)"
  ) {
    problems.push(`expected only constraint ${pkey} PRIMARY KEY (id)`);
  }

  const extensionSchemas = (
    await sql<{ q: string }>`
      SELECT DISTINCT quote_ident(n.nspname) AS q FROM pg_extension e
      JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm'
    `.execute(db)
  ).rows.map((r) => r.q);
  const actualIndexes = await sql<{ name: string; definition: string; valid: boolean }>`
    SELECT ic.relname AS name, pg_get_indexdef(i.indexrelid) AS definition, i.indisvalid AS valid
    FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_class ic ON ic.oid = i.indexrelid
    WHERE n.nspname = ${schema} AND c.relname = ${spec.name}
  `.execute(db);
  for (const index of actualIndexes.rows) {
    const expected = spec.indexes[index.name];
    if (!expected) {
      problems.push(`unexpected index ${index.name}`);
    } else if (
      !index.valid ||
      normalizeIndexDef(index.definition, quoted, extensionSchemas) !== normalizeIndexDef(expected, "{schema}")
    ) {
      problems.push(`index ${index.name} differs: ${index.definition}`);
    }
  }
  if (problems.length) {
    throw new HubError("bad_request.validation_failure", `Incompatible ${spec.name} table: ${problems.join("; ")}`);
  }

  for (const [name, definition] of Object.entries(spec.indexes)) {
    if (actualIndexes.rows.some((existing) => existing.name === name)) continue;
    if (name === pkey) throw new HubError("bad_request.validation_failure", `Missing ${pkey} index`);
    const ddl = definition
      .replace(" ON {schema}.", ` ON ${quoted}.`)
      .replaceAll(" gin_trgm_ops", ` ${quotedTrgm}.gin_trgm_ops`);
    await sql.raw(ddl).execute(db);
  }
  return outcome;
}
