import type { Queue } from "bullmq";
import type { Cluster, Redis } from "ioredis";
import { sql } from "kysely";
import { bytesToHex } from "../../utils";
import type { AppDb } from "../db";
import { pendingMigrations } from "../db";
import { log } from "../log";
import { RUN_STALE_MS } from "../projections/runs";
import type { Projection } from "../projections/types";

/** A section either has data or the error that prevented reading it; never a silent zero. */
export type Section<T> = { ok: true; data: T } | { ok: false; error: string };

async function section<T>(read: () => Promise<T>): Promise<Section<T>> {
  try {
    return { ok: true, data: await read() };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}

export type DashboardContext = {
  db: AppDb;
  schema: string;
  projections: Projection[];
  redis: Redis | Cluster | null;
  queue: Queue | null;
};

const num = (value: unknown) => (value === null || value === undefined ? null : Number(value));

export type RunRow = {
  id: string;
  kind: string;
  projections: string[];
  fids: number[] | null;
  trigger: string;
  status: string;
  stale: boolean;
  total: number | null;
  done: number;
  failedCount: number;
  lastFid: number | null;
  details: unknown;
  error: string | null;
  startedAt: Date;
  updatedAt: Date;
  finishedAt: Date | null;
};

function runRow(row: Record<string, unknown>): RunRow {
  const status = String(row["status"]);
  const updatedAt = row["updatedAt"] as Date;
  return {
    id: String(row["id"]),
    kind: String(row["kind"]),
    projections: (row["projections"] as string[]) ?? [],
    fids: (row["fids"] as string[] | null)?.map(Number) ?? null,
    trigger: String(row["trigger"]),
    status,
    stale: status === "running" && Date.now() - updatedAt.getTime() > RUN_STALE_MS,
    total: num(row["total"]),
    done: Number(row["done"]),
    failedCount: Number(row["failedCount"]),
    lastFid: num(row["lastFid"]),
    details: row["details"] ?? null,
    error: (row["error"] as string | null) ?? null,
    startedAt: row["startedAt"] as Date,
    updatedAt,
    finishedAt: (row["finishedAt"] as Date | null) ?? null,
  };
}

export async function listRuns(db: AppDb, options: { projection?: string; limit?: number } = {}): Promise<RunRow[]> {
  let query = db
    .selectFrom("shuttleProjectionRuns")
    .selectAll()
    .orderBy("startedAt", "desc")
    .limit(Math.min(options.limit ?? 50, 200));
  if (options.projection) {
    // A full raw backfill (no projections listed) touches every projection.
    query = query.where(
      sql<boolean>`(${options.projection} = ANY(projections) OR cardinality(projections) = 0)`,
    ) as typeof query;
  }
  return (await query.execute()).map((row) => runRow(row as unknown as Record<string, unknown>));
}

export type IssueFilter = { projection?: string; reason?: string; fid?: number; offset?: number; limit?: number };

export async function listIssues(db: AppDb, filter: IssueFilter, maxLimit = 200) {
  let query = db.selectFrom("shuttleProjectionIssues");
  if (filter.projection) query = query.where("projection", "=", filter.projection);
  if (filter.reason) query = query.where("reason", "=", filter.reason);
  if (filter.fid !== undefined) query = query.where("fid", "=", filter.fid);
  const limit = Math.max(1, Math.min(filter.limit ?? 20, maxLimit));
  const offset = Math.max(0, filter.offset ?? 0);
  const [rows, count] = await Promise.all([
    query
      .select(["id", "projection", "fid", "hash", "reason", "payload", "createdAt"])
      .orderBy("createdAt", "desc")
      .orderBy("id")
      .limit(limit)
      .offset(offset)
      .execute(),
    query.select(sql<string>`count(*)`.as("count")).executeTakeFirst(),
  ]);
  return {
    total: Number(count?.count ?? 0),
    offset,
    limit,
    items: rows.map((row) => ({
      id: row.id,
      projection: row.projection,
      fid: num(row.fid),
      hash: bytesToHex(row.hash),
      reason: row.reason,
      payload: row.payload,
      createdAt: row.createdAt,
    })),
  };
}

async function issueSummary(db: AppDb) {
  const rows = await db
    .selectFrom("shuttleProjectionIssues")
    .select([
      "projection",
      "reason",
      sql<string>`count(*)`.as("count"),
      sql<string>`count(*) FILTER (WHERE created_at > now() - interval '24 hours')`.as("last24h"),
      sql<Date>`max(created_at)`.as("lastAt"),
    ])
    .groupBy(["projection", "reason"])
    .orderBy(sql`count(*)`, "desc")
    .execute();
  return rows.map((row) => ({
    projection: row.projection,
    reason: row.reason,
    count: Number(row.count),
    last24h: Number(row.last24h),
    lastAt: row.lastAt,
  }));
}

async function migrations(db: AppDb, schema: string) {
  const executed = await db
    .selectFrom("kyselyMigration" as never)
    .select(["name" as never, "timestamp" as never])
    .orderBy("name" as never)
    .execute()
    .catch(() => [] as { name: string; timestamp: string }[]);
  const pending = await pendingMigrations(db as never, schema, log);
  return {
    executed: (executed as { name: string; timestamp: string }[]).map((m) => ({ name: m.name, at: m.timestamp })),
    pending,
    runsTable: !pending.includes("007_projection_runs"),
  };
}

function fields(entry: unknown[]): Map<string, unknown> {
  const map = new Map<string, unknown>();
  for (let i = 0; i + 1 < entry.length; i += 2) map.set(String(entry[i]), entry[i + 1]);
  return map;
}

async function scanKeys(redis: Redis | Cluster, pattern: string): Promise<string[]> {
  const keys = new Set<string>();
  let cursor = "0";
  do {
    const [next, batch] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 1000);
    for (const key of batch) keys.add(key);
    cursor = next;
  } while (cursor !== "0");
  return [...keys].sort();
}

// SCAN walks the whole keyspace, so stream discovery is cached; the per-stream numbers are not.
const STREAM_KEYS_TTL_MS = 5 * 60 * 1000;
let streamKeysCache: { at: number; keys: string[] } | null = null;

async function streamKeys(redis: Redis | Cluster): Promise<string[]> {
  if (streamKeysCache && Date.now() - streamKeysCache.at < STREAM_KEYS_TTL_MS) return streamKeysCache.keys;
  const keys = (await scanKeys(redis, "hub:*:evt:msg:*")).filter((key) => /:evt:msg:[^:]+$/.test(key));
  streamKeysCache = { at: Date.now(), keys };
  return keys;
}

/** Hub event streams (`hub:<host>:evt:msg:<shard>`), their consumer groups and dead-letter queues. */
async function streams(redis: Redis | Cluster) {
  const keys = await streamKeys(redis);
  const result = [];
  for (const key of keys) {
    if ((await redis.type(key)) !== "stream") {
      streamKeysCache = null;
      continue;
    }
    const match = /^hub:(.+):evt:msg:([^:]+)$/.exec(key);
    const host = match?.[1] ?? "";
    const shard = match?.[2] ?? "";
    const groups = ((await redis.xinfo("GROUPS", key)) as unknown[][]).map((entry) => {
      const f = fields(entry);
      return {
        name: String(f.get("name")),
        consumers: num(f.get("consumers")),
        pending: num(f.get("pending")),
        lag: num(f.get("lag")),
        lastDeliveredId: f.get("last-delivered-id") ? String(f.get("last-delivered-id")) : null,
      };
    });
    const dlqKey = `${key}:dlq`;
    const [length, dlq, lastEventId] = await Promise.all([
      redis.xlen(key),
      redis.xlen(dlqKey),
      redis.get(`hub:${host}:${shard}:last-hub-event-id`),
    ]);
    result.push({ key, host, shard, length, groups, dlqKey, dlq, lastEventId: num(lastEventId) });
  }
  return result;
}

async function backfillQueue(queue: Queue) {
  const [workers, counts] = await Promise.all([
    queue.getWorkers(),
    queue.getJobCounts("waiting", "active", "delayed", "failed", "completed", "paused"),
  ]);
  return { workers: workers.length, counts };
}

/** Upper bound for a full backfill when the hub isn't reachable from here: highest registered fid. */
export async function suggestedMaxFid(db: AppDb): Promise<number | null> {
  const row = await db
    .selectFrom("onchain_events")
    .select(sql<string | null>`max(fid)`.as("maxFid"))
    .executeTakeFirst();
  return num(row?.maxFid);
}

export async function getStatus(ctx: DashboardContext) {
  const { db } = ctx;
  const [migrationState, issues, runs, redisStreams, queue, maxFid] = await Promise.all([
    section(() => migrations(db, ctx.schema)),
    section(() => issueSummary(db)),
    section(() => listRuns(db, { limit: 100 })),
    ctx.redis
      ? section(() => streams(ctx.redis as Redis | Cluster))
      : Promise.resolve<Section<never>>({ ok: false, error: "未配置 REDIS_URL" }),
    ctx.queue
      ? section(() => backfillQueue(ctx.queue as Queue))
      : Promise.resolve<Section<never>>({ ok: false, error: "未配置 REDIS_URL" }),
    section(() => suggestedMaxFid(db)),
  ]);

  const projections = await Promise.all(
    ctx.projections.map(async (projection) => {
      const own = issues.ok ? issues.data.filter((i) => i.projection === projection.name) : [];
      const ownRuns = runs.ok
        ? runs.data.filter((r) => r.projections.length === 0 || r.projections.includes(projection.name))
        : [];
      return {
        name: projection.name,
        table: projection.table ?? null,
        reconcileTypes: projection.reconcileTypes,
        stats: projection.stats ? await section(() => projection.stats?.(db) ?? Promise.resolve({})) : null,
        issues: issues.ok
          ? {
              ok: true as const,
              data: {
                total: own.reduce((sum, i) => sum + i.count, 0),
                last24h: own.reduce((sum, i) => sum + i.last24h, 0),
                byReason: own,
              },
            }
          : issues,
        lastRebuild: ownRuns.find((r) => r.kind === "rebuild") ?? null,
        lastBackfill: ownRuns.find((r) => r.kind === "backfill") ?? null,
      };
    }),
  );

  // Issues recorded under a name that is no longer registered still deserve a row.
  const registered = new Set(ctx.projections.map((p) => p.name));
  const orphanIssues = issues.ok ? issues.data.filter((i) => !registered.has(i.projection)) : [];

  return {
    generatedAt: new Date(),
    schema: ctx.schema,
    migrations: migrationState,
    streams: redisStreams,
    queue,
    maxFid,
    projections,
    orphanIssues,
    runs: runs.ok ? { ok: true as const, data: runs.data.slice(0, 20) } : runs,
  };
}
