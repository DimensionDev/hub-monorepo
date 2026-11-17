import { sql } from "kysely";
import type { DB } from "../../shuttle";
import type { AppDb } from "../db";
import { ProjectionDispatcher } from "./dispatcher";

export type RunKind = "rebuild" | "backfill";
export type RunTrigger = "cli" | "dashboard";

/** A `running` row whose heartbeat is older than this is shown as possibly interrupted. */
export const RUN_STALE_MS = 5 * 60 * 1000;
const MAX_FAILURE_DETAILS = 50;

export async function startRun(
  db: AppDb,
  run: { kind: RunKind; projections: string[]; fids?: number[]; trigger: RunTrigger; total?: number },
): Promise<string> {
  const row = await db
    .insertInto("shuttleProjectionRuns")
    .values({
      kind: run.kind,
      projections: run.projections,
      fids: run.fids?.length ? [...new Set(run.fids)].sort((a, b) => a - b) : null,
      trigger: run.trigger,
      status: "running",
      total: run.total ?? null,
    })
    .returning("id")
    .executeTakeFirstOrThrow();
  return row.id;
}

export async function progressRun(
  db: AppDb,
  id: string,
  progress: { done: number; failedCount: number; lastFid: number | null },
): Promise<void> {
  await db
    .updateTable("shuttleProjectionRuns")
    .set({ ...progress, updatedAt: sql`now()` })
    .where("id", "=", id)
    .execute();
}

export async function finishRun(
  db: AppDb,
  id: string,
  result: { done?: number; failedCount?: number; lastFid?: number | null; details?: unknown; error?: string | null },
): Promise<void> {
  const failed = (result.failedCount ?? 0) > 0 || !!result.error;
  await db
    .updateTable("shuttleProjectionRuns")
    .set({
      ...(result.done !== undefined ? { done: result.done } : {}),
      ...(result.failedCount !== undefined ? { failedCount: result.failedCount } : {}),
      ...(result.lastFid !== undefined ? { lastFid: result.lastFid } : {}),
      details: result.details === undefined ? null : JSON.stringify(result.details),
      error: result.error ?? null,
      status: failed ? "failed" : "succeeded",
      updatedAt: sql`now()`,
      finishedAt: sql`now()`,
    })
    .where("id", "=", id)
    .execute();
}

/** A run of this kind that is still running and has a fresh heartbeat. */
export async function activeRun(db: AppDb, kind: RunKind): Promise<{ id: string; startedAt: Date } | undefined> {
  return db
    .selectFrom("shuttleProjectionRuns")
    .select(["id", "startedAt"])
    .where("kind", "=", kind)
    .where("status", "=", "running")
    .where("updatedAt", ">", sql<Date>`now() - ${`${RUN_STALE_MS / 1000} seconds`}::interval`)
    .orderBy("startedAt", "desc")
    .executeTakeFirst();
}

/**
 * Backfill job accounting, called by the worker. Counts are incremented atomically because jobs
 * run concurrently; the run is closed once every queued job finished or exhausted its retries.
 */
export async function recordBackfillJob(db: AppDb, id: string, outcome: "done" | "failed", lastFid?: number) {
  const row = await db
    .updateTable("shuttleProjectionRuns")
    .set((eb) => ({
      ...(outcome === "done" ? { done: eb("done", "+", 1) } : { failedCount: eb("failedCount", "+", 1) }),
      ...(lastFid !== undefined ? { lastFid: sql`GREATEST(coalesce(last_fid, 0), ${lastFid})` } : {}),
      updatedAt: sql`now()`,
    }))
    .where("id", "=", id)
    .where("kind", "=", "backfill")
    .returning(["done", "failedCount", "total", "status"])
    .executeTakeFirst();
  if (!row || row.total === null || row.status !== "running") return;
  if (row.done + row.failedCount >= row.total) {
    await db
      .updateTable("shuttleProjectionRuns")
      .set({
        status: row.failedCount > 0 ? "failed" : "succeeded",
        error: row.failedCount > 0 ? `${row.failedCount} reconcile job(s) failed after retries` : null,
        finishedAt: sql`now()`,
      })
      .where("id", "=", id)
      .where("status", "=", "running")
      .execute();
  }
}

/**
 * `rebuild` with a run record: recompute projections from raw rows already in the DB.
 * Shared by the CLI command and the dashboard. Refuses to start while another rebuild is live.
 */
export async function runRebuild(
  db: DB,
  dispatcher: ProjectionDispatcher,
  options: {
    projections: string[];
    fids: number[];
    trigger: RunTrigger;
    onFid?: (projection: string, fid: number, count: number) => void;
    onStarted?: (runId: string) => void;
  },
): Promise<{ runId: string; rebuilt: number; failed: { projection: string; fid: number; error: string }[] }> {
  const appDb = db as unknown as AppDb;
  const names = dispatcher.names(options.projections);
  const running = await activeRun(appDb, "rebuild");
  if (running) throw new Error(`Rebuild ${running.id} is already running`);
  const runId = await startRun(appDb, {
    kind: "rebuild",
    projections: names,
    fids: options.fids,
    trigger: options.trigger,
  });
  options.onStarted?.(runId);
  let count = 0;
  let failedCount = 0;
  let lastFid: number | null = null;
  let lastBeat = Date.now();
  try {
    const { rebuilt, failed } = await dispatcher.rebuild(db, {
      projections: names,
      fids: options.fids,
      onFid: async (projection, fid) => {
        count++;
        lastFid = fid;
        options.onFid?.(projection, fid, count);
        // Heartbeat every few seconds, after the fid's transaction committed.
        if (Date.now() - lastBeat < 3000) return;
        lastBeat = Date.now();
        await progressRun(appDb, runId, { done: count, failedCount, lastFid });
      },
      onFailure: () => {
        failedCount++;
      },
    });
    await finishRun(appDb, runId, {
      done: rebuilt,
      failedCount: failed.length,
      lastFid,
      details: failed.length ? failed.slice(0, MAX_FAILURE_DETAILS) : undefined,
      error: failed.length ? `${failed.length} fid(s) failed` : null,
    });
    return { runId, rebuilt, failed };
  } catch (error) {
    await finishRun(appDb, runId, {
      done: count,
      failedCount,
      lastFid,
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
