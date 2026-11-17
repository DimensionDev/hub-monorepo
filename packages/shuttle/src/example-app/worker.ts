import { Cluster, Redis } from "ioredis";
import { Job, Queue, Worker } from "bullmq";
import { App } from "./app";
import { pino } from "pino";
import type { AppDb } from "./db";
import { recordBackfillJob, startRun, type RunTrigger } from "./projections/runs";

const QUEUE_NAME = "default";

export function getWorker(app: App, redis: Redis | Cluster, log: pino.Logger, concurrency = 1) {
  const worker = new Worker(
    QUEUE_NAME,
    async (job: Job) => {
      if (job.name === "reconcile") {
        const start = Date.now();
        const fids = job.data.fids as number[];
        const projectionsOnly = job.data.projectionsOnly as boolean | undefined;
        await app.reconcileFids(fids, projectionsOnly);
        const elapsed = (Date.now() - start) / 1000;
        const lastFid = fids[fids.length - 1];
        log.info(`Reconciled ${fids.length} upto ${lastFid} in ${elapsed}s at ${new Date().toISOString()}`);
        if (job.data.runId) await recordBackfillJob(app.appDb, job.data.runId, "done", lastFid);
      } else if (job.name === "completionMarker") {
        // TODO: Update key in redis so event streaming can start
        const startedAt = new Date(job.data.startedAt as number);
        const duration = (Date.now() - startedAt.getTime()) / 1000 / 60;
        log.info(
          `Reconciliation started at ${startedAt.toISOString()} complete at ${new Date().toISOString()} ${duration} minutes`,
        );
      }
    },
    {
      autorun: false, // Don't start yet
      useWorkerThreads: concurrency > 1,
      concurrency,
      connection: redis,
      removeOnComplete: { count: 100 }, // Keep at most this many completed jobs
      removeOnFail: { count: 100 }, // Keep at most this many failed jobs
    },
  );
  // A job that exhausted its retries counts as failed for its backfill run.
  worker.on("failed", async (job) => {
    if (!job || job.name !== "reconcile" || !job.data.runId) return;
    if (job.attemptsMade < (job.opts.attempts ?? 1)) return;
    try {
      await recordBackfillJob(app.appDb, job.data.runId, "failed");
    } catch (error) {
      log.error(error, "Failed to record failed backfill job");
    }
  });

  return worker;
}

const BACKFILL_BATCH_SIZE = 10;

/**
 * Queue reconcile jobs (explicit `fids`, or every fid up to `maxFid` in batches) with a run record
 * that the worker updates per job. Used by the `backfill` command and the local dashboard.
 */
export async function enqueueBackfill(
  db: AppDb,
  queue: Queue,
  options: {
    fids: number[];
    maxFid?: number;
    projectionsOnly: boolean;
    projections: string[];
    trigger: RunTrigger;
  },
): Promise<{ runId: string; jobs: number }> {
  const batches: number[][] = [];
  if (options.fids.length) {
    batches.push(options.fids);
  } else {
    const maxFid = options.maxFid;
    if (!maxFid || !Number.isSafeInteger(maxFid) || maxFid <= 0) throw new Error("maxFid is required without fids");
    for (let start = 1; start <= maxFid; start += BACKFILL_BATCH_SIZE) {
      batches.push(Array.from({ length: BACKFILL_BATCH_SIZE }, (_, i) => start + i));
    }
  }
  const runId = await startRun(db, {
    kind: "backfill",
    projections: options.projections,
    fids: options.fids,
    trigger: options.trigger,
    total: batches.length,
  });
  const startedAt = Date.now();
  for (const fids of batches) {
    await queue.add("reconcile", { fids, runId, projectionsOnly: options.projectionsOnly });
  }
  await queue.add("completionMarker", { startedAt, runId });
  return { runId, jobs: batches.length };
}

export function getQueue(redis: Redis | Cluster) {
  return new Queue("default", {
    connection: redis,
    defaultJobOptions: { attempts: 3, backoff: { delay: 1000, type: "exponential" } },
  });
}
