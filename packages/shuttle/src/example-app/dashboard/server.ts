import { readFile } from "node:fs/promises";
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { isIP } from "node:net";
import { Queue } from "bullmq";
import { getDbClient } from "../../shuttle";
import { getRedisClient } from "../../shuttle/redis";
import { type AppDb, pendingMigrations } from "../db";
import { POSTGRES_SCHEMA, POSTGRES_URL, REDIS_URL } from "../env";
import { log } from "../log";
import { ProjectionDispatcher, projections } from "../projections";
import { finishRun, runRebuild } from "../projections/runs";
import { enqueueBackfill } from "../worker";
import { DashboardContext, getStatus, IssueFilter, listIssues, listRuns } from "./status";

const MAX_FIDS = 10_000;
const EXPORT_LIMIT = 10_000;
const ASSETS: Record<string, string> = {
  "/": "index.html",
  "/index.html": "index.html",
  "/app.js": "app.js",
  "/styles.css": "styles.css",
};
const TYPES: Record<string, string> = {
  html: "text/html; charset=utf-8",
  js: "text/javascript; charset=utf-8",
  css: "text/css; charset=utf-8",
};

export class HttpError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
  }
}

/** Only loopback addresses: the dashboard can start rebuilds/backfills and has no auth. */
export function parseLoopbackAddr(addr: string): { host: string; port: number } {
  const index = addr.lastIndexOf(":");
  const host = addr.slice(0, index).replace(/^\[|\]$/g, "") || "127.0.0.1";
  const port = Number(addr.slice(index + 1));
  if (!Number.isInteger(port) || port <= 0 || port > 65535) throw new Error(`无效端口：${addr}`);
  const loopback = host === "localhost" || host === "::1" || (isIP(host) === 4 && host.startsWith("127."));
  if (!loopback) throw new Error(`只允许监听本机地址（127.0.0.1 / ::1 / localhost），收到 ${host}`);
  return { host, port };
}

export function parseFids(value: unknown): number[] {
  if (value === undefined || value === null || value === "") return [];
  const list = Array.isArray(value) ? value : String(value).split(/[\s,]+/).filter(Boolean);
  const fids = list.map((v) => Number(v));
  if (fids.some((fid) => !Number.isSafeInteger(fid) || fid <= 0)) throw new HttpError(400, "FID 必须是正整数");
  if (fids.length > MAX_FIDS) throw new HttpError(400, `一次最多 ${MAX_FIDS} 个 FID`);
  return [...new Set(fids)].sort((a, b) => a - b);
}

function optionalInt(value: string | null, name: string): number | undefined {
  if (value === null || value === "") return undefined;
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < 0) throw new HttpError(400, `${name} 必须是非负整数`);
  return n;
}

function send(res: ServerResponse, status: number, body: unknown, type = "application/json; charset=utf-8") {
  res.writeHead(status, { "Content-Type": type, "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff" });
  res.end(typeof body === "string" || Buffer.isBuffer(body) ? body : JSON.stringify(body));
}

async function readJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  // A JSON content type can't be sent by a cross-site form, so this doubles as CSRF protection.
  if (!String(req.headers["content-type"] ?? "").startsWith("application/json")) {
    throw new HttpError(415, "需要 application/json");
  }
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > 64 * 1024) throw new HttpError(413, "请求过大");
    chunks.push(chunk as Buffer);
  }
  if (!size) return {};
  try {
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error();
    return body;
  } catch {
    throw new HttpError(400, "请求体不是 JSON 对象");
  }
}

function csvCell(value: unknown): string {
  const text =
    value === null || value === undefined ? "" : typeof value === "object" ? JSON.stringify(value) : String(value);
  // Neutralise spreadsheet formulas, then quote.
  const safe = /^[=+\-@\t\r]/.test(text) ? `'${text}` : text;
  return `"${safe.replaceAll('"', '""')}"`;
}

export class Dashboard {
  private rebuild: { runId: string; promise: Promise<unknown> } | null = null;
  private readonly dispatcher: ProjectionDispatcher;

  constructor(private readonly ctx: DashboardContext) {
    this.dispatcher = new ProjectionDispatcher(projections, ctx.schema);
  }

  private filter(url: URL): IssueFilter {
    const projection = url.searchParams.get("projection") || undefined;
    if (projection && !this.ctx.projections.some((p) => p.name === projection)) {
      // Unregistered names are allowed only if they appear in issues; they're just a filter value.
      if (!/^[a-z0-9_]{1,64}$/.test(projection)) throw new HttpError(400, "无效的投影名");
    }
    const reason = url.searchParams.get("reason") || undefined;
    if (reason && reason.length > 500) throw new HttpError(400, "原因过长");
    return {
      projection,
      reason,
      fid: optionalInt(url.searchParams.get("fid"), "fid"),
      offset: optionalInt(url.searchParams.get("offset"), "offset"),
      limit: optionalInt(url.searchParams.get("limit"), "limit"),
    };
  }

  private async requireRunsTable() {
    const pending = await pendingMigrations(this.ctx.db as never, this.ctx.schema, log).catch((error: Error) => {
      throw new HttpError(503, `读取迁移状态失败：${error.message}`);
    });
    if (pending.length) {
      throw new HttpError(409, `数据库有未执行的迁移（${pending.join(", ")}），先部署 shard`);
    }
  }

  private async startRebuild(body: Record<string, unknown>) {
    await this.requireRunsTable();
    if (this.rebuild) throw new HttpError(409, `本页面已有重建在运行（${this.rebuild.runId}）`);
    const names = Array.isArray(body["projections"]) ? body["projections"].map(String) : [];
    let selected: string[];
    try {
      selected = this.dispatcher.names(names);
    } catch (error) {
      throw new HttpError(400, (error as Error).message);
    }
    const fids = parseFids(body["fids"]);
    let resolveStarted: (id: string) => void = () => {};
    const started = new Promise<string>((resolve) => {
      resolveStarted = resolve;
    });
    const promise = runRebuild(this.ctx.db as never, this.dispatcher, {
      projections: selected,
      fids,
      trigger: "dashboard",
      onStarted: resolveStarted,
    });
    // Whichever comes first: the run row exists, or it was refused before starting.
    const runId = await Promise.race([started, promise.then((r) => r.runId)]).catch((error: Error) => {
      throw new HttpError(409, error.message);
    });
    this.rebuild = { runId, promise };
    promise
      .then((r) => log.info({ runId, rebuilt: r.rebuilt, failed: r.failed.length }, "Dashboard rebuild finished"))
      .catch((error) => log.error({ runId, error: String(error) }, "Dashboard rebuild failed"))
      .finally(() => {
        this.rebuild = null;
      });
    return { runId };
  }

  private async startBackfill(body: Record<string, unknown>) {
    await this.requireRunsTable();
    const queue = this.ctx.queue;
    if (!queue) throw new HttpError(409, "没有配置 REDIS_URL，无法排队 backfill");
    const workers = await queue.getWorkers().catch((error: Error) => {
      throw new HttpError(503, `连接 Redis 失败：${error.message}`);
    });
    if (!workers.length) {
      throw new HttpError(409, "backfill worker 不在线：先在服务器执行 docker compose --profile worker up -d");
    }
    const fids = parseFids(body["fids"]);
    let maxFid: number | undefined;
    if (!fids.length) {
      maxFid = Number(body["maxFid"]);
      if (!Number.isSafeInteger(maxFid) || maxFid <= 0) throw new HttpError(400, "不指定 FID 时需要 FID 上限");
    }
    // The dashboard only ever queues projection-only backfills (a full raw backfill widens the gap
    // of raw rows that are never dispatched to later projections; see README).
    return enqueueBackfill(this.ctx.db, queue, {
      fids,
      maxFid,
      projectionsOnly: true,
      projections: this.ctx.projections.map((p) => p.name),
      trigger: "dashboard",
    });
  }

  private async exportIssues(url: URL, res: ServerResponse) {
    const filter = { ...this.filter(url), offset: 0, limit: EXPORT_LIMIT };
    const page = await listIssues(this.ctx.db, filter, EXPORT_LIMIT);
    const header = ["created_at", "projection", "fid", "hash", "reason", "payload"];
    const lines = [header.join(",")];
    for (const issue of page.items) {
      lines.push(
        [
          issue.createdAt?.toISOString?.() ?? issue.createdAt,
          issue.projection,
          issue.fid,
          issue.hash,
          issue.reason,
          issue.payload,
        ]
          .map(csvCell)
          .join(","),
      );
    }
    res.writeHead(200, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="projection-issues.csv"`,
      "Cache-Control": "no-store",
    });
    res.end(`﻿${lines.join("\n")}\n`);
  }

  async handle(req: IncomingMessage, res: ServerResponse) {
    const url = new URL(req.url ?? "/", "http://localhost");
    // Reject DNS-rebinding requests: the Host must be a loopback name.
    const host = String(req.headers.host ?? "")
      .replace(/:\d+$/, "")
      .replace(/^\[|\]$/g, "");
    if (!["127.0.0.1", "localhost", "::1"].includes(host)) return send(res, 403, { error: "forbidden host" });

    if (req.method === "GET" && ASSETS[url.pathname]) {
      const file = ASSETS[url.pathname] as string;
      const body = await readFile(new URL(`./assets/${file}`, import.meta.url));
      return send(res, 200, body, TYPES[file.split(".").pop() ?? ""] ?? "application/octet-stream");
    }
    const route = `${req.method} ${url.pathname}`;
    switch (route) {
      case "GET /api/status":
        return send(res, 200, {
          ...(await getStatus(this.ctx)),
          dashboard: { rebuild: this.rebuild?.runId ?? null },
        });
      case "GET /api/issues":
        return send(res, 200, await listIssues(this.ctx.db, this.filter(url)));
      case "GET /api/issues/export":
        return this.exportIssues(url, res);
      case "GET /api/runs":
        return send(
          res,
          200,
          await listRuns(this.ctx.db, {
            projection: url.searchParams.get("projection") || undefined,
            limit: optionalInt(url.searchParams.get("limit"), "limit"),
          }),
        );
      case "POST /api/rebuild":
        return send(res, 202, await this.startRebuild(await readJson(req)));
      case "POST /api/backfill":
        return send(res, 202, await this.startBackfill(await readJson(req)));
      default:
        return send(res, 404, { error: "not found" });
    }
  }

  /** On shutdown, close a rebuild this process started so it isn't left `running`. */
  async abandon(reason: string) {
    if (!this.rebuild) return;
    await finishRun(this.ctx.db, this.rebuild.runId, { error: reason }).catch(() => undefined);
  }

  listener() {
    return (req: IncomingMessage, res: ServerResponse) => {
      this.handle(req, res).catch((error) => {
        const status = error instanceof HttpError ? error.status : 500;
        if (status === 500) log.error(error, "dashboard request failed");
        if (!res.headersSent) send(res, status, { error: error instanceof Error ? error.message : String(error) });
        else res.end();
      });
    };
  }
}

export async function startDashboard(options: { host: string; port: number; envFile: string }) {
  const db = getDbClient(POSTGRES_URL, POSTGRES_SCHEMA) as unknown as AppDb;
  let redis = null;
  let queue = null;
  if (process.env["REDIS_URL"]) {
    // Fail fast instead of queueing commands while Redis is unreachable.
    redis = getRedisClient(REDIS_URL, { maxRetriesPerRequest: 1, enableOfflineQueue: false, lazyConnect: false });
    redis.on("error", () => undefined);
    const queueConnection = getRedisClient(REDIS_URL, { enableOfflineQueue: false });
    queueConnection.on("error", () => undefined);
    queue = new Queue("default", {
      connection: queueConnection,
      defaultJobOptions: { attempts: 3, backoff: { delay: 1000, type: "exponential" } },
    });
    queue.on("error", () => undefined);
  }
  const dashboard = new Dashboard({ db, schema: POSTGRES_SCHEMA, projections, redis, queue });
  const server = createServer(dashboard.listener());
  await new Promise<void>((resolve, reject) => {
    server.once("error", (error: NodeJS.ErrnoException) =>
      reject(error.code === "EADDRINUSE" ? new Error(`端口 ${options.port} 已被占用，用 --addr 换一个`) : error),
    );
    server.listen(options.port, options.host, resolve);
  });
  const shown = options.host.includes(":") ? `[${options.host}]` : options.host;
  log.info(
    { envFile: options.envFile, schema: POSTGRES_SCHEMA },
    `Projection dashboard on http://${shown}:${options.port}`,
  );

  const shutdown = async () => {
    server.close();
    await dashboard.abandon("dashboard 进程退出，重建中断");
    await queue?.close().catch(() => undefined);
    redis?.disconnect();
    await db.destroy().catch(() => undefined);
    process.exit(0);
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
  return server;
}
