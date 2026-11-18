import { randomUUID } from "node:crypto";
import pg from "pg";
import { sql } from "kysely";
import {
  Factories,
  FarcasterNetwork,
  HubEvent,
  HubEventType,
  HubRpcClient,
  Message,
  MessagesResponse,
  MessageType,
} from "@dimensiondev/hub-nodejs";
import { ok } from "neverthrow";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import { backfillFidForProjections, ProjectionDispatcher, projections } from "./example-app/projections";
import { MessageReconciliation } from "./shuttle/messageReconciliation";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";
import { dropTestDatabase } from "./testDatabase";
import { MessageProcessor } from "./shuttle/messageProcessor";
import type { AppDb } from "./example-app/db";
import { recordBackfillJob, runRebuild } from "./example-app/projections/runs";
import { enqueueBackfill } from "./example-app/worker";
import { getStatus, listIssues, listRuns } from "./example-app/dashboard/status";
import { messageSubject, recordIssue } from "./example-app/projections/issues";

// Never fall back to the application's production connection or dotenv configuration.
const testUrl = process.env["TEST_DATABASE_URL"];
const integration = testUrl ? describe : describe.skip;

integration("projection framework", () => {
  const database = `projections_test_${randomUUID().replaceAll("-", "")}`;
  let admin: pg.Client;
  let db: DB;

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: testUrl });
    await admin.connect();
    await admin.query(`CREATE DATABASE "${database}"`);
    if (!testUrl) throw new Error("TEST_DATABASE_URL is required for integration tests");
    const url = new URL(testUrl);
    url.pathname = `/${database}`;
    db = getDbClient(url.toString(), "public");
    const result = await migrateToLatest(db, "public", log);
    if (result.isErr()) throw result.error;
  });

  afterAll(async () => {
    await db?.destroy();
    if (admin) {
      await dropTestDatabase(admin, database);
      await admin.end();
    }
  });

  beforeEach(async () => {
    await sql`TRUNCATE public.messages, public.blocks, public.shuttle_message_state,
      public.shuttle_projection_issues, public.onchain_events, public.shuttle_projection_runs`.execute(db);
  });

  const handler = { onHubEvent: App.prototype.onHubEvent, handleMessageMerge: App.prototype.handleMessageMerge };
  const dispatcher = new ProjectionDispatcher(projections, "public");

  const link = (target: number, timestamp: number, hash: number, type = MessageType.LINK_ADD) =>
    Message.create({
      data: {
        fid: 100,
        type,
        timestamp,
        network: FarcasterNetwork.MAINNET,
        linkBody: { type: "block", targetFid: target },
      },
      hash: Buffer.alloc(20, hash),
    });
  const mergeEvent = (id: number, message: Message) =>
    HubEvent.create({
      id,
      shardIndex: 1,
      timestamp: 2000 + id,
      type: HubEventType.MERGE_MESSAGE,
      mergeMessageBody: { message, deletedMessages: [] },
    });
  const revokeEvent = (id: number, message: Message) =>
    HubEvent.create({
      id,
      shardIndex: 1,
      timestamp: 2000 + id,
      type: HubEventType.REVOKE_MESSAGE,
      revokeMessageBody: { message },
    });
  const state = async () =>
    (
      await sql<{ blockedFid: number; timestamp: Date; deletedAt: Date | null }>`
        SELECT blocked_fid, timestamp AT TIME ZONE 'UTC' AS timestamp, deleted_at AT TIME ZONE 'UTC' AS deleted_at
        FROM public.blocks ORDER BY blocked_fid`.execute(db)
    ).rows;

  const history = [
    link(200, 1000, 1),
    link(200, 1100, 2, MessageType.LINK_REMOVE),
    link(300, 1050, 3),
    link(400, 1200, 4),
  ];

  // Active relationships (and their timestamps) are order independent. Tombstones are not: per the
  // original rule "inactive and never existed -> no row", a REMOVE that arrives before its ADD
  // leaves no soft-deleted row, while in-order delivery leaves one. Consumers read deleted_at IS NULL.
  test("in-order, reversed, concurrent and backfilled delivery converge to the same active blocks", async () => {
    const results = [];

    for (const [i, message] of history.entries())
      await HubEventProcessor.processHubEvent(db, mergeEvent(i + 1, message), handler);
    results.push(await state());

    await sql`TRUNCATE public.messages, public.blocks, public.shuttle_message_state`.execute(db);
    for (const [i, message] of [...history].reverse().entries())
      await HubEventProcessor.processHubEvent(db, mergeEvent(i + 1, message), handler);
    results.push(await state());

    await sql`TRUNCATE public.messages, public.blocks, public.shuttle_message_state`.execute(db);
    await Promise.all(
      history.map((message, i) => HubEventProcessor.processHubEvent(db, mergeEvent(i + 1, message), handler)),
    );
    results.push(await state());

    await sql`TRUNCATE public.messages, public.blocks, public.shuttle_message_state`.execute(db);
    for (const message of history) expect(await dispatcher.processMissingMessage(db, message)).toBe(true);
    results.push(await state());

    expect(results[0]?.map((r) => [r.blockedFid, r.deletedAt === null])).toEqual([
      [200, false],
      [300, true],
      [400, true],
    ]);
    const activeRows = (rows: Awaited<ReturnType<typeof state>>) => rows.filter((r) => r.deletedAt === null);
    for (const result of results.slice(1)) {
      expect(activeRows(result)).toEqual(activeRows(results[0] ?? []));
      expect(result.find((r) => r.blockedFid === 200)?.deletedAt ?? "absent").not.toBeNull();
    }
  });

  test("backfill never revives a revoked message or duplicates an existing one", async () => {
    const add = link(200, 1000, 1);
    await HubEventProcessor.processHubEvent(db, mergeEvent(1, add), handler);
    await HubEventProcessor.processHubEvent(db, revokeEvent(2, add), handler);
    const revoked = await state();
    expect(revoked[0]?.deletedAt).not.toBeNull();

    await dispatcher.processMissingMessage(db, add);
    expect(await state()).toEqual(revoked);
    const stored = await db.selectFrom("messages").select(["revokedAt"]).execute();
    expect(stored).toHaveLength(1);
    expect(stored[0]?.revokedAt).not.toBeNull();
  });

  test("a later hub event still applies on top of a backfilled message", async () => {
    const add = link(200, 1000, 1);
    await dispatcher.processMissingMessage(db, add);
    expect((await state())[0]?.deletedAt).toBeNull();
    await HubEventProcessor.processHubEvent(db, revokeEvent(5, add), handler);
    expect((await state())[0]?.deletedAt).not.toBeNull();
  });

  test("projection-only backfill fetches link families, projects blocks once and skips follows", async () => {
    const follow = Message.create({
      data: {
        fid: 100,
        type: MessageType.LINK_ADD,
        timestamp: 1000,
        network: FarcasterNetwork.MAINNET,
        linkBody: { type: "follow", targetFid: 999 },
      },
      hash: Buffer.alloc(20, 9),
    });
    const compactState = Message.create({
      data: {
        fid: 100,
        type: MessageType.LINK_COMPACT_STATE,
        timestamp: 1300,
        network: FarcasterNetwork.MAINNET,
        linkCompactStateBody: { type: "block", targetFids: [400, 500] },
      },
      hash: Buffer.alloc(20, 8),
    });
    const calls: string[] = [];
    const hub = {
      getAllLinkMessagesByFid: async () => {
        calls.push("links");
        return ok(MessagesResponse.create({ messages: [...history, follow] }));
      },
      getLinkCompactStateMessageByFid: async () => {
        calls.push("compact");
        return ok(MessagesResponse.create({ messages: [compactState] }));
      },
    };
    const reconciler = new MessageReconciliation(hub as unknown as HubRpcClient, db, log);

    const first = await backfillFidForProjections(reconciler, db, dispatcher, projections, 100);
    expect(calls).toEqual(["links", "compact"]);
    expect(first).toEqual({ missing: 6, projected: 5 });
    expect((await state()).filter((r) => r.deletedAt === null).map((r) => r.blockedFid)).toEqual([400, 500]);
    const stored = await db.selectFrom("messages").select("hash").execute();
    expect(stored).toHaveLength(5);
    expect(stored.some((m) => Buffer.from(m.hash).equals(follow.hash))).toBe(false);

    const before = await state();
    const second = await backfillFidForProjections(reconciler, db, dispatcher, projections, 100);
    expect(second).toEqual({ missing: 1, projected: 0 });
    expect(await state()).toEqual(before);
  });

  test("rebuild projects raw messages stored before the projection existed", async () => {
    // Simulate rows written by the generic path (or an older image) with no projection run.
    for (const message of history) await MessageProcessor.storeMessage(message, db, "merge", undefined, false);
    const otherUser = Message.create({
      data: {
        fid: 101,
        type: MessageType.LINK_ADD,
        timestamp: 1000,
        network: FarcasterNetwork.MAINNET,
        linkBody: { type: "block", targetFid: 700 },
      },
      hash: Buffer.alloc(20, 7),
    });
    await MessageProcessor.storeMessage(otherUser, db, "merge", undefined, false);
    // A stale active row the stored messages say was removed.
    await sql`INSERT INTO public.blocks (id, created_at, updated_at, timestamp, blocker_fid, blocked_fid)
      VALUES (gen_random_uuid(), now(), now(), now(), 100, 200)`.execute(db);
    expect(await state()).toHaveLength(1);

    const fids: number[] = [];
    const result = await dispatcher.rebuild(db, { pageSize: 1, onFid: (_p, fid) => fids.push(fid) });
    expect(result).toEqual({ rebuilt: 2, failed: [] });
    expect(fids).toEqual([100, 101]);
    const rebuilt = await state();
    expect(rebuilt.filter((r) => r.deletedAt === null).map((r) => r.blockedFid)).toEqual([300, 400, 700]);
    expect(rebuilt.find((r) => r.blockedFid === 200)?.deletedAt).not.toBeNull();
    // Rebuild reads raw rows; it never writes lifecycle state or messages.
    expect(
      await db
        .selectFrom("shuttleMessageState" as never)
        .selectAll()
        .execute(),
    ).toHaveLength(0);

    // Idempotent: nothing changes, including updated_at.
    const before = (await sql`SELECT id, updated_at FROM public.blocks ORDER BY id`.execute(db)).rows;
    expect(await dispatcher.rebuild(db, { projections: ["blocks"], fids: [101, 100] })).toEqual({
      rebuilt: 2,
      failed: [],
    });
    expect((await sql`SELECT id, updated_at FROM public.blocks ORDER BY id`.execute(db)).rows).toEqual(before);

    await expect(dispatcher.rebuild(db, { projections: ["casts"] })).rejects.toThrow("Unknown projections: casts");
  });

  test("rebuild reports a fid whose stored data is invalid and continues", async () => {
    const foreign = Message.create({
      data: {
        fid: 100,
        type: MessageType.LINK_ADD,
        timestamp: 1000,
        network: FarcasterNetwork.TESTNET,
        linkBody: { type: "block", targetFid: 200 },
      },
      hash: Buffer.alloc(20, 5),
    });
    await MessageProcessor.storeMessage(foreign, db, "merge", undefined, false);
    await MessageProcessor.storeMessage(
      Message.create({
        data: {
          fid: 101,
          type: MessageType.LINK_ADD,
          timestamp: 1000,
          network: FarcasterNetwork.MAINNET,
          linkBody: { type: "block", targetFid: 700 },
        },
        hash: Buffer.alloc(20, 6),
      }),
      db,
      "merge",
      undefined,
      false,
    );
    const result = await dispatcher.rebuild(db);
    expect(result.rebuilt).toBe(1);
    expect(result.failed.map((f) => [f.projection, f.fid])).toEqual([["blocks", 100]]);
    expect((await state()).map((r) => r.blockedFid)).toEqual([700]);
  });

  test("messages no projection cares about fall back to the generic path", async () => {
    const cast = await Factories.CastAddMessage.create();
    expect(await dispatcher.processMissingMessage(db, cast)).toBe(false);
    expect(await dispatcher.processEvent(mergeEvent(1, cast), db)).toBe(false);
  });

  test("replayed on-chain events are stored once", async () => {
    const onChainEvent = Factories.StorageRentOnChainEvent.build({ blockNumber: 77, logIndex: 3 });
    const event = HubEvent.create({
      id: 9,
      type: HubEventType.MERGE_ON_CHAIN_EVENT,
      mergeOnChainEventBody: { onChainEvent },
    });
    await HubEventProcessor.processHubEvent(db, event, handler);
    await HubEventProcessor.processHubEvent(db, event, handler);
    const rows = await db.selectFrom("onchain_events").select(["blockNumber", "logIndex"]).execute();
    expect(rows).toEqual([{ blockNumber: 77, logIndex: 3 }]);
  });

  test("migration 006 removes identical on-chain duplicates and refuses conflicting ones", async () => {
    const insert = (body: object) =>
      sql`INSERT INTO public.onchain_events (chain_id, block_timestamp, fid, block_number, log_index, type, tx_hash, body)
        VALUES (10, now(), 1, 5, 1, 4, '\\x01', ${JSON.stringify(body)}::json)`.execute(db);
    const reopen = async () => {
      await sql`DROP INDEX IF EXISTS public.onchain_events_chain_block_log_unique`.execute(db);
      await sql`DROP TABLE IF EXISTS public.shuttle_message_state`.execute(db);
      // Later migrations are idempotent and must be re-run after the ones reopened here.
      await sql`DELETE FROM public.kysely_migration
        WHERE name IN ('005_blocks', '006_projection_framework', '007_projection_runs', '008_reference_tables')`.execute(
        db,
      );
    };

    await reopen();
    await insert({ units: 1 });
    await insert({ units: 1 });
    let migration = await migrateToLatest(db, "public", log);
    if (migration.isErr()) throw migration.error;
    expect(await db.selectFrom("onchain_events").select("id").execute()).toHaveLength(1);

    await reopen();
    await insert({ units: 2 });
    migration = await migrateToLatest(db, "public", log);
    expect(migration.isErr()).toBe(true);
    if (migration.isErr()) expect(String(migration.error)).toContain("conflicting duplicates");

    await sql`DELETE FROM public.onchain_events WHERE body::text LIKE '%2%'`.execute(db);
    migration = await migrateToLatest(db, "public", log);
    if (migration.isErr()) throw migration.error;
  });
  describe("run history and dashboard status", () => {
    const appDb = () => db as unknown as AppDb;
    const runs = async () => (await listRuns(appDb())).reverse();

    test("rebuild records a succeeded run, then a failed one for invalid stored data", async () => {
      for (const message of history) await MessageProcessor.storeMessage(message, db, "merge", undefined, false);
      const ok = await runRebuild(db, dispatcher, { projections: ["blocks"], fids: [], trigger: "cli" });
      expect(ok.rebuilt).toBe(1);
      const foreign = Message.create({
        data: {
          fid: 101,
          type: MessageType.LINK_ADD,
          timestamp: 1000,
          network: FarcasterNetwork.TESTNET,
          linkBody: { type: "block", targetFid: 200 },
        },
        hash: Buffer.alloc(20, 9),
      });
      await MessageProcessor.storeMessage(foreign, db, "merge", undefined, false);
      await runRebuild(db, dispatcher, { projections: ["blocks"], fids: [101, 100], trigger: "dashboard" });

      const [first, second] = await runs();
      expect(first).toMatchObject({ kind: "rebuild", status: "succeeded", done: 1, failedCount: 0, fids: null });
      expect(first?.projections).toEqual(["blocks"]);
      expect(first?.finishedAt).not.toBeNull();
      expect(second).toMatchObject({
        status: "failed",
        done: 1,
        failedCount: 1,
        fids: [100, 101],
        trigger: "dashboard",
      });
      expect((second?.details as { fid: number }[]).map((d) => d.fid)).toEqual([101]);

      await expect(runRebuild(db, dispatcher, { projections: ["casts"], fids: [], trigger: "cli" })).rejects.toThrow(
        "Unknown projections: casts",
      );
    });

    test("a live rebuild blocks another; a stale one does not", async () => {
      await sql`INSERT INTO public.shuttle_projection_runs (kind, projections, trigger, status)
        VALUES ('rebuild', '{blocks}', 'cli', 'running')`.execute(db);
      await expect(runRebuild(db, dispatcher, { projections: [], fids: [1], trigger: "cli" })).rejects.toThrow(
        "already running",
      );
      await sql`UPDATE public.shuttle_projection_runs SET updated_at = now() - interval '10 minutes'`.execute(db);
      await runRebuild(db, dispatcher, { projections: [], fids: [1], trigger: "cli" });
      const list = await listRuns(appDb());
      expect(list.find((r) => r.status === "running")?.stale).toBe(true);
    });

    test("backfill runs close once every queued job finished or failed", async () => {
      const added: { name: string; data: Record<string, unknown> }[] = [];
      const queue = { add: async (name: string, data: Record<string, unknown>) => added.push({ name, data }) };
      const { runId, jobs } = await enqueueBackfill(appDb(), queue as never, {
        fids: [],
        maxFid: 25,
        projectionsOnly: true,
        projections: ["blocks"],
        trigger: "dashboard",
      });
      expect(jobs).toBe(3);
      expect(added.filter((j) => j.name === "reconcile").map((j) => j.data["fids"])).toEqual([
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        [21, 22, 23, 24, 25, 26, 27, 28, 29, 30],
      ]);
      expect(added.every((j) => j.data["runId"] === runId)).toBe(true);
      expect(added[0]?.data["projectionsOnly"]).toBe(true);

      await recordBackfillJob(appDb(), runId, "done", 20);
      await recordBackfillJob(appDb(), runId, "done", 10);
      expect((await runs())[0]).toMatchObject({ status: "running", done: 2, total: 3, lastFid: 20 });
      await recordBackfillJob(appDb(), runId, "failed");
      expect((await runs())[0]).toMatchObject({ status: "failed", done: 2, failedCount: 1 });
      await expect(
        enqueueBackfill(appDb(), queue as never, { fids: [], projectionsOnly: true, projections: [], trigger: "cli" }),
      ).rejects.toThrow("maxFid");
    });

    test("status reports projection stats, issues by reason and filtered issue pages", async () => {
      for (let i = 0; i < history.length; i++)
        await dispatcher.processEvent(mergeEvent(i + 1, history[i] as Message), db);
      const bad = (fid: number, hash: number) =>
        Message.create({
          data: {
            fid,
            type: MessageType.LINK_ADD,
            timestamp: 1000,
            network: FarcasterNetwork.TESTNET,
            linkBody: { type: "block", targetFid: 5 },
          },
          hash: Buffer.alloc(20, hash),
        });
      await dispatcher.processEvent(mergeEvent(10, bad(100, 20)), db);
      await dispatcher.processEvent(mergeEvent(11, bad(102, 21)), db);
      await recordIssue(appDb(), "blocks", messageSubject(bad(103, 22)), "Another reason");

      const status = await getStatus({ db: appDb(), schema: "public", projections, redis: null, queue: null });
      expect(status.migrations).toMatchObject({ ok: true, data: { pending: [] } });
      expect(status.streams.ok).toBe(false);
      const blocks = status.projections.find((p) => p.name === "blocks");
      expect(blocks?.stats).toEqual({ ok: true, data: { total: 3, active: 2, blockers: 1 } });
      expect(blocks?.issues.ok && blocks.issues.data.total).toBe(3);
      expect(blocks?.issues.ok && blocks.issues.data.byReason.map((r) => [r.reason, r.count])).toEqual([
        ["Invalid mainnet block message", 2],
        ["Another reason", 1],
      ]);

      const page = await listIssues(appDb(), {
        projection: "blocks",
        reason: "Invalid mainnet block message",
        limit: 1,
      });
      expect(page.total).toBe(2);
      expect(page.items).toHaveLength(1);
      expect(page.items[0]?.hash).toMatch(/^0x[0-9a-f]{40}$/);
      expect((await listIssues(appDb(), { fid: 102 })).items.map((i) => i.fid)).toEqual([102]);
    });
  });
});
