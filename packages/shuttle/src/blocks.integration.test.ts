import { randomUUID } from "node:crypto";
import pg from "pg";
import { sql } from "kysely";
import { FarcasterNetwork, HubEvent, HubEventType, Message, MessageType } from "@dimensiondev/hub-nodejs";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";
import { MessageProcessor } from "./shuttle/messageProcessor";

// Never fall back to the application's production connection or dotenv configuration.
const testUrl = process.env["TEST_DATABASE_URL"];
const integration = testUrl ? describe : describe.skip;

integration("blocks incremental projection", () => {
  const database = `blocks_test_${randomUUID().replaceAll("-", "")}`;
  let admin: pg.Client;
  let db: DB;
  let databaseUrl: string;

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: testUrl });
    await admin.connect();
    await admin.query(`CREATE DATABASE "${database}"`);
    if (!testUrl) throw new Error("TEST_DATABASE_URL is required for integration tests");
    const url = new URL(testUrl);
    url.pathname = `/${database}`;
    databaseUrl = url.toString();
    db = getDbClient(databaseUrl, "public");
    const result = await migrateToLatest(db, "public", log);
    if (result.isErr()) throw result.error;
  });

  afterAll(async () => {
    await db?.destroy();
    if (admin) {
      await admin.query(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
      await admin.end();
    }
  });

  beforeEach(async () => {
    await sql`TRUNCATE public.messages, public.blocks, public.shuttle_message_state,
      public.shuttle_projection_issues`.execute(db);
  });

  const processEvent = (event: HubEvent) =>
    HubEventProcessor.processHubEvent(db, event, {
      onHubEvent: App.prototype.onHubEvent,
      handleMessageMerge: App.prototype.handleMessageMerge,
    });

  const link = (
    target: number,
    timestamp: number,
    hash: number,
    options: {
      type?: MessageType;
      fid?: number;
      displayTimestamp?: number;
      network?: FarcasterNetwork;
    } = {},
  ) =>
    Message.create({
      data: {
        fid: options.fid ?? 100,
        type: options.type ?? MessageType.LINK_ADD,
        timestamp,
        network: options.network ?? FarcasterNetwork.MAINNET,
        linkBody: { type: "block", targetFid: target, displayTimestamp: options.displayTimestamp },
      },
      hash: Buffer.alloc(20, hash),
    });
  const compact = (targets: number[], timestamp: number, hash: number, linkType = "block") =>
    Message.create({
      data: {
        fid: 100,
        type: MessageType.LINK_COMPACT_STATE,
        timestamp,
        network: FarcasterNetwork.MAINNET,
        linkCompactStateBody: { type: linkType, targetFids: targets },
      },
      hash: Buffer.alloc(20, hash),
    });
  const merge = (id: number, message: Message, deletedMessages: Message[] = []) =>
    processEvent(
      HubEvent.create({
        id,
        shardIndex: 1,
        timestamp: 2000 + id,
        type: HubEventType.MERGE_MESSAGE,
        mergeMessageBody: { message, deletedMessages },
      }),
    );
  const invalidate = (id: number, message: Message, type: HubEventType.PRUNE_MESSAGE | HubEventType.REVOKE_MESSAGE) =>
    processEvent(
      HubEvent.create({
        id,
        shardIndex: 1,
        timestamp: 2000 + id,
        type,
        ...(type === HubEventType.PRUNE_MESSAGE
          ? { pruneMessageBody: { message } }
          : { revokeMessageBody: { message } }),
      }),
    );
  const rows = async () =>
    (
      await sql<{
        id: string;
        blockerFid: number;
        blockedFid: number;
        timestamp: Date;
        deletedAt: Date | null;
        createdAt: Date;
        updatedAt: Date;
      }>`SELECT id, blocker_fid, blocked_fid, timestamp AT TIME ZONE 'UTC' AS timestamp,
    deleted_at AT TIME ZONE 'UTC' AS deleted_at, created_at AT TIME ZONE 'UTC' AS created_at,
    updated_at AT TIME ZONE 'UTC' AS updated_at FROM public.blocks ORDER BY blocker_fid, blocked_fid`.execute(db)
    ).rows;

  test("stores a new block and its raw message together", async () => {
    const message = Message.create({
      data: {
        fid: 100,
        type: MessageType.LINK_ADD,
        network: FarcasterNetwork.MAINNET,
        timestamp: 1000,
        linkBody: { type: "block", targetFid: 200 },
      },
      hash: Buffer.alloc(20, 1),
    });
    await processEvent(
      HubEvent.create({
        id: 1,
        shardIndex: 1,
        timestamp: 1100,
        type: HubEventType.MERGE_MESSAGE,
        mergeMessageBody: { message },
      }),
    );
    const result = await sql<{ blockerFid: number; blockedFid: number; deletedAt: null }>`
      SELECT blocker_fid, blocked_fid, deleted_at FROM public.blocks
    `.execute(db);
    expect(result.rows).toEqual([{ blockerFid: 100, blockedFid: 200, deletedAt: null }]);
    const raw = await db.selectFrom("messages").select("hash").execute();
    expect(raw.map((row) => Buffer.from(row.hash))).toEqual([Buffer.alloc(20, 1)]);
    expect((await rows())[0]?.id).toBe("9d6c3e7b-84f9-5810-93a1-298c44555a2c");
  });

  test("compact creates its full set, retains add times, and an empty set removes existing members", async () => {
    const a = link(200, 1000, 1);
    const b = link(300, 1001, 2);
    await merge(1, a);
    await merge(2, b);
    const state = compact([200, 400], 1100, 3);
    await merge(3, state, [b]);
    expect(
      (await rows()).map(({ blockedFid, timestamp, deletedAt }) => ({ blockedFid, timestamp, deletedAt })),
    ).toEqual([
      { blockedFid: 200, timestamp: new Date("2021-01-01T00:16:40Z"), deletedAt: null },
      { blockedFid: 300, timestamp: new Date("2021-01-01T00:16:41Z"), deletedAt: new Date("2021-01-01T00:18:20Z") },
      { blockedFid: 400, timestamp: new Date("2021-01-01T00:18:20Z"), deletedAt: null },
    ]);
    await merge(4, compact([], 1200, 4), [a, state]);
    expect((await rows()).every((row) => row.deletedAt !== null)).toBe(true);
    expect((await rows()).find((row) => row.blockedFid === 400)?.deletedAt).toEqual(new Date("2021-01-01T00:20:00Z"));
    expect(
      (await db.selectFrom("messages").select("deletedAt").where("hash", "=", b.hash).executeTakeFirstOrThrow())
        .deletedAt,
    ).toEqual(new Date("2021-01-01T00:33:23Z"));
  });

  test("remove without an existing block creates no row; restoring a block preserves its identity", async () => {
    await merge(1, link(900, 900, 9, { type: MessageType.LINK_REMOVE }));
    expect(await rows()).toEqual([]);
    const add = link(200, 1000, 1);
    await merge(2, add);
    const before = (await rows())[0];
    await merge(3, link(200, 1100, 2, { type: MessageType.LINK_REMOVE }), [add]);
    expect((await rows())[0]).toMatchObject({
      id: before?.id,
      timestamp: before?.timestamp,
      deletedAt: new Date("2021-01-01T00:18:20Z"),
    });
    await merge(4, link(200, 1200, 3));
    expect((await rows())[0]).toMatchObject({
      id: before?.id,
      createdAt: before?.createdAt,
      timestamp: new Date("2021-01-01T00:20:00Z"),
      deletedAt: null,
    });
  });

  test("duplicate delivery preserves all business timestamps and older merges cannot undo revocation", async () => {
    const add = link(200, 1000, 1);
    await merge(1, add);
    const before = await rows();
    await merge(1, add);
    await merge(2, add);
    expect(await rows()).toEqual(before);
    await invalidate(4, add, HubEventType.REVOKE_MESSAGE);
    const revoked = await rows();
    await merge(3, add);
    await merge(1, add);
    expect(await rows()).toEqual(revoked);
    expect(revoked[0]?.deletedAt).toEqual(new Date("2021-01-01T00:33:24Z"));
    const raw = await db.selectFrom("messages").select(["deletedAt", "revokedAt"]).executeTakeFirstOrThrow();
    expect(raw.revokedAt).toEqual(new Date("2021-01-01T00:33:24Z"));
  });

  test("a newer event for one relationship does not discard an older event for another", async () => {
    await merge(20, link(200, 1000, 1));
    await merge(10, link(300, 1001, 2));
    expect((await rows()).map((r) => r.blockedFid)).toEqual([200, 300]);
  });

  test("ordinary events leave unrelated historical relationships unmaterialized", async () => {
    await MessageProcessor.storeMessage(link(300, 900, 3), db, "merge", undefined, false);
    await MessageProcessor.storeMessage(link(200, 1000, 1), db, "merge", undefined, false);
    // Even when raw capture already has this hash, its first managed event must project it.
    await merge(1, link(200, 1000, 1));
    expect((await rows()).map((r) => r.blockedFid)).toEqual([200]);
  });

  test("ordinary conflict resolution is timestamp, remove-wins, then unsigned hash order", async () => {
    const first = link(200, 1100, 1, { displayTimestamp: 500 });
    await merge(1, first);
    await merge(2, link(200, 1000, 128));
    expect((await rows())[0]?.timestamp).toEqual(new Date("2021-01-01T00:08:20Z"));
    const higher = link(200, 1100, 255, { displayTimestamp: 600 });
    await merge(3, higher);
    expect((await rows())[0]?.timestamp).toEqual(new Date("2021-01-01T00:10:00Z"));
    await merge(4, link(200, 1100, 2, { type: MessageType.LINK_REMOVE }));
    expect((await rows())[0]?.deletedAt).toEqual(new Date("2021-01-01T00:18:20Z"));
  });

  test("newer ordinary protocol time survives an older compact despite an older display time", async () => {
    const newer = link(200, 1200, 1, { displayTimestamp: 500 });
    await merge(3, newer);
    await merge(2, compact([], 1100, 2));
    expect((await rows())[0]).toMatchObject({ timestamp: new Date("2021-01-01T00:08:20Z"), deletedAt: null });
  });

  test("equal-time compaction follows the explicit deletion list and accepts subsequent equal-time links", async () => {
    const add = link(200, 1000, 1);
    await merge(1, add);
    await merge(2, compact([], 1000, 2), [add]);
    expect((await rows())[0]?.deletedAt).toEqual(new Date("2021-01-01T00:16:40Z"));
    await merge(3, link(200, 1000, 3));
    expect((await rows())[0]?.deletedAt).toBeNull();
    await merge(1, add);
    expect(
      (await db.selectFrom("messages").select("deletedAt").where("hash", "=", add.hash).executeTakeFirstOrThrow())
        .deletedAt,
    ).not.toBeNull();
  });

  test("a compact tombstone received before its original merge cannot be resurrected", async () => {
    const old = link(200, 1000, 1);
    await merge(2, compact([], 1100, 2), [old]);
    await merge(1, old);
    expect(await rows()).toEqual([]);
    expect(
      (await db.selectFrom("messages").select("deletedAt").where("hash", "=", old.hash).executeTakeFirstOrThrow())
        .deletedAt,
    ).not.toBeNull();
  });

  test.each([HubEventType.PRUNE_MESSAGE, HubEventType.REVOKE_MESSAGE] as const)(
    "invalidating compact-only support removes it but retains a separately supported relationship (%s)",
    async (operation) => {
      const state = compact([200, 300], 1000, 1);
      await merge(1, state);
      await merge(2, link(300, 1100, 2));
      await invalidate(3, state, operation);
      expect((await rows()).map(({ blockedFid, deletedAt }) => ({ blockedFid, deletedAt }))).toEqual([
        { blockedFid: 200, deletedAt: new Date("2021-01-01T00:33:23Z") },
        { blockedFid: 300, deletedAt: null },
      ]);
      const before = await rows();
      await merge(1, state);
      expect(await rows()).toEqual(before);
    },
  );

  test("pruning an old add preserves newer support and invalidating a remove does not restore its deleted add", async () => {
    const old = link(200, 1000, 1);
    const newer = link(200, 1100, 2);
    await merge(1, old);
    await merge(2, newer, [old]);
    await invalidate(3, old, HubEventType.PRUNE_MESSAGE);
    expect((await rows())[0]?.deletedAt).toBeNull();
    const remove = link(200, 1200, 3, { type: MessageType.LINK_REMOVE });
    await merge(4, remove, [newer]);
    const deleted = await rows();
    await invalidate(5, remove, HubEventType.REVOKE_MESSAGE);
    expect(await rows()).toEqual(deleted);
  });

  test("concurrent events for the same user create one row and preserve the winning state", async () => {
    const add = link(200, 1000, 1);
    await Promise.all(Array.from({ length: 6 }, () => merge(1, add)));
    expect(await rows()).toHaveLength(1);
    const remove = link(200, 1200, 3, { type: MessageType.LINK_REMOVE });
    await Promise.all([merge(3, remove, [add]), merge(2, link(200, 1100, 2)), merge(1, add)]);
    expect(await rows()).toHaveLength(1);
    expect((await rows())[0]?.deletedAt).toEqual(new Date("2021-01-01T00:20:00Z"));
  });

  test("follow messages and opposite-direction relationships remain independent", async () => {
    await merge(1, link(200, 1000, 1));
    await merge(2, link(100, 1000, 2, { fid: 200 }));
    const follow = compact([], 1200, 3, "follow");
    await merge(3, follow);
    expect(await rows()).toHaveLength(2);
    expect((await rows()).every((r) => r.deletedAt === null)).toBe(true);
  });

  test("failed business writes roll back the raw message and lifecycle state, then retry succeeds", async () => {
    await sql`CREATE FUNCTION public.fail_block_write() RETURNS trigger LANGUAGE plpgsql AS
      'BEGIN RAISE EXCEPTION ''injected blocks failure''; END'`.execute(db);
    await sql`CREATE TRIGGER fail_blocks BEFORE INSERT ON public.blocks
      FOR EACH ROW EXECUTE FUNCTION public.fail_block_write()`.execute(db);
    const add = link(200, 1000, 1);
    try {
      await expect(merge(1, add)).rejects.toThrow("injected blocks failure");
      expect(await db.selectFrom("messages").select("hash").execute()).toEqual([]);
      expect((await sql`SELECT * FROM public.shuttle_message_state`.execute(db)).rows).toEqual([]);
    } finally {
      await sql`DROP TRIGGER fail_blocks ON public.blocks`.execute(db);
      await sql`DROP FUNCTION public.fail_block_write()`.execute(db);
    }
    await merge(1, add);
    expect(await rows()).toHaveLength(1);
  });

  test("records non-mainnet or malformed block messages as issues without storing them", async () => {
    const foreign = link(200, 1000, 1, { network: FarcasterNetwork.TESTNET });
    await merge(1, foreign);
    await merge(2, link(0, 1000, 2));
    await merge(3, link(0, 1000, 2)); // replay records no duplicate issue
    expect(await db.selectFrom("messages").select("hash").execute()).toEqual([]);
    expect(await rows()).toEqual([]);
    const issues = (
      await sql<{ projection: string; reason: string }>`SELECT projection, reason FROM public.shuttle_projection_issues
        ORDER BY reason`.execute(db)
    ).rows;
    expect(issues).toHaveLength(2);
    expect(issues.every((issue) => issue.projection === "blocks")).toBe(true);
    expect(issues.map((issue) => issue.reason).join(" ")).toMatch(/mainnet[\s\S]*target|target[\s\S]*mainnet/);
  });

  test("equal-time active compact conflicts and duplicate existing relationships fail atomically", async () => {
    await merge(1, compact([200], 1000, 1));
    await expect(merge(2, compact([300], 1000, 2))).rejects.toThrow("Ambiguous");
    expect(await db.selectFrom("messages").select("hash").execute()).toHaveLength(1);
    await sql`INSERT INTO public.blocks SELECT ${randomUUID()}::uuid, created_at, updated_at, timestamp,
      deleted_at, blocker_fid, blocked_fid FROM public.blocks`.execute(db);
    await expect(merge(3, link(200, 1100, 3))).rejects.toThrow("Duplicate blocks");
    expect(await db.selectFrom("messages").select("hash").execute()).toHaveLength(1);
  });

  test("rejects older equal-time active compact conflicts even when a newer compact is read first", async () => {
    for (const message of [compact([400], 2000, 3), compact([200], 1000, 1), compact([300], 1000, 2)]) {
      await MessageProcessor.storeMessage(message, db, "merge", undefined, false);
    }
    await expect(merge(4, link(500, 3000, 4))).rejects.toThrow("Ambiguous");
    expect(await db.selectFrom("messages").select("hash").execute()).toHaveLength(3);
  });

  test("rejects malformed historical JSON bodies instead of trusting the raw-message validation alone", async () => {
    const old = link(200, 1000, 1);
    await MessageProcessor.storeMessage(old, db, "merge", undefined, false);
    await sql`UPDATE public.messages SET body = '{"type":"block","targetFid":"200"}'::json`.execute(db);
    await expect(merge(2, link(300, 1100, 2))).rejects.toThrow("block body");
    expect(await db.selectFrom("messages").select("hash").execute()).toHaveLength(1);
  });

  test("uses the configured schema and UTC independently of the connection timezone", async () => {
    const schema = "blocks_custom";
    const scoped = getDbClient(`${databaseUrl}?options=-c%20timezone%3DAmerica%2FEdmonton`, schema);
    try {
      const migration = await migrateToLatest(scoped, schema, log);
      if (migration.isErr()) throw migration.error;
      const app = {
        dbSchema: schema,
        onHubEvent: App.prototype.onHubEvent,
        handleMessageMerge: App.prototype.handleMessageMerge,
      };
      const process = (event: HubEvent) => HubEventProcessor.processHubEvent(scoped, event, app);
      const add = link(200, 1000, 1);
      const event = HubEvent.create({
        id: 1,
        shardIndex: 1,
        timestamp: 2001,
        type: HubEventType.MERGE_MESSAGE,
        mergeMessageBody: { message: add },
      });
      await process(event);
      const first = (
        await sql`SELECT id, timestamp::text AS timestamp, created_at::text AS created_at,
        updated_at::text AS updated_at, deleted_at::text AS deleted_at FROM blocks_custom.blocks`.execute(scoped)
      ).rows;
      expect(first[0]).toMatchObject({ timestamp: "2021-01-01 00:16:40", deletedAt: null });
      await process(event);
      expect(
        (
          await sql`SELECT id, timestamp::text AS timestamp, created_at::text AS created_at,
        updated_at::text AS updated_at, deleted_at::text AS deleted_at FROM blocks_custom.blocks`.execute(scoped)
        ).rows,
      ).toEqual(first);
      expect(await rows()).toEqual([]);
    } finally {
      await scoped.destroy();
      await sql`DROP SCHEMA IF EXISTS blocks_custom CASCADE`.execute(db);
    }
  });

  test("adopts a compatible blocks table, repairs a missing index and rejects incompatible structure", async () => {
    await merge(1, link(200, 1000, 1));
    const before = await rows();
    const reopenMigration = async () => {
      await sql`DROP TABLE public.shuttle_message_state`.execute(db);
      await sql`DELETE FROM public.kysely_migration
        WHERE name IN ('005_blocks', '006_projection_framework', '007_projection_runs', '008_reference_tables')`.execute(
        db,
      );
    };
    await reopenMigration();
    await sql`DROP INDEX public.blocks_updated_at_idx`.execute(db);
    let migration = await migrateToLatest(db, "public", log);
    if (migration.isErr()) throw migration.error;
    expect(await rows()).toEqual(before);
    expect(
      (await sql`SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename='blocks'`.execute(db)).rows,
    ).toHaveLength(4);
    await reopenMigration();
    await sql`ALTER TABLE public.blocks ADD COLUMN unexpected text`.execute(db);
    migration = await migrateToLatest(db, "public", log);
    expect(migration.isErr()).toBe(true);
    if (migration.isErr()) expect(String(migration.error)).toContain("Incompatible blocks columns");
    expect(await rows()).toEqual(before);
    await sql`ALTER TABLE public.blocks DROP COLUMN unexpected`.execute(db);
    migration = await migrateToLatest(db, "public", log);
    if (migration.isErr()) throw migration.error;
  });
});
