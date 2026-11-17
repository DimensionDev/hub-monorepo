import { randomUUID } from "node:crypto";
import pg from "pg";
import { sql } from "kysely";
import { Factories, FarcasterNetwork, HubEvent, HubEventType, Message, MessageType } from "@dimensiondev/hub-nodejs";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import { ProjectionDispatcher, projections } from "./example-app/projections";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";

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
      await admin.query(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
      await admin.end();
    }
  });

  beforeEach(async () => {
    await sql`TRUNCATE public.messages, public.blocks, public.shuttle_message_state,
      public.shuttle_projection_issues, public.onchain_events`.execute(db);
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
      await sql`DELETE FROM public.kysely_migration WHERE name IN ('005_blocks', '006_projection_framework')`.execute(
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
});
