import { randomUUID } from "node:crypto";
import pg from "pg";
import { sql } from "kysely";
import { HubEvent, HubEventType, OnChainEvent, OnChainEventType } from "@dimensiondev/hub-nodejs";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";
import { dropTestDatabase } from "./testDatabase";

// Never fall back to the application's production connection or dotenv configuration.
const testUrl = process.env["TEST_DATABASE_URL"];
const integration = testUrl ? describe : describe.skip;

// The receipt logic is shared with storage_rentals (covered in storageRentals.integration.test.ts);
// these tests cover what tier_purchases adds: Base chain, payer bytes, forDays and a bigint fid.
integration("tier_purchases projection", () => {
  const database = `tiers_test_${randomUUID().replaceAll("-", "")}`;
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
    await sql`TRUNCATE public.tier_purchases, public.onchain_events, public.shuttle_projection_issues`.execute(db);
  });

  const handler = { onHubEvent: App.prototype.onHubEvent, handleMessageMerge: App.prototype.handleMessageMerge };
  const FID = 1000;
  const time = (block: number) => 1_800_000_000 + block * 2;
  let eventId = 0;

  const purchase = (opts: { block: number; tx?: number; log?: number; chainId?: number; payer?: Uint8Array }) =>
    OnChainEvent.create({
      type: OnChainEventType.EVENT_TYPE_TIER_PURCHASE,
      chainId: opts.chainId ?? 8453,
      blockNumber: opts.block,
      blockHash: Buffer.alloc(32, 1),
      blockTimestamp: time(opts.block),
      transactionHash: Buffer.alloc(32, opts.tx ?? opts.block % 256),
      logIndex: opts.log ?? 0,
      fid: FID,
      txIndex: 0,
      version: 1,
      tierPurchaseEventBody: { tierType: 1, forDays: 30, payer: opts.payer ?? Buffer.alloc(20, 9) },
    });

  const deliver = async (event: OnChainEvent) => {
    eventId++;
    await HubEventProcessor.processHubEvent(
      db,
      HubEvent.create({
        id: eventId,
        shardIndex: 1,
        timestamp: eventId,
        type: HubEventType.MERGE_ON_CHAIN_EVENT,
        mergeOnChainEventBody: { onChainEvent: event },
      }),
      handler,
    );
  };

  const purchases = async () =>
    (
      await sql<Record<string, unknown>>`
        SELECT id, fid, tier_type, duration_in_days, encode(payer, 'hex') AS payer,
          timestamp AT TIME ZONE 'UTC' AS timestamp, encode(transaction_hash, 'hex') AS tx, log_index
        FROM public.tier_purchases ORDER BY transaction_hash, log_index`.execute(db)
    ).rows;
  const issues = async () =>
    (
      await sql<{ reason: string }>`SELECT reason FROM public.shuttle_projection_issues
        WHERE projection = 'tier_purchases' ORDER BY reason`.execute(db)
    ).rows.map((r) => r.reason);

  test("a TIER_PURCHASE creates a receipt with the Go-compatible id", async () => {
    await deliver(purchase({ block: 100, tx: 7, log: 3 }));
    expect(await purchases()).toEqual([
      {
        // Go: uuid.NewSHA1(NameSpaceURL, "farcaster-db-conversion:tier_purchases:8453:<07 x32>:3")
        id: "f3ef3cc5-16a5-59db-8e92-ccffed6a0080",
        fid: FID,
        tierType: 1,
        durationInDays: 30,
        payer: "09".repeat(20),
        timestamp: new Date(time(100) * 1000),
        tx: "07".repeat(32),
        logIndex: 3,
      },
    ]);
    expect(await issues()).toEqual([]);
  });

  test("replays leave the row untouched", async () => {
    const event = purchase({ block: 100 });
    await deliver(event);
    const before = await purchases();
    await deliver(event);
    expect(await purchases()).toEqual(before);
  });

  test("receipts on another chain or without a 20-byte payer are issues and are not stored", async () => {
    await deliver(purchase({ block: 100, chainId: 10 }));
    await deliver(purchase({ block: 101, payer: Buffer.alloc(19, 1) }));
    expect(await purchases()).toEqual([]);
    expect(await db.selectFrom("onchain_events").select("id").execute()).toHaveLength(0);
    expect(await issues()).toEqual([
      `Missing or unrepresentable payer for fid ${FID} log 0`,
      `TIER_PURCHASE event on chain 10, expected 8453, for fid ${FID} log 0`,
    ]);
  });
});
