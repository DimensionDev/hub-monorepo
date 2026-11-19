import { randomUUID } from "node:crypto";
import pg from "pg";
import { sql } from "kysely";
import {
  HubEvent,
  HubEventType,
  HubRpcClient,
  OnChainEvent,
  OnChainEventResponse,
  OnChainEventType,
} from "@dimensiondev/hub-nodejs";
import { ok } from "neverthrow";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import type { AppDb } from "./example-app/db";
import { backfillFidForProjections, ProjectionDispatcher, projections } from "./example-app/projections";
import { rentalId, storageRentalsProjection } from "./example-app/projections/storageRentals";
import { MessageReconciliation } from "./shuttle/messageReconciliation";
import { OnChainEventReconciliation } from "./shuttle/onChainEventReconciliation";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";
import { dropTestDatabase } from "./testDatabase";

// Never fall back to the application's production connection or dotenv configuration.
const testUrl = process.env["TEST_DATABASE_URL"];
const integration = testUrl ? describe : describe.skip;

integration("storage_rentals projection", () => {
  const database = `rentals_test_${randomUUID().replaceAll("-", "")}`;
  let admin: pg.Client;
  let db: DB;
  const appDb = () => db as unknown as AppDb;

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
    await sql`TRUNCATE public.storage_rentals, public.onchain_events, public.shuttle_projection_issues`.execute(db);
  });

  const handler = { onHubEvent: App.prototype.onHubEvent, handleMessageMerge: App.prototype.handleMessageMerge };
  const dispatcher = new ProjectionDispatcher(projections, "public");
  const FID = 1000;
  const time = (block: number) => 1_800_000_000 + block * 2;
  let eventId = 0;

  const rent = (opts: {
    block: number;
    tx?: number;
    log?: number;
    units?: number;
    expiry?: number;
    fid?: number;
    chainId?: number;
    noBody?: boolean;
  }): OnChainEvent =>
    OnChainEvent.create({
      type: OnChainEventType.EVENT_TYPE_STORAGE_RENT,
      chainId: opts.chainId ?? 10,
      blockNumber: opts.block,
      blockHash: Buffer.alloc(32, 1),
      blockTimestamp: time(opts.block),
      transactionHash: Buffer.alloc(32, opts.tx ?? opts.block % 256),
      logIndex: opts.log ?? 0,
      fid: opts.fid ?? FID,
      txIndex: 0,
      version: 1,
      ...(opts.noBody
        ? {}
        : {
            storageRentEventBody: {
              payer: Buffer.alloc(20, 9),
              units: opts.units ?? 1,
              expiry: opts.expiry ?? 0,
            },
          }),
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

  /** Stores raw rows directly, as rows written before the projection existed. */
  const storeRaw = async (event: OnChainEvent, body: unknown) => {
    await db
      .insertInto("onchain_events")
      .values({
        chainId: event.chainId,
        fid: event.fid,
        blockTimestamp: new Date(event.blockTimestamp * 1000),
        blockNumber: event.blockNumber,
        logIndex: event.logIndex,
        txHash: event.transactionHash,
        type: event.type,
        body: body as never,
      })
      .execute();
  };

  const rentals = async () =>
    (
      await sql<{
        id: string;
        fid: number;
        units: number;
        expiry: Date;
        timestamp: Date;
        tx: string;
        logIndex: number;
        createdAt: Date;
        updatedAt: Date;
      }>`SELECT id, fid, units, expiry AT TIME ZONE 'UTC' AS expiry, timestamp AT TIME ZONE 'UTC' AS timestamp,
          encode(transaction_hash, 'hex') AS tx, log_index, created_at, updated_at
        FROM public.storage_rentals ORDER BY transaction_hash, log_index`.execute(db)
    ).rows;
  const issues = async () =>
    (
      await sql<{ fid: number; reason: string }>`
        SELECT fid, reason FROM public.shuttle_projection_issues
        WHERE projection = 'storage_rentals' ORDER BY reason`.execute(db)
    ).rows;
  const at = (seconds: number) => new Date(seconds * 1000);
  const stored = async () => (await db.selectFrom("onchain_events").select("id").execute()).length;

  test("a STORAGE_RENT creates a receipt with the Go-compatible id and the stored expiry", async () => {
    const event = rent({ block: 100, tx: 7, log: 3, units: 2 });
    await deliver(event);
    // The raw body's expiry is computed by Shuttle from the block time; it is copied as is.
    const body = await db.selectFrom("onchain_events").select("body").executeTakeFirstOrThrow();
    const expiry = (body.body as { expiry: number }).expiry;
    expect(expiry).toBeGreaterThan(time(100));
    expect(await rentals()).toEqual([
      expect.objectContaining({
        // Go: uuid.NewSHA1(NameSpaceURL, "farcaster-db-conversion:storage_rentals:10:<07 x32>:3")
        id: "5ef980a7-7827-5410-b13d-1b4196389229",
        fid: FID,
        units: 2,
        expiry: at(expiry),
        timestamp: at(time(100)),
        tx: "07".repeat(32),
        logIndex: 3,
      }),
    ]);
    expect(await issues()).toEqual([]);
  });

  test("each receipt is its own row: repeat rentals, receipts sharing a transaction, expired ones", async () => {
    await deliver(rent({ block: 100, units: 1 }));
    await deliver(rent({ block: 200, tx: 5, log: 0, units: 2 }));
    await deliver(rent({ block: 200, tx: 5, log: 1, units: 3 }));
    const rows = await rentals();
    expect(rows.map((r) => [r.units, r.logIndex])).toEqual([
      [2, 0],
      [3, 1],
      [1, 0],
    ]);
    // Expiry is the rental's own; long past expiry still keeps the row.
    expect(rows.every((r) => r.expiry.getTime() > r.timestamp.getTime())).toBe(true);
    expect(await storageRentalsProjection.stats?.(appDb())).toMatchObject({ total: 3, renters: 1 });
  });

  test("replays and out-of-order delivery converge without touching updated_at", async () => {
    const events = [rent({ block: 100 }), rent({ block: 200 })];
    for (const event of events) await deliver(event);
    const before = await rentals();
    await deliver(events[0] as OnChainEvent);
    expect(await stored()).toBe(2);
    expect(await rentals()).toEqual(before);

    await sql`TRUNCATE public.storage_rentals, public.onchain_events`.execute(db);
    expect(await dispatcher.processMissingOnChainEvents(db, [...events].reverse())).toBe(2);
    const after = await rentals();
    expect(after.map(({ createdAt, updatedAt, ...r }) => r)).toEqual(before.map(({ createdAt, updatedAt, ...r }) => r));
  });

  test("invalid receipts are issues and are not stored", async () => {
    await deliver(rent({ block: 100, chainId: 8453 }));
    await deliver(rent({ block: 101, noBody: true }));
    await deliver(rent({ block: 102, fid: 2 ** 31 }));
    expect(await stored()).toBe(0);
    expect(await rentals()).toEqual([]);
    expect((await issues()).map((i) => i.reason)).toEqual([
      `Missing or unrepresentable units, expiry for fid ${FID} log 0`,
      `STORAGE_RENT event on chain 8453, expected 10, for fid ${FID} log 0`,
      `fid out of range for fid ${2 ** 31} log 0`,
    ]);
  });

  test("rebuild projects stored rows and records bad ones as issues without failing the fid", async () => {
    const good = rent({ block: 100, units: 4 });
    const bad = rent({ block: 101 });
    await storeRaw(good, { payer: "0x09", units: 4, expiry: time(100) + 1000 });
    await storeRaw(bad, { payer: "0x09", units: null, expiry: time(101) + 1000 });
    expect(await storageRentalsProjection.rebuildFids(appDb(), 0, 10)).toEqual([FID]);

    // Raw events never change, so failing (and rolling back) the fid would block its good receipts.
    expect(await dispatcher.rebuild(db, { projections: ["storage_rentals"] })).toEqual({ rebuilt: 1, failed: [] });
    expect(await rentals()).toEqual([
      expect.objectContaining({ units: 4, expiry: at(time(100) + 1000), timestamp: at(time(100)) }),
    ]);
    expect((await issues()).map((i) => i.reason)).toEqual([`Missing or unrepresentable units for fid ${FID} log 0`]);
    // Idempotent: a second rebuild writes nothing new and records the issue once.
    const before = await rentals();
    await dispatcher.rebuild(db, { projections: ["storage_rentals"] });
    expect(await rentals()).toEqual(before);
    expect(await issues()).toHaveLength(1);
  });

  test("a fid 0 rental keeps the generic raw path instead of failing the hub event", async () => {
    await deliver(rent({ block: 100, fid: 0 }));
    expect(await stored()).toBe(1);
    expect(await rentals()).toEqual([]);
    expect(await issues()).toEqual([]);
  });

  test("units 0 is a value, not a missing field", async () => {
    await storeRaw(rent({ block: 100 }), { units: 0, expiry: time(100) + 1 });
    expect(await dispatcher.rebuild(db, { projections: ["storage_rentals"] })).toEqual({ rebuilt: 1, failed: [] });
    expect((await rentals()).map((r) => r.units)).toEqual([0]);
  });

  test("the same receipt at two blocks merges when it agrees and is an issue when it conflicts", async () => {
    const body = { units: 1, expiry: time(100) + 10 };
    await storeRaw(rent({ block: 100, tx: 3 }), body);
    await storeRaw(rent({ block: 105, tx: 3 }), body);
    // Same block time is part of the receipt; a re-included copy keeps it.
    await sql`UPDATE onchain_events SET block_timestamp = ${at(time(100))}`.execute(db);
    expect(await dispatcher.rebuild(db, { projections: ["storage_rentals"] })).toEqual({ rebuilt: 1, failed: [] });
    expect(await rentals()).toHaveLength(1);

    await sql`TRUNCATE public.storage_rentals, public.onchain_events`.execute(db);
    await storeRaw(rent({ block: 100, tx: 3 }), body);
    await storeRaw(rent({ block: 105, tx: 3 }), { ...body, units: 2 });
    await storeRaw(rent({ block: 110, tx: 4 }), body);
    expect(await dispatcher.rebuild(db, { projections: ["storage_rentals"] })).toEqual({ rebuilt: 1, failed: [] });
    expect((await rentals()).map((r) => r.tx)).toEqual(["04".repeat(32)]);
    expect((await issues()).map((i) => i.reason)).toEqual([`Conflicting STORAGE_RENT receipts for fid ${FID} log 0`]);
  });

  test("a receipt whose fid changed is updated in place, keeping id and created_at", async () => {
    const event = rent({ block: 100, tx: 7, log: 3 });
    const id = rentalId(`10:${"07".repeat(32)}:3`);
    const created = "2020-01-01T00:00:00.000Z";
    await appDb()
      .insertInto("storageRentals")
      .values({
        id,
        createdAt: created,
        updatedAt: created,
        fid: 42,
        units: 1,
        expiry: created,
        timestamp: created,
        transactionHash: Buffer.alloc(32, 7),
        logIndex: 3,
      })
      .execute();
    await deliver(event);
    const [row] = await rentals();
    expect(row).toMatchObject({ id, fid: FID, timestamp: at(time(100)) });
    expect(row?.createdAt.toISOString().slice(0, 10)).toBe("2020-01-01");
    expect(row?.updatedAt.toISOString().slice(0, 10)).not.toBe("2020-01-01");
  });

  test("projection-only backfill pulls the fid's STORAGE_RENT history from the hub", async () => {
    const history = [rent({ block: 100 }), rent({ block: 200, units: 5 })];
    await deliver(history[1] as OnChainEvent); // only part of the history was captured live
    const requested: OnChainEventType[] = [];
    const hub = {
      getOnChainEvents: async (req: { fid: number; eventType: OnChainEventType }) => {
        requested.push(req.eventType);
        return ok(
          OnChainEventResponse.create({
            events: req.eventType === OnChainEventType.EVENT_TYPE_STORAGE_RENT ? history : [],
          }),
        );
      },
    };
    const messages = new MessageReconciliation({} as HubRpcClient, db, log);
    const onChain = new OnChainEventReconciliation(hub as unknown as HubRpcClient, db, log);

    const first = await backfillFidForProjections(messages, db, dispatcher, [storageRentalsProjection], FID, onChain);
    expect(requested).toEqual([OnChainEventType.EVENT_TYPE_STORAGE_RENT]);
    expect(first).toEqual({ missing: 1, projected: 1 });
    expect((await rentals()).map((r) => r.units)).toEqual([1, 5]);
    expect(await issues()).toEqual([]);

    const second = await backfillFidForProjections(messages, db, dispatcher, [storageRentalsProjection], FID, onChain);
    expect(second).toEqual({ missing: 0, projected: 0 });
  });
});
