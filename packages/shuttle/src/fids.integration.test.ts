import { randomUUID } from "node:crypto";
import pg from "pg";
import { sql } from "kysely";
import {
  HubEvent,
  HubEventType,
  HubRpcClient,
  IdRegisterEventType,
  OnChainEvent,
  OnChainEventResponse,
  OnChainEventType,
} from "@dimensiondev/hub-nodejs";
import { ok } from "neverthrow";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import type { AppDb } from "./example-app/db";
import { backfillFidForProjections, ProjectionDispatcher, projections } from "./example-app/projections";
import { FID_ISSUES, fidsProjection } from "./example-app/projections/fids";
import { adoptReferenceTable, normalizeIndexDef } from "./example-app/reference/adopt";
import { referenceTables } from "./example-app/reference/schema";
import { MessageReconciliation } from "./shuttle/messageReconciliation";
import { OnChainEventReconciliation } from "./shuttle/onChainEventReconciliation";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";
import { bytesToHex } from "./utils";

// Never fall back to the application's production connection or dotenv configuration.
const testUrl = process.env["TEST_DATABASE_URL"];
const integration = testUrl ? describe : describe.skip;

integration("reference tables and fids projection", () => {
  const database = `fids_test_${randomUUID().replaceAll("-", "")}`;
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
      await admin.query(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
      await admin.end();
    }
  });

  beforeEach(async () => {
    await sql`TRUNCATE public.fids, public.onchain_events, public.shuttle_projection_issues`.execute(db);
  });

  describe("migration 008", () => {
    const catalog = async (schema: string, table: string) => {
      const columns = await sql<{ name: string; type: string; nn: boolean; def: string | null }>`
        SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull AS nn,
          pg_get_expr(d.adbin, d.adrelid) AS def
        FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE n.nspname = ${schema} AND c.relname = ${table} AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum`.execute(db);
      const indexes = await sql<{ name: string; def: string }>`
        SELECT indexname AS name, indexdef AS def FROM pg_indexes
        WHERE schemaname = ${schema} AND tablename = ${table} ORDER BY indexname`.execute(db);
      return { columns: columns.rows.map((c) => [c.name, c.type, c.nn, c.def]), indexes: indexes.rows };
    };

    test("creates every business table exactly as the reference defines it", async () => {
      expect(referenceTables.map((t) => t.name)).toEqual([
        "casts",
        "channel_follows",
        "channel_members",
        "channels",
        "fids",
        "follow_counts",
        "follows",
        "neynar_user_scores",
        "profile_external_accounts",
        "profiles",
        "reactions",
        "signers",
        "storage_rentals",
        "tier_purchases",
        "user_labels",
        "usernames",
        "verifications",
      ]);
      for (const spec of referenceTables) {
        const actual = await catalog("public", spec.name);
        expect(actual.columns).toEqual(spec.columns);
        expect(Object.fromEntries(actual.indexes.map((i) => [i.name, normalizeIndexDef(i.def, "public")]))).toEqual(
          Object.fromEntries(Object.entries(spec.indexes).map(([name, def]) => [name, normalizeIndexDef(def, "x")])),
        );
      }
      const trgm = await sql<{ extname: string }>`SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'`.execute(
        db,
      );
      expect(trgm.rows).toHaveLength(1);
    });

    test("adopts compatible tables, adds missing indexes and rejects incompatible structure", async () => {
      const schema = "adopt_test";
      const fids = referenceTables.find((t) => t.name === "fids");
      const casts = referenceTables.find((t) => t.name === "casts");
      if (!fids || !casts) throw new Error("missing spec");
      await sql`DROP SCHEMA IF EXISTS adopt_test CASCADE`.execute(db);
      await sql`CREATE SCHEMA adopt_test`.execute(db);
      try {
        expect(await adoptReferenceTable(db, schema, fids, "public")).toBe("created");
        expect(await adoptReferenceTable(db, schema, fids, "public")).toBe("adopted");
        // gin_trgm_ops lives in public; the qualified opclass still matches the spec.
        expect(await adoptReferenceTable(db, schema, casts, "public")).toBe("created");
        expect(await adoptReferenceTable(db, schema, casts, "public")).toBe("adopted");

        // The Go repair's target-only flag column is allowed on fids.
        await sql`ALTER TABLE adopt_test.fids ADD COLUMN neynar_backfilled boolean NOT NULL DEFAULT false`.execute(db);
        await sql`DROP INDEX adopt_test.fids_timestamp_idx`.execute(db);
        expect(await adoptReferenceTable(db, schema, fids, "public")).toBe("adopted");
        expect((await catalog(schema, "fids")).indexes.map((i) => i.name)).toContain("fids_timestamp_idx");

        await sql`CREATE INDEX fids_extra_idx ON adopt_test.fids (created_at)`.execute(db);
        await expect(adoptReferenceTable(db, schema, fids, "public")).rejects.toThrow(
          "unexpected index fids_extra_idx",
        );
        await sql`DROP INDEX adopt_test.fids_extra_idx`.execute(db);

        await sql`ALTER TABLE adopt_test.fids ALTER COLUMN fid TYPE bigint`.execute(db);
        await expect(adoptReferenceTable(db, schema, fids, "public")).rejects.toThrow("column fid differs");
        await sql`ALTER TABLE adopt_test.fids ALTER COLUMN fid TYPE integer`.execute(db);

        await sql`ALTER TABLE adopt_test.fids ADD COLUMN surprise text`.execute(db);
        await expect(adoptReferenceTable(db, schema, fids, "public")).rejects.toThrow("unexpected column surprise");
      } finally {
        await sql`DROP SCHEMA IF EXISTS adopt_test CASCADE`.execute(db);
      }
    });
  });

  describe("fids projection", () => {
    const handler = { onHubEvent: App.prototype.onHubEvent, handleMessageMerge: App.prototype.handleMessageMerge };
    const dispatcher = new ProjectionDispatcher(projections, "public");
    const FID = 1000;
    const address = (n: number) => Buffer.alloc(20, n);
    const time = (block: number) => 1_700_000_000 + block * 2;
    let eventId = 0;

    const accountEvent = (opts: {
      block: number;
      log?: number;
      type: IdRegisterEventType;
      from?: number;
      to?: number;
      recovery?: number;
      fid?: number;
      chainId?: number;
    }): OnChainEvent =>
      OnChainEvent.create({
        type: OnChainEventType.EVENT_TYPE_ID_REGISTER,
        chainId: opts.chainId ?? 10,
        blockNumber: opts.block,
        blockHash: Buffer.alloc(32, 1),
        blockTimestamp: time(opts.block),
        transactionHash: Buffer.alloc(32, opts.block % 256),
        logIndex: opts.log ?? 0,
        fid: opts.fid ?? FID,
        txIndex: 0,
        version: 1,
        idRegisterEventBody: {
          eventType: opts.type,
          from: opts.from === undefined ? new Uint8Array() : address(opts.from),
          to: opts.to === undefined ? new Uint8Array() : address(opts.to),
          recoveryAddress: opts.recovery === undefined ? new Uint8Array() : address(opts.recovery),
        },
      });
    const register = (block: number, to: number, recovery: number, fid?: number) =>
      accountEvent({ block, type: IdRegisterEventType.REGISTER, to, recovery, fid });
    const transfer = (block: number, from: number, to: number) =>
      accountEvent({ block, type: IdRegisterEventType.TRANSFER, from, to });
    const changeRecovery = (block: number, recovery: number) =>
      accountEvent({ block, type: IdRegisterEventType.CHANGE_RECOVERY, recovery });

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

    const account = async (fid = FID) => {
      const rows = (
        await sql<{
          id: string;
          custody: string;
          recovery: string | null;
          timestamp: Date;
          registeredAt: Date | null;
          updatedAt: Date;
        }>`SELECT id, '0x' || encode(custody_address, 'hex') AS custody,
            '0x' || encode(recovery_address, 'hex') AS recovery,
            timestamp AT TIME ZONE 'UTC' AS timestamp, registered_at AT TIME ZONE 'UTC' AS registered_at,
            updated_at
          FROM public.fids WHERE fid = ${fid}`.execute(db)
      ).rows;
      expect(rows.length).toBeLessThanOrEqual(1);
      return rows[0];
    };
    const issues = async () =>
      (
        await sql<{ fid: number; reason: string; payload: unknown }>`
          SELECT fid, reason, payload FROM public.shuttle_projection_issues
          WHERE projection = 'fids' ORDER BY fid, reason`.execute(db)
      ).rows;
    const at = (block: number) => new Date(time(block) * 1000);
    const hex = (n: number) => bytesToHex(address(n));

    const history = [register(100, 1, 2), transfer(200, 1, 3), changeRecovery(300, 4)];
    const expected = {
      id: "411cde8d-ff17-5154-ba13-3fda5c0128b3", // Go: uuid.NewSHA1(NameSpaceURL, "farcaster-db-conversion:fids:1000")
      custody: hex(3),
      recovery: hex(4),
      timestamp: at(200), // the recovery change does not move the ownership time
      registeredAt: at(100),
    };

    test("a REGISTER creates the account with the Go-compatible id", async () => {
      await deliver(register(100, 1, 2));
      expect(await account()).toMatchObject({
        id: expected.id,
        custody: hex(1),
        recovery: hex(2),
        timestamp: at(100),
        registeredAt: at(100),
      });
      expect(await issues()).toEqual([]);
    });

    test("in-order, reversed, single-transaction and backfilled delivery converge", async () => {
      const results = [];
      for (const event of history) await deliver(event);
      results.push(await account());

      await sql`TRUNCATE public.fids, public.onchain_events`.execute(db);
      for (const event of [...history].reverse()) await deliver(event);
      results.push(await account());

      await sql`TRUNCATE public.fids, public.onchain_events`.execute(db);
      expect(await dispatcher.processMissingOnChainEvents(db, [...history].reverse())).toBe(3);
      results.push(await account());

      for (const result of results) expect(result).toMatchObject(expected);
      expect(await issues()).toEqual([]);
    });

    test("replays are stored once and leave the row untouched", async () => {
      for (const event of history) await deliver(event);
      const before = await account();
      await deliver(history[1] as OnChainEvent);
      expect(await db.selectFrom("onchain_events").select("id").execute()).toHaveLength(3);
      expect(await account()).toEqual(before);
    });

    test("without a REGISTER transfers give a partial row until the registration arrives", async () => {
      await deliver(transfer(200, 1, 3));
      expect(await account()).toMatchObject({
        custody: hex(3),
        recovery: null,
        timestamp: at(200),
        registeredAt: null,
      });
      expect(await issues()).toEqual([
        { fid: FID, reason: FID_ISSUES.pending, payload: { missingFields: ["registered_at", "recovery_address"] } },
      ]);
      await deliver(changeRecovery(300, 4));
      expect(await account()).toMatchObject({
        custody: hex(3),
        recovery: hex(4),
        timestamp: at(200),
        registeredAt: null,
      });
      expect(await issues()).toEqual([
        { fid: FID, reason: FID_ISSUES.pending, payload: { missingFields: ["registered_at"] } },
      ]);

      await deliver(register(100, 1, 2));
      expect(await account()).toMatchObject(expected);
      expect(await issues()).toEqual([]);
    });

    test("only a recovery change gives no custody, so the fid stays pending without a row", async () => {
      await deliver(changeRecovery(50, 6));
      expect(await account()).toBeUndefined();
      expect(await issues()).toEqual([
        {
          fid: FID,
          reason: FID_ISSUES.pending,
          payload: { missingFields: ["registered_at", "custody_address", "timestamp"] },
        },
      ]);
    });

    test("a partial account moves custody but never clears what an existing row knows", async () => {
      const now = new Date().toISOString();
      await appDb()
        .insertInto("fids")
        .values({
          id: expected.id,
          createdAt: now,
          updatedAt: now,
          fid: FID,
          custodyAddress: address(9),
          recoveryAddress: address(8),
          timestamp: at(50).toISOString(),
          registeredAt: at(10).toISOString(),
        })
        .execute();
      await deliver(transfer(200, 9, 3));
      expect(await account()).toMatchObject({
        custody: hex(3),
        recovery: hex(8),
        timestamp: at(200),
        registeredAt: at(10),
      });
      expect((await issues()).map((i) => i.reason)).toEqual([FID_ISSUES.pending]);
    });

    test("a REGISTER after a transfer is still an inconsistent history", async () => {
      await deliver(transfer(50, 1, 1));
      await deliver(register(100, 1, 2));
      expect((await issues()).map((i) => i.reason)).toEqual([FID_ISSUES.invalid]);
    });

    test("a stored row newer than the on-chain history is not moved backwards", async () => {
      const now = new Date().toISOString();
      await appDb()
        .insertInto("fids")
        .values({
          id: expected.id,
          createdAt: now,
          updatedAt: now,
          fid: FID,
          custodyAddress: address(7),
          recoveryAddress: address(2),
          timestamp: at(900).toISOString(),
          registeredAt: at(100).toISOString(),
        })
        .execute();
      const before = await account();
      await deliver(register(100, 1, 2));
      expect(await account()).toEqual(before);
      expect((await issues()).map((i) => i.reason)).toEqual([FID_ISSUES.stale]);
    });

    test("a custody gap follows the chain and is reported until the missing transfer arrives", async () => {
      await deliver(register(100, 1, 2));
      await deliver(transfer(200, 5, 3)); // `from` is not the current custody: a transfer is missing
      expect(await account()).toMatchObject({ custody: hex(3), recovery: hex(2), timestamp: at(200) });
      const gapIssue = {
        fid: FID,
        reason: FID_ISSUES.gap,
        payload: { gaps: [{ blockNumber: 200, logIndex: 0, expectedFrom: hex(1), actualFrom: hex(5) }] },
      };
      expect(await issues()).toEqual([gapIssue]);

      // A gap is not a failure: rebuild writes the row and keeps the issue.
      const result = await dispatcher.rebuild(db, { projections: ["fids"], fids: [FID] });
      expect(result).toMatchObject({ rebuilt: 1, failed: [] });
      expect(await issues()).toEqual([gapIssue]);

      await deliver(transfer(150, 1, 5));
      expect(await account()).toMatchObject({ custody: hex(3), timestamp: at(200) });
      expect(await issues()).toEqual([]);
    });

    test("a second REGISTER to the same custody is a re-registration", async () => {
      await deliver(register(100, 1, 2));
      await deliver(register(150, 1, 4));
      expect(await account()).toMatchObject({
        custody: hex(1),
        recovery: hex(4),
        timestamp: at(150),
        registeredAt: at(100),
      });
      expect(await issues()).toEqual([]);
    });

    test("a second REGISTER to another custody is an issue live and a failure on rebuild", async () => {
      await deliver(register(100, 1, 2));
      await deliver(register(150, 6, 4));
      expect(await db.selectFrom("onchain_events").select("id").execute()).toHaveLength(2);
      expect(await account()).toMatchObject({ custody: hex(1), timestamp: at(100) });
      expect(await issues()).toEqual([
        { fid: FID, reason: FID_ISSUES.invalid, payload: { error: "Inconsistent registration history" } },
      ]);

      const result = await dispatcher.rebuild(db, { projections: ["fids"], fids: [FID] });
      expect(result.rebuilt).toBe(0);
      expect(result.failed).toEqual([{ projection: "fids", fid: FID, error: "Inconsistent registration history" }]);
    });

    test("a REGISTER that is not the first event is an inconsistent history", async () => {
      await deliver(changeRecovery(50, 4));
      await deliver(register(100, 1, 2));
      expect(await account()).toBeUndefined();
      expect((await issues()).map((i) => i.reason)).toEqual([FID_ISSUES.invalid]);
    });

    test("invalid events are reported and not stored", async () => {
      await deliver(
        accountEvent({ block: 100, type: IdRegisterEventType.REGISTER, to: 1, recovery: 2, chainId: 8453 }),
      );
      await deliver(register(101, 1, 2, 2 ** 31));
      await deliver(accountEvent({ block: 102, type: IdRegisterEventType.REGISTER, to: 1 })); // no recovery
      expect(await db.selectFrom("onchain_events").select("id").execute()).toHaveLength(0);
      expect((await issues()).map((i) => i.reason)).toEqual([
        "ID_REGISTER event on chain 8453, expected 10",
        "Invalid recoveryAddress address for fid 1000",
        `fid ${2 ** 31} out of range`,
      ]);
    });

    test("on-chain events nobody projects are still stored by the generic path", async () => {
      await deliver(
        OnChainEvent.create({
          type: OnChainEventType.EVENT_TYPE_STORAGE_RENT,
          chainId: 10,
          blockNumber: 5,
          blockTimestamp: time(5),
          transactionHash: Buffer.alloc(32, 5),
          logIndex: 1,
          fid: FID,
          storageRentEventBody: { payer: address(1), units: 1, expiry: 1 },
        }),
      );
      expect(await db.selectFrom("onchain_events").select("type").execute()).toEqual([
        { type: OnChainEventType.EVENT_TYPE_STORAGE_RENT },
      ]);
      expect(await account()).toBeUndefined();
    });

    test("rebuild projects ID_REGISTER rows stored before the projection existed", async () => {
      // Insert in reverse chain order so onchain_events ids disagree with chain order.
      for (const event of [...history].reverse()) {
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
            body: {
              eventType: event.idRegisterEventBody?.eventType ?? 0,
              from: bytesToHex(event.idRegisterEventBody?.from ?? new Uint8Array()),
              to: bytesToHex(event.idRegisterEventBody?.to ?? new Uint8Array()),
              recoveryAddress: bytesToHex(event.idRegisterEventBody?.recoveryAddress ?? new Uint8Array()),
            },
          })
          .execute();
      }
      expect(await fidsProjection.rebuildFids(appDb(), 0, 10)).toEqual([FID]);
      expect(await dispatcher.rebuild(db, { projections: ["fids"] })).toEqual({ rebuilt: 1, failed: [] });
      expect(await account()).toMatchObject(expected);
      const before = await account();
      expect(await dispatcher.rebuild(db, { projections: ["fids"] })).toEqual({ rebuilt: 1, failed: [] });
      expect(await account()).toEqual(before);
    });

    test("projection-only backfill pulls the fid's ID_REGISTER history from the hub", async () => {
      await deliver(transfer(200, 1, 3)); // only part of the history was captured live
      const requested: OnChainEventType[] = [];
      const hub = {
        getOnChainEvents: async (req: { fid: number; eventType: OnChainEventType }) => {
          requested.push(req.eventType);
          return ok(
            OnChainEventResponse.create({
              events: req.eventType === OnChainEventType.EVENT_TYPE_ID_REGISTER ? history : [],
            }),
          );
        },
      };
      const messages = new MessageReconciliation({} as HubRpcClient, db, log);
      const onChain = new OnChainEventReconciliation(hub as unknown as HubRpcClient, db, log);

      const first = await backfillFidForProjections(messages, db, dispatcher, [fidsProjection], FID, onChain);
      expect(requested).toEqual([OnChainEventType.EVENT_TYPE_ID_REGISTER]);
      expect(first).toEqual({ missing: 2, projected: 2 });
      expect(await account()).toMatchObject(expected);
      expect(await issues()).toEqual([]);

      const second = await backfillFidForProjections(messages, db, dispatcher, [fidsProjection], FID, onChain);
      expect(second).toEqual({ missing: 0, projected: 0 });
    });
  });
});
