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
  SignerEventType,
} from "@dimensiondev/hub-nodejs";
import { ok } from "neverthrow";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import type { AppDb } from "./example-app/db";
import { backfillFidForProjections, ProjectionDispatcher, projections } from "./example-app/projections";
import { applicationFid, SIGNER_ISSUES, signersProjection } from "./example-app/projections/signers";
import { MessageReconciliation } from "./shuttle/messageReconciliation";
import { OnChainEventReconciliation } from "./shuttle/onChainEventReconciliation";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";
import { dropTestDatabase } from "./testDatabase";
import { signedKeyMetadata } from "./testSigners";
import { bytesToHex } from "./utils";

describe("applicationFid", () => {
  test("reads requestFid from the tuple, not the leading offset word", () => {
    expect(applicationFid(bytesToHex(signedKeyMetadata(9152)))._unsafeUnwrap()).toBe(9152);
  });

  test("rejects malformed metadata", () => {
    const good = signedKeyMetadata(9152);
    expect(applicationFid("0x").isErr()).toBe(true);
    expect(applicationFid("not hex").isErr()).toBe(true);
    expect(applicationFid(bytesToHex(signedKeyMetadata(0))).isErr()).toBe(true);
    const shifted = Buffer.from(good);
    shifted.writeUInt8(64, 31); // offset word != 32
    expect(applicationFid(bytesToHex(shifted)).isErr()).toBe(true);
    const padded = Buffer.from(good);
    padded[32 + 32] = 1; // requestSigner padding
    expect(applicationFid(bytesToHex(padded)).isErr()).toBe(true);
    expect(applicationFid(bytesToHex(good.subarray(0, good.length - 32))).isErr()).toBe(true);
  });
});

// Never fall back to the application's production connection or dotenv configuration.
const testUrl = process.env["TEST_DATABASE_URL"];
const integration = testUrl ? describe : describe.skip;

integration("signers projection", () => {
  const database = `signers_test_${randomUUID().replaceAll("-", "")}`;
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
    await sql`TRUNCATE public.signers, public.onchain_events, public.shuttle_projection_issues`.execute(db);
  });

  const handler = { onHubEvent: App.prototype.onHubEvent, handleMessageMerge: App.prototype.handleMessageMerge };
  const dispatcher = new ProjectionDispatcher(projections, "public");
  const FID = 1000;
  const KEY = Buffer.alloc(32, 0x11);
  const time = (block: number) => 1_800_000_000 + block * 2;
  const at = (block: number) => new Date(time(block) * 1000);
  let eventId = 0;

  const signerEvent = (opts: {
    block: number;
    type: SignerEventType;
    key?: Buffer;
    app?: number;
    metadata?: Buffer;
    metadataType?: number;
    keyType?: number;
    fid?: number;
    chainId?: number;
  }): OnChainEvent =>
    OnChainEvent.create({
      type: OnChainEventType.EVENT_TYPE_SIGNER,
      chainId: opts.chainId ?? 10,
      blockNumber: opts.block,
      blockHash: Buffer.alloc(32, 1),
      blockTimestamp: time(opts.block),
      transactionHash: Buffer.alloc(32, opts.block % 256),
      logIndex: 0,
      fid: opts.fid ?? FID,
      txIndex: 0,
      version: 1,
      signerEventBody: {
        key: opts.key ?? KEY,
        keyType: opts.keyType ?? 1,
        eventType: opts.type,
        metadata:
          opts.type === SignerEventType.ADD ? opts.metadata ?? signedKeyMetadata(opts.app ?? 9152) : new Uint8Array(),
        metadataType: opts.type === SignerEventType.ADD ? opts.metadataType ?? 1 : 0,
      },
    });
  const add = (block: number, app = 9152, key = KEY) => signerEvent({ block, type: SignerEventType.ADD, app, key });
  const remove = (block: number, key = KEY) => signerEvent({ block, type: SignerEventType.REMOVE, key });
  const reset = (block: number, key = KEY) => signerEvent({ block, type: SignerEventType.ADMIN_RESET, key });

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

  const signers = async () =>
    (
      await sql<{
        id: string;
        fid: number;
        signer: string;
        appFid: number;
        timestamp: Date;
        deletedAt: Date | null;
        updatedAt: Date;
      }>`SELECT id, fid, encode(signer, 'hex') AS signer, app_fid, timestamp AT TIME ZONE 'UTC' AS timestamp,
          deleted_at AT TIME ZONE 'UTC' AS deleted_at, updated_at
        FROM public.signers ORDER BY signer`.execute(db)
    ).rows.map((r) => ({ ...r, fid: Number(r.fid), appFid: Number(r.appFid) }));
  const issues = async () =>
    (
      await sql<{ reason: string; payload: { detail?: string } }>`
        SELECT reason, payload FROM public.shuttle_projection_issues
        WHERE projection = 'signers' ORDER BY reason`.execute(db)
    ).rows;

  test("an ADD creates the signer with the decoded app fid and the Go-compatible id", async () => {
    await deliver(add(100));
    expect(await signers()).toEqual([
      expect.objectContaining({
        // Go: uuid.NewSHA1(NameSpaceURL, "farcaster-db-conversion:signers:1000:<11 x32>")
        id: "a3ee18a0-05e5-5931-a66d-ba771ebc699a",
        fid: FID,
        signer: "11".repeat(32),
        appFid: 9152,
        timestamp: at(100),
        deletedAt: null,
      }),
    ]);
    expect(await issues()).toEqual([]);
  });

  test("REMOVE soft-deletes, ADMIN_RESET restores the original ADD, a later ADD re-registers", async () => {
    await deliver(add(100, 9152));
    await deliver(remove(200));
    expect((await signers())[0]).toMatchObject({ appFid: 9152, timestamp: at(100), deletedAt: at(200) });
    await deliver(add(250, 1)); // ignored: the key was removed
    expect((await signers())[0]).toMatchObject({ appFid: 9152, deletedAt: at(200) });
    await deliver(reset(300));
    expect((await signers())[0]).toMatchObject({ appFid: 9152, timestamp: at(100), deletedAt: null });
    await deliver(add(400, 7));
    expect((await signers())[0]).toMatchObject({ appFid: 7, timestamp: at(400), deletedAt: null });
  });

  test("any delivery order converges, and replays leave the row untouched", async () => {
    const history = [add(100), remove(200), reset(300)];
    for (const event of history) await deliver(event);
    const before = await signers();
    await deliver(history[1] as OnChainEvent);
    expect(await signers()).toEqual(before);

    await sql`TRUNCATE public.signers, public.onchain_events`.execute(db);
    expect(await dispatcher.processMissingOnChainEvents(db, [...history].reverse())).toBe(3);
    expect((await signers()).map(({ updatedAt, ...r }) => r)).toEqual(before.map(({ updatedAt, ...r }) => r));
  });

  test("a REMOVE without its ADD is pending until the ADD arrives", async () => {
    await deliver(remove(200));
    expect(await signers()).toEqual([]);
    expect((await issues()).map((i) => i.reason)).toEqual([SIGNER_ISSUES.pending]);
    await deliver(add(100));
    expect((await signers())[0]).toMatchObject({ appFid: 9152, deletedAt: at(200) });
    expect(await issues()).toEqual([]);
  });

  test("an ADD whose app fid cannot be read is an issue, not a row", async () => {
    await deliver(signerEvent({ block: 100, type: SignerEventType.ADD, metadata: Buffer.alloc(64) }));
    await deliver(signerEvent({ block: 101, type: SignerEventType.ADD, key: Buffer.alloc(32, 2), metadataType: 2 }));
    expect(await signers()).toEqual([]);
    expect((await issues()).map((i) => [i.reason, i.payload.detail])).toEqual([
      [SIGNER_ISSUES.app, "metadata is not a canonical signed-key ABI tuple"],
      [SIGNER_ISSUES.app, "unsupported metadataType 2"],
    ]);
  });

  test("keyType 2 keys are left out, as in the reference", async () => {
    await deliver(signerEvent({ block: 100, type: SignerEventType.ADD, keyType: 2 }));
    await deliver(remove(200));
    expect(await signers()).toEqual([]);
    expect(await issues()).toEqual([]);
  });

  test("invalid events are issues and are not stored", async () => {
    await deliver(signerEvent({ block: 100, type: SignerEventType.ADD, chainId: 8453 }));
    await deliver(signerEvent({ block: 101, type: SignerEventType.ADD, key: Buffer.alloc(20, 1) }));
    expect(await db.selectFrom("onchain_events").select("id").execute()).toHaveLength(0);
    expect((await issues()).map((i) => i.reason)).toEqual([
      `Expected a 32-byte signer key for fid ${FID} log 0`,
      `SIGNER event on chain 8453, expected 10, for fid ${FID} log 0`,
    ]);
  });

  test("rebuild projects SIGNER rows stored before the projection existed", async () => {
    for (const event of [add(100), remove(200)]) {
      const body = event.signerEventBody as NonNullable<typeof event.signerEventBody>;
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
            eventType: body.eventType,
            key: bytesToHex(body.key),
            keyType: body.keyType,
            metadata: bytesToHex(body.metadata),
            metadataType: body.metadataType,
          },
        })
        .execute();
    }
    expect(await signersProjection.rebuildFids(appDb(), 0, 10)).toEqual([FID]);
    expect(await dispatcher.rebuild(db, { projections: ["signers"] })).toEqual({ rebuilt: 1, failed: [] });
    expect((await signers())[0]).toMatchObject({ appFid: 9152, timestamp: at(100), deletedAt: at(200) });
  });

  test("projection-only backfill pulls SIGNER history from the hub", async () => {
    const history = [add(100), remove(200)];
    const requested: OnChainEventType[] = [];
    const hub = {
      getOnChainEvents: async (req: { fid: number; eventType: OnChainEventType }) => {
        requested.push(req.eventType);
        return ok(
          OnChainEventResponse.create({ events: req.eventType === OnChainEventType.EVENT_TYPE_SIGNER ? history : [] }),
        );
      },
    };
    const messages = new MessageReconciliation({} as HubRpcClient, db, log);
    const onChain = new OnChainEventReconciliation(hub as unknown as HubRpcClient, db, log);
    const result = await backfillFidForProjections(messages, db, dispatcher, [signersProjection], FID, onChain);
    expect(requested).toEqual([OnChainEventType.EVENT_TYPE_SIGNER]);
    expect(result).toEqual({ missing: 2, projected: 2 });
    expect((await signers())[0]).toMatchObject({ appFid: 9152, deletedAt: at(200) });
  });
});
