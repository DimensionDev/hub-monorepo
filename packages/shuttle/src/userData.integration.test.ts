import { randomUUID } from "node:crypto";
import pg from "pg";
import { sql } from "kysely";
import {
  FarcasterNetwork,
  HubEvent,
  HubEventType,
  Message,
  MessageType,
  OnChainEvent,
  OnChainEventType,
  Protocol,
  SignerEventType,
  UserDataType,
} from "@dimensiondev/hub-nodejs";
import { App } from "./example-app/app";
import { migrateToLatest } from "./example-app/db";
import { ProjectionDispatcher, projections } from "./example-app/projections";
import { ACCOUNT_ISSUE, base58Decode, coordinates } from "./example-app/projections/userData";
import { log } from "./log";
import { DB, getDbClient, HubEventProcessor } from "./shuttle";
import { signedKeyMetadata } from "./testSigners";
import { dropTestDatabase } from "./testDatabase";
import { farcasterTimeToDate } from "./utils";

describe("user data helpers", () => {
  test("base58 decodes Solana addresses", () => {
    expect(base58Decode("11111111111111111111111111111111")?.toString("hex")).toBe("00".repeat(32));
    expect(base58Decode("558DZt4C7CA8DuGUsCAEA98vjvHKvo5ujTwr7GMwjquq")?.toString("hex")).toBe(
      "3c7d3b1976a98be1118033f3303c97cb13da3ba64cddf388be5832a75fca00e8",
    );
    expect(base58Decode("0OIl")).toBeUndefined();
  });

  test("geo locations parse to coordinates", () => {
    expect(coordinates("geo:34.05,-118.24")).toEqual({ latitude: 34.05, longitude: -118.24 });
    expect(coordinates("Los Angeles")).toBeNull();
    expect(coordinates("geo:91,0")).toBeNull();
  });
});

// Never fall back to the application's production connection or dotenv configuration.
const testUrl = process.env["TEST_DATABASE_URL"];
const integration = testUrl ? describe : describe.skip;

integration("verifications, profiles and profile_external_accounts", () => {
  const database = `userdata_test_${randomUUID().replaceAll("-", "")}`;
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
    await sql`TRUNCATE public.messages, public.onchain_events, public.signers, public.verifications,
      public.profiles, public.profile_external_accounts, public.shuttle_message_state,
      public.shuttle_projection_issues`.execute(db);
  });

  const FID = 100;
  const KEY = Buffer.alloc(32, 0x22);
  const at = (timestamp: number) => farcasterTimeToDate(timestamp);
  let eventId = 0;
  const processEvent = (event: HubEvent) =>
    HubEventProcessor.processHubEvent(db, event, {
      onHubEvent: App.prototype.onHubEvent,
      handleMessageMerge: App.prototype.handleMessageMerge,
    });
  const merge = (message: Message, deletedMessages: Message[] = []) =>
    processEvent(
      HubEvent.create({
        id: ++eventId,
        shardIndex: 1,
        timestamp: 2000 + eventId,
        type: HubEventType.MERGE_MESSAGE,
        mergeMessageBody: { message, deletedMessages },
      }),
    );
  const revoke = (message: Message) =>
    processEvent(
      HubEvent.create({
        id: ++eventId,
        shardIndex: 1,
        timestamp: 2000 + eventId,
        type: HubEventType.REVOKE_MESSAGE,
        revokeMessageBody: { message },
      }),
    );
  const addSigner = (app: number) =>
    processEvent(
      HubEvent.create({
        id: ++eventId,
        shardIndex: 1,
        timestamp: 2000 + eventId,
        type: HubEventType.MERGE_ON_CHAIN_EVENT,
        mergeOnChainEventBody: {
          onChainEvent: OnChainEvent.create({
            type: OnChainEventType.EVENT_TYPE_SIGNER,
            chainId: 10,
            blockNumber: 50,
            blockHash: Buffer.alloc(32, 1),
            blockTimestamp: 1_700_000_000,
            transactionHash: Buffer.alloc(32, 9),
            logIndex: 0,
            fid: FID,
            txIndex: 0,
            version: 1,
            signerEventBody: {
              key: KEY,
              keyType: 1,
              eventType: SignerEventType.ADD,
              metadata: signedKeyMetadata(app),
              metadataType: 1,
            },
          }),
        },
      }),
    );

  const message = (
    type: MessageType,
    timestamp: number,
    hash: number,
    body: Record<string, unknown>,
    signer: Buffer = KEY,
  ) =>
    Message.create({
      data: { fid: FID, type, timestamp, network: FarcasterNetwork.MAINNET, ...body },
      hash: Buffer.alloc(20, hash),
      signer,
    });
  const ETH = Buffer.alloc(20, 0xab);
  const verify = (timestamp: number, hash: number, address: Buffer = ETH, protocol = Protocol.ETHEREUM) =>
    message(MessageType.VERIFICATION_ADD_ETH_ADDRESS, timestamp, hash, {
      verificationAddAddressBody: { address, protocol, claimSignature: Buffer.alloc(65), blockHash: Buffer.alloc(32) },
    });
  const unverify = (timestamp: number, hash: number, address: Buffer = ETH, protocol = Protocol.ETHEREUM) =>
    message(MessageType.VERIFICATION_REMOVE, timestamp, hash, { verificationRemoveBody: { address, protocol } });
  const userData = (type: UserDataType, value: string, timestamp: number, hash: number, signer?: Buffer) =>
    message(MessageType.USER_DATA_ADD, timestamp, hash, { userDataBody: { type, value } }, signer);

  const verifications = async () =>
    (
      await sql<{ address: string; protocol: number; appFid: number | null; timestamp: Date; deletedAt: Date | null }>`
        SELECT encode(address, 'hex') AS address, protocol, app_fid, timestamp AT TIME ZONE 'UTC' AS timestamp,
          deleted_at AT TIME ZONE 'UTC' AS deleted_at FROM public.verifications ORDER BY address`.execute(db)
    ).rows;
  const profile = async () =>
    (await sql<Record<string, unknown>>`SELECT * FROM public.profiles WHERE fid = ${FID}`.execute(db)).rows[0];
  const accounts = async () =>
    (
      await sql<{ type: number; account: string; appFid: number; timestamp: Date; deletedAt: Date | null }>`
        SELECT type, account, app_fid, timestamp AT TIME ZONE 'UTC' AS timestamp,
          deleted_at AT TIME ZONE 'UTC' AS deleted_at FROM public.profile_external_accounts ORDER BY type`.execute(db)
    ).rows;

  describe("verifications", () => {
    test("an ADD is live; a later REMOVE soft-deletes; a newer ADD revives", async () => {
      await addSigner(9152);
      const add = verify(1000, 1);
      await merge(add);
      expect(await verifications()).toEqual([
        { address: "ab".repeat(20), protocol: 0, appFid: 9152, timestamp: at(1000), deletedAt: null },
      ]);
      const remove = unverify(1100, 2);
      await merge(remove, [add]);
      expect(await verifications()).toEqual([expect.objectContaining({ timestamp: at(1000), deletedAt: at(1100) })]);
      await merge(verify(1200, 3), [remove]);
      expect(await verifications()).toEqual([expect.objectContaining({ timestamp: at(1200), deletedAt: null })]);
    });

    test("Solana addresses keep their 32 bytes and protocol 1", async () => {
      const sol = Buffer.alloc(32, 0x5a);
      await merge(verify(1000, 1, sol, Protocol.SOLANA));
      expect(await verifications()).toEqual([expect.objectContaining({ address: "5a".repeat(32), protocol: 1 })]);
    });

    test("app_fid is NULL until the signer arrives, then filled in", async () => {
      await merge(verify(1000, 1));
      expect((await verifications())[0]?.appFid).toBeNull();
      await addSigner(9152);
      expect(Number((await verifications())[0]?.appFid)).toBe(9152);
    });

    test("a revoked ADD soft-deletes at the revoke time", async () => {
      const add = verify(1000, 1);
      await merge(add);
      await revoke(add);
      const [row] = await verifications();
      expect(row?.deletedAt).not.toBeNull();
    });

    test("a REMOVE with no ADD on record writes nothing; addresses of the wrong length are rejected", async () => {
      await merge(unverify(1000, 1));
      await merge(verify(1000, 2, Buffer.alloc(19, 1)));
      expect(await verifications()).toEqual([]);
      const issues = await sql<{ reason: string }>`SELECT reason FROM public.shuttle_projection_issues
        WHERE projection = 'verifications'`.execute(db);
      expect(issues.rows.map((r) => r.reason)).toEqual([`Invalid Ethereum address for fid ${FID}`]);
    });
  });

  describe("profiles", () => {
    test("each field takes its latest value; unset stays NULL and an explicit empty string stays", async () => {
      await merge(userData(UserDataType.DISPLAY, "Old", 1000, 1));
      await merge(userData(UserDataType.DISPLAY, "Dan", 1100, 2));
      await merge(userData(UserDataType.BIO, "", 1000, 3));
      await merge(userData(UserDataType.LOCATION, "geo:34.05,-118.24", 1000, 4));
      await merge(userData(UserDataType.USER_DATA_PRIMARY_ADDRESS_ETHEREUM, `0x${"cd".repeat(20)}`, 1000, 5));
      await merge(
        userData(
          UserDataType.USER_DATA_PRIMARY_ADDRESS_SOLANA,
          "558DZt4C7CA8DuGUsCAEA98vjvHKvo5ujTwr7GMwjquq",
          1000,
          6,
        ),
      );
      const row = await profile();
      expect(row).toMatchObject({
        displayName: "Dan",
        bio: "",
        pfpUrl: null,
        location: "geo:34.05,-118.24",
        deletedAt: null,
      });
      expect(row?.["latitude"]).toBeCloseTo(34.05);
      expect(row?.["longitude"]).toBeCloseTo(-118.24);
      expect(Buffer.from(row?.["primaryEthAddress"] as Uint8Array).toString("hex")).toBe("cd".repeat(20));
      expect(Buffer.from(row?.["primarySolAddress"] as Uint8Array).toString("hex")).toBe(
        "3c7d3b1976a98be1118033f3303c97cb13da3ba64cddf388be5832a75fca00e8",
      );
    });

    test("an older message arriving late does not override a newer one", async () => {
      await merge(userData(UserDataType.PFP, "new", 1100, 2));
      await merge(userData(UserDataType.PFP, "old", 1000, 1));
      expect((await profile())?.["pfpUrl"]).toBe("new");
    });

    test("a revoked value is unset", async () => {
      const pfp = userData(UserDataType.PFP, "x", 1000, 1);
      await merge(pfp);
      await merge(userData(UserDataType.DISPLAY, "Dan", 1000, 2));
      await revoke(pfp);
      expect(await profile()).toMatchObject({ pfpUrl: null, displayName: "Dan" });
    });
  });

  describe("profile_external_accounts", () => {
    test("needs the signer's app fid: an issue until the SIGNER arrives, then the row", async () => {
      await merge(userData(UserDataType.TWITTER, "dwr", 1000, 1));
      expect(await accounts()).toEqual([]);
      const pending = await sql<{ reason: string }>`SELECT reason FROM public.shuttle_projection_issues
        WHERE projection = 'profile_external_accounts'`.execute(db);
      expect(pending.rows.map((r) => r.reason)).toEqual([ACCOUNT_ISSUE]);
      await addSigner(9152);
      expect(await accounts()).toEqual([
        { type: UserDataType.TWITTER, account: "dwr", appFid: 9152, timestamp: at(1000), deletedAt: null },
      ]);
      const cleared = await sql`SELECT 1 FROM public.shuttle_projection_issues
        WHERE projection = 'profile_external_accounts'`.execute(db);
      expect(cleared.rows).toEqual([]);
    });

    test("a change signed by an unknown key still updates the row, keeping its app fid", async () => {
      await addSigner(9152);
      const first = userData(UserDataType.TWITTER, "dwr", 1000, 1);
      await merge(first);
      await merge(userData(UserDataType.TWITTER, "", 1100, 2, Buffer.alloc(32, 0x77)), [first]);
      expect(await accounts()).toEqual([
        { type: UserDataType.TWITTER, account: "", appFid: 9152, timestamp: at(1100), deletedAt: at(1100) },
      ]);
      const pending = await sql<{ reason: string }>`SELECT reason FROM public.shuttle_projection_issues
        WHERE projection = 'profile_external_accounts'`.execute(db);
      expect(pending.rows.map((r) => r.reason)).toEqual([ACCOUNT_ISSUE]);
    });

    test("one row per type; a new value replaces the account and an empty value soft-deletes", async () => {
      await addSigner(9152);
      const first = userData(UserDataType.TWITTER, "dwr", 1000, 1);
      await merge(first);
      const second = userData(UserDataType.TWITTER, "dwr2", 1100, 2);
      await merge(second, [first]);
      await merge(userData(UserDataType.GITHUB, "danromero", 1000, 3));
      expect((await accounts()).map((a) => [a.type, a.account, a.deletedAt])).toEqual([
        [UserDataType.TWITTER, "dwr2", null],
        [UserDataType.GITHUB, "danromero", null],
      ]);
      await merge(userData(UserDataType.TWITTER, "", 1200, 4), [second]);
      expect((await accounts())[0]).toMatchObject({ account: "", timestamp: at(1200), deletedAt: at(1200) });
    });
  });

  test("rebuild projects stored messages and signers, in registration order", async () => {
    await addSigner(9152);
    await merge(verify(1000, 1));
    await merge(userData(UserDataType.TWITTER, "dwr", 1000, 2));
    await merge(userData(UserDataType.DISPLAY, "Dan", 1000, 3));
    await sql`TRUNCATE public.signers, public.verifications, public.profiles, public.profile_external_accounts`.execute(
      db,
    );
    const dispatcher = new ProjectionDispatcher(projections, "public");
    const result = await dispatcher.rebuild(db, {
      projections: ["signers", "verifications", "profiles", "profile_external_accounts"],
    });
    expect(result.failed).toEqual([]);
    expect(Number((await verifications())[0]?.appFid)).toBe(9152);
    expect((await accounts())[0]?.account).toBe("dwr");
    expect((await profile())?.["displayName"]).toBe("Dan");
  });
});
