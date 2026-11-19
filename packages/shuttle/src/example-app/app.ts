import {
  DB,
  getDbClient,
  getHubClient,
  MessageHandler,
  StoreMessageOperation,
  MessageReconciliation,
  RedisClient,
  HubEventProcessor,
  EventStreamHubSubscriber,
  EventStreamConnection,
  HubEventStreamConsumer,
  HubSubscriber,
  MessageState,
} from "../index"; // If you want to use this as a standalone app, replace this import with "@dimensiondev/shuttle"
import { AppDb, migrateToLatest, pendingMigrations } from "./db";
import { bytesToHexString, HubEvent, GetInfoRequest, Message } from "@dimensiondev/hub-nodejs";
import { log } from "./log";
import { Command } from "@commander-js/extra-typings";
import { readFileSync } from "fs";
import {
  BACKFILL_FIDS,
  CONCURRENCY,
  HUB_HOST,
  HUB_SSL,
  MAX_FID,
  POSTGRES_URL,
  POSTGRES_SCHEMA,
  REDIS_URL,
  SHARD_INDEX,
  TOTAL_SHARDS,
  USE_STREAMING_RPCS_FOR_BACKFILL,
  SUBSCRIBE_RPC_TIMEOUT,
  STREAM_MAX_DELIVERIES,
  BACKFILL_PROJECTIONS_ONLY,
  REBUILD_PROJECTIONS,
} from "./env";
import * as process from "node:process";
import url from "node:url";
import { ok, Result } from "neverthrow";
import { enqueueBackfill, getQueue, getWorker } from "./worker";
import { Queue } from "bullmq";
import { backfillFidForProjections, onChainReconcileTypesFor, ProjectionDispatcher, projections } from "./projections";
import { OnChainEventReconciliation } from "../shuttle/onChainEventReconciliation";
import { runRebuild } from "./projections/runs";
import { storeOnChainEvent } from "./raw/onchainEvents";

const hubId = "shuttle";

// Keyed by schema; App methods are also invoked with a bare `this` in tests (schema undefined).
const dispatchers = new Map<string, ProjectionDispatcher>();
function dispatcherFor(schema: string | undefined): ProjectionDispatcher {
  const key = schema ?? "public";
  let dispatcher = dispatchers.get(key);
  if (!dispatcher) {
    dispatcher = new ProjectionDispatcher(projections, key);
    dispatchers.set(key, dispatcher);
  }
  return dispatcher;
}

export class App implements MessageHandler {
  private readonly db: DB;
  private readonly dbSchema: string;
  private hubSubscriber: HubSubscriber;
  private streamConsumer: HubEventStreamConsumer;
  public redis: RedisClient;
  private readonly hubId;

  constructor(
    db: DB,
    dbSchema: string,
    redis: RedisClient,
    hubSubscriber: HubSubscriber,
    streamConsumer: HubEventStreamConsumer,
  ) {
    this.db = db;
    this.dbSchema = dbSchema;
    this.redis = redis;
    this.hubSubscriber = hubSubscriber;
    this.hubId = hubId;
    this.streamConsumer = streamConsumer;
  }

  static create(
    dbUrl: string,
    dbSchema: string,
    redisUrl: string,
    hubUrl: string,
    totalShards: number,
    shardIndex: number,
    hubSSL = false,
  ) {
    const db = getDbClient(dbUrl, dbSchema);
    const hub = getHubClient(hubUrl, { ssl: hubSSL });
    const redis = RedisClient.create(redisUrl);
    const eventStreamForWrite = new EventStreamConnection(redis.client);
    const eventStreamForRead = new EventStreamConnection(redis.client);
    const shardKey = totalShards === 0 ? "all" : `${shardIndex}`;
    const hubSubscriber = new EventStreamHubSubscriber(
      hubId,
      hub,
      shardIndex,
      eventStreamForWrite,
      redis,
      shardKey,
      log,
      undefined,
      SUBSCRIBE_RPC_TIMEOUT,
    );
    const streamConsumer = new HubEventStreamConsumer(hub, eventStreamForRead, shardKey, {
      maxDeliveries: STREAM_MAX_DELIVERIES,
    });

    return new App(db, dbSchema, redis, hubSubscriber, streamConsumer);
  }

  get appDb(): AppDb {
    return this.db as unknown as AppDb;
  }

  async onHubEvent(event: HubEvent, txn: DB): Promise<boolean> {
    if (await dispatcherFor(this.dbSchema).processEvent(event, txn)) return true;
    await storeOnChainEvent(event, txn);
    return false;
  }

  async handleMessageMerge(
    message: Message,
    _txn: DB,
    operation: StoreMessageOperation,
    state: MessageState,
    isNew: boolean,
    wasMissed: boolean,
  ): Promise<void> {
    if (!isNew) {
      // Message was already in the db, no-op
      return;
    }

    const messageDesc = wasMissed ? `missed message (${operation})` : `message (${operation})`;
    log.info(`${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${message.data?.type})`);
  }

  async start() {
    await this.ensureMigrations();
    // Hub subscriber listens to events from the hub and writes them to a redis stream. This allows for scaling by
    // splitting events to multiple streams
    await this.hubSubscriber.start();

    // Sleep 10 seconds to give the subscriber a chance to create the stream for the first time.
    await new Promise((resolve) => setTimeout(resolve, 10_000));

    log.info("Starting stream consumer");
    // Stream consumer reads from the redis stream and inserts them into postgres
    await this.streamConsumer.start(async (event) => {
      await this.processHubEvent(event);
      return ok({ skipped: false });
    });
  }

  async reconcileFids(fids: number[], projectionsOnly = BACKFILL_PROJECTIONS_ONLY) {
    const reconciler = new MessageReconciliation(
      // biome-ignore lint/style/noNonNullAssertion: client is always initialized
      this.hubSubscriber.hubClient!,
      this.db,
      log,
      undefined,
    );
    // biome-ignore lint/style/noNonNullAssertion: client is always initialized
    const onChainReconciler = new OnChainEventReconciliation(this.hubSubscriber.hubClient!, this.db, log);
    if (projectionsOnly) {
      const dispatcher = dispatcherFor(this.dbSchema);
      for (const fid of fids) {
        const { missing, projected } = await backfillFidForProjections(
          reconciler,
          this.db,
          dispatcher,
          projections,
          fid,
          onChainReconciler,
        );
        if (projected > 0) log.info({ fid, missing, projected }, "Backfilled projected messages");
      }
      return;
    }
    for (const fid of fids) {
      // On-chain history the projections need (e.g. ID_REGISTER for fids) goes through the same pipeline,
      // first: projections that attribute messages to an app read the signers SIGNER events write.
      const onChainTypes = onChainReconcileTypesFor(projections);
      if (onChainTypes.length) {
        const missing: Parameters<ProjectionDispatcher["processMissingOnChainEvents"]>[1] = [];
        await onChainReconciler.reconcileOnChainEventsForFid(
          fid,
          async (event, missingInDb) => {
            if (missingInDb) missing.push(event);
          },
          { types: onChainTypes },
        );
        await dispatcherFor(this.dbSchema).processMissingOnChainEvents(this.db, missing);
      }
      await reconciler.reconcileMessagesForFid(
        fid,
        async (message, missingInDb, prunedInDb, revokedInDb) => {
          if (missingInDb) {
            const projected = await dispatcherFor(this.dbSchema).processMissingMessage(this.db, message);
            if (!projected) await HubEventProcessor.handleMissingMessage(this.db, message, this);
          } else if (prunedInDb || revokedInDb) {
            const messageDesc = prunedInDb ? "pruned" : revokedInDb ? "revoked" : "existing";
            log.info(`Reconciled ${messageDesc} message ${bytesToHexString(message.hash)._unsafeUnwrap()}`);
          }
        },
        async (message, missingInHub) => {
          if (missingInHub) {
            log.info(`Message ${bytesToHexString(message.hash)._unsafeUnwrap()} is missing in the hub`);
          }
        },
      );
    }
  }

  async backfillFids(fids: number[], backfillQueue: Queue) {
    let maxFid: number | undefined;
    if (fids.length === 0) {
      maxFid = MAX_FID ? parseInt(MAX_FID) : undefined;
      if (!maxFid) {
        const getInfoResult = await this.hubSubscriber.hubClient?.getInfo(GetInfoRequest.create({}));
        if (getInfoResult?.isErr()) {
          log.error("Failed to get max fid", getInfoResult.error);
          throw getInfoResult.error;
        } else {
          maxFid = getInfoResult?._unsafeUnwrap()?.dbStats?.numFidRegistrations;
          if (!maxFid) {
            log.error("Failed to get max fid");
            throw new Error("Failed to get max fid");
          }
        }
      }
      log.info(`Queuing up fids upto: ${maxFid}`);
    }
    const { runId, jobs } = await enqueueBackfill(this.db as unknown as AppDb, backfillQueue, {
      fids,
      maxFid,
      projectionsOnly: BACKFILL_PROJECTIONS_ONLY,
      projections: BACKFILL_PROJECTIONS_ONLY ? projections.map((p) => p.name) : [],
      trigger: "cli",
    });
    log.info({ runId, jobs }, "Backfill jobs queued");
  }

  private async processHubEvent(hubEvent: HubEvent) {
    await HubEventProcessor.processHubEvent(this.db, hubEvent, this);
  }

  async ensureMigrations() {
    const result = await migrateToLatest(this.db, this.dbSchema, log);
    if (result.isErr()) {
      log.error("Failed to migrate database", result.error);
      throw result.error;
    }
  }

  async stop() {
    this.hubSubscriber.stop();
    const lastEventId = await this.redis.getLastProcessedEvent(this.hubId);
    log.info(`Stopped at eventId: ${lastEventId}`);
  }
}

//If the module is being run directly, start the shuttle
if (import.meta.url.endsWith(url.pathToFileURL(process.argv[1] || "").toString())) {
  async function start() {
    log.info({ hub: HUB_HOST, schema: POSTGRES_SCHEMA }, "Creating shuttle app");
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    log.info("Starting shuttle");
    await app.start();
  }

  async function backfill() {
    log.info({ hub: HUB_HOST, schema: POSTGRES_SCHEMA }, "Creating shuttle app");
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    const fids = BACKFILL_FIDS ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid)) : [];
    log.info(`Backfilling fids: ${fids}`);
    const backfillQueue = getQueue(app.redis.client);
    await app.backfillFids(fids, backfillQueue);

    // Start the worker after initiating a backfill
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
    return;
  }

  async function worker() {
    log.info({ hub: HUB_HOST, schema: POSTGRES_SCHEMA }, "Starting worker");
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
  }

  async function rebuild() {
    const fids = BACKFILL_FIDS ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid)) : [];
    log.info(
      { schema: POSTGRES_SCHEMA, projections: REBUILD_PROJECTIONS, fids },
      "Rebuilding projections from raw tables",
    );
    const db = getDbClient(POSTGRES_URL, POSTGRES_SCHEMA);
    try {
      // Rebuild never migrates: the shards own migrations, deploy them first.
      const pending = await pendingMigrations(db, POSTGRES_SCHEMA, log);
      if (pending.length) throw new Error(`Pending migrations ${pending.join(", ")}; deploy the shards first`);
      const startedAt = Date.now();
      const { runId, rebuilt, failed } = await runRebuild(db, dispatcherFor(POSTGRES_SCHEMA), {
        projections: REBUILD_PROJECTIONS,
        fids,
        trigger: "cli",
        onStarted: (id) => log.info({ runId: id }, "Rebuild run started"),
        onFid: (projection, fid, count) => {
          if (count % 1000 === 0) log.info({ projection, fid, count }, "Rebuild progress");
        },
      });
      for (const failure of failed) log.error(failure, "Rebuild failed for fid");
      log.info({ runId, rebuilt, failed: failed.length, seconds: (Date.now() - startedAt) / 1000 }, "Rebuild finished");
      if (failed.length) globalThis.process.exitCode = 1;
    } finally {
      await db.destroy();
    }
  }

  // for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  //   process.on(signal, async () => {
  //     log.info(`Received ${signal}. Shutting down...`);
  //     (async () => {
  //       await sleep(10_000);
  //       log.info(`Shutdown took longer than 10s to complete. Forcibly terminating.`);
  //       process.exit(1);
  //     })();
  //     await app?.stop();
  //     process.exit(1);
  //   });
  // }

  const program = new Command()
    .name("shuttle")
    .description("Synchronizes a Farcaster Hub with a Postgres database")
    .version(JSON.parse(readFileSync("./package.json").toString()).version);

  program.command("start").description("Starts the shuttle").action(start);
  program.command("backfill").description("Queue up backfill for the worker").action(backfill);
  program.command("worker").description("Starts the backfill worker").action(worker);
  program
    .command("rebuild")
    .description("Rebuild projections from raw rows already in the DB (REBUILD_PROJECTIONS, FIDS)")
    .action(rebuild);

  program.parse(process.argv);
}
