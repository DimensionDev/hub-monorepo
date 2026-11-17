import { HubEvent, HubEventType } from "@dimensiondev/hub-nodejs";
import { err, ok } from "neverthrow";
import { pino } from "pino";
import { EventStreamConnection, HubEventStreamConsumer } from "./eventStream";
import { HubClient } from "./hub";
import { getRedisClient } from "./redis";
import { Redis } from "ioredis";

// Use a dedicated db so other integration tests calling FLUSHDB on db 0 don't interfere
const REDIS_URL = process.env["EVENT_STREAM_TEST_REDIS_URL"] || "redis://localhost:16379/15";
const GROUP = "hub_events";
const DAY = 24 * 60 * 60 * 1000;

const silentLog = pino({ level: "silent" });

let redis: Redis;
let stream: EventStreamConnection;
let host: string;

const hubFor = (h: string) => ({ host: h }) as unknown as HubClient;

const encodeEvent = (id: number) =>
  Buffer.from(HubEvent.encode(HubEvent.create({ id, type: HubEventType.MERGE_MESSAGE })).finish());

const cleanup = async () => {
  const keys = await redis.keys(`hub:${host}:*`);
  if (keys.length) await redis.del(...keys);
};

beforeAll(async () => {
  redis = getRedisClient(REDIS_URL);
  stream = new EventStreamConnection(redis);
  await stream.waitUntilReady();
});

beforeEach(() => {
  host = `event-stream-test-${Date.now()}-${Math.random().toString(36).slice(2)}`;
});

afterEach(async () => {
  await cleanup();
});

afterAll(async () => {
  await redis.quit();
});

describe("HubEventStreamConsumer dead-letter stream", () => {
  test("an always-failing event is moved to :dlq and acked after maxDeliveries", async () => {
    const maxDeliveries = 3;
    const consumer = new HubEventStreamConsumer(
      hubFor(host),
      stream,
      "all",
      { maxDeliveries, eventProcessingTimeout: 20 },
      silentLog,
    );
    const eventBytes = encodeEvent(42);
    const streamId = (await stream.add(consumer.streamKey, eventBytes)) as string;

    let calls = 0;
    await consumer.start(async () => {
      calls++;
      return err(new Error("boom"));
    });

    try {
      const deadline = Date.now() + 10_000;
      while ((await redis.xlen(consumer.deadLetterKey)) === 0 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 20));
      }
    } finally {
      consumer.stop();
    }
    // Let the run loop settle so no further deliveries are in flight
    await new Promise((r) => setTimeout(r, 100));

    expect(calls).toBe(maxDeliveries);

    const dlq = await redis.xrangeBuffer(consumer.deadLetterKey, "-", "+");
    expect(dlq).toHaveLength(1);
    const fields = new Map<string, Buffer>();
    const flat = (dlq[0]?.[1] ?? []) as Buffer[];
    for (let i = 0; i + 1 < flat.length; i += 2) fields.set(String(flat[i]), flat[i + 1] as Buffer);
    expect(fields.get("d")?.equals(eventBytes)).toBe(true);
    expect(fields.get("err")?.toString()).toBe("boom");
    expect(fields.get("id")?.toString()).toBe(streamId);

    const summary = await stream.pendingSummary(consumer.streamKey, GROUP);
    expect(summary.count).toBe(0);
  });

  test("a stale event that succeeds within the limit is acked and not dead-lettered", async () => {
    const consumer = new HubEventStreamConsumer(
      hubFor(host),
      stream,
      "all",
      { maxDeliveries: 3, eventProcessingTimeout: 20 },
      silentLog,
    );
    await stream.createGroup(consumer.streamKey, GROUP);
    await stream.add(consumer.streamKey, encodeEvent(1));
    await stream.reserve(consumer.streamKey, GROUP, 10); // delivery 1, never acked
    await new Promise((r) => setTimeout(r, 50));

    const processed = await consumer.processStale(async () => ok({ skipped: false }));
    expect(processed).toBe(1);
    expect(await redis.exists(consumer.deadLetterKey)).toBe(0);
    expect((await stream.pendingSummary(consumer.streamKey, GROUP)).count).toBe(0);
  });
});

describe("HubEventStreamConsumer.clearOldEvents", () => {
  const addOldEntries = async (key: string, start: number, n: number, firstMs: number) => {
    const pipeline = redis.pipeline();
    const ids: string[] = [];
    for (let i = start; i < start + n; i++) {
      const id = `${firstMs + i}-0`;
      ids.push(id);
      pipeline.xadd(key, id, "d", encodeEvent(i));
    }
    await pipeline.exec();
    return ids;
  };

  test("never removes a pending entry older than the threshold", async () => {
    const consumer = new HubEventStreamConsumer(hubFor(host), stream, "all", {}, silentLog);
    const key = consumer.streamKey;
    const oldMs = Date.now() - 2 * DAY;

    const ids = await addOldEntries(key, 0, 500, oldMs);
    await stream.createGroup(key, GROUP);
    const reserved = await stream.reserve(key, GROUP, 500);
    expect(reserved).toHaveLength(500);

    const pendingId = ids[450] as string;
    await stream.ack(
      key,
      GROUP,
      ids.filter((id) => id !== pendingId),
    );

    await consumer.clearOldEvents();

    // Entries before the pending one were trimmed (so the trim actually ran)...
    expect(await redis.xlen(key)).toBeLessThan(500);
    // ...but the pending entry and everything after it survived
    expect(await redis.xrange(key, pendingId, pendingId)).toHaveLength(1);
    expect(await redis.xrange(key, pendingId, "+")).toHaveLength(50);
    expect((await stream.pendingSummary(key, GROUP)).oldestId).toBe(pendingId);
  });

  test("never removes old entries the group hasn't read yet", async () => {
    const consumer = new HubEventStreamConsumer(hubFor(host), stream, "all", {}, silentLog);
    const key = consumer.streamKey;
    const oldMs = Date.now() - 2 * DAY;

    const ids = await addOldEntries(key, 0, 500, oldMs);
    await stream.createGroup(key, GROUP);
    const reserved = await stream.reserve(key, GROUP, 300);
    await stream.ack(
      key,
      GROUP,
      reserved.map(({ id }) => id),
    );

    await consumer.clearOldEvents();

    expect(await redis.xlen(key)).toBeLessThan(500);
    expect(await redis.xrange(key, ids[300] as string, "+")).toHaveLength(200);
  });

  test("still trims everything older than the threshold when nothing is pending", async () => {
    const consumer = new HubEventStreamConsumer(hubFor(host), stream, "all", {}, silentLog);
    const key = consumer.streamKey;
    const ids = await addOldEntries(key, 0, 500, Date.now() - 2 * DAY);
    await stream.createGroup(key, GROUP);
    await stream.reserve(key, GROUP, 500);
    await stream.ack(key, GROUP, ids);
    await stream.add(key, encodeEvent(999)); // recent, unread entry

    await consumer.clearOldEvents();

    // The unread recent entry bounds MINID above all old entries; approximate trim may leave a partial node
    expect(await redis.xlen(key)).toBeLessThan(200);
  });
});
