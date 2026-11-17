import pg from "pg";
import Redis from "ioredis";

const db = new pg.Client({ connectionString: process.env.POSTGRES_URL, connectionTimeoutMillis: 5000 });
const redis = new Redis(process.env.REDIS_URL, {
  lazyConnect: true,
  connectTimeout: 5000,
  maxRetriesPerRequest: 1,
  retryStrategy: () => null,
});
redis.on("error", () => {});
try {
  await Promise.all([db.connect(), redis.connect()]);
  const tables = await db.query("select tablename from pg_tables where schemaname = 'public' order by tablename");
  const database = await db.query("select current_database() as name");
  const ping = await redis.ping();
  const hub = await fetch(`${process.env.HUB_HTTP_URL}/v1/info`, { signal: AbortSignal.timeout(5000) });
  if (!hub.ok) throw new Error(`Snapchain HTTP ${hub.status}`);
  console.log(JSON.stringify({ database: database.rows[0].name, tables: tables.rows.map(row => row.tablename), redis: ping, snapchain: "reachable" }, null, 2));
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  redis.disconnect();
  await db.end();
}
