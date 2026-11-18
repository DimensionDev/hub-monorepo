import type pg from "pg";

/**
 * Drops a per-test database after its Kysely client was destroyed.
 *
 * pg-pool's `end()` resolves before its clients' sockets are closed, so a forced drop right away
 * can terminate backends that are still shutting down; the server's 57P01 then surfaces as an
 * unhandled pool error. Wait (briefly) for the backends to go away first; FORCE stays as the
 * fallback for anything that really leaked.
 */
export async function dropTestDatabase(admin: pg.Client, database: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { rows } = await admin.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname = $1",
      [database],
    );
    if (!rows[0]?.n) break;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  await admin.query(`DROP DATABASE IF EXISTS "${database}" WITH (FORCE)`);
}
