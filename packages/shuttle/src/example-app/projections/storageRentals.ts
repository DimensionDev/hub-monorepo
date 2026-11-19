import { OnChainEventType } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import {
  type Value,
  integer,
  MAX_INT32,
  MAX_UNIX_SECONDS,
  ReceiptSpec,
  receiptId,
  receiptProjection,
} from "./receipts";

/**
 * storage_rentals: one row per on-chain STORAGE_RENT receipt (rules in docs/farcaster-db/000012).
 * `expiry` is already computed by Shuttle from the block time (1–3 years by unit generation) and is
 * copied as is. LEND_STORAGE messages are not rentals.
 */
export const storageRentalsSpec: ReceiptSpec = {
  name: "storage_rentals",
  table: "storageRentals",
  label: "STORAGE_RENT",
  eventType: OnChainEventType.EVENT_TYPE_STORAGE_RENT,
  chainId: 10,
  maxFid: MAX_INT32,
  timeColumns: ["expiry"],
  decode: (body) => {
    const units = integer(body, "units", MAX_INT32);
    const expiry = integer(body, "expiry", MAX_UNIX_SECONDS);
    const missing = [...(units === undefined ? ["units"] : []), ...(expiry === undefined ? ["expiry"] : [])];
    if (units === undefined || expiry === undefined) return { values: {} as Record<string, Value>, missing };
    return { values: { units, expiry: new Date(expiry * 1000) }, missing };
  },
};

export function rentalId(key: string): string {
  return receiptId(storageRentalsSpec, key);
}

export const storageRentalsProjection = receiptProjection(storageRentalsSpec, async (db) => {
  const row = await db
    .selectFrom("storageRentals")
    .select([
      sql<string>`count(*)`.as("total"),
      sql<string>`count(*) FILTER (WHERE expiry > now() AT TIME ZONE 'UTC')`.as("unexpired"),
      sql<string>`count(DISTINCT fid)`.as("renters"),
    ])
    .executeTakeFirst();
  return {
    total: Number(row?.total ?? 0),
    unexpired: Number(row?.unexpired ?? 0),
    renters: Number(row?.renters ?? 0),
  };
});
