import { OnChainEventType } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import { type Value, integer, MAX_INT32, ReceiptSpec, receiptProjection } from "./receipts";

function address(value: unknown): Buffer | undefined {
  if (typeof value !== "string" || value.length !== 42 || !value.startsWith("0x")) return undefined;
  if (!/^[0-9a-fA-F]{40}$/.test(value.slice(2))) return undefined;
  return Buffer.from(value.slice(2), "hex");
}

/**
 * tier_purchases: one row per on-chain TIER_PURCHASE receipt on Base (rules in
 * docs/farcaster-db/000013, Go `decodeTier`): tier_type, duration_in_days (body `forDays`) and the
 * 20-byte payer. fid is bigint in the target.
 */
export const tierPurchasesSpec: ReceiptSpec = {
  name: "tier_purchases",
  table: "tierPurchases",
  label: "TIER_PURCHASE",
  eventType: OnChainEventType.EVENT_TYPE_TIER_PURCHASE,
  chainId: 8453,
  maxFid: Number.MAX_SAFE_INTEGER,
  timeColumns: [],
  decode: (body) => {
    const tierType = integer(body, "tierType", MAX_INT32);
    const durationInDays = integer(body, "forDays", Number.MAX_SAFE_INTEGER);
    const payer = address(body["payer"]);
    const missing = [
      ...(tierType === undefined ? ["tier_type"] : []),
      ...(durationInDays === undefined ? ["duration_in_days"] : []),
      ...(payer === undefined ? ["payer"] : []),
    ];
    if (tierType === undefined || durationInDays === undefined || payer === undefined)
      return { values: {} as Record<string, Value>, missing };
    return { values: { tierType, durationInDays, payer }, missing };
  },
};

export const tierPurchasesProjection = receiptProjection(tierPurchasesSpec, async (db) => {
  const row = await db
    .selectFrom("tierPurchases")
    .select([sql<string>`count(*)`.as("total"), sql<string>`count(DISTINCT fid)`.as("buyers")])
    .executeTakeFirst();
  return { total: Number(row?.total ?? 0), buyers: Number(row?.buyers ?? 0) };
});
