import { HubError, HubResult, OnChainEvent, OnChainEventType } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import { err, ok } from "neverthrow";
import type { OnChainEventRow } from "../../shuttle";
import type { AppDb } from "../db";
import { onChainEventBody } from "../raw/onchainEvents";
import { conversionId } from "./ids";
import { recordIssue } from "./issues";
import type { Projection, ProjectionContext, RawChange } from "./types";

/**
 * storage_rentals: one row per on-chain STORAGE_RENT receipt (rules in docs/farcaster-db/000012,
 * ported from the Go initializer's `initreceipts`). A receipt is identified by
 * (chain, transaction hash, log index), never by fid: one account renting twice has two rows, and
 * expired rentals stay.
 *
 * Every apply recomputes the fid's receipts from all stored type=4 `onchain_events`. Raw on-chain
 * events are immutable and only ever added, so the recompute is order independent; rows are only
 * inserted or updated, never deleted.
 */

const OPTIMISM = 10;
const MAX_INT32 = 2 ** 31 - 1;
/** 9999-12-31T23:59:59Z, the latest instant a `timestamp` column can hold. */
const MAX_UNIX_SECONDS = 253402300799;
const NAME = "storage_rentals";

type RentalEvent = {
  chainId: number;
  fid: number;
  blockNumber: number;
  logIndex: number;
  blockTimestamp: Date;
  txHash: Uint8Array;
  body: unknown;
};

export type Rental = {
  /** `<chain>:<tx hex>:<log index>`, the Go receipt key. */
  key: string;
  fid: number;
  units: number;
  expiry: Date;
  timestamp: Date;
  transactionHash: Buffer;
  logIndex: number;
  blockNumber: number;
};

function invalid(message: string) {
  return err(new HubError("bad_request.validation_failure", message));
}

/** A non-negative integer up to `max`; a missing or null value is not 0. */
function integer(body: Record<string, unknown>, key: string, max: number): number | undefined {
  const value = body[key];
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= max ? value : undefined;
}

export function receiptKey(chainId: number, txHash: Uint8Array, logIndex: number): string {
  return `${chainId}:${Buffer.from(txHash).toString("hex")}:${logIndex}`;
}

export function rentalId(key: string): string {
  return conversionId(NAME, key);
}

/** Per-event checks, shared by arriving events and stored rows. Messages name the log index so
 * several receipts in one transaction keep separate issues. */
export function validateRentalEvent(e: RentalEvent): HubResult<Rental> {
  const at = `fid ${e.fid} log ${e.logIndex}`;
  if (e.chainId !== OPTIMISM)
    return invalid(`STORAGE_RENT event on chain ${e.chainId}, expected ${OPTIMISM}, for ${at}`);
  // fid 0 never reaches here (see isRentalEvent); the reference does hold a few fid 0 rentals.
  if (!Number.isSafeInteger(e.fid) || e.fid <= 0 || e.fid > MAX_INT32) return invalid(`fid out of range for ${at}`);
  if (!Number.isSafeInteger(e.blockNumber) || e.blockNumber < 0 || !Number.isSafeInteger(e.logIndex) || e.logIndex < 0)
    return invalid(`Invalid chain position for ${at}`);
  if (e.logIndex > MAX_INT32) return invalid(`Invalid chain position for ${at}`);
  const time = e.blockTimestamp instanceof Date ? e.blockTimestamp.getTime() : Number.NaN;
  if (!(time > 0) || time / 1000 > MAX_UNIX_SECONDS) return invalid(`Missing block time for ${at}`);
  if (e.txHash.length !== 32) return invalid(`Invalid transaction hash for ${at}`);
  const body = (e.body && typeof e.body === "object" ? e.body : {}) as Record<string, unknown>;
  const units = integer(body, "units", MAX_INT32);
  const expiry = integer(body, "expiry", MAX_UNIX_SECONDS);
  const missing = [...(units === undefined ? ["units"] : []), ...(expiry === undefined ? ["expiry"] : [])];
  if (units === undefined || expiry === undefined)
    return invalid(`Missing or unrepresentable ${missing.join(", ")} for ${at}`);
  return ok({
    key: receiptKey(e.chainId, e.txHash, e.logIndex),
    fid: e.fid,
    units,
    // Shuttle already computed the expiry from the block time; use it as is.
    expiry: new Date(expiry * 1000),
    timestamp: new Date(time),
    transactionHash: Buffer.from(e.txHash),
    logIndex: e.logIndex,
    blockNumber: e.blockNumber,
  });
}

function fromOnChainEvent(event: OnChainEvent): RentalEvent {
  return {
    chainId: event.chainId,
    fid: event.fid,
    blockNumber: event.blockNumber,
    logIndex: event.logIndex,
    blockTimestamp: new Date(event.blockTimestamp * 1000),
    txHash: event.transactionHash,
    body: onChainEventBody(event),
  };
}

function sameRental(a: Rental, b: Rental): boolean {
  return (
    a.fid === b.fid &&
    a.units === b.units &&
    a.expiry.getTime() === b.expiry.getTime() &&
    a.timestamp.getTime() === b.timestamp.getTime()
  );
}

export type ReducedRentals = {
  rentals: Rental[];
  invalid: { event: RentalEvent; error: HubError }[];
  conflicts: { event: RentalEvent; key: string }[];
};

/**
 * The receipts a fid's STORAGE_RENT events describe, sorted by key. The same receipt seen twice
 * (e.g. re-included at another block) merges when its content agrees, keeping the later block;
 * conflicting content is reported and the receipt left out.
 */
export function reduceRentals(events: RentalEvent[]): ReducedRentals {
  const byKey = new Map<string, Rental>();
  const conflicted = new Set<string>();
  const result: ReducedRentals = { rentals: [], invalid: [], conflicts: [] };
  for (const event of events) {
    const validation = validateRentalEvent(event);
    if (validation.isErr()) {
      result.invalid.push({ event, error: validation.error });
      continue;
    }
    const rental = validation.value;
    const previous = byKey.get(rental.key);
    if (!previous) {
      byKey.set(rental.key, rental);
    } else if (!sameRental(previous, rental)) {
      if (!conflicted.has(rental.key)) result.conflicts.push({ event, key: rental.key });
      conflicted.add(rental.key);
    } else if (rental.blockNumber > previous.blockNumber) {
      byKey.set(rental.key, rental);
    }
  }
  result.rentals = [...byKey.values()].filter((r) => !conflicted.has(r.key)).sort((a, b) => (a.key < b.key ? -1 : 1));
  return result;
}

async function loadEvents(db: AppDb, fid: number): Promise<RentalEvent[]> {
  const rows: OnChainEventRow[] = await db
    .selectFrom("onchain_events")
    .selectAll()
    .where("fid", "=", fid)
    .where("type", "=", OnChainEventType.EVENT_TYPE_STORAGE_RENT)
    .execute();
  return rows.map((row) => ({
    chainId: Number(row.chainId),
    fid: Number(row.fid),
    blockNumber: Number(row.blockNumber),
    logIndex: row.logIndex,
    blockTimestamp: row.blockTimestamp,
    txHash: row.txHash,
    body: row.body,
  }));
}

function issuePayload(event: RentalEvent): Record<string, unknown> {
  return {
    type: OnChainEventType.EVENT_TYPE_STORAGE_RENT,
    chainId: event.chainId,
    blockNumber: event.blockNumber,
    logIndex: event.logIndex,
  };
}

export type RentalResult = { inserted: number; updated: number; unchanged: number; skipped: number };

/**
 * Recomputes one fid's receipts. Invalid and conflicting receipts are recorded as issues (the
 * dispatcher's validation issues use the same subject and reason, so they merge) and skipped; the
 * other receipts are still written. Rebuild does not fail on them either: raw events are immutable,
 * so throwing would roll back the fid's good receipts on every attempt; the issue is the record.
 */
export async function projectRentals(db: AppDb, fid: number): Promise<RentalResult> {
  const reduced = reduceRentals(await loadEvents(db, fid));
  for (const { event, error } of reduced.invalid) {
    await recordIssue(db, NAME, { fid: event.fid || null, hash: event.txHash }, error.message, issuePayload(event));
  }
  for (const { event } of reduced.conflicts) {
    await recordIssue(
      db,
      NAME,
      { fid: event.fid || null, hash: event.txHash },
      `Conflicting STORAGE_RENT receipts for fid ${event.fid} log ${event.logIndex}`,
      issuePayload(event),
    );
  }

  const result: RentalResult = {
    inserted: 0,
    updated: 0,
    unchanged: 0,
    skipped: reduced.invalid.length + reduced.conflicts.length,
  };
  const ids = reduced.rentals.map((r) => rentalId(r.key));
  // Looked up by id: the reference has no index on transaction_hash. A receipt whose fid was
  // corrected is found here too and updated in place, keeping its id and created_at.
  const existing = ids.length
    ? await db
        .selectFrom("storageRentals")
        .select([
          "id",
          "fid",
          "units",
          "transactionHash",
          "logIndex",
          sql<Date>`expiry AT TIME ZONE 'UTC'`.as("expiry"),
          sql<Date>`timestamp AT TIME ZONE 'UTC'`.as("timestamp"),
        ])
        .where("id", "in", ids)
        .execute()
    : [];
  const rows = new Map(existing.map((row) => [row.id, row]));
  const now = new Date().toISOString();
  for (const rental of reduced.rentals) {
    const id = rentalId(rental.key);
    const values = {
      fid: rental.fid,
      units: rental.units,
      expiry: rental.expiry.toISOString(),
      timestamp: rental.timestamp.toISOString(),
      transactionHash: rental.transactionHash,
      logIndex: rental.logIndex,
    };
    const row = rows.get(id);
    if (!row) {
      await db
        .insertInto("storageRentals")
        .values({ id, createdAt: now, updatedAt: now, ...values })
        .execute();
      result.inserted++;
      continue;
    }
    if (
      Number(row.fid) === rental.fid &&
      row.units === rental.units &&
      row.expiry.getTime() === rental.expiry.getTime() &&
      row.timestamp.getTime() === rental.timestamp.getTime() &&
      row.transactionHash !== null &&
      Buffer.from(row.transactionHash).equals(rental.transactionHash) &&
      row.logIndex === rental.logIndex
    ) {
      result.unchanged++;
      continue;
    }
    await db
      .updateTable("storageRentals")
      .set({ ...values, updatedAt: now })
      .where("id", "=", id)
      .execute();
    result.updated++;
  }

  return result;
}

/**
 * fid 0 rentals (rent paid to no account) are left to the generic raw path: the dispatcher locks
 * per fid and rejects fid 0, so taking them would fail the hub event. They are a known gap.
 */
function isRentalEvent(change: RawChange): boolean {
  return (
    change.kind === "onchain" && change.event.type === OnChainEventType.EVENT_TYPE_STORAGE_RENT && change.event.fid > 0
  );
}

async function applyRentals({ db }: ProjectionContext, changes: RawChange[]): Promise<void> {
  const fids = new Set<number>();
  for (const change of changes) if (change.kind === "onchain") fids.add(change.event.fid);
  for (const fid of [...fids].sort((a, b) => a - b)) await projectRentals(db, fid);
}

/** Fids with stored STORAGE_RENT events or existing rows, ascending, after `afterFid`. */
async function rebuildFids(db: AppDb, afterFid: number, limit: number): Promise<number[]> {
  const rows = await sql<{ fid: number }>`
    SELECT fid FROM (
      SELECT DISTINCT fid FROM onchain_events
      WHERE type = ${OnChainEventType.EVENT_TYPE_STORAGE_RENT} AND fid > ${afterFid}
      UNION
      SELECT DISTINCT fid FROM storage_rentals WHERE fid > ${afterFid}
    ) f ORDER BY fid LIMIT ${limit}`.execute(db);
  return rows.rows.map((row) => Number(row.fid));
}

export const storageRentalsProjection: Projection = {
  name: NAME,
  reconcileTypes: [],
  onChainReconcileTypes: [OnChainEventType.EVENT_TYPE_STORAGE_RENT],
  interestedIn: isRentalEvent,
  validate: (change) => {
    if (change.kind !== "onchain") return invalid("Not an on-chain event");
    const result = validateRentalEvent(fromOnChainEvent(change.event));
    return result.isErr() ? err(result.error) : ok(undefined);
  },
  apply: applyRentals,
  rebuildFids,
  rebuild: async ({ db }, fid) => {
    await projectRentals(db, fid);
  },
  table: NAME,
  stats: async (db) => {
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
  },
};
