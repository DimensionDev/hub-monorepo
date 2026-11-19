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
 * Receipt tables (storage_rentals, tier_purchases): one row per on-chain receipt, ported from the
 * Go initializer's `initreceipts`, which also shares one module between them. A receipt is
 * identified by (chain, transaction hash, log index), never by fid: one account paying twice has
 * two rows, and expired receipts stay.
 *
 * Every apply recomputes the fid's receipts from all stored `onchain_events` of the type. Raw
 * on-chain events are immutable and only ever added, so the recompute is order independent; rows
 * are only inserted or updated, never deleted.
 */

export const MAX_INT32 = 2 ** 31 - 1;
/** 9999-12-31T23:59:59Z, the latest instant a `timestamp` column can hold. */
export const MAX_UNIX_SECONDS = 253402300799;

export type ReceiptEvent = {
  chainId: number;
  fid: number;
  blockNumber: number;
  logIndex: number;
  blockTimestamp: Date;
  txHash: Uint8Array;
  body: unknown;
};

/** Column value written for a receipt; Dates are compared by instant, bytes by content. */
export type Value = number | string | Date | Buffer;
type Table = "storageRentals" | "tierPurchases";

export type ReceiptSpec = {
  name: string;
  table: Table;
  /** Event name used in issue reasons, e.g. STORAGE_RENT. */
  label: string;
  eventType: OnChainEventType.EVENT_TYPE_STORAGE_RENT | OnChainEventType.EVENT_TYPE_TIER_PURCHASE;
  chainId: number;
  maxFid: number;
  /** Body columns (camelCase) or the missing target columns. */
  decode(body: Record<string, unknown>): { values: Record<string, Value>; missing: string[] };
  /** Body columns that are `timestamp without time zone`. */
  timeColumns: string[];
};

export type Receipt = {
  /** `<chain>:<tx hex>:<log index>`, the Go receipt key. */
  key: string;
  fid: number;
  values: Record<string, Value>;
  timestamp: Date;
  transactionHash: Buffer;
  logIndex: number;
  blockNumber: number;
};

function invalid(message: string) {
  return err(new HubError("bad_request.validation_failure", message));
}

/** A non-negative integer up to `max`; a missing or null value is not 0. */
export function integer(body: Record<string, unknown>, key: string, max: number): number | undefined {
  const value = body[key];
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= max ? value : undefined;
}

export function receiptKey(chainId: number, txHash: Uint8Array, logIndex: number): string {
  return `${chainId}:${Buffer.from(txHash).toString("hex")}:${logIndex}`;
}

export function receiptId(spec: Pick<ReceiptSpec, "name">, key: string): string {
  return conversionId(spec.name, key);
}

/** Per-event checks, shared by arriving events and stored rows. Every message names the fid and
 * log index so several receipts in one transaction keep separate issues. */
export function validateReceiptEvent(spec: ReceiptSpec, e: ReceiptEvent): HubResult<Receipt> {
  const at = `fid ${e.fid} log ${e.logIndex}`;
  if (e.chainId !== spec.chainId)
    return invalid(`${spec.label} event on chain ${e.chainId}, expected ${spec.chainId}, for ${at}`);
  // fid 0 never reaches here (see the interest guard); the dispatcher rejects it.
  if (!Number.isSafeInteger(e.fid) || e.fid <= 0 || e.fid > spec.maxFid) return invalid(`fid out of range for ${at}`);
  if (
    !Number.isSafeInteger(e.blockNumber) ||
    e.blockNumber < 0 ||
    !Number.isSafeInteger(e.logIndex) ||
    e.logIndex < 0 ||
    e.logIndex > MAX_INT32
  )
    return invalid(`Invalid chain position for ${at}`);
  const time = e.blockTimestamp instanceof Date ? e.blockTimestamp.getTime() : Number.NaN;
  if (!(time > 0) || time / 1000 > MAX_UNIX_SECONDS) return invalid(`Missing block time for ${at}`);
  if (e.txHash.length !== 32) return invalid(`Invalid transaction hash for ${at}`);
  const body = (e.body && typeof e.body === "object" ? e.body : {}) as Record<string, unknown>;
  const { values, missing } = spec.decode(body);
  if (missing.length) return invalid(`Missing or unrepresentable ${missing.join(", ")} for ${at}`);
  return ok({
    key: receiptKey(e.chainId, e.txHash, e.logIndex),
    fid: e.fid,
    values,
    timestamp: new Date(time),
    transactionHash: Buffer.from(e.txHash),
    logIndex: e.logIndex,
    blockNumber: e.blockNumber,
  });
}

function fromOnChainEvent(event: OnChainEvent): ReceiptEvent {
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

function sameValue(a: unknown, b: Value): boolean {
  if (b instanceof Date) return a instanceof Date && a.getTime() === b.getTime();
  if (Buffer.isBuffer(b)) return a instanceof Uint8Array && Buffer.from(a).equals(b);
  if (typeof b === "number") return a !== null && Number(a) === b;
  return a === b;
}

function sameReceipt(a: Receipt, b: Receipt): boolean {
  return (
    a.fid === b.fid &&
    a.timestamp.getTime() === b.timestamp.getTime() &&
    Object.entries(b.values).every(([k, v]) => sameValue(a.values[k], v))
  );
}

export type ReducedReceipts = {
  receipts: Receipt[];
  invalid: { event: ReceiptEvent; error: HubError }[];
  conflicts: { event: ReceiptEvent; key: string }[];
};

/**
 * The receipts a fid's events describe, sorted by key. The same receipt seen twice (e.g.
 * re-included at another block) merges when its content agrees, keeping the later block;
 * conflicting content is reported and the receipt left out.
 */
export function reduceReceipts(spec: ReceiptSpec, events: ReceiptEvent[]): ReducedReceipts {
  const byKey = new Map<string, Receipt>();
  const conflicted = new Set<string>();
  const result: ReducedReceipts = { receipts: [], invalid: [], conflicts: [] };
  for (const event of events) {
    const validation = validateReceiptEvent(spec, event);
    if (validation.isErr()) {
      result.invalid.push({ event, error: validation.error });
      continue;
    }
    const receipt = validation.value;
    const previous = byKey.get(receipt.key);
    if (!previous) {
      byKey.set(receipt.key, receipt);
    } else if (!sameReceipt(previous, receipt)) {
      if (!conflicted.has(receipt.key)) result.conflicts.push({ event, key: receipt.key });
      conflicted.add(receipt.key);
    } else if (receipt.blockNumber > previous.blockNumber) {
      byKey.set(receipt.key, receipt);
    }
  }
  result.receipts = [...byKey.values()].filter((r) => !conflicted.has(r.key)).sort((a, b) => (a.key < b.key ? -1 : 1));
  return result;
}

async function loadEvents(db: AppDb, spec: ReceiptSpec, fid: number): Promise<ReceiptEvent[]> {
  const rows: OnChainEventRow[] = await db
    .selectFrom("onchain_events")
    .selectAll()
    .where("fid", "=", fid)
    .where("type", "=", spec.eventType)
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

function issuePayload(spec: ReceiptSpec, event: ReceiptEvent): Record<string, unknown> {
  return { type: spec.eventType, chainId: event.chainId, blockNumber: event.blockNumber, logIndex: event.logIndex };
}

function snake(column: string): string {
  return column.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

export type ReceiptResult = { inserted: number; updated: number; unchanged: number; skipped: number };

/**
 * Recomputes one fid's receipts. Invalid and conflicting receipts are recorded as issues (the
 * dispatcher's validation issues use the same subject and reason, so they merge) and skipped; the
 * other receipts are still written. Rebuild does not fail on them either: raw events are immutable,
 * so throwing would roll back the fid's good receipts on every attempt; the issue is the record.
 */
export async function projectReceipts(db: AppDb, spec: ReceiptSpec, fid: number): Promise<ReceiptResult> {
  const reduced = reduceReceipts(spec, await loadEvents(db, spec, fid));
  for (const { event, error } of reduced.invalid) {
    await recordIssue(
      db,
      spec.name,
      { fid: event.fid || null, hash: event.txHash },
      error.message,
      issuePayload(spec, event),
    );
  }
  for (const { event } of reduced.conflicts) {
    await recordIssue(
      db,
      spec.name,
      { fid: event.fid || null, hash: event.txHash },
      `Conflicting ${spec.label} receipts for fid ${event.fid} log ${event.logIndex}`,
      issuePayload(spec, event),
    );
  }

  const result: ReceiptResult = {
    inserted: 0,
    updated: 0,
    unchanged: 0,
    skipped: reduced.invalid.length + reduced.conflicts.length,
  };
  const ids = reduced.receipts.map((r) => receiptId(spec, r.key));
  // Looked up by id: the reference has no index on transaction_hash. A receipt whose fid was
  // corrected is found here too and updated in place, keeping its id and created_at.
  const bodyColumns = Object.keys(reduced.receipts[0]?.values ?? {});
  const existing = ids.length
    ? ((await db
        .selectFrom(spec.table)
        .select([
          "id",
          "fid",
          "transactionHash",
          "logIndex",
          sql<Date>`timestamp AT TIME ZONE 'UTC'`.as("timestamp"),
          ...bodyColumns.map((c) =>
            spec.timeColumns.includes(c)
              ? sql<Date>`${sql.ref(snake(c))} AT TIME ZONE 'UTC'`.as(c)
              : sql<unknown>`${sql.ref(snake(c))}`.as(c),
          ),
        ])
        .where("id", "in", ids)
        .execute()) as unknown as Record<string, unknown>[])
    : [];
  const rows = new Map(existing.map((row) => [row["id"] as string, row]));
  const now = new Date().toISOString();
  const toDb = (v: Value) => (v instanceof Date ? v.toISOString() : v);
  for (const receipt of reduced.receipts) {
    const id = receiptId(spec, receipt.key);
    const expected: Record<string, Value> = {
      fid: receipt.fid,
      timestamp: receipt.timestamp,
      transactionHash: receipt.transactionHash,
      logIndex: receipt.logIndex,
      ...receipt.values,
    };
    const values = Object.fromEntries(Object.entries(expected).map(([k, v]) => [k, toDb(v)]));
    const row = rows.get(id);
    if (!row) {
      await db
        .insertInto(spec.table)
        .values({ id, createdAt: now, updatedAt: now, ...values } as never)
        .execute();
      result.inserted++;
      continue;
    }
    if (Object.entries(expected).every(([k, v]) => sameValue(row[k], v))) {
      result.unchanged++;
      continue;
    }
    await db
      .updateTable(spec.table)
      .set({ ...values, updatedAt: now } as never)
      .where("id", "=", id)
      .execute();
    result.updated++;
  }
  return result;
}

/** Fids with stored events of the type or existing rows, ascending, after `afterFid`. */
async function rebuildFids(db: AppDb, spec: ReceiptSpec, afterFid: number, limit: number): Promise<number[]> {
  const rows = await sql<{ fid: number }>`
    SELECT fid FROM (
      SELECT DISTINCT fid FROM onchain_events WHERE type = ${spec.eventType} AND fid > ${afterFid}
      UNION
      SELECT DISTINCT fid FROM ${sql.table(snake(spec.table))} WHERE fid > ${afterFid}
    ) f ORDER BY fid LIMIT ${limit}`.execute(db);
  return rows.rows.map((row) => Number(row.fid));
}

/**
 * fid 0 receipts (paid to no account) are left to the generic raw path: the dispatcher locks per
 * fid and rejects fid 0, so taking them would fail the hub event. They are a known gap.
 */
export function receiptProjection(
  spec: ReceiptSpec,
  stats: (db: AppDb) => Promise<Record<string, number>>,
): Projection {
  const interestedIn = (change: RawChange) =>
    change.kind === "onchain" && change.event.type === spec.eventType && change.event.fid > 0;
  return {
    name: spec.name,
    reconcileTypes: [],
    onChainReconcileTypes: [spec.eventType],
    interestedIn,
    validate: (change) => {
      if (change.kind !== "onchain") return invalid("Not an on-chain event");
      const result = validateReceiptEvent(spec, fromOnChainEvent(change.event));
      return result.isErr() ? err(result.error) : ok(undefined);
    },
    apply: async ({ db }: ProjectionContext, changes: RawChange[]) => {
      const fids = new Set<number>();
      for (const change of changes) if (change.kind === "onchain") fids.add(change.event.fid);
      for (const fid of [...fids].sort((a, b) => a - b)) await projectReceipts(db, spec, fid);
    },
    rebuildFids: (db, afterFid, limit) => rebuildFids(db, spec, afterFid, limit),
    rebuild: async ({ db }, fid) => {
      await projectReceipts(db, spec, fid);
    },
    table: spec.name,
    stats,
  };
}
