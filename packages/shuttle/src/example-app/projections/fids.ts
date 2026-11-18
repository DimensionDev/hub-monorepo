import { createHash } from "node:crypto";
import { HubError, HubResult, IdRegisterEventType, OnChainEvent, OnChainEventType } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import { err, ok } from "neverthrow";
import type { OnChainEventRow } from "../../shuttle";
import type { AppDb } from "../db";
import { onChainEventBody } from "../raw/onchainEvents";
import { conversionId } from "./ids";
import { clearIssue, recordIssue } from "./issues";
import type { Projection, ProjectionContext, RawChange } from "./types";

/**
 * fids: one row per account, derived from the account's ID_REGISTER on-chain events (rules in
 * docs/farcaster-db/000008, ported from the Go initializer's `reduce`).
 *
 * Every apply recomputes the fid from all stored type=3 `onchain_events`. Raw on-chain events are
 * immutable and only ever added, so the recompute is order independent and monotonic.
 */

const OPTIMISM = 10;
const MAX_INT32 = 2 ** 31 - 1;
const NAME = "fids";

/** Fid-level issue reasons; each fid has at most one of them at a time. */
export const FID_ISSUES = {
  pending: "missing REGISTER",
  invalid: "invalid account history",
  stale: "stored account is newer than the on-chain history",
  gap: "custody history has gaps",
} as const;

type AccountEvent = {
  chainId: number;
  fid: number;
  blockNumber: number;
  logIndex: number;
  blockTimestamp: Date;
  txHash: Uint8Array;
  body: unknown;
};

type Body = { eventType: number; from?: string; to?: string; recoveryAddress?: string };

/** `registeredAt` is null (and `recovery` may be) for a partial account: the REGISTER is missing. */
export type Account = {
  custody: Buffer;
  recovery: Buffer | null;
  registeredAt: Date | null;
  timestamp: Date;
};

function address(value: unknown): Buffer | undefined {
  if (typeof value !== "string" || value.length !== 42 || !value.startsWith("0x")) return undefined;
  if (!/^[0-9a-fA-F]{40}$/.test(value.slice(2))) return undefined;
  return Buffer.from(value.slice(2), "hex");
}

function invalid(message: string) {
  return err(new HubError("bad_request.validation_failure", message));
}

/** Per-event checks, shared by arriving events and stored rows. */
export function validateAccountEvent(e: AccountEvent): HubResult<Body> {
  if (e.chainId !== OPTIMISM) return invalid(`ID_REGISTER event on chain ${e.chainId}, expected ${OPTIMISM}`);
  if (!Number.isSafeInteger(e.fid) || e.fid <= 0 || e.fid > MAX_INT32) return invalid(`fid ${e.fid} out of range`);
  if (!Number.isSafeInteger(e.blockNumber) || e.blockNumber < 0 || !Number.isSafeInteger(e.logIndex) || e.logIndex < 0)
    return invalid(`Invalid chain position for fid ${e.fid}`);
  if (!(e.blockTimestamp instanceof Date) || !(e.blockTimestamp.getTime() > 0))
    return invalid(`Missing block time for fid ${e.fid}`);
  if (e.txHash.length !== 32) return invalid(`Invalid transaction hash for fid ${e.fid}`);
  const body = e.body as Body | null;
  if (!body || typeof body !== "object") return invalid(`Missing ID_REGISTER body for fid ${e.fid}`);
  const required: Record<number, (keyof Body)[]> = {
    [IdRegisterEventType.REGISTER]: ["to", "recoveryAddress"],
    [IdRegisterEventType.TRANSFER]: ["from", "to"],
    [IdRegisterEventType.CHANGE_RECOVERY]: ["recoveryAddress"],
  };
  const fields = required[body.eventType];
  if (!fields) return invalid(`Unknown ID_REGISTER event type ${body.eventType} for fid ${e.fid}`);
  for (const field of fields) {
    if (!address(body[field])) return invalid(`Invalid ${field} address for fid ${e.fid}`);
  }
  return ok(body);
}

function fromOnChainEvent(event: OnChainEvent): AccountEvent {
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

function position(a: AccountEvent, b: AccountEvent): number {
  return a.blockNumber - b.blockNumber || a.logIndex - b.logIndex;
}

function sameEvent(a: AccountEvent, b: AccountEvent): boolean {
  const x = a.body as Body;
  const y = b.body as Body;
  return (
    a.chainId === b.chainId &&
    a.fid === b.fid &&
    a.blockTimestamp.getTime() === b.blockTimestamp.getTime() &&
    Buffer.from(a.txHash).equals(Buffer.from(b.txHash)) &&
    x.eventType === y.eventType &&
    x.from === y.from &&
    x.to === y.to &&
    x.recoveryAddress === y.recoveryAddress
  );
}

/** A transfer whose `from` is not the custody captured so far: an event is missing from the history. */
export type CustodyGap = { blockNumber: number; logIndex: number; expectedFrom: string; actualFrom: string };

export type Reduced =
  | { kind: "account"; account: Account; gaps: CustodyGap[]; missingFields: string[] }
  | { kind: "pending"; missingFields: string[] };

/**
 * The account an ordered ID_REGISTER history describes. REGISTER must be the first event; a later
 * REGISTER to the same custody is a re-registration (it moves recovery and `timestamp`, never
 * `registered_at`). A transfer that does not continue the current custody means an event is
 * missing: the chain is authoritative, so the transfer is applied and the gap reported.
 * Recovery changes do not move `timestamp`.
 *
 * Without a REGISTER (snapchain misses a few), transfers still give a partial account: custody and
 * `timestamp` from the transfers, recovery from the latest recovery change or null, and
 * `registered_at` null; `missingFields` lists what is unknown. With no transfer either there is no
 * custody, and the fid is pending.
 */
export function reduceAccount(input: AccountEvent[]): HubResult<Reduced> {
  const events = [...input].sort(position);
  let account: Account | undefined;
  let recoveryBeforeTransfer: Buffer | null = null;
  const gaps: CustodyGap[] = [];
  const seen = new Set<number>();
  for (const [i, e] of events.entries()) {
    const validation = validateAccountEvent(e);
    if (validation.isErr()) return err(validation.error);
    const body = validation.value;
    const first = events[0] as AccountEvent;
    if (e.fid !== first.fid) return invalid("Mixed account histories");
    const previous = events[i - 1];
    if (previous && position(previous, e) === 0) {
      if (!sameEvent(previous, e))
        return err(new HubError("bad_request.conflict", "Conflicting account events at the same chain position"));
      continue;
    }
    seen.add(body.eventType);
    switch (body.eventType) {
      case IdRegisterEventType.REGISTER:
        if (account) {
          if (account.registeredAt === null || !(address(body.to) as Buffer).equals(account.custody))
            return err(new HubError("bad_request.conflict", "Inconsistent registration history"));
          account.recovery = address(body.recoveryAddress) as Buffer;
          account.timestamp = e.blockTimestamp;
          break;
        }
        if (i !== 0) return err(new HubError("bad_request.conflict", "Inconsistent registration history"));
        account = {
          custody: address(body.to) as Buffer,
          recovery: address(body.recoveryAddress) as Buffer,
          registeredAt: e.blockTimestamp,
          timestamp: e.blockTimestamp,
        };
        break;
      case IdRegisterEventType.TRANSFER:
        if (!account) {
          account = {
            custody: address(body.to) as Buffer,
            recovery: recoveryBeforeTransfer,
            registeredAt: null,
            timestamp: e.blockTimestamp,
          };
          break;
        }
        if (!(address(body.from) as Buffer).equals(account.custody)) {
          gaps.push({
            blockNumber: e.blockNumber,
            logIndex: e.logIndex,
            expectedFrom: `0x${account.custody.toString("hex")}`,
            actualFrom: (body.from as string).toLowerCase(),
          });
        }
        account.custody = address(body.to) as Buffer;
        account.timestamp = e.blockTimestamp;
        break;
      case IdRegisterEventType.CHANGE_RECOVERY:
        if (account) account.recovery = address(body.recoveryAddress) as Buffer;
        else recoveryBeforeTransfer = address(body.recoveryAddress) as Buffer;
        break;
    }
  }
  const missingFields: string[] = [];
  if (!seen.has(IdRegisterEventType.REGISTER)) {
    missingFields.push("registered_at");
    if (!account) missingFields.push("custody_address", "timestamp");
    if (!seen.has(IdRegisterEventType.CHANGE_RECOVERY)) missingFields.push("recovery_address");
  }
  if (account) return ok({ kind: "account", account, gaps, missingFields });
  return ok({ kind: "pending", missingFields });
}

/** Deterministic issue key for problems that belong to a whole fid rather than one event. */
export function fidIssueHash(fid: number): Buffer {
  return createHash("sha1").update(`${NAME}:${fid}`).digest().subarray(0, 20);
}

async function setFidIssue(
  db: AppDb,
  fid: number,
  current: keyof typeof FID_ISSUES | null,
  payload?: Record<string, unknown>,
): Promise<void> {
  const hash = fidIssueHash(fid);
  for (const [key, reason] of Object.entries(FID_ISSUES)) {
    if (key !== current) await clearIssue(db, NAME, hash, reason);
  }
  if (current) await recordIssue(db, NAME, { fid, hash }, FID_ISSUES[current], payload, { replace: true });
}

async function loadEvents(db: AppDb, fid: number): Promise<AccountEvent[]> {
  const rows: OnChainEventRow[] = await db
    .selectFrom("onchain_events")
    .selectAll()
    .where("fid", "=", fid)
    .where("type", "=", OnChainEventType.EVENT_TYPE_ID_REGISTER)
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

function sameBytes(a: Uint8Array | null, b: Uint8Array | null): boolean {
  if (a === null || b === null) return a === b;
  return Buffer.from(a).equals(Buffer.from(b));
}

/**
 * Recomputes one fid. Data problems are recorded as fid issues; with `strict` (rebuild) they are
 * also thrown so the fid is reported as failed. Returns what happened to the row.
 */
export async function projectFid(
  db: AppDb,
  fid: number,
  strict: boolean,
): Promise<"inserted" | "updated" | "unchanged" | "pending" | "stale" | "invalid"> {
  const fail = async (error: HubError) => {
    await setFidIssue(db, fid, "invalid", { error: error.message });
    if (strict) throw error;
    return "invalid" as const;
  };

  const existing = await db
    .selectFrom("fids")
    .select([
      "id",
      "custodyAddress",
      "recoveryAddress",
      sql<Date>`timestamp AT TIME ZONE 'UTC'`.as("timestamp"),
      sql<Date | null>`registered_at AT TIME ZONE 'UTC'`.as("registeredAt"),
    ])
    .where("fid", "=", fid)
    .execute();
  if (existing.length > 1) return fail(new HubError("bad_request.conflict", `Duplicate fids rows for fid ${fid}`));

  const reduced = reduceAccount(await loadEvents(db, fid));
  if (reduced.isErr()) return fail(reduced.error);
  if (reduced.value.kind === "pending") {
    // No custody to write; an existing row (e.g. repaired from Neynar) is left as it is.
    await setFidIssue(db, fid, "pending", { missingFields: reduced.value.missingFields });
    return "pending";
  }

  const { account, gaps, missingFields } = reduced.value;
  // Gaps and a missing REGISTER are reported, not fatal: the row still follows the chain.
  const settle = () => {
    if (missingFields.length)
      return setFidIssue(db, fid, "pending", gaps.length ? { missingFields, gaps } : { missingFields });
    return setFidIssue(db, fid, gaps.length ? "gap" : null, gaps.length ? { gaps } : undefined);
  };
  const row = existing[0];
  const now = new Date().toISOString();
  if (!row) {
    await db
      .insertInto("fids")
      .values({
        id: conversionId(NAME, String(fid)),
        createdAt: now,
        updatedAt: now,
        fid,
        custodyAddress: account.custody,
        recoveryAddress: account.recovery,
        timestamp: account.timestamp.toISOString(),
        registeredAt: account.registeredAt?.toISOString() ?? null,
      })
      .execute();
    await settle();
    return "inserted";
  }
  if (row.timestamp.getTime() > account.timestamp.getTime()) {
    // The stored row reflects more history than onchain_events holds; don't move it backwards.
    await setFidIssue(db, fid, "stale", {
      storedTimestamp: row.timestamp.toISOString(),
      computedTimestamp: account.timestamp.toISOString(),
    });
    return "stale";
  }
  await settle();
  // A partial account never clears what the row already knows.
  const recovery = account.recovery ?? row.recoveryAddress;
  const registeredAt = account.registeredAt ?? row.registeredAt;
  if (
    sameBytes(row.custodyAddress, account.custody) &&
    sameBytes(row.recoveryAddress, recovery) &&
    row.timestamp.getTime() === account.timestamp.getTime() &&
    row.registeredAt?.getTime() === registeredAt?.getTime()
  ) {
    return "unchanged";
  }
  await db
    .updateTable("fids")
    .set({
      custodyAddress: account.custody,
      recoveryAddress: recovery,
      timestamp: account.timestamp.toISOString(),
      registeredAt: registeredAt?.toISOString() ?? null,
      updatedAt: now,
    })
    .where("id", "=", row.id)
    .execute();
  return "updated";
}

function isAccountEvent(change: RawChange): boolean {
  return change.kind === "onchain" && change.event.type === OnChainEventType.EVENT_TYPE_ID_REGISTER;
}

async function applyFids({ db }: ProjectionContext, changes: RawChange[]): Promise<void> {
  const fids = new Set<number>();
  for (const change of changes) if (change.kind === "onchain") fids.add(change.event.fid);
  for (const fid of [...fids].sort((a, b) => a - b)) await projectFid(db, fid, false);
}

/** Fids with stored ID_REGISTER events or existing rows, ascending, after `afterFid`. */
async function rebuildFids(db: AppDb, afterFid: number, limit: number): Promise<number[]> {
  const rows = await sql<{ fid: number }>`
    SELECT fid FROM (
      SELECT DISTINCT fid FROM onchain_events
      WHERE type = ${OnChainEventType.EVENT_TYPE_ID_REGISTER} AND fid > ${afterFid}
      UNION
      SELECT DISTINCT fid FROM fids WHERE fid > ${afterFid}
    ) f ORDER BY fid LIMIT ${limit}`.execute(db);
  return rows.rows.map((row) => Number(row.fid));
}

export const fidsProjection: Projection = {
  name: NAME,
  reconcileTypes: [],
  onChainReconcileTypes: [OnChainEventType.EVENT_TYPE_ID_REGISTER],
  interestedIn: isAccountEvent,
  validate: (change) => {
    if (change.kind !== "onchain") return invalid("Not an on-chain event");
    const result = validateAccountEvent(fromOnChainEvent(change.event));
    return result.isErr() ? err(result.error) : ok(undefined);
  },
  apply: applyFids,
  rebuildFids,
  rebuild: async ({ db }, fid) => {
    await projectFid(db, fid, true);
  },
  table: NAME,
  stats: async (db) => {
    const fids = await db.selectFrom("fids").select(sql<string>`count(*)`.as("total")).executeTakeFirst();
    const pending = await db
      .selectFrom("shuttleProjectionIssues")
      .select(sql<string>`count(*)`.as("pending"))
      .where("projection", "=", NAME)
      .where("reason", "=", FID_ISSUES.pending)
      .executeTakeFirst();
    return { total: Number(fids?.total ?? 0), pending: Number(pending?.pending ?? 0) };
  },
};
