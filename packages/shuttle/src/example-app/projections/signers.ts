import { createHash } from "node:crypto";
import { HubError, HubResult, OnChainEvent, OnChainEventType, SignerEventType } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import { err, ok } from "neverthrow";
import type { OnChainEventRow } from "../../shuttle";
import type { AppDb } from "../db";
import { onChainEventBody } from "../raw/onchainEvents";
import { conversionId } from "./ids";
import { clearIssue, recordIssue } from "./issues";
import type { Projection, ProjectionContext, RawChange } from "./types";

/**
 * signers: one row per (fid, signer key), from the fid's on-chain SIGNER events (rules in
 * docs/farcaster-db/000010, ported from the Go initializer's `initsigners`). `app_fid` is the
 * SignedKeyRequestMetadata `requestFid` of the key's registration.
 *
 * Every apply recomputes the fid from all stored type=1 `onchain_events` (immutable, add-only), so
 * the result is order independent. Gasless KEY_ADD / KEY_REMOVE messages are not projected: the
 * reference table holds on-chain keys only.
 *
 * Projections that attribute messages to an app (`app_fid`) look keys up here with `appFids` and
 * also take SIGNER changes, so they recompute when a key arrives after the messages it signed.
 * They are registered after this projection, so the signer rows are written first.
 */

const OPTIMISM = 10;
const NAME = "signers";

type SignerEvent = {
  chainId: number;
  fid: number;
  blockNumber: number;
  logIndex: number;
  blockTimestamp: Date;
  txHash: Uint8Array;
  body: unknown;
};

type Body = { eventType: number; key: string; keyType: number; metadata: string; metadataType: number };

/** Per-key issues; each key has at most one at a time. */
export const SIGNER_ISSUES = {
  pending: "missing signer ADD",
  app: "unknown signer app fid",
  conflict: "conflicting signer history",
} as const;

function invalid(message: string) {
  return err(new HubError("bad_request.validation_failure", message));
}

function keyBytes(value: unknown): Buffer | undefined {
  if (typeof value !== "string" || value.length !== 66 || !value.startsWith("0x")) return undefined;
  if (!/^[0-9a-fA-F]{64}$/.test(value.slice(2))) return undefined;
  return Buffer.from(value.slice(2), "hex");
}

/** Per-event checks, shared by arriving events and stored rows. */
export function validateSignerEvent(e: SignerEvent): HubResult<Body> {
  const at = `fid ${e.fid} log ${e.logIndex}`;
  if (e.chainId !== OPTIMISM) return invalid(`SIGNER event on chain ${e.chainId}, expected ${OPTIMISM}, for ${at}`);
  if (!Number.isSafeInteger(e.fid) || e.fid <= 0) return invalid(`fid out of range for ${at}`);
  if (!Number.isSafeInteger(e.blockNumber) || e.blockNumber < 0 || !Number.isSafeInteger(e.logIndex) || e.logIndex < 0)
    return invalid(`Invalid chain position for ${at}`);
  if (!(e.blockTimestamp instanceof Date) || !(e.blockTimestamp.getTime() > 0))
    return invalid(`Missing block time for ${at}`);
  if (e.txHash.length !== 32) return invalid(`Invalid transaction hash for ${at}`);
  const body = (e.body && typeof e.body === "object" ? e.body : {}) as Body;
  if (!keyBytes(body.key)) return invalid(`Expected a 32-byte signer key for ${at}`);
  if (![SignerEventType.ADD, SignerEventType.REMOVE, SignerEventType.ADMIN_RESET].includes(body.eventType))
    return invalid(`Unknown signer eventType ${body.eventType} for ${at}`);
  if (body.eventType === SignerEventType.ADD && body.keyType !== 1 && body.keyType !== 2)
    return invalid(`Unsupported signer keyType ${body.keyType} for ${at}`);
  return ok(body);
}

const MAX_BIGINT = (1n << 63n) - 1n;

function word(bytes: Buffer, offset: number): bigint {
  return BigInt(`0x${bytes.subarray(offset, offset + 32).toString("hex")}`);
}

/**
 * The `requestFid` of an ABI-encoded SignedKeyRequestMetadata: one dynamic tuple
 * (requestFid uint256, requestSigner address, signature bytes, deadline uint256), so the first word
 * is the tuple offset (32), not the fid. Ported from Go `application`.
 */
export function applicationFid(metadata: unknown): HubResult<number> {
  const bad = (message: string) => err(new HubError("bad_request.validation_failure", message));
  if (typeof metadata !== "string" || !metadata.startsWith("0x") || !/^(0x)([0-9a-fA-F]{2})*$/.test(metadata))
    return bad("metadata is not hex");
  const b = Buffer.from(metadata.slice(2), "hex");
  if (b.length < 192 || b.length % 32 !== 0 || word(b, 0) !== 32n)
    return bad("metadata is not a canonical signed-key ABI tuple");
  const tuple = b.subarray(32);
  const request = word(tuple, 0);
  if (request <= 0n || request > MAX_BIGINT) return bad("metadata requestFid is outside positive bigint range");
  if (request > BigInt(Number.MAX_SAFE_INTEGER)) return bad("metadata requestFid is not a safe integer");
  if (tuple.subarray(32, 44).some((v) => v !== 0)) return bad("invalid requestSigner padding");
  const offset = word(tuple, 64);
  if (offset < 128n || offset % 32n !== 0n || offset > BigInt(tuple.length - 32))
    return bad("invalid metadata signature offset");
  const off = Number(offset);
  const length = word(tuple, off);
  if (length < 1n || length > BigInt(tuple.length - off - 32)) return bad("invalid metadata signature length");
  const n = Number(length);
  const padded = Math.ceil(n / 32) * 32;
  if (padded > tuple.length - off - 32) return bad("truncated metadata signature");
  if (tuple.subarray(off + 32 + n, off + 32 + padded).some((v) => v !== 0))
    return bad("invalid metadata signature padding");
  return ok(Number(request));
}

function fromOnChainEvent(event: OnChainEvent): SignerEvent {
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

function position(a: SignerEvent, b: SignerEvent): number {
  return a.blockNumber - b.blockNumber || a.logIndex - b.logIndex;
}

function sameEvent(a: SignerEvent, b: SignerEvent): boolean {
  return (
    a.chainId === b.chainId &&
    a.fid === b.fid &&
    a.blockTimestamp.getTime() === b.blockTimestamp.getTime() &&
    Buffer.from(a.txHash).equals(Buffer.from(b.txHash)) &&
    JSON.stringify(a.body) === JSON.stringify(b.body)
  );
}

export type SignerDecision =
  | { kind: "signer"; key: Buffer; appFid: number; timestamp: Date; deletedAt: Date | null }
  | { kind: "issue"; key: Buffer; issue: keyof typeof SIGNER_ISSUES; detail: string }
  | { kind: "skip"; key: Buffer };

/**
 * One decision per key from an fid's SIGNER history, sorted by key. Same-position duplicates must
 * be identical (otherwise the whole fid is rejected). Per key, in chain order (Go `reduce`):
 * ADD registers (ignored after a REMOVE), REMOVE soft-deletes, ADMIN_RESET restores the first
 * captured ADD and allows a later ADD to register again.
 */
export function reduceSigners(input: SignerEvent[]): HubResult<SignerDecision[]> {
  const events = [...input].sort(position);
  const grouped = new Map<string, SignerEvent[]>();
  for (const [i, e] of events.entries()) {
    const validation = validateSignerEvent(e);
    if (validation.isErr()) return err(validation.error);
    const previous = events[i - 1];
    if (previous && position(previous, e) === 0) {
      if (!sameEvent(previous, e))
        return err(new HubError("bad_request.conflict", "Conflicting signer events at the same chain position"));
      continue;
    }
    const key = (validation.value.key as string).toLowerCase();
    grouped.set(key, [...(grouped.get(key) ?? []), e]);
  }
  const decisions: SignerDecision[] = [];
  for (const keyHex of [...grouped.keys()].sort()) {
    const history = grouped.get(keyHex) as SignerEvent[];
    const key = keyBytes(keyHex) as Buffer;
    // The reference holds keyType 1 keys only (checked against every keyType 2 ADD captured on
    // 2026-11-17..19: none is there), so keyType 2 keys are left out without an issue.
    if (history.some((e) => (e.body as Body).eventType === SignerEventType.ADD && (e.body as Body).keyType !== 1)) {
      decisions.push({ kind: "skip", key });
      continue;
    }
    let add: SignerEvent | undefined;
    let original: SignerEvent | undefined;
    let removed = false;
    let canRegister = true;
    let deletedAt: Date | null = null;
    let conflict = false;
    for (const e of history) {
      const body = e.body as Body;
      if (body.eventType === SignerEventType.ADD) {
        if (removed) continue;
        if (add && !canRegister) {
          conflict = true;
          break;
        }
        add = e;
        original ??= e;
        canRegister = false;
        deletedAt = null;
      } else if (body.eventType === SignerEventType.REMOVE) {
        removed = true;
        canRegister = false;
        deletedAt = e.blockTimestamp;
      } else {
        // KeyRegistry resets the key to NULL; a later ADD may register it again.
        canRegister = true;
        removed = false;
        add = original;
        if (add) deletedAt = null;
      }
    }
    if (conflict) {
      decisions.push({ kind: "issue", key, issue: "conflict", detail: "multiple active registrations" });
      continue;
    }
    if (!add) {
      decisions.push({ kind: "issue", key, issue: "pending", detail: "missing preceding signer ADD" });
      continue;
    }
    const body = add.body as Body;
    if (body.metadataType !== 1) {
      decisions.push({ kind: "issue", key, issue: "app", detail: `unsupported metadataType ${body.metadataType}` });
      continue;
    }
    const app = applicationFid(body.metadata);
    if (app.isErr()) {
      decisions.push({ kind: "issue", key, issue: "app", detail: app.error.message });
      continue;
    }
    decisions.push({ kind: "signer", key, appFid: app.value, timestamp: add.blockTimestamp, deletedAt });
  }
  return ok(decisions);
}

/** Deterministic issue key for one (fid, key). */
export function signerIssueHash(fid: number, key: Uint8Array): Buffer {
  return createHash("sha1")
    .update(`${NAME}:${fid}:${Buffer.from(key).toString("hex")}`)
    .digest()
    .subarray(0, 20);
}

async function setKeyIssue(
  db: AppDb,
  fid: number,
  key: Uint8Array,
  current: keyof typeof SIGNER_ISSUES | null,
  detail?: string,
): Promise<void> {
  const hash = signerIssueHash(fid, key);
  for (const [name, reason] of Object.entries(SIGNER_ISSUES)) {
    if (name !== current) await clearIssue(db, NAME, hash, reason);
  }
  if (current) {
    await recordIssue(
      db,
      NAME,
      { fid, hash },
      SIGNER_ISSUES[current],
      { signer: `0x${Buffer.from(key).toString("hex")}`, detail },
      { replace: true },
    );
  }
}

async function loadEvents(db: AppDb, fid: number): Promise<SignerEvent[]> {
  const rows: OnChainEventRow[] = await db
    .selectFrom("onchain_events")
    .selectAll()
    .where("fid", "=", fid)
    .where("type", "=", OnChainEventType.EVENT_TYPE_SIGNER)
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

export function signerId(fid: number, key: Uint8Array): string {
  return conversionId(NAME, `${fid}:${Buffer.from(key).toString("hex")}`);
}

/**
 * Recomputes one fid's signers. A history problem for the whole fid (conflicting events at one
 * chain position) is an issue on every stored key's fid hash and leaves the rows alone; per-key
 * problems skip that key. Rows are only inserted or updated.
 */
export async function projectSigners(db: AppDb, fid: number): Promise<void> {
  const fidHash = createHash("sha1").update(`${NAME}:${fid}`).digest().subarray(0, 20);
  const reduced = reduceSigners(await loadEvents(db, fid));
  if (reduced.isErr()) {
    await recordIssue(
      db,
      NAME,
      { fid, hash: fidHash },
      "invalid signer history",
      { error: reduced.error.message },
      {
        replace: true,
      },
    );
    return;
  }
  await clearIssue(db, NAME, fidHash, "invalid signer history");

  const existing = await db
    .selectFrom("signers")
    .select([
      "id",
      "signer",
      "appFid",
      sql<Date>`timestamp AT TIME ZONE 'UTC'`.as("timestamp"),
      sql<Date | null>`deleted_at AT TIME ZONE 'UTC'`.as("deletedAt"),
    ])
    .where("fid", "=", fid)
    .execute();
  const rows = new Map(existing.map((row) => [row.id, row]));
  const now = new Date().toISOString();
  for (const decision of reduced.value) {
    if (decision.kind === "skip") {
      await setKeyIssue(db, fid, decision.key, null);
      continue;
    }
    if (decision.kind === "issue") {
      await setKeyIssue(db, fid, decision.key, decision.issue, decision.detail);
      continue;
    }
    await setKeyIssue(db, fid, decision.key, null);
    const id = signerId(fid, decision.key);
    const row = rows.get(id);
    const values = {
      appFid: decision.appFid,
      timestamp: decision.timestamp.toISOString(),
      deletedAt: decision.deletedAt?.toISOString() ?? null,
    };
    if (!row) {
      await db
        .insertInto("signers")
        .values({ id, createdAt: now, updatedAt: now, fid, signer: decision.key, ...values })
        .execute();
      continue;
    }
    if (
      Number(row.appFid) === decision.appFid &&
      row.timestamp.getTime() === decision.timestamp.getTime() &&
      (row.deletedAt?.getTime() ?? null) === (decision.deletedAt?.getTime() ?? null)
    )
      continue;
    await db
      .updateTable("signers")
      .set({ ...values, updatedAt: now })
      .where("id", "=", id)
      .execute();
  }
}

/**
 * App fids of the given signer keys of `fid`, from the signers table (removed keys included: a
 * message keeps the app it was signed with). Keys with no row are missing from the map.
 */
export async function appFids(db: AppDb, fid: number, keys: Uint8Array[]): Promise<Map<string, number>> {
  const unique = [...new Map(keys.map((k) => [Buffer.from(k).toString("hex"), Buffer.from(k)])).values()];
  if (!unique.length) return new Map();
  const rows = await db
    .selectFrom("signers")
    .select(["signer", "appFid"])
    .where("fid", "=", fid)
    .where("signer", "in", unique)
    .execute();
  return new Map(rows.map((row) => [Buffer.from(row.signer).toString("hex"), Number(row.appFid)]));
}

/** A SIGNER event other projections recompute on (their app fids may change). */
export function isSignerChange(change: RawChange): boolean {
  return change.kind === "onchain" && change.event.type === OnChainEventType.EVENT_TYPE_SIGNER && change.event.fid > 0;
}

async function applySigners({ db }: ProjectionContext, changes: RawChange[]): Promise<void> {
  const fids = new Set<number>();
  for (const change of changes) if (change.kind === "onchain") fids.add(change.event.fid);
  for (const fid of [...fids].sort((a, b) => a - b)) await projectSigners(db, fid);
}

async function rebuildFids(db: AppDb, afterFid: number, limit: number): Promise<number[]> {
  const rows = await sql<{ fid: number }>`
    SELECT fid FROM (
      SELECT DISTINCT fid FROM onchain_events
      WHERE type = ${OnChainEventType.EVENT_TYPE_SIGNER} AND fid > ${afterFid}
      UNION
      SELECT DISTINCT fid FROM signers WHERE fid > ${afterFid}
    ) f ORDER BY fid LIMIT ${limit}`.execute(db);
  return rows.rows.map((row) => Number(row.fid));
}

export const signersProjection: Projection = {
  name: NAME,
  reconcileTypes: [],
  onChainReconcileTypes: [OnChainEventType.EVENT_TYPE_SIGNER],
  interestedIn: isSignerChange,
  validate: (change) => {
    if (change.kind !== "onchain") return invalid("Not an on-chain event");
    const result = validateSignerEvent(fromOnChainEvent(change.event));
    return result.isErr() ? err(result.error) : ok(undefined);
  },
  apply: applySigners,
  rebuildFids,
  rebuild: ({ db }, fid) => projectSigners(db, fid),
  table: NAME,
  stats: async (db) => {
    const row = await db
      .selectFrom("signers")
      .select([sql<string>`count(*)`.as("total"), sql<string>`count(*) FILTER (WHERE deleted_at IS NULL)`.as("active")])
      .executeTakeFirst();
    const pending = await db
      .selectFrom("shuttleProjectionIssues")
      .select(sql<string>`count(*)`.as("pending"))
      .where("projection", "=", NAME)
      .executeTakeFirst();
    return {
      total: Number(row?.total ?? 0),
      active: Number(row?.active ?? 0),
      pending: Number(pending?.pending ?? 0),
    };
  },
};
