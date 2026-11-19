import { FarcasterNetwork, HubError, HubResult, Message, MessageType, Protocol } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import { err, ok } from "neverthrow";
import type { AppDb } from "../db";
import { conversionId } from "./ids";
import { recordIssue } from "./issues";
import { compareMessages, decodeRow, invalidTime, isActive, StoredMessage } from "./messageRows";
import { appFids, isSignerChange } from "./signers";
import type { Projection, ProjectionContext, RawChange } from "./types";

/**
 * verifications: one row per (fid, protocol, address) from VERIFICATION_ADD_ETH_ADDRESS /
 * VERIFICATION_REMOVE messages (reference: one row per key, soft-deleted on removal). The winning
 * message per key follows the hub CRDT order (later timestamp, remove over add, higher hash):
 *
 * - an active ADD wins: the row is live, `timestamp` is that ADD's time, `app_fid` the app of the
 *   key that signed it (NULL until the signer is known);
 * - an active REMOVE wins: the row is soft-deleted at the remove's time;
 * - no active message, but an ADD was pruned / revoked / superseded: soft-deleted at that time.
 *
 * Every apply recomputes the fid from all stored messages; rows are only inserted or updated.
 * SIGNER changes recompute the fid too, so a late signer fills in `app_fid`.
 */

const NAME = "verifications";
const TYPES = [MessageType.VERIFICATION_ADD_ETH_ADDRESS, MessageType.VERIFICATION_REMOVE];

function invalid(message: string) {
  return err(new HubError("bad_request.validation_failure", message));
}

type Key = { protocol: Protocol; address: Buffer };

function keyOf(message: Message): HubResult<Key> {
  const data = message.data;
  const body = data?.verificationAddAddressBody ?? data?.verificationRemoveBody;
  if (!body) return invalid(`Missing verification body for fid ${data?.fid}`);
  const protocol = body.protocol ?? Protocol.ETHEREUM;
  const address = Buffer.from(body.address ?? []);
  const expected = protocol === Protocol.ETHEREUM ? 20 : protocol === Protocol.SOLANA ? 32 : undefined;
  if (expected === undefined) return invalid(`Unknown verification protocol ${protocol} for fid ${data?.fid}`);
  if (address.length !== expected)
    return invalid(`Invalid ${protocol === Protocol.ETHEREUM ? "Ethereum" : "Solana"} address for fid ${data?.fid}`);
  return ok({ protocol, address });
}

function validate(message: Message): HubResult<void> {
  const data = message.data;
  if (
    !data ||
    !TYPES.includes(data.type) ||
    data.network !== FarcasterNetwork.MAINNET ||
    !Number.isSafeInteger(data.fid) ||
    data.fid <= 0 ||
    message.hash.length !== 20
  )
    return invalid("Invalid mainnet verification message");
  const key = keyOf(message);
  return key.isErr() ? err(key.error) : ok(undefined);
}

const isRemove = (m: StoredMessage) => m.message.data?.type === MessageType.VERIFICATION_REMOVE;

export type VerificationDecision = Key & {
  timestamp: Date;
  deletedAt: Date | null;
  /** The ADD whose signer gives app_fid; absent when only removes are known. */
  signer: Uint8Array | null;
};

/** One decision per key with an ADD on record; a key seen only in removes has nothing to write. */
export function reduceVerifications(stored: StoredMessage[]): VerificationDecision[] {
  const byKey = new Map<string, { key: Key; messages: StoredMessage[] }>();
  for (const m of stored) {
    const key = keyOf(m.message);
    if (key.isErr()) continue; // validated on arrival; decodeRow already rejected malformed rows
    const id = `${key.value.protocol}:${key.value.address.toString("hex")}`;
    const entry = byKey.get(id) ?? { key: key.value, messages: [] };
    entry.messages.push(m);
    byKey.set(id, entry);
  }
  const decisions: VerificationDecision[] = [];
  for (const { key, messages } of byKey.values()) {
    const sorted = [...messages].sort((a, b) => compareMessages(a, b, isRemove));
    const adds = sorted.filter((m) => !isRemove(m));
    const latestAdd = adds[adds.length - 1];
    if (!latestAdd) continue;
    const active = sorted.filter((m) => isActive(m.row));
    const winner = active[active.length - 1];
    if (winner && !isRemove(winner)) {
      decisions.push({ ...key, timestamp: winner.row.timestamp, deletedAt: null, signer: winner.row.signer });
    } else if (winner) {
      decisions.push({
        ...key,
        timestamp: latestAdd.row.timestamp,
        deletedAt: winner.row.timestamp,
        signer: latestAdd.row.signer,
      });
    } else {
      decisions.push({
        ...key,
        timestamp: latestAdd.row.timestamp,
        deletedAt: invalidTime(latestAdd.row),
        signer: latestAdd.row.signer,
      });
    }
  }
  return decisions;
}

export function verificationId(fid: number, protocol: number, address: Uint8Array): string {
  return conversionId(NAME, `${fid}:${protocol}:${Buffer.from(address).toString("hex")}`);
}

export async function projectVerifications(db: AppDb, fid: number): Promise<void> {
  const rows = await db.selectFrom("messages").selectAll().where("fid", "=", fid).where("type", "in", TYPES).execute();
  const stored: StoredMessage[] = [];
  for (const row of rows) {
    const decoded = decodeRow(row);
    if (decoded.isErr()) {
      await recordIssue(db, NAME, { fid, hash: row.hash }, decoded.error.message);
      continue;
    }
    stored.push(decoded.value);
  }
  const decisions = reduceVerifications(stored);
  const apps = await appFids(
    db,
    fid,
    decisions.flatMap((d) => (d.signer ? [d.signer] : [])),
  );
  const existing = await db
    .selectFrom("verifications")
    .select([
      "id",
      "appFid",
      sql<Date>`timestamp AT TIME ZONE 'UTC'`.as("timestamp"),
      sql<Date | null>`deleted_at AT TIME ZONE 'UTC'`.as("deletedAt"),
    ])
    .where("fid", "=", fid)
    .execute();
  const byId = new Map(existing.map((row) => [row.id, row]));
  const now = new Date().toISOString();
  for (const d of decisions) {
    const id = verificationId(fid, d.protocol, d.address);
    const appFid = d.signer ? apps.get(Buffer.from(d.signer).toString("hex")) ?? null : null;
    const row = byId.get(id);
    const values = {
      timestamp: d.timestamp.toISOString(),
      deletedAt: d.deletedAt?.toISOString() ?? null,
      appFid,
    };
    if (!row) {
      await db
        .insertInto("verifications")
        .values({ id, createdAt: now, updatedAt: now, fid, address: d.address, protocol: d.protocol, ...values })
        .execute();
      continue;
    }
    if (
      row.timestamp.getTime() === d.timestamp.getTime() &&
      (row.deletedAt?.getTime() ?? null) === (d.deletedAt?.getTime() ?? null) &&
      (row.appFid === null ? null : Number(row.appFid)) === appFid
    )
      continue;
    await db
      .updateTable("verifications")
      .set({ ...values, updatedAt: now })
      .where("id", "=", id)
      .execute();
  }
}

function isVerification(change: RawChange): boolean {
  return change.kind === "message" && TYPES.includes(change.message.data?.type as MessageType);
}

async function apply({ db }: ProjectionContext, changes: RawChange[]): Promise<void> {
  const fids = new Set<number>();
  for (const change of changes) {
    const fid = change.kind === "message" ? change.message.data?.fid : change.event.fid;
    if (fid) fids.add(fid);
  }
  for (const fid of [...fids].sort((a, b) => a - b)) await projectVerifications(db, fid);
}

async function rebuildFids(db: AppDb, afterFid: number, limit: number): Promise<number[]> {
  const rows = await sql<{ fid: number }>`
    SELECT fid FROM (
      SELECT DISTINCT fid FROM messages WHERE type IN (${sql.join(TYPES)}) AND fid > ${afterFid}
      UNION
      SELECT DISTINCT fid FROM verifications WHERE fid > ${afterFid}
    ) f ORDER BY fid LIMIT ${limit}`.execute(db);
  return rows.rows.map((row) => Number(row.fid));
}

export const verificationsProjection: Projection = {
  name: NAME,
  // getAllVerificationMessagesByFid returns adds and removes.
  reconcileTypes: [MessageType.VERIFICATION_ADD_ETH_ADDRESS],
  interestedIn: (change) => isVerification(change) || isSignerChange(change),
  validate: (change) => (change.kind === "message" ? validate(change.message) : ok(undefined)),
  apply,
  rebuildFids,
  rebuild: ({ db }, fid) => projectVerifications(db, fid),
  table: NAME,
  stats: async (db) => {
    const row = await db
      .selectFrom("verifications")
      .select([sql<string>`count(*)`.as("total"), sql<string>`count(*) FILTER (WHERE deleted_at IS NULL)`.as("active")])
      .executeTakeFirst();
    return { total: Number(row?.total ?? 0), active: Number(row?.active ?? 0) };
  },
};
