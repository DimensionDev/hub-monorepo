import { createHash } from "node:crypto";
import { FarcasterNetwork, HubError, HubResult, Message, MessageType, UserDataType } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import { err, ok } from "neverthrow";
import type { AppDb } from "../db";
import { conversionId } from "./ids";
import { clearIssue, recordIssue } from "./issues";
import { compareMessages, decodeRow, invalidTime, isActive, StoredMessage } from "./messageRows";
import { appFids, isSignerChange } from "./signers";
import type { Projection, ProjectionContext, RawChange } from "./types";

/**
 * profiles and profile_external_accounts, both from USER_DATA_ADD messages. Per (fid, user data
 * type) the hub keeps one value: the active message that is latest by timestamp, then hash.
 * Superseded, pruned and revoked messages do not count; a type with no active message is unset.
 *
 * Every apply recomputes the fid from all its stored USER_DATA_ADD rows; rows are only inserted
 * or updated.
 */

const TYPE = MessageType.USER_DATA_ADD;

function invalid(message: string) {
  return err(new HubError("bad_request.validation_failure", message));
}

function validate(message: Message): HubResult<void> {
  const data = message.data;
  if (
    !data ||
    data.type !== TYPE ||
    data.network !== FarcasterNetwork.MAINNET ||
    !Number.isSafeInteger(data.fid) ||
    data.fid <= 0 ||
    message.hash.length !== 20 ||
    !data.userDataBody ||
    typeof data.userDataBody.value !== "string"
  )
    return invalid("Invalid mainnet user data message");
  return ok(undefined);
}

function isUserData(change: RawChange, types: readonly UserDataType[]): boolean {
  if (change.kind !== "message") return false;
  const data = change.message.data;
  return data?.type === TYPE && types.includes(data.userDataBody?.type as UserDataType);
}

type Winner = { stored: StoredMessage; value: string };
/** Per type: the active winner, or the latest inactive message when none is active. */
type TypeState = { winner?: Winner; lastInactive?: StoredMessage };

const neverRemove = () => false;

async function loadUserData(
  db: AppDb,
  projection: string,
  fid: number,
  types: readonly UserDataType[],
): Promise<Map<UserDataType, TypeState>> {
  const rows = await db.selectFrom("messages").selectAll().where("fid", "=", fid).where("type", "=", TYPE).execute();
  const byType = new Map<UserDataType, StoredMessage[]>();
  for (const row of rows) {
    const decoded = decodeRow(row);
    if (decoded.isErr()) {
      await recordIssue(db, projection, { fid, hash: row.hash }, decoded.error.message);
      continue;
    }
    const type = decoded.value.message.data?.userDataBody?.type as UserDataType;
    if (!types.includes(type)) continue;
    byType.set(type, [...(byType.get(type) ?? []), decoded.value]);
  }
  const states = new Map<UserDataType, TypeState>();
  for (const [type, messages] of byType) {
    const sorted = [...messages].sort((a, b) => compareMessages(a, b, neverRemove));
    const active = sorted.filter((m) => isActive(m.row));
    const winner = active[active.length - 1];
    const inactive = sorted.filter((m) => !isActive(m.row));
    states.set(type, {
      winner: winner ? { stored: winner, value: winner.message.data?.userDataBody?.value ?? "" } : undefined,
      lastInactive: inactive[inactive.length - 1],
    });
  }
  return states;
}

async function messageFids(
  db: AppDb,
  afterFid: number,
  table: "profiles" | "profile_external_accounts",
  limit: number,
) {
  const rows = await sql<{ fid: number }>`
    SELECT fid FROM (
      SELECT DISTINCT fid FROM messages WHERE type = ${TYPE} AND fid > ${afterFid}
      UNION
      SELECT DISTINCT fid FROM ${sql.table(table)} WHERE fid > ${afterFid}
    ) f ORDER BY fid LIMIT ${limit}`.execute(db);
  return rows.rows.map((row) => Number(row.fid));
}

function changedFids(changes: RawChange[]): number[] {
  const fids = new Set<number>();
  for (const change of changes) {
    const fid = change.kind === "message" ? change.message.data?.fid : change.event.fid;
    if (fid) fids.add(fid);
  }
  return [...fids].sort((a, b) => a - b);
}

// --- profiles ------------------------------------------------------------------------------------

const PROFILE_TYPES = [
  UserDataType.PFP,
  UserDataType.DISPLAY,
  UserDataType.BIO,
  UserDataType.URL,
  UserDataType.USERNAME,
  UserDataType.LOCATION,
  UserDataType.BANNER,
  UserDataType.USER_DATA_PRIMARY_ADDRESS_ETHEREUM,
  UserDataType.USER_DATA_PRIMARY_ADDRESS_SOLANA,
  UserDataType.PROFILE_TOKEN,
] as const;

const BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/** Bitcoin-alphabet base58 (Solana addresses); undefined when not valid base58. */
export function base58Decode(value: string): Buffer | undefined {
  if (!value) return undefined;
  let n = 0n;
  for (const char of value) {
    const digit = BASE58.indexOf(char);
    if (digit < 0) return undefined;
    n = n * 58n + BigInt(digit);
  }
  const hex = n === 0n ? "" : n.toString(16);
  const body = Buffer.from(hex.length % 2 ? `0${hex}` : hex, "hex");
  const zeros = value.match(/^1*/)?.[0].length ?? 0;
  return Buffer.concat([Buffer.alloc(zeros), body]);
}

function ethAddress(value: string): Buffer | null {
  return /^0x[0-9a-fA-F]{40}$/.test(value) ? Buffer.from(value.slice(2), "hex") : null;
}

function solAddress(value: string): Buffer | null {
  const bytes = base58Decode(value);
  return bytes?.length === 32 ? bytes : null;
}

/** `geo:<lat>,<lon>` (the hub's location format) to coordinates. */
export function coordinates(location: string): { latitude: number; longitude: number } | null {
  const match = /^geo:(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)$/.exec(location);
  if (!match) return null;
  const latitude = Number(match[1]);
  const longitude = Number(match[2]);
  if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
  return { latitude, longitude };
}

export type ProfileValues = {
  bio: string | null;
  pfpUrl: string | null;
  url: string | null;
  username: string | null;
  displayName: string | null;
  location: string | null;
  latitude: number | null;
  longitude: number | null;
  primaryEthAddress: Buffer | null;
  primarySolAddress: Buffer | null;
  bannerUrl: string | null;
  tokenUri: string | null;
};

/** Profile columns from the winning values. Unset types are NULL; an explicit "" stays "". */
export function profileValues(values: Map<UserDataType, string>): ProfileValues {
  const text = (type: UserDataType) => values.get(type) ?? null;
  const location = text(UserDataType.LOCATION);
  const coords = location ? coordinates(location) : null;
  const eth = text(UserDataType.USER_DATA_PRIMARY_ADDRESS_ETHEREUM);
  const sol = text(UserDataType.USER_DATA_PRIMARY_ADDRESS_SOLANA);
  return {
    bio: text(UserDataType.BIO),
    pfpUrl: text(UserDataType.PFP),
    url: text(UserDataType.URL),
    username: text(UserDataType.USERNAME),
    displayName: text(UserDataType.DISPLAY),
    location,
    latitude: coords?.latitude ?? null,
    longitude: coords?.longitude ?? null,
    primaryEthAddress: eth ? ethAddress(eth) : null,
    primarySolAddress: sol ? solAddress(sol) : null,
    bannerUrl: text(UserDataType.BANNER),
    tokenUri: text(UserDataType.PROFILE_TOKEN),
  };
}

function sameValue(a: unknown, b: unknown): boolean {
  if (b instanceof Buffer || a instanceof Uint8Array) {
    return a instanceof Uint8Array && b instanceof Uint8Array && Buffer.from(a).equals(Buffer.from(b));
  }
  if (typeof b === "number" && a !== null) return Math.abs(Number(a) - b) < 1e-4; // real column
  return a === b;
}

export async function projectProfile(db: AppDb, fid: number): Promise<void> {
  const states = await loadUserData(db, "profiles", fid, PROFILE_TYPES);
  const values = new Map<UserDataType, string>();
  for (const [type, state] of states) if (state.winner) values.set(type, state.winner.value);
  const row = await db.selectFrom("profiles").selectAll().where("fid", "=", fid).executeTakeFirst();
  // No profile message on record: nothing to create (an existing row is left alone).
  if (!row && states.size === 0) return;
  const next = profileValues(values);
  const now = new Date().toISOString();
  if (!row) {
    await db
      .insertInto("profiles")
      .values({ id: conversionId("profiles", String(fid)), createdAt: now, updatedAt: now, fid, ...next })
      .execute();
    return;
  }
  const current = row as unknown as Record<string, unknown>;
  if (Object.entries(next).every(([k, v]) => sameValue(current[k], v))) return;
  await db
    .updateTable("profiles")
    .set({ ...next, updatedAt: now })
    .where("id", "=", row.id)
    .execute();
}

export const profilesProjection: Projection = {
  name: "profiles",
  reconcileTypes: [MessageType.USER_DATA_ADD],
  interestedIn: (change) => isUserData(change, PROFILE_TYPES),
  validate: (change) => (change.kind === "message" ? validate(change.message) : ok(undefined)),
  apply: async ({ db }: ProjectionContext, changes) => {
    for (const fid of changedFids(changes)) await projectProfile(db, fid);
  },
  rebuildFids: (db, afterFid, limit) => messageFids(db, afterFid, "profiles", limit),
  rebuild: ({ db }, fid) => projectProfile(db, fid),
  table: "profiles",
  stats: async (db) => {
    const row = await db.selectFrom("profiles").select(sql<string>`count(*)`.as("total")).executeTakeFirst();
    return { total: Number(row?.total ?? 0) };
  },
};

// --- profile_external_accounts ------------------------------------------------------------------

const ACCOUNT_TYPES = [UserDataType.TWITTER, UserDataType.GITHUB] as const;
const PEA = "profile_external_accounts";
export const ACCOUNT_ISSUE = "unknown signer app fid";

function accountIssueHash(fid: number, type: number): Buffer {
  return createHash("sha1").update(`${PEA}:${fid}:${type}`).digest().subarray(0, 20);
}

/**
 * One row per (fid, type): `account` and `timestamp` from the winning message, `app_fid` from its
 * signer (required: without a known signer the row is not written and an issue records why, until
 * the SIGNER arrives). An empty value, or no active message after one was set, soft-deletes.
 */
export async function projectAccounts(db: AppDb, fid: number): Promise<void> {
  const states = await loadUserData(db, PEA, fid, ACCOUNT_TYPES);
  const signers = [...states.values()].flatMap((s) => {
    const m = s.winner?.stored ?? s.lastInactive;
    return m ? [m.row.signer] : [];
  });
  const apps = await appFids(db, fid, signers);
  const existing = await db
    .selectFrom("profileExternalAccounts")
    .select([
      "id",
      "type",
      "account",
      "appFid",
      sql<Date>`timestamp AT TIME ZONE 'UTC'`.as("timestamp"),
      sql<Date | null>`deleted_at AT TIME ZONE 'UTC'`.as("deletedAt"),
    ])
    .where("fid", "=", fid)
    .execute();
  const now = new Date().toISOString();
  for (const [type, state] of states) {
    const basis = state.winner?.stored ?? state.lastInactive;
    if (!basis) continue;
    const id = conversionId(PEA, `${fid}:${type}`);
    const row = existing.find((r) => r.id === id);
    const account = state.winner ? state.winner.value : row?.account ?? basis.message.data?.userDataBody?.value ?? "";
    const deletedAt = state.winner
      ? state.winner.value === ""
        ? state.winner.stored.row.timestamp
        : null
      : invalidTime(basis.row);
    const appFid = apps.get(Buffer.from(basis.row.signer).toString("hex"));
    const hash = accountIssueHash(fid, type);
    if (appFid === undefined) {
      await recordIssue(
        db,
        PEA,
        { fid, hash },
        ACCOUNT_ISSUE,
        { type, signer: `0x${Buffer.from(basis.row.signer).toString("hex")}` },
        { replace: true },
      );
      continue;
    }
    await clearIssue(db, PEA, hash, ACCOUNT_ISSUE);
    const timestamp = basis.row.timestamp;
    const values = { account, appFid, timestamp: timestamp.toISOString(), deletedAt: deletedAt?.toISOString() ?? null };
    if (!row) {
      await db
        .insertInto("profileExternalAccounts")
        .values({ id, createdAt: now, updatedAt: now, fid, type, ...values })
        .execute();
      continue;
    }
    if (
      row.account === account &&
      Number(row.appFid) === appFid &&
      row.timestamp.getTime() === timestamp.getTime() &&
      (row.deletedAt?.getTime() ?? null) === (deletedAt?.getTime() ?? null)
    )
      continue;
    await db
      .updateTable("profileExternalAccounts")
      .set({ ...values, updatedAt: now })
      .where("id", "=", id)
      .execute();
  }
}

export const profileExternalAccountsProjection: Projection = {
  name: PEA,
  reconcileTypes: [MessageType.USER_DATA_ADD],
  interestedIn: (change) => isUserData(change, ACCOUNT_TYPES) || isSignerChange(change),
  validate: (change) => (change.kind === "message" ? validate(change.message) : ok(undefined)),
  apply: async ({ db }: ProjectionContext, changes) => {
    for (const fid of changedFids(changes)) await projectAccounts(db, fid);
  },
  rebuildFids: (db, afterFid, limit) => messageFids(db, afterFid, PEA, limit),
  rebuild: ({ db }, fid) => projectAccounts(db, fid),
  table: PEA,
  stats: async (db) => {
    const row = await db
      .selectFrom("profileExternalAccounts")
      .select([sql<string>`count(*)`.as("total"), sql<string>`count(*) FILTER (WHERE deleted_at IS NULL)`.as("active")])
      .executeTakeFirst();
    const pending = await db
      .selectFrom("shuttleProjectionIssues")
      .select(sql<string>`count(*)`.as("pending"))
      .where("projection", "=", PEA)
      .where("reason", "=", ACCOUNT_ISSUE)
      .executeTakeFirst();
    return {
      total: Number(row?.total ?? 0),
      active: Number(row?.active ?? 0),
      pending: Number(pending?.pending ?? 0),
    };
  },
};
