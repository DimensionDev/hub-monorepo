import { FarcasterNetwork, HubError, HubResult, Message, MessageType } from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import { err, ok, Result } from "neverthrow";
import { LinkBodyJson, MessageRow } from "../../shuttle";
import { convertProtobufMessageBodyToJson, farcasterTimeToDate } from "../../utils";
import { blockId } from "./ids";
import type { Projection, ProjectionContext, RawChange } from "./types";

type BlockMessage = MessageRow & { body: LinkBodyJson };
type Scope = { targets: Set<number>; full: boolean };
type Decision = { active: true; timestamp: Date } | { active: false; deletedAt: Date };
type BlockState = { decisions: Map<number, Decision>; compact?: BlockMessage };

function isBlock(message: Message): boolean {
  const data = message.data;
  return (
    ((data?.type === MessageType.LINK_ADD || data?.type === MessageType.LINK_REMOVE) &&
      data.linkBody?.type === "block") ||
    (data?.type === MessageType.LINK_COMPACT_STATE && data.linkCompactStateBody?.type === "block")
  );
}

function validate(message: Message): HubResult<void> {
  const data = message.data;
  if (
    !data ||
    !isBlock(message) ||
    data.network !== FarcasterNetwork.MAINNET ||
    !Number.isSafeInteger(data.fid) ||
    data.fid <= 0 ||
    message.hash.length !== 20
  ) {
    return err(new HubError("bad_request.validation_failure", "Invalid mainnet block message"));
  }
  const targets = data.linkCompactStateBody?.targetFids ?? [data.linkBody?.targetFid];
  if (targets.some((fid) => !Number.isSafeInteger(fid) || (fid ?? 0) <= 0)) {
    return err(new HubError("bad_request.validation_failure", `Invalid block target for fid ${data.fid}`));
  }
  return ok(undefined);
}

function validFid(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isBlockBody(body: unknown, type: MessageType): body is LinkBodyJson {
  if (!body || typeof body !== "object" || !("type" in body) || body.type !== "block") return false;
  if (type === MessageType.LINK_COMPACT_STATE) {
    return "targetFids" in body && Array.isArray(body.targetFids) && body.targetFids.every(validFid);
  }
  return (
    "targetFid" in body &&
    validFid(body.targetFid) &&
    (!("displayTimestamp" in body) ||
      (typeof body.displayTimestamp === "number" &&
        Number.isSafeInteger(body.displayTimestamp) &&
        body.displayTimestamp > 0 &&
        Number.isFinite(new Date(body.displayTimestamp).getTime())))
  );
}

function parseBlockRow(row: MessageRow): HubResult<BlockMessage> {
  const decoded = Result.fromThrowable(
    (raw: Uint8Array) => Message.decode(raw),
    () => new HubError("bad_request.parse_failure", `Invalid raw block message for fid ${row.fid}`),
  )(row.raw);
  if (decoded.isErr()) return err(decoded.error);
  const validation = validate(decoded.value);
  if (validation.isErr()) return err(validation.error);
  const raw = decoded.value;
  if (
    raw.data?.fid !== row.fid ||
    raw.data.type !== row.type ||
    farcasterTimeToDate(raw.data.timestamp).getTime() !== row.timestamp.getTime() ||
    !Buffer.from(raw.hash).equals(Buffer.from(row.hash))
  ) {
    return err(new HubError("bad_request.validation_failure", `Mismatched block message for fid ${row.fid}`));
  }
  const body = row.body;
  const expected = convertProtobufMessageBodyToJson(raw);
  if (
    !isBlockBody(body, row.type) ||
    !isBlockBody(expected, row.type) ||
    body.targetFid !== expected.targetFid ||
    body.displayTimestamp !== expected.displayTimestamp ||
    JSON.stringify(body.targetFids) !== JSON.stringify(expected.targetFids)
  ) {
    return err(new HubError("bad_request.validation_failure", `Invalid or mismatched block body for fid ${row.fid}`));
  }
  return ok({ ...row, body });
}

function active(message: BlockMessage): boolean {
  return message.deletedAt === null && message.prunedAt === null && message.revokedAt === null;
}

function compare(a: BlockMessage, b: BlockMessage): number {
  return (
    a.timestamp.getTime() - b.timestamp.getTime() ||
    Number(a.type === MessageType.LINK_REMOVE) - Number(b.type === MessageType.LINK_REMOVE) ||
    Buffer.compare(Buffer.from(a.hash), Buffer.from(b.hash))
  );
}

function invalidTime(message: BlockMessage): Date {
  const times = [message.deletedAt, message.prunedAt, message.revokedAt].filter((time): time is Date => time !== null);
  return new Date(Math.min(...times.map((time) => time.getTime())));
}

function reduce(rows: MessageRow[]): HubResult<BlockState> {
  const messages: BlockMessage[] = [];
  for (const row of rows) {
    const parsed = parseBlockRow(row);
    if (parsed.isErr()) return err(parsed.error);
    messages.push(parsed.value);
  }
  let compact: BlockMessage | undefined;
  const compactHashes = new Map<number, Uint8Array>();
  for (const message of messages) {
    if (message.type !== MessageType.LINK_COMPACT_STATE || !active(message)) continue;
    const sameTime = compactHashes.get(message.timestamp.getTime());
    if (sameTime && !Buffer.from(sameTime).equals(Buffer.from(message.hash))) {
      return err(
        new HubError("bad_request.conflict", `Ambiguous equal-time active block compact states for fid ${message.fid}`),
      );
    }
    compactHashes.set(message.timestamp.getTime(), message.hash);
    if (!compact || message.timestamp > compact.timestamp) compact = message;
  }
  const members = new Set(compact?.body.targetFids ?? []);
  const decisions = new Map<number, Decision>();
  if (compact) {
    for (const target of members) decisions.set(target, { active: true, timestamp: compact.timestamp });
  }
  const ordinary = new Map<number, BlockMessage>();
  const invalid = new Map<number, BlockMessage>();
  for (const message of messages) {
    if (message.type === MessageType.LINK_COMPACT_STATE) continue;
    const target = message.body.targetFid;
    if (!target)
      return err(new HubError("bad_request.validation_failure", `Missing block target for fid ${message.fid}`));
    if (!active(message)) {
      const prior = invalid.get(target);
      if (!prior || compare(message, prior) > 0) invalid.set(target, message);
      continue;
    }
    if (
      compact &&
      message.timestamp < compact.timestamp &&
      (message.type === MessageType.LINK_REMOVE || !members.has(target))
    )
      continue;
    const prior = ordinary.get(target);
    if (!prior || compare(message, prior) > 0) ordinary.set(target, message);
  }
  for (const [target, message] of ordinary) {
    if (message.type === MessageType.LINK_REMOVE) {
      decisions.set(target, { active: false, deletedAt: message.timestamp });
    } else {
      const display = message.body.displayTimestamp;
      if (
        display !== undefined &&
        (!Number.isSafeInteger(display) || display <= 0 || !Number.isFinite(new Date(display).getTime()))
      ) {
        return err(
          new HubError("bad_request.validation_failure", `Invalid block display timestamp for fid ${message.fid}`),
        );
      }
      decisions.set(target, { active: true, timestamp: display === undefined ? message.timestamp : new Date(display) });
    }
  }
  for (const [target, message] of invalid) {
    if (message.type === MessageType.LINK_REMOVE || decisions.has(target)) continue;
    const deletedAt =
      compact && !members.has(target) && message.timestamp <= compact.timestamp
        ? compact.timestamp
        : invalidTime(message);
    decisions.set(target, { active: false, deletedAt });
  }
  // A compact may be the only stored evidence for a relationship. Invalidating it
  // removes that support, but never defeats another currently active message.
  for (const message of [...messages].sort((a, b) => compare(b, a))) {
    if (message.type !== MessageType.LINK_COMPACT_STATE || active(message)) continue;
    for (const target of message.body.targetFids ?? []) {
      if (!decisions.has(target))
        decisions.set(target, {
          active: false,
          deletedAt:
            compact && !members.has(target) && message.timestamp <= compact.timestamp
              ? compact.timestamp
              : invalidTime(message),
        });
    }
  }
  return ok({ decisions, compact });
}

/** Touched targets per blocker fid; a compact state rescans every relationship of the fid. */
function scopes(changes: RawChange[]): Map<number, Scope> {
  const touched = new Map<number, Scope>();
  for (const { message } of changes) {
    const data = message.data;
    if (!data) continue;
    const scope = touched.get(data.fid) ?? { targets: new Set<number>(), full: false };
    if (data.type === MessageType.LINK_COMPACT_STATE) scope.full = true;
    for (const target of data.linkCompactStateBody?.targetFids ?? [data.linkBody?.targetFid]) {
      if (target) scope.targets.add(target);
    }
    touched.set(data.fid, scope);
  }
  return touched;
}

async function applyBlocks({ db }: ProjectionContext, changes: RawChange[]): Promise<void> {
  for (const [fid, scope] of scopes(changes)) {
    const messages = await db
      .selectFrom("messages")
      .selectAll()
      .where("fid", "=", fid)
      .where("type", "in", [MessageType.LINK_ADD, MessageType.LINK_REMOVE, MessageType.LINK_COMPACT_STATE])
      .where(sql<string>`body ->> 'type'`, "=", "block")
      .execute();
    const reduced = reduce(messages);
    if (reduced.isErr()) throw reduced.error;
    const projection = reduced.value;
    const existing = await db
      .selectFrom("blocks")
      .select([
        "id",
        "blockedFid",
        sql<Date>`timestamp AT TIME ZONE 'UTC'`.as("timestamp"),
        sql<Date | null>`deleted_at AT TIME ZONE 'UTC'`.as("deletedAt"),
      ])
      .where("blockerFid", "=", fid)
      .execute();
    const byTarget = new Map<number, typeof existing[number]>();
    for (const row of existing) {
      if (byTarget.has(row.blockedFid))
        throw new HubError("bad_request.conflict", `Duplicate blocks relationship ${fid}:${row.blockedFid}`);
      byTarget.set(row.blockedFid, row);
      if (scope.full) scope.targets.add(row.blockedFid);
    }
    if (scope.full && projection.compact) {
      for (const target of projection.decisions.keys()) scope.targets.add(target);
    }
    for (const target of scope.targets) {
      const row = byTarget.get(target);
      const decision: Decision | undefined =
        projection.decisions.get(target) ??
        (scope.full && projection.compact ? { active: false, deletedAt: projection.compact.timestamp } : undefined);
      if (!decision) continue;
      const now = new Date().toISOString();
      if (decision.active) {
        const timestamp = decision.timestamp;
        if (!row) {
          await db
            .insertInto("blocks")
            .values({
              id: blockId(fid, target),
              blockerFid: fid,
              blockedFid: target,
              timestamp: timestamp.toISOString(),
              createdAt: now,
              updatedAt: now,
              deletedAt: null,
            })
            .execute();
        } else if (row.deletedAt !== null || row.timestamp.getTime() !== timestamp.getTime()) {
          await db
            .updateTable("blocks")
            .set({ timestamp: timestamp.toISOString(), deletedAt: null, updatedAt: now })
            .where("id", "=", row.id)
            .execute();
        }
      } else if (row && row.deletedAt === null) {
        await db
          .updateTable("blocks")
          .set({ deletedAt: decision.deletedAt.toISOString(), updatedAt: now })
          .where("id", "=", row.id)
          .execute();
      }
    }
  }
}

export const blocksProjection: Projection = {
  name: "blocks",
  // getAllLinkMessagesByFid returns adds and removes; reconciliation also fetches compact states.
  reconcileTypes: [MessageType.LINK_ADD],
  interestedIn: (change) => isBlock(change.message),
  validate: (change) => validate(change.message),
  apply: applyBlocks,
};
