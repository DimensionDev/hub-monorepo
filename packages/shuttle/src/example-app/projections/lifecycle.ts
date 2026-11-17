import { HubError } from "@dimensiondev/hub-nodejs";
import type { AppDb } from "../db";
import type { ProjectionSource, RawChange } from "./types";

/**
 * Decides whether a projected message's lifecycle change may be applied, so that replays,
 * out-of-order delivery and backfill can never move a message back to an older state.
 *
 * - event: apply only if newer than the last recorded event for this (fid, hash).
 * - backfill: insert-only. A message that already has a state row or a stored row is left
 *   alone; backfill never revives a deleted/pruned/revoked message.
 */
export async function shouldApply(db: AppDb, change: RawChange, source: ProjectionSource): Promise<boolean> {
  const fid = change.message.data?.fid;
  if (!fid) throw new HubError("bad_request.validation_failure", "Missing message fid");
  const previous = await db
    .selectFrom("shuttleMessageState")
    .select(["shardIndex", "eventId"])
    .where("fid", "=", fid)
    .where("hash", "=", change.message.hash)
    .executeTakeFirst();
  if (source.kind === "backfill") {
    if (change.operation !== "merge" || previous) return false;
    const stored = await db
      .selectFrom("messages")
      .select("id")
      .where("hash", "=", change.message.hash)
      .executeTakeFirst();
    return !stored;
  }
  if (!previous || previous.eventId === null) return true;
  if (previous.shardIndex !== null && previous.shardIndex !== source.shardIndex)
    throw new HubError("bad_request.conflict", `Message shard changed for fid ${fid}`);
  return previous.eventId < source.eventId;
}

export async function recordState(db: AppDb, change: RawChange, source: ProjectionSource): Promise<void> {
  const fid = change.message.data?.fid;
  if (!fid) throw new HubError("bad_request.validation_failure", "Missing message fid");
  const state =
    source.kind === "event"
      ? {
          shardIndex: source.shardIndex,
          eventId: source.eventId,
          operation: change.operation,
          eventTimestamp: source.eventTimestamp,
        }
      : { shardIndex: null, eventId: null, operation: change.operation, eventTimestamp: new Date() };
  await db
    .insertInto("shuttleMessageState")
    .values({ fid, hash: change.message.hash, ...state })
    .onConflict((oc) => oc.columns(["fid", "hash"]).doUpdateSet(state))
    .execute();
}
