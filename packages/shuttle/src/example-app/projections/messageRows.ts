import { HubError, HubResult, Message } from "@dimensiondev/hub-nodejs";
import { err, ok, Result } from "neverthrow";
import type { MessageRow } from "../../shuttle";
import { farcasterTimeToDate } from "../../utils";

/**
 * Helpers for projections that recompute from stored `messages` rows. Rows are decoded from `raw`
 * (the signed protobuf), not from the JSON `body`, so byte fields (addresses, keys) keep their
 * exact encoding; the row's own columns must agree with the decoded message.
 */

export type StoredMessage = { row: MessageRow; message: Message };

export function decodeRow(row: MessageRow): HubResult<StoredMessage> {
  const decoded = Result.fromThrowable(
    (raw: Uint8Array) => Message.decode(raw),
    () => new HubError("bad_request.parse_failure", `Invalid raw message for fid ${row.fid}`),
  )(row.raw);
  if (decoded.isErr()) return err(decoded.error);
  const message = decoded.value;
  if (
    message.data?.fid !== Number(row.fid) ||
    message.data.type !== row.type ||
    farcasterTimeToDate(message.data.timestamp).getTime() !== row.timestamp.getTime() ||
    !Buffer.from(message.hash).equals(Buffer.from(row.hash))
  ) {
    return err(new HubError("bad_request.validation_failure", `Mismatched stored message for fid ${row.fid}`));
  }
  return ok({ row, message });
}

/** Not deleted (superseded), pruned or revoked. */
export function isActive(row: MessageRow): boolean {
  return row.deletedAt === null && row.prunedAt === null && row.revokedAt === null;
}

/** When an inactive message stopped counting: the earliest of its delete / prune / revoke times. */
export function invalidTime(row: MessageRow): Date {
  const times = [row.deletedAt, row.prunedAt, row.revokedAt].filter((t): t is Date => t !== null);
  return new Date(Math.min(...times.map((t) => t.getTime())));
}

/**
 * Hub CRDT order for messages competing for the same key: later timestamp wins, a remove beats an
 * add at the same timestamp, then the higher hash.
 */
export function compareMessages(a: StoredMessage, b: StoredMessage, isRemove: (m: StoredMessage) => boolean): number {
  return (
    a.row.timestamp.getTime() - b.row.timestamp.getTime() ||
    Number(isRemove(a)) - Number(isRemove(b)) ||
    Buffer.compare(Buffer.from(a.row.hash), Buffer.from(b.row.hash))
  );
}
