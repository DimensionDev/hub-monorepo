import type { Message, OnChainEvent } from "@dimensiondev/hub-nodejs";
import { bytesToHex } from "../../utils";
import type { AppDb } from "../db";
import { log } from "../log";

/** What an issue is about: a message, an on-chain event (keyed by tx hash) or a whole fid. */
export type IssueSubject = { fid: number | null; hash: Uint8Array };

export function messageSubject(message: Message): IssueSubject {
  return { fid: message.data?.fid ?? null, hash: message.hash };
}

export function onChainSubject(event: OnChainEvent): IssueSubject {
  return { fid: event.fid || null, hash: event.transactionHash };
}

/**
 * Records an expected data gap; idempotent so replays don't duplicate it. With `replace`, an
 * existing issue with the same key gets the new payload (used for per-fid issues whose details
 * change as more history arrives).
 */
export async function recordIssue(
  db: AppDb,
  projection: string,
  subject: IssueSubject,
  reason: string,
  payload?: Record<string, unknown>,
  options: { replace?: boolean } = {},
): Promise<void> {
  log.warn({ projection, hash: bytesToHex(subject.hash), fid: subject.fid, reason }, "projection issue");
  const json = payload ? JSON.stringify(payload) : null;
  await db
    .insertInto("shuttleProjectionIssues")
    .values({ projection, fid: subject.fid, hash: subject.hash, reason, payload: json })
    .onConflict((oc) =>
      options.replace
        ? oc.constraint("shuttle_projection_issues_key").doUpdateSet({ payload: json })
        : oc.constraint("shuttle_projection_issues_key").doNothing(),
    )
    .execute();
}

/** Removes issues that no longer apply (e.g. a fid whose missing history has been filled in). */
export async function clearIssue(db: AppDb, projection: string, hash: Uint8Array, reason: string): Promise<void> {
  await db
    .deleteFrom("shuttleProjectionIssues")
    .where("projection", "=", projection)
    .where("hash", "=", hash)
    .where("reason", "=", reason)
    .execute();
}
