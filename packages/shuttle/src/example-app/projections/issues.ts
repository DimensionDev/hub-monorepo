import type { Message } from "@dimensiondev/hub-nodejs";
import { bytesToHex } from "../../utils";
import type { AppDb } from "../db";
import { log } from "../log";

/** Records an expected data gap; idempotent so replays don't duplicate it. */
export async function recordIssue(
  db: AppDb,
  projection: string,
  message: Message,
  reason: string,
  payload?: Record<string, unknown>,
): Promise<void> {
  log.warn({ projection, hash: bytesToHex(message.hash), fid: message.data?.fid, reason }, "projection issue");
  await db
    .insertInto("shuttleProjectionIssues")
    .values({
      projection,
      fid: message.data?.fid ?? null,
      hash: message.hash,
      reason,
      payload: payload ? JSON.stringify(payload) : null,
    })
    .onConflict((oc) => oc.constraint("shuttle_projection_issues_key").doNothing())
    .execute();
}
