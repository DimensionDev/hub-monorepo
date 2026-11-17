import {
  HubError,
  HubEvent,
  isMergeMessageHubEvent,
  isPruneMessageHubEvent,
  isRevokeMessageHubEvent,
  Message,
  extractTimestampFromEvent,
} from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import type { DB } from "../../shuttle";
import { MessageProcessor } from "../../shuttle/messageProcessor";
import type { AppDb } from "../db";
import { recordIssue } from "./issues";
import { recordState, shouldApply } from "./lifecycle";
import type { Projection, ProjectionSource, RawChange } from "./types";

export function mutations(event: HubEvent): RawChange[] {
  if (isMergeMessageHubEvent(event)) {
    return [
      ...event.mergeMessageBody.deletedMessages.map(
        (message): RawChange => ({ kind: "message", message, operation: "delete" }),
      ),
      { kind: "message", message: event.mergeMessageBody.message, operation: "merge" },
    ];
  }
  if (isPruneMessageHubEvent(event))
    return [{ kind: "message", message: event.pruneMessageBody.message, operation: "prune" }];
  if (isRevokeMessageHubEvent(event))
    return [{ kind: "message", message: event.revokeMessageBody.message, operation: "revoke" }];
  return [];
}

function shouldValidateMessages() {
  return process.env["SHUTTLE_VALIDATE_MESSAGES"] === "true";
}

/**
 * Owns every hub event (or backfilled message) that at least one projection cares about:
 * per-fid locking, raw message storage behind the lifecycle guard, then the projections, all
 * in the caller's transaction. Events nobody projects are left to HubEventProcessor.
 */
export class ProjectionDispatcher {
  constructor(private readonly projections: Projection[], private readonly schema = "public") {}

  private interested(change: RawChange): Projection[] {
    return this.projections.filter((p) => p.interestedIn(change));
  }

  /** Returns true when the event was handled here and generic processing must be skipped. */
  async processEvent(event: HubEvent, txn: DB): Promise<boolean> {
    const changes = mutations(event);
    if (!changes.some((c) => this.interested(c).length > 0)) return false;
    if (
      !Number.isSafeInteger(event.id) ||
      event.id <= 0 ||
      !Number.isSafeInteger(event.timestamp) ||
      event.timestamp <= 0
    ) {
      throw new HubError(
        "bad_request.validation_failure",
        "Projected events require a positive, safe event id and timestamp",
      );
    }
    const source: ProjectionSource = {
      kind: "event",
      shardIndex: event.shardIndex,
      eventId: event.id,
      eventTimestamp: new Date(extractTimestampFromEvent(event)),
    };
    await this.run(txn as unknown as AppDb, changes, source);
    return true;
  }

  /**
   * Reconcile path for a message missing from the DB. Returns false (nothing done) when no
   * projection cares, so the caller can fall back to the generic handler.
   */
  async processMissingMessage(db: DB, message: Message): Promise<boolean> {
    const change: RawChange = { kind: "message", message, operation: "merge" };
    if (this.interested(change).length === 0) return false;
    await db.transaction().execute((trx) => this.run(trx as unknown as AppDb, [change], { kind: "backfill" }));
    return true;
  }

  private async run(db: AppDb, changes: RawChange[], source: ProjectionSource): Promise<void> {
    const fids = [...new Set(changes.map((c) => c.message.data?.fid))].sort((a, b) => (a ?? 0) - (b ?? 0));
    for (const fid of fids) {
      if (!fid) throw new HubError("bad_request.validation_failure", "Missing message fid");
      // One namespace for every projection so multi-table events can't deadlock each other.
      await sql`SELECT pg_advisory_xact_lock(hashtextextended(
        ${this.schema} || ':shuttle:fid:' || ${String(fid)}, 0))`.execute(db);
    }

    const applied = new Map<Projection, RawChange[]>();
    for (const change of changes) {
      const projections = this.interested(change);
      if (projections.length === 0) {
        await MessageProcessor.storeMessage(
          change.message,
          db as unknown as DB,
          change.operation,
          undefined,
          shouldValidateMessages(),
        );
        continue;
      }
      const accepted: Projection[] = [];
      for (const projection of projections) {
        const validation = projection.validate?.(change);
        if (validation?.isErr()) {
          await recordIssue(db, projection.name, change.message, validation.error.message, {
            operation: change.operation,
            type: change.message.data?.type,
          });
        } else {
          accepted.push(projection);
        }
      }
      // A message a projection rejects is not stored: projections recompute from `messages`,
      // and an invalid row there would poison every later recompute for the fid.
      if (accepted.length === 0) continue;
      if (!(await shouldApply(db, change, source))) continue;
      await MessageProcessor.storeMessage(
        change.message,
        db as unknown as DB,
        change.operation,
        undefined,
        shouldValidateMessages(),
      );
      if (change.operation !== "merge" && source.kind === "event") {
        // Use the hub's event time, not wall-clock, so the projection is reproducible.
        await db
          .updateTable("messages")
          .set({
            deletedAt: change.operation === "delete" ? source.eventTimestamp : null,
            prunedAt: change.operation === "prune" ? source.eventTimestamp : null,
            revokedAt: change.operation === "revoke" ? source.eventTimestamp : null,
          })
          .where("hash", "=", change.message.hash)
          .execute();
      }
      await recordState(db, change, source);
      for (const projection of accepted) {
        const list = applied.get(projection) ?? [];
        list.push(change);
        applied.set(projection, list);
      }
    }

    for (const [projection, list] of applied) {
      await projection.apply({ db, schema: this.schema, source }, list);
    }
  }
}
