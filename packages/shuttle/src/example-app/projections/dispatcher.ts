import {
  HubError,
  HubEvent,
  isMergeMessageHubEvent,
  isMergeOnChainHubEvent,
  isPruneMessageHubEvent,
  isRevokeMessageHubEvent,
  Message,
  OnChainEvent,
  extractTimestampFromEvent,
} from "@dimensiondev/hub-nodejs";
import { sql } from "kysely";
import type { DB } from "../../shuttle";
import { MessageProcessor } from "../../shuttle/messageProcessor";
import type { AppDb } from "../db";
import { storeOnChainEventRow } from "../raw/onchainEvents";
import { messageSubject, onChainSubject, recordIssue } from "./issues";
import { recordState, shouldApply } from "./lifecycle";
import { changeFid, type Projection, type ProjectionSource, type RawChange } from "./types";

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
  if (isMergeOnChainHubEvent(event)) return [{ kind: "onchain", event: event.mergeOnChainEventBody.onChainEvent }];
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
    // The event id orders message lifecycle changes; immutable on-chain events don't need it.
    if (
      changes.some((c) => c.kind === "message") &&
      (!Number.isSafeInteger(event.id) ||
        event.id <= 0 ||
        !Number.isSafeInteger(event.timestamp) ||
        event.timestamp <= 0)
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

  /**
   * Backfill a batch of messages missing from the DB in one transaction, so each projection
   * recomputes a fid once instead of once per message. Messages no projection cares about are
   * ignored (not stored). Returns how many messages were projected.
   */
  async processMissingMessages(db: DB, messages: Message[]): Promise<number> {
    const changes = messages
      .map((message): RawChange => ({ kind: "message", message, operation: "merge" }))
      .filter((change) => this.interested(change).length > 0);
    if (changes.length === 0) return 0;
    await db.transaction().execute((trx) => this.run(trx as unknown as AppDb, changes, { kind: "backfill" }));
    return changes.length;
  }

  /**
   * Backfill on-chain events missing from the DB in one transaction (they are immutable, so there
   * is no lifecycle to guard; the raw insert is idempotent). Returns how many were projected.
   */
  async processMissingOnChainEvents(db: DB, events: OnChainEvent[]): Promise<number> {
    const changes = events
      .map((event): RawChange => ({ kind: "onchain", event }))
      .filter((change) => this.interested(change).length > 0);
    if (changes.length === 0) return 0;
    await db.transaction().execute((trx) => this.run(trx as unknown as AppDb, changes, { kind: "backfill" }));
    return changes.length;
  }

  /**
   * Rebuild projections from raw rows already stored in the DB (rows stored before a projection
   * existed are never dispatched to it). Each fid runs in its own transaction under the shared
   * per-fid lock, so it is safe while the shards keep consuming. A fid that fails is rolled back,
   * reported and skipped; rerunning is idempotent.
   */
  async rebuild(
    db: DB,
    options: {
      projections?: string[];
      fids?: number[];
      pageSize?: number;
      onFid?: (p: string, fid: number) => unknown;
      onFailure?: (p: string, fid: number) => void;
    } = {},
  ): Promise<{ rebuilt: number; failed: { projection: string; fid: number; error: string }[] }> {
    const names = this.names(options.projections);
    const selected = this.projections.filter((p) => names.includes(p.name));
    const pageSize = options.pageSize ?? 1000;
    let rebuilt = 0;
    const failed: { projection: string; fid: number; error: string }[] = [];

    for (const projection of selected) {
      const rebuildFid = async (fid: number) => {
        try {
          await db.transaction().execute(async (trx) => {
            const appDb = trx as unknown as AppDb;
            await this.lock(appDb, fid);
            await projection.rebuild({ db: appDb, schema: this.schema, source: { kind: "backfill" } }, fid);
          });
        } catch (error) {
          options.onFailure?.(projection.name, fid);
          failed.push({
            projection: projection.name,
            fid,
            error: error instanceof Error ? error.message : String(error),
          });
          return;
        }
        rebuilt++;
        await options.onFid?.(projection.name, fid);
      };
      if (options.fids?.length) {
        for (const fid of [...options.fids].sort((a, b) => a - b)) await rebuildFid(fid);
        continue;
      }
      let after = 0;
      for (;;) {
        const fids = await projection.rebuildFids(db as unknown as AppDb, after, pageSize);
        if (fids.length === 0) break;
        for (const fid of fids) await rebuildFid(fid);
        after = fids[fids.length - 1] ?? after;
      }
    }
    return { rebuilt, failed };
  }

  /** Validates projection names; an empty list means every registered projection. */
  names(requested: string[] = []): string[] {
    const known = new Set(this.projections.map((p) => p.name));
    const unknown = requested.filter((name) => !known.has(name));
    if (unknown.length) throw new Error(`Unknown projections: ${unknown.join(", ")}`);
    return requested.length ? [...new Set(requested)] : [...known];
  }

  private async lock(db: AppDb, fid: number): Promise<void> {
    // One namespace for every projection so multi-table events can't deadlock each other.
    await sql`SELECT pg_advisory_xact_lock(hashtextextended(
      ${this.schema} || ':shuttle:fid:' || ${String(fid)}, 0))`.execute(db);
  }

  private async run(db: AppDb, changes: RawChange[], source: ProjectionSource): Promise<void> {
    const fids = [...new Set(changes.map(changeFid))].sort((a, b) => (a ?? 0) - (b ?? 0));
    for (const fid of fids) {
      if (!fid) throw new HubError("bad_request.validation_failure", "Missing message fid");
      await this.lock(db, fid);
    }

    const applied = new Map<Projection, RawChange[]>();
    for (const change of changes) {
      const projections = this.interested(change);
      if (change.kind === "onchain") {
        const accepted: Projection[] = [];
        for (const projection of projections) {
          const validation = projection.validate?.(change);
          if (validation?.isErr()) {
            await recordIssue(db, projection.name, onChainSubject(change.event), validation.error.message, {
              type: change.event.type,
              chainId: change.event.chainId,
              blockNumber: change.event.blockNumber,
              logIndex: change.event.logIndex,
            });
          } else {
            accepted.push(projection);
          }
        }
        // Same rule as messages: projections recompute from onchain_events, so a row every
        // interested projection rejects is not stored. Nobody interested: plain raw storage.
        if (projections.length > 0 && accepted.length === 0) continue;
        await storeOnChainEventRow(change.event, db as unknown as DB);
        for (const projection of accepted) {
          const list = applied.get(projection) ?? [];
          list.push(change);
          applied.set(projection, list);
        }
        continue;
      }
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
          await recordIssue(db, projection.name, messageSubject(change.message), validation.error.message, {
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
