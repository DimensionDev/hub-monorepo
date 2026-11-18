import type { HubResult, Message, OnChainEvent } from "@dimensiondev/hub-nodejs";
import type { StoreMessageOperation } from "../../shuttle";
import type { ReconcilableMessageType } from "../../shuttle/messageReconciliation";
import type { ReconcilableOnChainEventType } from "../../shuttle/onChainEventReconciliation";
import type { AppDb } from "../db";

/** Where a change came from. Backfill (reconcile) messages carry no hub event id. */
export type ProjectionSource =
  | { kind: "event"; shardIndex: number; eventId: number; eventTimestamp: Date }
  | { kind: "backfill" };

export type MessageChange = { kind: "message"; message: Message; operation: StoreMessageOperation };
/** An on-chain event is immutable: it is only ever merged, never deleted, pruned or revoked. */
export type OnChainChange = { kind: "onchain"; event: OnChainEvent };
export type RawChange = MessageChange | OnChainChange;

/** The fid a change belongs to (locked by the dispatcher). */
export function changeFid(change: RawChange): number | undefined {
  return change.kind === "message" ? change.message.data?.fid : change.event.fid;
}

export type ProjectionContext = { db: AppDb; schema: string; source: ProjectionSource };

/**
 * A business-table projection. It runs inside the hub event's transaction, after the raw
 * messages it cares about have been stored, while the dispatcher holds the per-fid locks.
 *
 * `apply` must be order independent: recompute from the stored `messages` rows (or from the
 * current target row plus a watermark), never from the arriving change alone. Events can be
 * replayed, processed concurrently or arrive late through backfill.
 */
export interface Projection {
  name: string;
  /** Hub `getAll*MessagesByFid` families that can contain this projection's messages (for backfill). */
  reconcileTypes: ReconcilableMessageType[];
  /** On-chain event types to fetch from the hub during projection-only backfill. */
  onChainReconcileTypes?: ReconcilableOnChainEventType[];
  interestedIn(change: RawChange): boolean;
  /** Expected data problems are reported here and recorded as issues; the change is skipped. */
  validate?(change: RawChange): HubResult<void>;
  apply(ctx: ProjectionContext, changes: RawChange[]): Promise<void>;
  /**
   * Rebuild support: raw rows stored before this projection existed are never dispatched to it
   * (reconcile skips messages already in the DB). `rebuildFids` pages the fids that have relevant
   * raw rows or existing target rows; `rebuild` recomputes one fid from the stored raw data.
   */
  rebuildFids(db: AppDb, afterFid: number, limit: number): Promise<number[]>;
  rebuild(ctx: ProjectionContext, fid: number): Promise<void>;
  /** Business table this projection writes (shown on the dashboard). */
  table?: string;
  /** Headline counts for the dashboard, e.g. `{ total, active }`. Keep these cheap. */
  stats?(db: AppDb): Promise<Record<string, number>>;
}
