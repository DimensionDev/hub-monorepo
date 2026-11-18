import type { Message, OnChainEvent } from "@dimensiondev/hub-nodejs";
import type { DB } from "../../shuttle";
import type { MessageReconciliation, ReconcilableMessageType } from "../../shuttle/messageReconciliation";
import type {
  OnChainEventReconciliation,
  ReconcilableOnChainEventType,
} from "../../shuttle/onChainEventReconciliation";
import type { ProjectionDispatcher } from "./dispatcher";
import type { Projection } from "./types";

export function reconcileTypesFor(projections: Projection[]): ReconcilableMessageType[] {
  return [...new Set(projections.flatMap((p) => p.reconcileTypes))];
}

export function onChainReconcileTypesFor(projections: Projection[]): ReconcilableOnChainEventType[] {
  return [...new Set(projections.flatMap((p) => p.onChainReconcileTypes ?? []))];
}

/**
 * Projection-only backfill for one fid: fetch only the message families and on-chain event types
 * the projections need, collect what is missing from the DB and project each kind in one
 * transaction. Messages no projection cares about (e.g. follows) are dropped, and the DB-side pass
 * is skipped. On-chain events are fetched only when an `onChainReconciler` is given.
 */
export async function backfillFidForProjections(
  reconciler: MessageReconciliation,
  db: DB,
  dispatcher: ProjectionDispatcher,
  projections: Projection[],
  fid: number,
  onChainReconciler?: OnChainEventReconciliation,
): Promise<{ missing: number; projected: number }> {
  const messageTypes = reconcileTypesFor(projections);
  const missing: Message[] = [];
  if (messageTypes.length) {
    await reconciler.reconcileMessagesForFid(
      fid,
      async (message, missingInDb) => {
        if (missingInDb) missing.push(message);
      },
      undefined,
      undefined,
      undefined,
      messageTypes,
    );
  }
  let projected = await dispatcher.processMissingMessages(db, missing);

  const onChainTypes = onChainReconcileTypesFor(projections);
  const missingOnChain: OnChainEvent[] = [];
  if (onChainReconciler && onChainTypes.length) {
    await onChainReconciler.reconcileOnChainEventsForFid(
      fid,
      async (event, missingInDb) => {
        if (missingInDb) missingOnChain.push(event);
      },
      { types: onChainTypes },
    );
    projected += await dispatcher.processMissingOnChainEvents(db, missingOnChain);
  }
  return { missing: missing.length + missingOnChain.length, projected };
}
