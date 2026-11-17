import type { Message } from "@dimensiondev/hub-nodejs";
import type { DB } from "../../shuttle";
import type { MessageReconciliation, ReconcilableMessageType } from "../../shuttle/messageReconciliation";
import type { ProjectionDispatcher } from "./dispatcher";
import type { Projection } from "./types";

export function reconcileTypesFor(projections: Projection[]): ReconcilableMessageType[] {
  return [...new Set(projections.flatMap((p) => p.reconcileTypes))];
}

/**
 * Projection-only backfill for one fid: fetch only the message families the projections need,
 * collect the messages missing from the DB and project them in one transaction. Messages no
 * projection cares about (e.g. follows) are dropped, and the DB-side pass is skipped.
 */
export async function backfillFidForProjections(
  reconciler: MessageReconciliation,
  db: DB,
  dispatcher: ProjectionDispatcher,
  projections: Projection[],
  fid: number,
): Promise<{ missing: number; projected: number }> {
  const missing: Message[] = [];
  await reconciler.reconcileMessagesForFid(
    fid,
    async (message, missingInDb) => {
      if (missingInDb) missing.push(message);
    },
    undefined,
    undefined,
    undefined,
    reconcileTypesFor(projections),
  );
  const projected = await dispatcher.processMissingMessages(db, missing);
  return { missing: missing.length, projected };
}
