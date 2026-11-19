import { blocksProjection } from "./blocks";
import { fidsProjection } from "./fids";
import { signersProjection } from "./signers";
import { storageRentalsProjection } from "./storageRentals";
import { tierPurchasesProjection } from "./tierPurchases";
import type { Projection } from "./types";

export { ProjectionDispatcher, mutations } from "./dispatcher";
export { backfillFidForProjections, onChainReconcileTypesFor, reconcileTypesFor } from "./backfill";
export { blockId, conversionId, uuidV5 } from "./ids";
export type { Projection, ProjectionContext, ProjectionSource, RawChange } from "./types";

/**
 * Registered business-table projections, applied in this order within an event and rebuilt in this
 * order. signers comes before every projection that looks up app fids (they also take SIGNER
 * changes and recompute), so the signer rows they read are already written.
 */
export const projections: Projection[] = [
  blocksProjection,
  fidsProjection,
  storageRentalsProjection,
  tierPurchasesProjection,
  signersProjection,
];
