import { blocksProjection } from "./blocks";
import { fidsProjection } from "./fids";
import { storageRentalsProjection } from "./storageRentals";
import type { Projection } from "./types";

export { ProjectionDispatcher, mutations } from "./dispatcher";
export { backfillFidForProjections, onChainReconcileTypesFor, reconcileTypesFor } from "./backfill";
export { blockId, conversionId, uuidV5 } from "./ids";
export type { Projection, ProjectionContext, ProjectionSource, RawChange } from "./types";

/** Registered business-table projections. Add new tables here. */
export const projections: Projection[] = [blocksProjection, fidsProjection, storageRentalsProjection];
