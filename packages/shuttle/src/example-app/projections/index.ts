import { blocksProjection } from "./blocks";
import type { Projection } from "./types";

export { ProjectionDispatcher, mutations } from "./dispatcher";
export { blockId, conversionId, uuidV5 } from "./ids";
export type { Projection, ProjectionContext, ProjectionSource, RawChange } from "./types";

/** Registered business-table projections. Add new tables here. */
export const projections: Projection[] = [blocksProjection];
