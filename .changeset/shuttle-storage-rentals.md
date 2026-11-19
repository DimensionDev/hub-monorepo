---
"@dimensiondev/shuttle": minor
---

The example app adds the storage_rentals projection: one row per on-chain STORAGE_RENT receipt (keyed by chain, transaction hash and log index, with Go-compatible ids), recomputed per fid from `onchain_events`, with STORAGE_RENT on-chain backfill. Invalid or conflicting receipts are recorded as projection issues and skipped.
