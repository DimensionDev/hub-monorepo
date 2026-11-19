---
"@dimensiondev/shuttle": minor
---

The example app adds projections for signers (on-chain SIGNER events, app fid from the SignedKeyRequest metadata), tier_purchases (sharing the receipt logic with storage_rentals), verifications, profiles and profile_external_accounts. Projections that need an app fid look keys up in signers and recompute when a SIGNER event arrives. Backfill now reconciles on-chain events before messages.
