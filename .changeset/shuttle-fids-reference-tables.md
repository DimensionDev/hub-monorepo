---
"@dimensiondev/shuttle": major
---

Rename the raw username proof table from `usernames` to `username_proofs` (`HubTables.username_proofs`; `UsernamesTable` is kept as a deprecated alias of `UsernameProofsTable`) so the business `usernames` table can share the schema. The example app now creates every reference business table (migration 008), projects on-chain events through the projection dispatcher, and adds the fids projection with ID_REGISTER on-chain backfill.

`getDbClient` now attaches a pool `error` listener, so an idle connection dropped by the server is logged instead of crashing the process.
