---
description: Simulate the next business day arriving and run the nightly build
argument-hint: [days]
---
Run `.\tasks.ps1 advance-day -Days ${ARGUMENTS:-1}` and show which files arrived.
If `.\tasks.ps1 bronze` exists (Phase 4 onward), load them into bronze, then run
`dbt source freshness` and `dbt build --selector nightly`, and summarize what changed:
new rows in incremental facts, new snapshot versions, and any new warnings or errors.
