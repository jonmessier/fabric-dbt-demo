---
description: Summarize warnings, errors and skips from the most recent dbt run
---
Read `dbt/target/run_results.json` (and `dbt/target/sources.json` if it exists).
Give a short summary: counts by status, then for each warn, error and skip the node name,
what it checks, the failure count or message, and where the failing rows are stored
(`audit.<test_name>` when `store_failures` is on). Group by source system. Finish with the
most likely cause of each problem and the smallest command that would re-run just that part.
Do not fix anything unless asked.
