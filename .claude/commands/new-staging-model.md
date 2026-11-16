---
description: Scaffold a staging model for a source that is already declared
argument-hint: <system> <entity>   e.g. pos transaction
---
Create `stg_<system>__<entity>` for `$ARGUMENTS`, following the staging rules in `CLAUDE.md`
(1:1 with the source, explicit casts, trim, dedupe, no cross-source joins, view).
Add the YAML entry with description and key tests, add a unit test if there is dedupe or
unit-conversion logic, then run `dbt parse` and `dbt build -s <model>` and summarize the result.
