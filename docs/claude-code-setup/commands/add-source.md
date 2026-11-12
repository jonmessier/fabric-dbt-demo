---
description: Register a bronze table as a dbt source with freshness and a matching staging model
argument-hint: <system> <bronze_table>   e.g. erp erp_shipment
---
Add the bronze table `$ARGUMENTS` to the dbt project.

1. Read `docs/DESIGN.md` sections 3 and 5 and the layer rules in `CLAUDE.md`.
2. Add the table to `dbt/models/staging/<system>/_<system>__sources.yml` (create the file if needed):
   three-part source on `lh_bronze`, a description, `loaded_at_field: _ingested_at`, and the
   freshness thresholds from DESIGN section 3 for that system.
3. Create `stg_<system>__<entity>.sql`: rename to snake_case, cast every column explicitly (T-SQL),
   trim text, dedupe on the business key keeping the latest `_ingested_at`. No joins to other sources.
4. Add the model to `_<system>__models.yml` with a description, `unique` + `not_null` on the key,
   and a unit test for the dedupe if the source can deliver the same key twice.
5. Run `dbt parse`, then `dbt build -s stg_<system>__<entity>` and report what passed, warned and failed.
