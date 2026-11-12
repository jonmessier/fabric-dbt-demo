---
description: Scaffold a gold dim_ or fct_ model with an enforced contract and a unit test
argument-hint: <dim_or_fct_name> <one-line purpose>
---
Create the gold model described by: $ARGUMENTS

1. Check `docs/DESIGN.md` section 5 for where it belongs under `dbt/models/marts/` and which
   intermediate models it should read. Create an `int_` model first if the logic needs one.
2. Write the model: surrogate key, business-friendly column names, only `ref()` to silver or other gold.
3. YAML: description for the model and every column, `contract: {enforced: true}` with an explicit
   T-SQL `data_type` per column, key tests, and relationship tests to its dimensions.
4. Add at least one unit test that proves the main calculation with hand-written rows.
5. Run `dbt parse`, then `dbt build -s +<model>`; report results. Do not loosen a test to get green.
