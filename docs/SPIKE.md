# Spike: what works on Fabric

Purpose: confirm each assumption in [DESIGN.md](DESIGN.md) section 10 before building on it.
Work in the `LPB-Data-Dev` workspace with a throwaway dbt project under `spike/`.
For each item record: **result** (works / works with changes / does not work), the **evidence**
(command and output, or a screenshot path), and the **decision** it leads to.

Tenant: _fill in_ · Capacity: _fill in_ · Date: _fill in_

## Already checked without Fabric (2026-11-12)

| Item | Result |
|---|---|
| `dbt-fabric`, `fabric-cicd`, `ms-fabric-cli`, `sqlfluff` install together | Yes, on Linux with Python 3.13 (dbt-fabric 1.12.0, dbt-core 1.12.7, fabric-cicd 1.4.0). Not yet tried on Windows with Python 3.11. |
| Project skeleton parses | Yes: `dbt parse` finds 8 seeds and 11 tests with dbt-core 1.12.7 / dbt-fabric 1.12.0. |
| Data generator | Runs in about 2 seconds; planted defects verified; backfill + advance gives the same rows as one longer run. |
| Staging layer logic | 16 staging models, 80 data tests and 9 unit tests were run against the generated data in a local stand-in database (SQL translated from T-SQL). Unit tests pass; the only data-test hits are the planted warnings. Not yet run on Fabric, so T-SQL details (`bit`, `datetime2`, `trim`, cross-database views) are unproven. |
| Intermediate layer and snapshot | 5 intermediate models and 4 more unit tests checked the same way. Every invoice line finds its list price at the order date and matches it exactly apart from discounts. The planted 138% yield batch fails the `between` test as intended. |
| Bronze ingest notebook | Ran under local Spark 3.5.3 / Python 3.11 (Fabric Runtime 1.3 versions) with Parquet in place of Delta: backfill loads 97 files, a rerun loads 0, six new days load 74. dbt logic gives identical results on the Spark-written tables and on the raw files. Not yet run in Fabric. |
| Test argument syntax | Generic test arguments (`to:`, `field:`, `values:`) are written at the top level so they work on dbt 1.9. dbt 1.12 accepts them with a deprecation warning that asks for an `arguments:` block. Switch once item 1 below confirms the runtime is 1.10.5 or later. |

## Checklist

| # | Question | Result | Evidence | Decision |
|---|---|---|---|---|
| 1 | Which dbt-core and dbt-fabric versions does the Fabric dbt job runtime use in this tenant? | | | Pin `requirements.txt` to match |
| 2 | Does `dbt debug` connect from Windows with `authentication: CLI`? Is the `driver:` line in `profiles.yml` needed, and is a separate ODBC driver install needed? | | | Update `profiles.yml`, `check_prereqs.ps1` |
| 3 | Do seeds load with the `column_types` in `_seeds.yml`? | | | |
| 4 | Can a source read `lh_bronze` tables by three-part name from the warehouse, locally and from the dbt job? | | | Otherwise: shortcut or copy step |
| 5 | Do `unit_tests:` run on Fabric Warehouse (watch fixture type casting)? | | | |
| 6 | Do model contracts (`contract: {enforced: true}`) work, and which constraints are supported? | | | |
| 7 | Do snapshots work (`timestamp` strategy on `updated_at`)? | | | |
| 8 | Does an incremental model with `merge` or `delete+insert` work? | | | |
| 9 | Does `dbt source freshness` run locally and inside the dbt job? | | | |
| 10 | Does `store_failures` write to the `audit` schema? | | | |
| 11 | Does the dbt job accept `packages.yml`? | | | If yes, consider `dbt_utils` |
| 12 | How is the dbt job item stored in Git (where do the dbt project files live)? | | | Decides repo layout of `fabric/` vs `dbt/` |
| 13 | Does `fabric-cicd` deploy the dbt job (`DataBuildToolJob`) and can `parameter.yml` repoint its warehouse? | | | |
| 14 | Can the dbt job be started from a script (job scheduler API), or only through a pipeline? | | | |
| 15 | Can a service principal with GitHub OIDC run dbt and deploy? Which tenant settings and workspace roles were needed? | | | Write into `setup_identity.ps1` |
| 16 | How long do `dbt build` and the bronze notebook take on this capacity? | | | Demo timing |
| 17 | Does `notebooks/nb_ingest_bronze.py` upload as a notebook definition (format, `PARAMETERS CELL` marker), and does it run with `lh_bronze` as default lakehouse? Do relative `Files/landing` globs resolve through Hadoop? | | | `load_bronze.py` |
| 18 | Does the warehouse see new bronze rows immediately through the lakehouse SQL endpoint, or is there a sync delay to wait for before dbt runs? | | | Pipeline wait step |

## Findings and follow-ups

_Notes go here as the spike runs._
