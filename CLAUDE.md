# CLAUDE.md: Larkspur & Pine dbt-on-Fabric demo

Read `docs/DESIGN.md` before making structural changes. It is the source of truth for architecture, naming, and phase plan. If something you're asked to do conflicts with it, say so and propose a design update rather than silently diverging.

## What this repo is
A demonstration of dbt on Microsoft Fabric built like a real client engagement, for a fictional craft beverage manufacturer, **Larkspur & Pine Brewing Co.**
- **Bronze**: Lakehouse `lh_bronze`, raw append-only Delta tables loaded from a landing zone by notebook `nb_ingest_bronze`.
- **Silver**: Warehouse `wh_analytics`, schemas `ref` (seeds), `silver` (stg_/int_), `snapshots`. Built by dbt.
- **Gold**: Warehouse `wh_analytics.gold`, star schema (dim_/fct_), contracts enforced. Built by dbt. A semantic model reads only Gold.
- **Audit**: `wh_analytics.audit`, stored test failures + `dbt_run_log`.
- Environments: workspaces `LPB-Data-Dev|Test|Prod`. Dev is git-synced; Test/Prod are deployed only by `scripts/deploy.py` (fabric-cicd).

## Environment
- Windows, PowerShell, Python 3.11 venv at `.venv` (dbt-fabric only; versions pinned in `requirements.txt` to match the Fabric dbt job runtime: dbt-core 1.11, dbt-fabric 1.10.0). ODBC Driver 18 for SQL Server is required (dbt-fabric 1.10 uses pyodbc).
- Activate: `.\.venv\Scripts\Activate.ps1`
- Auth: `az login` (dbt-fabric `authentication: CLI`). CI uses a service principal via GitHub OIDC.
- Env-specific values come from `config/environments.yml` + `.env`. **Never hard-code workspace IDs, item IDs, endpoints, or tenant IDs** in SQL or scripts.
- Never commit `.env`, `profiles.yml` with secrets, `target/`, `logs/`, `data/landing/`.

## Commands
Use `.\tasks.ps1` as the entry point (`.\tasks.ps1 help` lists everything; tasks for later phases print "not built yet"). For ad-hoc dbt commands, load the environment once with `. .\scripts\load_env.ps1` (reads `.env`, sets `DBT_PROFILES_DIR`), then run dbt from the `dbt/` folder.

| Task | Command |
|---|---|
| Check prerequisites | `.\tasks.ps1 prereqs` |
| Provision an environment | `.\tasks.ps1 setup -Env dev` |
| Build the venv | `.\tasks.ps1 venv` |
| Generate data / load bronze | `.\tasks.ps1 data` (`-Clean`, `-Force`, `-WriteSeeds`) · `.\tasks.ps1 bronze -Env dev` |
| Simulate next business day | `.\tasks.ps1 advance-day -Env dev` |
| Full build | `.\tasks.ps1 build` (= `dbt run --empty` for missing incremental tables, then `dbt build --selector nightly`) |
| Publish / run the Fabric dbt job | `.\tasks.ps1 dbt-job -Env dev` (`-Run` also runs it in Fabric) |
| Deploy / run the daily pipeline | `.\tasks.ps1 pipeline -Env dev` (`-Run` uploads new landing files and runs it; includes the Teams alert `nb_dbt_alert`) |
| Deploy the semantic model and report | `.\tasks.ps1 powerbi -Env dev` (`-Export` pulls the report edited in Fabric back into `powerbi/`) |
| Lineage page | `.\tasks.ps1 lineage` (writes and opens `docs/lineage/dbt_lineage.html`) |
| Demo act 2 (fix the planted problems) | `.\tasks.ps1 demo-fix -Fix skus` (or `excise`, `undo`, `status`), then `.\tasks.ps1 build -Select "<seed>+"`; run sheet in `docs/DEMO_SCRIPT.md` |
| First build into an empty warehouse | `.\tasks.ps1 build -FirstBuild` (unit tests after the models) |
| Freshness | `dbt source freshness` |
| Unit tests only | `dbt test -s test_type:unit` |
| Check your work after a model change | `dbt parse` then `dbt build -s <model>+` (or `-s state:modified+ --defer --state ..\prod-artifacts`) |
| Reset demo to "act 1" | `.\tasks.ps1 reset -Env dev -Yes` (regenerates landing, reloads bronze, empties the dbt schemas, rebuilds) |
| Deploy | `.\tasks.ps1 deploy -Env test` |

## dbt conventions
- **Naming**: `stg_<system>__<entity>` (systems: `erp`, `prod`, `pos`, `dist`), `int_<description>`, `dim_<entity>`, `fct_<process>`, `snap_<source_entity>`, seeds `ref_<name>`.
- **Staging**: one model per source table; rename to snake_case, cast types, trim, dedupe, standardize units. No joins across sources. Materialized as views.
- **Intermediate**: unions, mappings, business logic. Ephemeral or views. Never referenced by BI.
- **Marts**: star schema, surrogate keys, business-friendly column names, `contract: {enforced: true}` with explicit `data_type` on every column. Tables; `fct_sales_line` is incremental.
- **Every model** has a YAML entry with a description and at least one test. **Every int_/fct_ with arithmetic, mapping, or dedupe logic has a unit test.** **Every source has freshness** (`loaded_at_field: _ingested_at`).
- **Severity**: use `config: {severity: warn}` or `warn_if`/`error_if` thresholds as listed in DESIGN §3. Don't downgrade an error to make a build pass; flag it instead.
- **No dbt packages** (no `packages.yml`) until the spike confirms the Fabric dbt job supports them. Write generic tests/macros in `tests/generic` and `macros/`.
- SQL dialect is T-SQL (Fabric Warehouse). No `LIMIT` (use `TOP`), no `ILIKE`, no `::` casts. sqlfluff config is `dbt/.sqlfluff`.
- Read a ref through an import CTE (`x as (select * from {{ ref('x') }})`), never `{{ ref('x') }} as x`: `--empty` wraps refs in an aliased subquery.
- Unit test fixtures: write `bit` values as `1`/`0` (not `true`/`false`), and give every `expect` row the same columns (Fabric, docs/SPIKE.md item 5).
- dbt ignores `--select`/`--exclude` when `--selector` is given; to exclude something from "everything", drop the selector.
- The Fabric dbt job ignores `selector`; it only takes `select`/`exclude`/`threads`. Keep `nightly` equal to "everything", or express changes as select/exclude in `scripts/dbt_job.py`.
- Bronze is loaded by `notebooks/nb_ingest_bronze.py` and must match `docs/BRONZE.md` (all strings, lower snake_case, `_source_file`/`_landed_date`/`_ingested_at`). Change the doc and the notebook together.
- Source data comes from `scripts/generate_data.py` (stdlib only, deterministic). Read its docstring before changing staging models; `-WriteSeeds` overwrites hand edits in `dbt/seeds`.
- Seeds are business-owned reference data. Keep them small, sorted, with a `_seeds.yml` declaring column types.

## Power BI (`powerbi/`)
- `LPB Analytics.SemanticModel` (TMDL, Direct Lake on OneLake over `wh_analytics.gold`) and `LPB Sales and Distribution.Report` (PBIR) are the source of truth; `scripts/powerbi.py` deploys them.
- `expressions.tmdl` keeps the `{{WORKSPACE_ID}}`/`{{WAREHOUSE_ID}}` placeholders; never commit real IDs. `definition.pbir` stays `byPath`; the script switches it to `byConnection` on deploy.
- Tables use business names (`Sales`, `Depletions`, ...) mapped to gold models in each partition's `entityName`. A gold column rename must be mirrored in the table's `sourceColumn`, or the model breaks (the dbt exposure `lpb_semantic_model` shows what is affected).
- Measures live on their fact table. Brand is reached through Product for line-level facts (snowflake) so no fact has two paths to Brand.
- Visual changes: prefer editing in Fabric and `.\tasks.ps1 powerbi -Export`; hand edits to visual JSON must validate against the `$schema` in each file.

## Scripts
- Python scripts in `scripts/` are idempotent, non-interactive (flags, not prompts), take `--env`, and print a final one-line JSON summary (`{"ok": true, ...}`) so results are easy to parse.
- Fabric API work uses the Fabric CLI (`fab`) or REST via `azure-identity`; deployment uses `fabric-cicd`.
- Destructive scripts (`teardown`, `reset`) require `--confirm` and refuse to run against `prod` without `--allow-prod`.

## Do / don't
- Do run `dbt parse` after editing YAML and build the smallest selection that proves the change.
- Do update `docs/DESIGN.md` (and add an ADR in `docs/decisions/`) when a design decision changes.
- Do record spike findings in `docs/SPIKE.md`.
- The repo is local only for now: commit on the branch, never `git push`, and don't set up Fabric Git sync or GitHub workflows until a remote is chosen.
- Don't run anything against `LPB-Data-Prod` unless explicitly asked.
- Don't edit files under `fabric/` by hand unless the task is about Fabric item definitions; they are synced from the Dev workspace.
