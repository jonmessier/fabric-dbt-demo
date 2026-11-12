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
- Windows, PowerShell, Python 3.11 venv at `.venv` (dbt-fabric only; versions pinned in `requirements.txt` to match the Fabric dbt job runtime).
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
| Full build | `.\tasks.ps1 build` (= `dbt build --selector nightly`) |
| Freshness | `dbt source freshness` |
| Unit tests only | `dbt test -s test_type:unit` |
| Check your work after a model change | `dbt parse` then `dbt build -s <model>+` (or `-s state:modified+ --defer --state ..\prod-artifacts`) |
| Reset demo to "act 1" | `.\tasks.ps1 reset -Env dev` |
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
- Bronze is loaded by `notebooks/nb_ingest_bronze.py` and must match `docs/BRONZE.md` (all strings, lower snake_case, `_source_file`/`_landed_date`/`_ingested_at`). Change the doc and the notebook together.
- Source data comes from `scripts/generate_data.py` (stdlib only, deterministic). Read its docstring before changing staging models; `-WriteSeeds` overwrites hand edits in `dbt/seeds`.
- Seeds are business-owned reference data. Keep them small, sorted, with a `_seeds.yml` declaring column types.

## Scripts
- Python scripts in `scripts/` are idempotent, non-interactive (flags, not prompts), take `--env`, and print a final one-line JSON summary (`{"ok": true, ...}`) so results are easy to parse.
- Fabric API work uses the Fabric CLI (`fab`) or REST via `azure-identity`; deployment uses `fabric-cicd`.
- Destructive scripts (`teardown`, `reset`) require `--confirm` and refuse to run against `prod` without `--allow-prod`.

## Do / don't
- Do run `dbt parse` after editing YAML and build the smallest selection that proves the change.
- Do update `docs/DESIGN.md` (and add an ADR in `docs/decisions/`) when a design decision changes.
- Do record spike findings in `docs/SPIKE.md`.
- Don't push to `main` directly; work on a branch and open a PR.
- Don't run anything against `LPB-Data-Prod` unless explicitly asked.
- Don't edit files under `fabric/` by hand unless the task is about Fabric item definitions; they are synced from the Dev workspace.
