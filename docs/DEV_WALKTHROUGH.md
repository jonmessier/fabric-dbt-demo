# dbt on Fabric: developer walkthrough

For showing the project to another developer who wants to learn dbt. It is code-first: open the files, run
the commands, look at what dbt did. (`DEMO_SCRIPT.md` is the shorter, results-first run sheet; use it later
for the business audience, once the report is cleaned up.)

About 50 minutes. Section 6 is the reference: every task and script we wrote, and how to reset and clean up.

---

## 1. The problem dbt solves

### What this usually looks like without dbt

A warehouse fed by stored procedures, notebooks or a pile of scheduled SQL scripts:

- **The run order lives in someone's head** (or in a pipeline someone wired by hand). Add a table and you
  have to find every place it needs to go in the order. Get it wrong and a report reads yesterday's data.
- **Everything refreshes, every time.** Nobody is sure what depends on what, so the safe option is to rebuild
  everything. When one distributor's file is late, you either wait for it or rerun the whole thing later.
- **Testing is ad hoc.** Checks are scattered through the procedures (if they exist), results go to a log
  nobody reads, and bad data reaches the report before anyone notices. There is no line between "this is
  wrong, stop" and "this is odd, tell someone".
- **Logic can't be tested in isolation.** To check the margin calculation you need a full copy of the data,
  and the case you care about (a credit memo, a refund) may not be in it.
- **Reference data lives in spreadsheets** on someone's desktop, and changes to it are invisible.
- **History gets lost.** The ERP overwrites a customer's address or price tier, and last year's orders now
  report under this year's values.
- **Documentation and lineage are separate artifacts** that are out of date the day they're written.

### What dbt changes

dbt is a build tool for SQL. You write each table as a `select` statement in its own file, and refer to other
tables with `{{ ref('model_name') }}` or `{{ source('system', 'table') }}` instead of hard-coded names. That one
habit gives you:

| Problem | dbt answer | Where it is in this project |
|---|---|---|
| Run order | `ref()`/`source()` build a dependency graph (DAG); dbt runs it in order, in parallel where it can | the lineage page; `dbt ls` |
| Full refresh every time | graph selection: build one node, everything downstream (`model+`), upstream (`+model`), by tag, by source, by selector | `.\tasks.ps1 build -Select "source:dist+"` |
| Ad hoc checks | data tests declared next to the model: `unique`, `not_null`, `relationships`, custom ones; each `warn` or `error` | 145 data tests; `_*__models.yml` files |
| Untestable logic | unit tests: hand-made input rows and expected output for one model, no real data needed | 16 unit tests |
| Spreadsheet reference data | seeds: CSVs in git, loaded as tables, reviewed like code | `dbt/seeds/` (8 files) |
| Lost history | snapshots: slowly changing dimension type 2, from a plain `select` | `snap_erp_customer` |
| Slow big tables | incremental models: process only new rows | `fct_sales_line` |
| Schema drift breaking reports | model contracts: column names and types enforced at build time | every gold model |
| Stale documentation | descriptions in YAML next to the code; docs site and lineage generated | `dbt docs`, `.\tasks.ps1 lineage` |

And it's all text in one git repo, so it diffs, reviews and deploys like any other code. dbt runs the same
project from a laptop and from Fabric's native dbt job.

### Why this demo uses a brewery

Larkspur & Pine Brewing Co. (fictional) has the classic mess: four source systems on four schedules, three
distributors that each send a different file layout, an ERP that overwrites history, and business-owned
reference data (product code mappings, tax rates, a 4-4-5 fiscal calendar). The data generator plants real
problems in it on purpose, so the tests have something to catch.

---

## 2. The architecture in one picture

```
data\landing (generated files)
  erp/  prod/  pos/  dist/            four source systems, one folder per business day
        │  .\tasks.ps1 bronze  (upload + nb_ingest_bronze)
        ▼
lh_bronze (Lakehouse)                 BRONZE: raw, append-only Delta tables, all strings, + _ingest_log
        │  dbt sources
        ▼
wh_analytics (Warehouse)              built by dbt
  ref        8 seeds                  business reference data
  silver     16 staging views         one per source table: rename, cast, dedupe
             5 intermediate views     unions, mappings, business logic
  snapshots  snap_erp_customer        customer history (SCD2)
  gold       7 dims + 6 facts         star schema, contracts enforced
  audit      dbt_run_log + failing rows of every test
        │
        ▼
LPB Analytics (Direct Lake semantic model) → LPB Sales and Distribution (report)

Daily: pl_daily_load = Ingest bronze → Check source freshness → Build with dbt (Fabric dbt job) → Alert Teams
```

---

## 3. Before the session

| Step | Command | Time |
|---|---|---|
| Back to the starting state ("act 1") | `.\tasks.ps1 reset -Env dev -Yes` | ~12 min |
| Last night's run, so Teams has the act 1 card | `.\tasks.ps1 pipeline -Env dev -Run` | ~9 min |
| Lineage page with those results | `.\tasks.ps1 lineage` | 10 s |

Open VS Code on the repo, a terminal in it, the lineage page, Teams, and the Fabric workspace `LPB-Data-Dev`.

For ad hoc dbt commands in the terminal, load the environment once:

```powershell
. .\scripts\load_env.ps1
cd dbt
```

---

## 4. The walkthrough

### 4.1 The project file (3 min)

**Open** `dbt/dbt_project.yml`.

- Folders decide behavior: everything in `models/staging` and `models/intermediate` becomes a **view** in
  `silver`, everything in `models/marts` a **table** in `gold`, seeds go to `ref`. One line per folder instead
  of a setting on every model.
- `data_tests: +store_failures: true, +schema: audit`: every test writes the rows it found into a table in
  `audit`, so a failure is something you can query, not a number in a log.
- `on-run-start` / `on-run-end` hooks call our macros: `log_run_results` writes one row per node to
  `audit.dbt_run_log`; `drop_dbt_tmp_relations` cleans up temp objects the Fabric adapter leaves behind.
- `dbt/profiles.yml` holds the connection, read from environment variables (`.env`), so no secrets in git.

### 4.2 Sources and a staging model (5 min)

**Open** `models/staging/dist/_dist__sources.yml`, then `stg_dist__depletion_northland.sql`.

- A **source** declares a bronze table dbt doesn't build, with a **freshness** rule (warn/error if no rows
  were loaded within N hours). `dbt source freshness` checks it; in Fabric the `nb_dbt_freshness` notebook
  runs it before the build, so dbt never builds on stale data.
- The staging pattern used in all 16 staging models: `{{ source(...) }}` → keep the newest version of each key
  (bronze is append-only, so re-sent files create duplicates) → rename and cast to proper types. Nothing
  clever, the same shape every time.
- Northland sends cases and kegs as two columns; the model unpivots them so all three distributors end up in
  the same shape. That is what staging is for: absorb each source's quirks once.

### 4.3 ref() and the graph (7 min)

**Show** the lineage page. Then in the terminal:

```powershell
dbt ls -s source:dist+ --resource-type model      # what a distributor file affects
dbt ls -s +fct_depletion --resource-type model    # what fct_depletion needs
dbt ls --selector gold                            # selectors.yml: named selections
dbt compile -s int_depletions_mapped              # see the SQL with ref() resolved; target\compiled\...
```

- Nobody wrote the run order. The graph comes from `ref()` and `source()` calls.
- On the lineage page, click **Distributor depletion reports**: 8 of 43 models, and the command to build just
  them. Click **Brewhouse production**: 6. **ERP**: most of the project, because orders feed almost everything.
  Click any model: what it's built from, what it feeds, its tests, its SQL.
- **The point for data that arrives from different sources:** when a distributor file lands you run
  `dbt build -s source:dist+`, and only those 8 models and their tests run. Everything else keeps what it has.
- Graph operators worth knowing: `model+` (and downstream), `+model` (and upstream), `+model+`, `2+model`
  (two levels up), `@model` (model, its children, and their parents), `tag:gold`, `source:erp`,
  `test_type:unit`, `state:modified` (needs a saved manifest; great for CI).

### 4.4 Data tests: warnings and errors (8 min)

**Open** `models/intermediate/_int__models.yml` (the `between` test on `yield_pct`), then
`tests/generic/between.sql`, then `tests/singular/assert_distributor_reports_not_late.sql`.

- **Generic tests** are declared in YAML against a column: built-in `unique`, `not_null`, `relationships`,
  `accepted_values`, plus our own `between` and `non_negative` (`tests/generic/`). A generic test is just a
  `select` that returns the bad rows, with the model and column passed in.
- **Singular tests** are one-off `select`s in `tests/singular/`: late distributor reports, an excise rate for
  every state we sell in, no unmapped SKUs in gold, gold revenue reconciling to the ERP.
- **Severity.** `error` means stop: downstream models are skipped. `warn` means log it and carry on. The
  choice is a business decision, written down next to the test with a comment saying why.
- **Show the results:** Teams card from last night's run, then the lineage page **Warnings & errors**, then
  in the warehouse:

```sql
-- One line per run: when it started (UTC) and how it went
select
    invocation_id,
    min(run_started_at) as started_utc,
    count(*) as nodes,
    sum(case when status in ('error', 'fail') then 1 else 0 end) as errors,
    sum(case when status = 'warn' then 1 else 0 end) as warnings,
    sum(case when status = 'skipped' then 1 else 0 end) as skipped
from audit.dbt_run_log
group by invocation_id
order by started_utc desc;

-- What needs attention in the latest run: errors first, then warnings, then skipped
select resource_type, node_name, status, failures, message
from audit.dbt_run_log
where invocation_id = (select top 1 invocation_id from audit.dbt_run_log order by run_started_at desc)
  and status not in ('success', 'pass')
order by case status when 'error' then 1 when 'fail' then 1 when 'warn' then 2 else 3 end, node_name;

-- The failing rows of one test (store_failures keeps them in the audit schema)
select * from audit.between_int_batch_summary_yield_pct__1__0;
```

  Every row of a run carries the same `run_started_at`, so group or filter by `invocation_id` rather than
  sorting the raw log. For act 1 the second query returns 15 rows: 2 failed tests, 6 warnings, and the 7
  skipped nodes (`fct_batch`, its 5 tests, and the exposure `lpb_semantic_model`, which dbt marks skipped
  because it depends on `fct_batch`).

- Act 1 has 2 errors (a batch with 138% yield; sales into North Dakota with no excise rate) and 6 warnings.
  The yield error skipped `fct_batch` and its tests: 7 nodes. Sales, distribution and the taproom still built.

### 4.5 Unit tests (5 min)

**Open** `models/marts/_marts__unit_tests.yml`, the test `fct_sales_line_incremental_load_takes_new_and_overlap_rows`.

- `given:` mocks each input (`ref()` or `source()`) with a few hand-written rows; `expect:` is the exact
  output. dbt runs the model's SQL against those rows. No real data, so the edge cases are always there: a
  credit memo, a refund, a merch line with no brand, a customer who moved tiers mid-month.
- There are 16, on the models where the logic is: volume conversion and returns
  (`int_sales_lines_unioned`), keeping unmapped SKUs visible (`int_depletions_mapped`), yield and cost per
  barrel (`int_batch_summary`), pricing at the tier and price in effect on the order date
  (`int_order_price_vs_list`), the 4-4-5 calendar (`dim_date`), the incremental logic (`fct_sales_line`), and
  more.
- Run them: `.\tasks.ps1 unit` (seconds). In `dbt build` they run **before** the model is built, so broken
  logic never reaches a table.
- **Try it live:** in `int_batch_summary.sql`, divide by `planned_volume_bbl` instead of `actual_volume_bbl`
  in the yield, run `dbt test -s int_batch_summary,test_type:unit`, read the diff dbt prints, undo.

### 4.6 Seeds, snapshot, incremental, contracts (5 min)

- **Seeds** (`dbt/seeds/*.csv`, documented and tested in `_seeds.yml`): product-code mappings per
  distributor, retail accounts, excise rates, state spellings, unit conversions, the fiscal calendar. The
  business owns them; a change is a pull request.
- **Snapshot** (`snapshots/snap_erp_customer.sql`): the ERP overwrites customers. The snapshot keeps every
  version with `dbt_valid_from`/`dbt_valid_to`, so `dim_customer` can give each order the customer's tier and
  territory *as they were on the order date*.
- **Incremental** (`models/marts/sales/fct_sales_line.sql`): `delete+insert` with a 3-day lookback, so late
  corrections to recent days are picked up but history isn't reprocessed. `is_incremental()` switches the
  `where` clause.
- **Contracts** (`contract: {enforced: true}` on gold, in `_sales__models.yml` etc.): every column's name and
  type is declared. Rename a column in the SQL without updating the contract and the build fails *before* the
  table changes, instead of Power BI breaking the next morning. The exposure `lpb_semantic_model`
  (`_exposures.yml`) records which gold tables the semantic model reads.

### 4.7 Recap: from the Teams card back to the bad rows (6 min)

Now that the parts of dbt are on the table, go back to last night's Teams card and trace each line to where
it came from.

**How a problem reaches Teams.** Every data test is a `select` that returns the bad rows. dbt runs it; zero
rows is a pass, anything else is a `warn` or a `fail` depending on the test's severity. With
`store_failures`, the rows themselves are saved to a table in `audit`. At the end of the run the
`log_run_results` hook writes one row per node to `audit.dbt_run_log`. Besides the status, it logs what the
node is about, straight from dbt's own metadata: the model a test is on (`parent_model`), the tested column,
the kind of test (`not_null`, `relationships`, `between`, `singular`, `unit`), its severity, the file it is
declared in, and its description. `nb_dbt_alert` turns the newest run into the card. Each problem gets three
lines: what failed (`stg_erp__customer.state_text · relationships · 1 row`), what the test checks (its
`description:` in YAML, or dbt's message if it has none), and `declared in … · severity …`. Errors also say
where to look, in two lines to paste into a query: `Bad rows: select * from audit.<test>` (the failing rows
that `store_failures` saved; for the yield error that is the whole 138% batch, B260826-2) and
`Model: silver.int_batch_summary` (the table the test is on, for context). Warnings leave those out to keep
the card short; their audit tables are in the console output and in `audit.dbt_run_log`. The description
is written for the business, so it belongs next to the test: generic tests take `description:` next to
`arguments:`, singular tests are described in `tests/singular/_singular__tests.yml`.

```
test (select returning bad rows) → dbt build → on-run-end hook → audit.dbt_run_log → nb_dbt_alert → Teams
```

**The eight lines on the act 1 card:**

| Card line | Test | Kind, declared in | Layer | Severity, and why |
|---|---|---|---|---|
| `int_batch_summary.yield_pct · between` · 1 row | yield between 0 and 1 | generic (`between`, ours), `_int__models.yml` | silver intermediate | **error**: a 138% yield means a keying error; cost per barrel would be wrong, so `fct_batch` is skipped |
| `fct_excise_tax_monthly.rate_per_barrel · not_null` · 7 rows | rate is never null | generic (built-in), `_sales__models.yml` | gold | **error**: an excise report with a missing state is wrong, not incomplete |
| `ref_excise_tax_rate · assert_excise_rate_for_every_sales_state` · 1 row | every state we sell into has a rate | singular, `tests/singular/` | silver | **warn**: same cause, caught earlier; sales models can still build |
| `stg_dist__depletion_northland.distributor_sku · relationships` · 14 rows | every Northland code is in the mapping seed | generic (built-in `relationships`), `_dist__models.yml` | silver staging | **warn**: new codes are normal; volume goes to an UNMAPPED product until someone maps it |
| `fct_depletion · assert_no_unmapped_skus_in_gold` · 3 rows | no unmapped codes reach gold | singular | gold | **warn**: the same problem, seen where the report reads it (3 code groups) |
| `stg_erp__invoice_line.shipment_no · relationships` · 37 rows | every invoice line points at a real shipment | generic (`relationships`), `_erp__models.yml` | silver staging | **warn**: an ERP data-entry problem; revenue is still right |
| `stg_erp__customer.state_text · relationships` · 1 row | every state spelling is in the state seed | generic (`relationships`), `_erp__models.yml` | silver staging | **warn**: one customer typed "Minnesotta"; fix it in the ERP |
| `stg_dist__depletion_hawkeye · assert_distributor_reports_not_late` · 1 row | all distributors reported the same latest month | singular | silver staging | **warn**: Hawkeye's September report is late; channel numbers are understated until it arrives |

For singular tests the model on the card is the last table the test reads, not something dbt declares, so
take it as a pointer: on the late-report test it says Hawkeye only because Hawkeye sorts last.

Plus **7 skipped**: `fct_batch`, its 5 tests, and the exposure `lpb_semantic_model`. They depend on
`int_batch_summary`, whose error test failed, so `dbt build` didn't build them.

**See the bad data.** Two ways to get at it:

- The test's own SQL: `dbt compile -s between_int_batch_summary_yield_pct__1__0` writes it to
  `dbt\target\compiled\lpb\models\intermediate\_int__models.yml\...`. Paste it into the SQL editor.
- The saved rows: `select * from audit.between_int_batch_summary_yield_pct__1__0`.

Or ask the question directly, which is what you'd do in real life. Run these against `wh_analytics`:

```sql
-- ERROR: the 138% batch, and the packaging run that caused it
select batch_id, brand_code, actual_volume_bbl, packaged_volume_bbl, yield_pct, packaging_runs
from silver.int_batch_summary
where yield_pct > 1;

select run_id, batch_id, packaging_line, units_packaged, packaged_volume_bbl, operator, recorded_at
from silver.stg_prod__packaging_run
where batch_id in (select batch_id from silver.int_batch_summary where yield_pct > 1)
order by recorded_at;

-- ERROR (gold) and WARN (silver): North Dakota sales with no excise rate
select state_code, sales_month, net_barrels, rate_per_barrel, excise_tax_amount
from gold.fct_excise_tax_monthly
where rate_per_barrel is null
order by sales_month;

-- WARN: Northland product codes that aren't in the mapping seed (same logic as the relationships test)
select n.distributor_sku, count(*) as depletion_rows, sum(n.quantity) as units
from silver.stg_dist__depletion_northland as n
left join ref.ref_distributor_sku_xref as x
    on x.distributor_sku = n.distributor_sku
where n.distributor_sku is not null
  and x.distributor_sku is null
group by n.distributor_sku;

-- WARN: the same codes in gold, where they land on the UNMAPPED product
select distributor_code, distributor_sku, count(*) as depletion_rows, sum(barrels) as barrels
from gold.fct_depletion
where is_sku_mapped = 0
group by distributor_code, distributor_sku;

-- WARN: invoice lines pointing at a shipment that doesn't exist
select i.invoice_no, i.document_type, i.item_no, i.shipment_no
from silver.stg_erp__invoice_line as i
left join silver.stg_erp__shipment as s
    on s.shipment_no = i.shipment_no
where i.shipment_no is not null
  and s.shipment_no is null
order by i.invoice_no;

-- WARN: the customer state spelling the seed doesn't know
select c.customer_no, c.state_raw, c.state_text
from silver.stg_erp__customer as c
left join ref.ref_state_region as r
    on r.state_text = c.state_text
where c.state_text is not null
  and r.state_text is null;

-- WARN: the late distributor report (latest month each distributor has sent)
select 'NORTHLAND' as distributor_code, max(report_month) as latest_report_month
from silver.stg_dist__depletion_northland
union all
select 'BADGER', max(report_month) from silver.stg_dist__depletion_badger
union all
select 'HAWKEYE', max(report_month) from silver.stg_dist__depletion_hawkeye;
```

Things to point out while these run:
- The excise problem shows up twice, in silver as a warning and in gold as an error. Test where the
  problem starts, and again where it hurts, with the severity that fits each place.
- The unmapped codes are *visible*, not dropped: `int_depletions_mapped` sends them to an UNMAPPED product
  (there's a unit test for exactly that), so the volume still adds up and the gap is easy to find.
- Two fixes are reference data the business owns (a mapping, a tax rate); one is source data (the
  packaging run); two belong in the ERP. That is the setup for act 2.

### 4.8 Act 2: fix it, one path at a time (8 min)

Same steps as `DEMO_SCRIPT.md` section 4; for a developer, show the diff and the selection each time.

```powershell
.\tasks.ps1 demo-fix -Fix skus          # 6 rows added to ref_distributor_sku_xref.csv
git diff dbt\seeds
.\tasks.ps1 build -Select "ref_distributor_sku_xref+"     # 5 of 43 models, 24 tests

.\tasks.ps1 demo-fix -Fix excise        # 1 row: North Dakota
.\tasks.ps1 build -Select "ref_excise_tax_rate+"          # 2 models, 5 tests

.\tasks.ps1 advance-day -Days 3         # corrected packaging run (day 1), Hawkeye's late report (day 3)
.\tasks.ps1 bronze -Env dev             # upload + ingest notebook in Fabric, ~2-3 min
.\tasks.ps1 build -Select "source:prod+ source:dist+"     # 14 models

.\tasks.ps1 build                       # full nightly: 0 errors, 2 warnings left
.\tasks.ps1 lineage
```

Things to point out:
- Bronze now holds the wrong packaging run *and* the corrected one. The staging model keeps the newest per
  `run_id` (by `recorded_at`), so the fix flows through without anyone editing bronze.
- dbt selected the tests too: a test runs when any model it reads is selected, so fixing a seed re-runs every
  test that could be affected.
- The two warnings left (invoice lines without a shipment; a misspelled state in the ERP) are for the
  business to fix at the source. Nothing is blocked by them.
- Run `nb_dbt_alert` in Fabric for the new Teams card, and check the report: unmapped depletion barrels is 0.

### 4.9 How it runs in Fabric (4 min)

**Show** `pl_daily_load` in the workspace and the last run in the Monitoring hub.

- **Fabric dbt job** (`dbt_lpb`): a Fabric item that holds a copy of `dbt/` and runs `dbt build` with the
  Fabric-hosted dbt. `scripts/dbt_job.py` publishes it from the repo; after changing dbt code, run
  `.\tasks.ps1 dbt-job -Env dev` or the pipeline keeps running the old copy.
- **Freshness notebook** before it: the dbt job can't run `dbt source freshness`, so `nb_dbt_freshness` does,
  and fails the pipeline if a source is in error.
- **Alert notebook** after it: reads the newest run from `audit.dbt_run_log` and posts the Teams card. It
  runs when dbt *completes*, so a failed build is reported, not swallowed.
- **Lessons learned** (details in `docs/SPIKE.md`): the job ignores `selector` (only `select`/`exclude`);
  zips built on Windows need forward slashes; the adapter leaves temp views behind (hence the cleanup macro);
  "Nested CTE" warnings are noise.

### 4.10 Hands-on for the developer (optional, 10+ min)

- Add a column to a staging model and its YAML, add a `not_null` test, run `dbt build -s <model>+`.
- Write a unit test for an edge case in `int_depletions_mapped`. Claude Code's `/add-unit-test` command
  scaffolds one.
- Add a new source table end to end with `/add-source` and `/new-staging-model`.
- Ask Claude Code `/explain-last-run` after a build: it reads the run results and explains the failures.

---

## 5. What to say when asked

| Question | Short answer |
|---|---|
| Is dbt an ETL tool? | The T only. It doesn't move data in; bronze ingestion is a notebook. dbt turns loaded data into tables, in the warehouse's own SQL |
| Where does it run? | Wherever you call it: a laptop, CI, or Fabric's dbt job. The SQL runs in the warehouse; dbt compiles and orchestrates |
| dbt Core vs dbt Cloud? | This is dbt Core (open source) with the `dbt-fabric` adapter. dbt Cloud is the hosted product; Fabric's dbt job plays that role here |
| Why a Warehouse, not a Lakehouse, for silver/gold? | The Fabric dbt job and the mature `dbt-fabric` adapter target the Warehouse (T-SQL) |
| How do you deploy? | Scripts publish the dbt job and pipeline per environment; Test/Prod and CI with fabric-cicd come later (DESIGN.md phase 7) |
| What does a full rebuild cost here? | A couple of minutes, because the data is small. The selection model matters at real volumes and with many sources |

---

## 6. Reference: what we wrote

All tasks go through one entry point: `.\tasks.ps1 <task> [-Env dev|test|prod]`. `.\tasks.ps1 help` lists them.
Scripts print progress to stderr and end with one line of JSON, so they're easy to read for people and for
Claude Code.

### 6.1 tasks.ps1

**Setup (once per machine / environment)**

| Task | What it does |
|---|---|
| `prereqs` | Checks dbt and the Fabric CLI in `.venv`, ODBC Driver 18, Azure CLI sign-in and `.env`. Installs nothing (`scripts/check_prereqs.ps1`) |
| `venv` | Creates `.venv` and installs the pinned `requirements.txt` (dbt-core 1.11, dbt-fabric 1.10) |
| `setup -Env dev` | Creates the workspace, `lh_bronze`, `wh_analytics` and its schemas, by name and idempotently; writes the SQL endpoint to `.env` (`scripts/provision.py`) |
| `debug` | `dbt debug`: checks the connection |

**Source data**

| Task | What it does |
|---|---|
| `data` | Generates ~18 months of history into `data\landing` (`scripts/generate_data.py backfill`). `-Clean` = no planted problems, `-WriteSeeds` = rewrite the seed CSVs, `-Force` = replace an existing landing zone |
| `advance-day -Days N` | Writes the next N business days' files, including the scheduled surprises (customer move, price change, re-sent POS file, corrected packaging run, late distributor report) |
| `bronze -Env dev` | Uploads new landing files to `lh_bronze`, deploys and runs `nb_ingest_bronze`, refreshes the SQL endpoint (`scripts/load_bronze.py`). `-ReloadAll` rebuilds every bronze table, `-SkipUpload` runs the notebook only |

**dbt (local, against Fabric)**

| Task | What it does |
|---|---|
| `parse` | `dbt parse`: checks the project without connecting |
| `seed` | `dbt seed` |
| `build` | `dbt build --selector nightly`. Failing tests are allowed (act 1); a model error still fails the task |
| `build -Select "<selection>"` | Builds one path only, e.g. `"source:prod+"` or `"ref_excise_tax_rate+"` |
| `build -FirstBuild` | Into an empty warehouse: everything except unit tests, then the unit tests (one reads the incremental table) |
| `unit` | `dbt test -s test_type:unit` |
| `freshness` | `dbt source freshness` |
| `docs` | `dbt docs generate`, then `dbt docs serve` |
| `lint` | sqlfluff on `dbt\models` |
| `lineage` | Builds and opens `docs\lineage\dbt_lineage.html` from the manifest and the latest results (`scripts/build_lineage.py`); `-Yes` builds without opening |

Every dbt command also keeps a copy of its results in `dbt\target\run_history\` (newest 30), which the lineage
page reads.

**Fabric deployment**

| Task | What it does |
|---|---|
| `dbt-job -Env dev` | Publishes `dbt\` as the Fabric dbt job `dbt_lpb` (`scripts/dbt_job.py`); `-Run` also runs it and prints the results |
| `pipeline -Env dev` | Deploys the three notebooks and `pl_daily_load`, copies the Teams webhook URL to the lakehouse (`scripts/pipeline.py`); `-Run` uploads new landing files, runs the pipeline and prints each activity |
| `powerbi -Env dev` | Deploys the semantic model and report from `powerbi\` and refreshes the model (`scripts/powerbi.py`); `-Export` pulls the report edited in Fabric back into `powerbi\` |

**Demo and reset**

| Task | What it does |
|---|---|
| `demo-fix -Fix skus` | Act 2: adds the missing distributor product codes to `ref_distributor_sku_xref.csv` (`scripts/demo_fix.py`) |
| `demo-fix -Fix excise` | Act 2: adds North Dakota to `ref_excise_tax_rate.csv` |
| `demo-fix -Fix undo` | Removes both again (back to act 1 seeds) |
| `demo-fix -Fix status` | Shows which fixes are in the seeds |
| `reset -Env dev -Yes` | Back to act 1, see 6.4 |

### 6.2 Scripts and notebooks

| File | Purpose |
|---|---|
| `scripts/fabric_common.py` | Shared helpers: Azure CLI tokens, Fabric REST client (long-running operations, item lookup by name), OneLake upload, SQL connection, `.env` and `config/environments.yml` reading |
| `scripts/provision.py` | Workspace, lakehouse, warehouse, schemas |
| `scripts/generate_data.py` | The fictional company's source systems: `backfill` and `advance`; deterministic for a given seed |
| `scripts/load_bronze.py` | Upload landing files, deploy/run the ingest notebook, refresh the SQL endpoint |
| `scripts/dbt_job.py` | Build the dbt job definition from `dbt\`, publish it, optionally run it and read its results |
| `scripts/pipeline.py` | Deploy the notebooks and `pl_daily_load`; run it |
| `scripts/powerbi.py` | Deploy the TMDL semantic model and PBIR report, refresh, export |
| `scripts/build_lineage.py` + `docs/lineage/lineage_template.html` | The interactive lineage page |
| `scripts/demo_fix.py` | Act 2 seed fixes and undo |
| `scripts/reset_demo.py` | The Fabric half of `reset` |
| `scripts/load_env.ps1` | Loads `.env` into the PowerShell session (dot-source it) |
| `scripts/check_prereqs.ps1` | Machine check |
| `notebooks/nb_ingest_bronze.py` | Loads new landing files into append-only bronze Delta tables; `_ingest_log` makes each file load once |
| `notebooks/nb_dbt_freshness.py` | Runs `dbt source freshness` in Fabric before the build |
| `notebooks/nb_dbt_alert.py` | Reads the newest run from `audit.dbt_run_log`, posts the Teams card (or saves it on a dry run) |

### 6.3 Inside the dbt project

| Item | Purpose |
|---|---|
| `macros/log_run_results.sql` | `on-run-end`: one row per node into `audit.dbt_run_log` |
| `macros/drop_dbt_tmp_relations.sql` | `on-run-start`/`end`: drops temp views and tables dbt-fabric 1.10 leaves behind |
| `macros/generate_schema_name.sql` | Schemas are used as written (`silver`, `gold`), not prefixed with the target schema |
| `tests/generic/between.sql`, `non_negative.sql` | Our reusable column tests |
| `tests/singular/*.sql` | Late distributor reports, excise rate per sales state, no unmapped SKUs in gold, gold revenue reconciles to the ERP |
| `selectors.yml` | `nightly` (everything), `reference_data` (seeds and downstream), `gold` (gold and upstream) |
| `models/marts/_exposures.yml` | The semantic model as an exposure: which gold tables it depends on |

Claude Code slash commands (`.claude/commands/`): `/new-staging-model`, `/new-mart`, `/add-source`,
`/add-unit-test`, `/advance-day`, `/explain-last-run`, `/demo-reset`.

### 6.4 Reset and clean-up

| You want to | Run | What it removes / restores |
|---|---|---|
| Rehearse again from the top | `.\tasks.ps1 reset -Env dev -Yes` (~12 min) | Undoes the act 2 seed fixes; regenerates `data\landing` (backfill only, so advanced days go); replaces `lh_bronze` `Files/landing` and reloads every bronze table; drops every table and view in `ref`, `silver`, `snapshots`, `gold`, `audit` (snapshot history and `dbt_run_log` included); republishes the dbt job; rebuilds with unit tests last. Fabric items (notebooks, pipeline, semantic model, report) stay |
| Undo only the seed fixes | `.\tasks.ps1 demo-fix -Fix undo`, then `.\tasks.ps1 build -Select "ref_distributor_sku_xref+ ref_excise_tax_rate+"` | The 7 seed rows |
| Reload bronze from the landing zone | `.\tasks.ps1 bronze -Env dev -ReloadAll` | Drops and reloads every bronze table and `_ingest_log` |
| Make the pipeline use your latest dbt code | `.\tasks.ps1 dbt-job -Env dev` | Replaces the job's copy of `dbt\` |
| Clear dbt's local build output | `dbt clean` (in `dbt\`) | `dbt\target` and `dbt_packages` (the lineage page needs `target\manifest.json` again: `.\tasks.ps1 parse`) |
| Start the landing zone over without touching Fabric | `.\tasks.ps1 data -Force` | `data\landing` only |

`reset` refuses prod. Nothing deletes the workspace yet (`teardown` is phase 7).
