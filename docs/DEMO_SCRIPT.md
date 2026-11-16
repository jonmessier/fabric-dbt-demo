# Demo script: dbt on Microsoft Fabric

About 25 minutes, in five parts. The point to land: **dbt knows how every table is built, so it rebuilds only
the path that changed, tests every step, and tells you (and the business) what broke.**

Larkspur & Pine Brewing Co. is fictional. Four source systems feed it: ERP, taproom POS, the brewhouse, and
three distributors that each send a different report layout. Bronze is raw files in a lakehouse; dbt builds
silver and gold in a Fabric warehouse; Power BI reads gold.

---

## Before the audience arrives (about 30 minutes)

| Step | Command / action | Time |
|---|---|---|
| 1. Back to act 1 | `.\tasks.ps1 reset -Env dev -Yes`: undoes the seed fixes, regenerates the landing zone, reloads bronze, empties the dbt schemas, republishes the dbt job, rebuilds | ~12 min |
| 2. Last night's run | `.\tasks.ps1 pipeline -Env dev -Run`: the act 1 card arrives in Teams (2 errors, 6 warnings, 7 skipped) | ~9 min |
| 3. Lineage page | `.\tasks.ps1 lineage` (opens `docs\lineage\dbt_lineage.html` with the act 1 results) | 10 s |
| 4. Check | `.\tasks.ps1 demo-fix -Fix status` shows `"skus": false, "excise": false` | 5 s |

Open and arrange, left to right:
1. The lineage page, full screen in a browser tab.
2. Teams, channel **dbt-alerts** with the act 1 card.
3. Fabric, workspace `LPB-Data-Dev`: the `pl_daily_load` run in the Monitoring hub, the report `LPB Sales and Distribution`, and the notebook `nb_dbt_alert`.
4. A terminal in `C:\Dev\dbt-tutorial`, font size up, `git status` clean.

---

## 1. The setup (2 min)

**Show:** the lineage page, no selection.

- "Four systems land files every day. Bronze keeps them exactly as they arrived. dbt turns them into silver
  (clean, one row per thing) and gold (the tables the business reports on)."
- Point at the columns: bronze sources, seeds (reference data owned by the business, kept in git as CSVs),
  staging, intermediate, gold, and the Power BI semantic model at the end.
- "Every line comes from a `ref()` or `source()` in the SQL. Nobody drew this; dbt works it out, and that is
  what makes everything after this possible."

## 2. Data arrives from different sources: rebuild one path (5 min)

**Show:** click **Distributor depletion reports** in the toolbar.

- Only the distributor path lights up: **8 of 43 models**. The panel shows the command,
  `dbt build -s source:dist+`, and lists the gold tables that are *not* touched.
- "Distributors report monthly, on their own schedule. When a file lands, we rebuild those 8 models and run
  their 35 tests. Sales, production and the taproom keep the data they have. No full refresh."
- Click **Brewhouse production**: 6 models. Click **ERP**: much bigger, because orders feed almost everything.
- Click `fct_sales_line`: incremental (only new days are processed), contract enforced (column names and
  types can't drift under Power BI), tests and unit tests listed, SQL one click away.

## 3. Last night's run: tests catch errors and warnings (5 min)

**Show:** Teams, the act 1 card. Then the lineage page, **Warnings & errors**.

- "Last night's pipeline: load bronze, check source freshness, dbt build, then this card. dbt ran 145 data
  tests and 16 unit tests."
- **Errors stop the line.** Batch B260826-2 shows a 138% yield: more beer packaged than was brewed, a keying
  error. That is an error test, so `fct_batch` and its tests (7 nodes) were skipped rather than publishing
  wrong cost-per-barrel numbers. Gold also has sales into North Dakota with no excise rate.
- **Warnings get logged, the build carries on.** A distributor report is late, Northland reports a new beer's
  codes we haven't mapped (14 rows), 37 invoice lines point at a shipment that doesn't exist, one customer's
  state is spelled "Minnesotta".
- "Each result is also a row in `audit.dbt_run_log` in the warehouse, and the failing rows are kept in the
  audit schema, so this is queryable history, not just a log file." (Optional: in the warehouse,
  `select top 20 * from audit.dbt_run_log order by run_started_at desc`.)
- In the Monitoring hub, the dbt activity is red and the alert step still ran. That's on purpose.
- **Unit tests** (if asked, or to fill time): `_marts__unit_tests.yml` holds hand-made input rows and the exact
  expected output for the margin and returns logic. They run before the model is built.

## 4. Act 2: fix it, one path at a time (8 min)

### 4a. Reference data: map the new beer (2 min)

```powershell
.\tasks.ps1 demo-fix -Fix skus
git diff dbt\seeds
.\tasks.ps1 build -Select "ref_distributor_sku_xref+"
```

- "Cranberry Bog Sour launched in September. Badger got the product codes, Northland and Hawkeye didn't.
  The fix is six rows in a CSV the business owns, reviewed like code."
- The build touches **5 of 43 models** and 24 tests. Both unmapped-SKU warnings are gone.
- On the lineage page, click the seed `ref_distributor_sku_xref`: it shows the same 5 and the same command.

### 4b. Reference data: the missing excise rate (1 min)

```powershell
.\tasks.ps1 demo-fix -Fix excise
.\tasks.ps1 build -Select "ref_excise_tax_rate+"
```

- One row; **2 models** rebuilt. The gold error and the silver warning on excise are gone.

### 4c. Source data: the brewery sends a correction (5 min)

```powershell
.\tasks.ps1 advance-day -Days 3
.\tasks.ps1 bronze -Env dev
.\tasks.ps1 build -Select "source:prod+ source:dist+"
```

- "Three business days pass. On day one the brewhouse re-sends the bad packaging run, corrected. On day
  three Hawkeye's late September report finally arrives."
- `bronze` uploads the new files and runs the ingest notebook in Fabric (about 2-3 minutes). **Fill the
  wait:** bronze is append-only, so the corrected run sits next to the wrong one; staging keeps the newest
  version per run. Or show a unit test.
- The build covers production and distributors only: **14 of 43 models**. The yield is back to 94.7%,
  `fct_batch` builds again, and the late-report warning is gone.
- "The new ERP orders and taproom sales from those three days are in bronze too, but we didn't need them for
  this fix. Tonight's pipeline picks them up."

## 5. The payoff (4 min)

```powershell
.\tasks.ps1 build
.\tasks.ps1 lineage
```

- The full nightly build (a couple of minutes; talk over it or show the report meanwhile): **0 errors,
  2 warnings**. The two left are business follow-ups, not data-platform problems: the invoice lines without a
  shipment, and the customer's state spelling, which belongs in the ERP.
- Fabric, `nb_dbt_alert`, **Run all** (about a minute): the new Teams card.
- The report, **Distributor Sell-through** page: **Unmapped depletion barrels** is now 0, and Cranberry Bog
  Sour shows up under Northland and Hawkeye. Direct Lake picked up the new gold data without a refresh.

**Close:** "Data from four systems on four schedules. dbt rebuilt only what each change touched, tested every
step, stopped what was wrong, and told the business in Teams. Everything you saw is code in one git repo."

---

## Fallbacks

| If | Then |
|---|---|
| Fabric is slow to start the ingest notebook | Skip 4c live; show the lineage page with **Brewhouse production** selected and explain. Run it afterwards |
| A local build fails to connect (`az login` expired) | `az login`, then rerun the same command |
| Teams doesn't show the new card | Fabric, `lh_bronze`, `Files/_alerts` (only on dry runs), or the notebook's output cell shows the headline |
| Someone asks "what about the full refresh?" | `.\tasks.ps1 build` is exactly that; it takes minutes here because the data is small, but on real volumes the point is not to need it |

## After a rehearsal

- `.\tasks.ps1 reset -Env dev -Yes` puts everything back to act 1: the seeds, the landing zone, bronze, the
  dbt schemas and the dbt job's copy of the project.
- The seed fixes are edits to tracked files. Don't commit them on the demo branch; `reset` (or
  `.\tasks.ps1 demo-fix -Fix undo`) removes them.
- If you ran the pipeline after the fixes without `reset`, note that the Fabric dbt job still has the act 1
  seeds until you run `.\tasks.ps1 dbt-job -Env dev`.
