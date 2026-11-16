# Larkspur & Pine: dbt on Microsoft Fabric

A demo of dbt on Microsoft Fabric, built the way a client project would be. The company is
fictional: **Larkspur & Pine Brewing Co.**, a regional craft beverage maker that sells through
distributors, a taproom, and direct accounts.

- **Bronze**: Lakehouse `lh_bronze`, raw files from four source systems, loaded as-is
- **Silver**: Warehouse `wh_analytics` (`ref`, `silver`, `snapshots`), built by dbt
- **Gold**: Warehouse `wh_analytics.gold`, a star schema with enforced contracts, built by dbt
- **Audit**: failing test rows and a run log, queryable in the warehouse

The full design is in [docs/DESIGN.md](docs/DESIGN.md). Working rules for Claude Code are in [CLAUDE.md](CLAUDE.md).

## Status

| Phase | What | State |
|---|---|---|
| 0 | Design | done |
| 1 | Spike: confirm what works on Fabric ([docs/SPIKE.md](docs/SPIKE.md)) | not started |
| 2 | Repo scaffold, data generator, dbt skeleton with seeds | done |
| 4 | Bronze ingest notebook (`notebooks/nb_ingest_bronze.py`) | written, tested on local Spark; not run in Fabric |
| 5 | dbt staging, snapshot, intermediate and gold (13 models with contracts), audit run log, 16 unit tests | written, checked locally; not run on Fabric |
| 3, 6, 7 | Fabric provisioning, dbt job, CI/CD | not started |

## Quick start (Windows, PowerShell)

```powershell
.\tasks.ps1 help          # list every task
.\tasks.ps1 venv          # build .venv from requirements.txt
.\tasks.ps1 prereqs       # check tools and sign-in
.\tasks.ps1 data          # generate ~18 months of source files into data\landing
.\tasks.ps1 advance-day   # generate the next business day
.\tasks.ps1 parse         # check the dbt project without connecting
```

To run dbt against Fabric, copy `.env.example` to `.env`, fill it in, run `az login`, then:

```powershell
. .\scripts\load_env.ps1
cd dbt
dbt debug
dbt seed
```

## The generated data

`scripts/generate_data.py` simulates four systems and writes files the way they would really arrive:

| System | Files | Folder |
|---|---|---|
| ERP | customers, items, effective-dated price list, orders, shipments, invoices (CSV) | `data/landing/erp/<date>/` |
| Brewery production | batches, ingredient usage, packaging runs, costs (JSON Lines) | `data/landing/prod/<date>/` |
| Taproom POS | transactions with nested line items (JSON Lines) | `data/landing/pos/<date>/` |
| Distributors | monthly depletion reports in three different layouts | `data/landing/dist/<date received>/` |

It is deterministic, and it plants realistic problems for dbt to catch: a POS file sent twice,
distributor product codes missing from the cross-reference seed, a late distributor file,
invoices pointing at shipments that do not exist, a batch that packaged more beer than it brewed,
a state with no excise rate, and messy state names. `-Clean` generates the same data without them.

`advance-day` then delivers surprises on a schedule: day 1 a bar (C-2001) moves and is upgraded from
price tier C to B and yesterday's POS file arrives again, day 2 a price increase, day 3 the late distributor file.

## Repo layout

```
CLAUDE.md, .claude/      instructions, permissions and slash commands for Claude Code
tasks.ps1                single entry point
config/environments.yml  workspace and item names per environment
scripts/                 data generator, environment loader, prerequisite check
notebooks/               Fabric notebook that loads the landing zone into bronze
dbt/                     the dbt project: seeds, staging, snapshot, intermediate, gold
docs/                    design, spike checklist, IDE setup notes
bootstrap.ps1, .vscode/  VS Code setup (see docs/ide-setup.md)
```
