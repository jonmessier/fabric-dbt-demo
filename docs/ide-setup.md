# VS Code + dbt Starter

A reproducible IDE setup for dbt development. Drop this into a repo and anyone who
clones it gets the same editor configuration.

## What's here

| File | Purpose |
|---|---|
| `.vscode/extensions.json` | Extension recommendations — VS Code prompts on folder open |
| `.vscode/settings.json` | Workspace settings (Jinja highlighting, linting, file exclusions) |
| `bootstrap.ps1` | Installs extensions, creates a venv, installs dbt + adapter |

## Quick start

```powershell
git clone <this-repo>
cd <this-repo>
.\bootstrap.ps1
code .
```

Then: Command Palette → **Python: Select Interpreter** → pick `.venv`.

## For your team

The `.vscode/` folder is the part that matters for sharing. Committing it means
teammates get the extension prompt and the settings automatically — no script, no
instructions. `bootstrap.ps1` is only needed for the Python side, which VS Code
can't configure on its own.

If someone dismisses the extension prompt: Command Palette →
**Extensions: Show Recommended Extensions**.

## Adapter options

```powershell
.\bootstrap.ps1 -Adapter duckdb        # default — no credentials, best for learning
.\bootstrap.ps1 -Adapter postgres
.\bootstrap.ps1 -Adapter snowflake
.\bootstrap.ps1 -Adapter fabric        # Fabric Warehouse (T-SQL)
.\bootstrap.ps1 -Adapter fabricspark   # Fabric Lakehouse (Spark/Livy)
```

**duckdb is the right starting point for learning.** It runs entirely locally, needs
no connection details, and every dbt concept — refs, models, tests, seeds, snapshots,
macros — behaves identically to a warehouse adapter. Learn on duckdb, then swap the
profile.

## Two adapters in one repo

`dbt-fabric` and `dbt-fabricspark` pin different transitive dependencies and will
conflict in a shared environment. Use separate venvs:

```powershell
.\bootstrap.ps1 -Adapter fabricspark -VenvName .venv-silver -SkipExtensions
.\bootstrap.ps1 -Adapter fabric      -VenvName .venv-gold   -SkipExtensions
```

VS Code switches interpreters per workspace folder, so a multi-root workspace with
`silver/` and `gold/` as separate roots gets the right environment in each.

## sqlfluff config

Dialect is set per dbt project, not in VS Code settings, so different layers can use
different dialects. Create a `.sqlfluff` beside each `dbt_project.yml`:

```ini
[sqlfluff]
templater = dbt
dialect = tsql          # or sparksql, duckdb, postgres, snowflake
max_line_length = 120

[sqlfluff:templater:dbt]
project_dir = .
profiles_dir = ~/.dbt

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = lower
```

## Notes

**The official dbt Labs VS Code extension won't work here.** It requires the dbt Fusion
engine and doesn't support dbt Core or community adapters. `dbt Power User` is the
one to use.

**Jinja highlighting is scoped to dbt folders on purpose.** Applying `jinja-sql` to all
`*.sql` files breaks the mssql extension's Execute Query, since it only runs against
files in plain `sql` mode. The globs in `settings.json` cover `models/`, `macros/`,
`tests/`, and `analyses/` only.

**Never commit `profiles.yml`.** It lives at `%USERPROFILE%\.dbt\profiles.yml` and holds
credentials. Add `profiles.yml`, `target/`, `dbt_packages/`, `logs/`, and `.venv*` to
`.gitignore`.

**Fabric Data Engineering extension** — if you need to author Fabric notebooks locally,
install it from the Marketplace by name. It's deliberately not in the script; the
publisher ID changes and a wrong ID fails silently.
