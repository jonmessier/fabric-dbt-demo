# ADR 0001: The dbt job is built from `dbt/` by a script

**Status:** accepted · **Date:** 2026-11-14 · **Supersedes:** DESIGN v0.4.1, which listed the dbt job among the git-synced items in `fabric/` and deployed by fabric-cicd

## Context

The spike (`docs/SPIKE.md` item 12) showed that a Fabric dbt job (`DataBuildToolJob`) stores the whole dbt
project inside its own item definition, as `Code/dbt/<file>` parts next to `dbt-content.json`. `dbt-content.json`
also holds the warehouse connection as IDs: workspace ID, warehouse ID and SQL endpoint.

If the job were git-synced into `fabric/` and deployed by fabric-cicd:
- `fabric/dbt_lpb.DataBuildToolJob/Code/dbt/` would be a second copy of `dbt/`, kept in step only by
  republishing from the UI.
- Dev's IDs would be committed, and `parameter.yml` would have to rewrite three values per environment.

The spike also found that the job silently drops a `selector` argument. Only `select`, `exclude` and
`threads` reach dbt.

## Decision

- `scripts/dbt_job.py` builds the job definition from `dbt/` and creates the job or replaces its definition.
  It looks up the warehouse IDs and endpoint by name for the target environment
  (`.\tasks.ps1 dbt-job -Env <env>`).
- `deploy.py` (phase 7) imports `job_definition()` and `ensure_dbt_job()` from it instead of deploying the job
  through fabric-cicd. The other items still go through fabric-cicd.
- The job runs `dbt build` with no selection, which is what selector `nightly` means today. If the nightly
  scope ever changes, express it as `select`/`exclude` in `dbt_job.py`.

## Consequences

- `dbt/` is the only copy of the project, and no environment IDs are in git.
- When Dev's Git sync is turned on (once there is a remote), exclude the dbt job from `fabric/`, or accept that
  the synced copy is read-only output.
- The definition shape is undocumented (Microsoft's docs name the part `dbtjob-content.json`; the real name is
  `dbt-content.json`). If a Fabric update changes it, `dbt_job.py` is the one place to fix.
- The Fabric item must exist before its first deploy (the script creates it). Deleting files from `dbt/`
  removes them from the job, because `updateDefinition` replaces the whole project.
