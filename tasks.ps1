<#
.SYNOPSIS
    One entry point for the Larkspur & Pine dbt-on-Fabric demo.

.EXAMPLE
    .\tasks.ps1 help
    .\tasks.ps1 data
    .\tasks.ps1 advance-day -Days 2
    .\tasks.ps1 build
    .\tasks.ps1 setup -Env dev

.NOTES
    For ad-hoc dbt commands, load the environment once and call dbt yourself:
        . .\scripts\load_env.ps1
        cd dbt
        dbt build -s +fct_depletion
#>
param(
    [Parameter(Position = 0)]
    [string]$Task = 'help',

    [Alias('Env')]
    [ValidateSet('dev', 'test', 'prod')]
    [string]$Environment = 'dev',

    [int]$Days = 1,
    [string]$Capacity,
    [switch]$Clean,
    [switch]$ReloadAll,
    [switch]$SkipUpload,
    [switch]$Run,
    [switch]$FirstBuild,
    [switch]$Yes,
    [switch]$WriteSeeds,
    [switch]$Force,
    [switch]$Export
)

$ErrorActionPreference = 'Stop'
$RepoRoot = $PSScriptRoot
$VenvPython = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$VenvDbt = Join-Path $RepoRoot '.venv\Scripts\dbt.exe'

function Get-Python {
    if (Test-Path $VenvPython) { return $VenvPython }
    return 'python'
}

function Assert-LastExit {
    param([string]$What)
    if ($LASTEXITCODE -ne 0) { throw "$What failed (exit code $LASTEXITCODE)" }
}

function Invoke-Dbt {
    # -AllowTestFailures: failing data tests are not an error (the planted "act 1" defects), but a model,
    # seed or snapshot that errors still is. Decided from target\run_results.json.
    param([string[]]$DbtArgs, [switch]$AllowTestFailures)
    if (-not (Test-Path $VenvDbt)) { throw "dbt not found in .venv. Run: .\tasks.ps1 venv" }
    . (Join-Path $RepoRoot 'scripts\load_env.ps1')
    Push-Location (Join-Path $RepoRoot 'dbt')
    try {
        Write-Host "dbt $($DbtArgs -join ' ')" -ForegroundColor Cyan
        & $VenvDbt @DbtArgs
        $dbtExit = $LASTEXITCODE
        Save-RunResults
        $global:LASTEXITCODE = $dbtExit
        if ($LASTEXITCODE -ne 0 -and $AllowTestFailures -and (Test-Path 'target\run_results.json')) {
            $results = (Get-Content 'target\run_results.json' -Raw | ConvertFrom-Json).results
            $errors = @($results | Where-Object { $_.status -eq 'error' })
            $fails = @($results | Where-Object { $_.status -eq 'fail' })
            if ($errors.Count -eq 0) {
                Write-Host "dbt $($DbtArgs[0]) finished with $($fails.Count) failing test(s): expected in act 1 (planted defects)." -ForegroundColor Yellow
                $global:LASTEXITCODE = 0
            }
        }
        Assert-LastExit "dbt $($DbtArgs[0])"
    } finally {
        Pop-Location
    }
}

function Save-RunResults {
    # Each dbt command overwrites target\run_results.json (build, then the unit tests, for example).
    # Keep a copy per command so the lineage page can show the latest status of every node.
    # Runs inside dbt\. Keeps the newest 30.
    if (-not (Test-Path 'target\run_results.json')) { return }
    $hist = 'target\run_history'
    New-Item -ItemType Directory -Force $hist | Out-Null
    $rr = Get-Item 'target\run_results.json'
    $name = '{0}_{1}.json' -f $rr.LastWriteTime.ToString('yyyyMMdd-HHmmss'), $DbtArgs[0]
    Copy-Item $rr.FullName (Join-Path $hist $name) -Force
    Get-ChildItem $hist -Filter *.json | Sort-Object Name -Descending | Select-Object -Skip 30 | Remove-Item
}

function Invoke-Script {
    param([string]$Script, [string[]]$ScriptArgs)
    $py = Get-Python
    Write-Host "python $Script $($ScriptArgs -join ' ')" -ForegroundColor Cyan
    & $py (Join-Path $RepoRoot "scripts\$Script") @ScriptArgs
    Assert-LastExit $Script
}

function Write-NotYet {
    param([string]$Phase, [string]$Script)
    Write-Host "Not built yet: '$Task' arrives in $Phase ($Script). See docs/DESIGN.md section 9." -ForegroundColor Yellow
    exit 2
}

Push-Location $RepoRoot
try {
    switch ($Task.ToLower()) {
        'help' {
            Write-Host @"

  Larkspur & Pine demo tasks        usage: .\tasks.ps1 <task> [-Env dev|test|prod]

  Local
    prereqs        Check tools, sign-in and .env
    venv           Create .venv and install requirements.txt
    data           Generate ~18 months of source files into data\landing
                   (-Clean = no planted defects, -WriteSeeds = rewrite dbt\seeds, -Force = replace)
    advance-day    Generate the next business day's files (-Days N)
    parse          dbt parse (checks the project without connecting)
    lint           sqlfluff lint dbt\models

  dbt against Fabric (needs .env and az login)
    debug          dbt debug
    seed           dbt seed
    build          dbt build --selector nightly
                   (-FirstBuild = into an empty warehouse: unit tests after the models)
    unit           dbt test -s test_type:unit
    freshness      dbt source freshness
    docs           dbt docs generate, then serve
    lineage        Build docs\lineage\dbt_lineage.html from the manifest and latest results, and open it
                   (dbt parse first if there is no manifest)

  Fabric
    setup          Create workspace, lakehouse, warehouse, schemas; writes FABRIC_SQL_ENDPOINT
                   (-Capacity "<name>" saves the capacity to .env on first run)
    bronze         Upload data\landing to lh_bronze, deploy and run nb_ingest_bronze
                   (-ReloadAll = rebuild every bronze table, -SkipUpload = notebook only)
    dbt-job        Publish dbt\ as the Fabric dbt job dbt_lpb (dbt build, all nodes)
                   (-Run = also run it in Fabric and print its dbt version and results)
    pipeline       Deploy nb_ingest_bronze, nb_dbt_freshness, nb_dbt_alert and pl_daily_load
                   (-Run = upload new landing files, run the pipeline, print each activity)
                   TEAMS_WEBHOOK_URL in .env = where the alert posts; empty = dry run
    powerbi        Deploy the semantic model and report from powerbi\, then refresh the model
                   (-Export = pull the report edited in Fabric back into powerbi\)
    reset          Put the demo back to its starting state ("act 1"); needs -Yes

  Fabric (not built yet)
    identity       App registration, GitHub OIDC, tenant checks  (Phase 3)
    deploy         Publish to test/prod with fabric-cicd         (Phase 7)
    teardown       Delete a workspace                            (Phase 7)

"@
        }
        'prereqs' {
            & (Join-Path $RepoRoot 'scripts\check_prereqs.ps1')
        }
        'venv' {
            if (-not (Test-Path $VenvPython)) {
                & python -m venv .venv
                Assert-LastExit 'python -m venv'
            }
            & $VenvPython -m pip install --upgrade pip --quiet
            & $VenvPython -m pip install -r requirements.txt
            Assert-LastExit 'pip install'
            & $VenvDbt --version
        }
        'data' {
            $genArgs = @('scripts\generate_data.py', 'backfill')
            if ($Clean) { $genArgs += '--clean' }
            if ($WriteSeeds) { $genArgs += '--write-seeds' }
            if ($Force) { $genArgs += '--force' }
            & (Get-Python) @genArgs
            Assert-LastExit 'generate_data backfill'
        }
        'advance-day' {
            & (Get-Python) 'scripts\generate_data.py' 'advance' '--days' $Days
            Assert-LastExit 'generate_data advance'
            Write-Host "Local files written. Load them with: .\tasks.ps1 bronze -Env $Environment, then .\tasks.ps1 build" -ForegroundColor Yellow
        }
        'parse'     { Invoke-Dbt @('parse') }
        'debug'     { Invoke-Dbt @('debug') }
        'seed'      { Invoke-Dbt @('seed') }
        'build' {
            if ($FirstBuild) {
                # Empty warehouse (new environment, or after reset): the fct_sales_line unit test reads
                # {{ this }}, which doesn't exist yet. Build everything without unit tests, then run them.
                # No --selector here: dbt ignores --exclude when --selector is given (nightly = everything).
                Invoke-Dbt @('build', '--exclude', 'test_type:unit') -AllowTestFailures
                Invoke-Dbt @('test', '-s', 'test_type:unit')
            } else {
                # --empty creates a missing incremental table with no rows when its parents exist
                # (e.g. after dropping it); existing tables are left unchanged (docs/SPIKE.md item 8).
                Invoke-Dbt @('run', '-s', 'config.materialized:incremental', '--empty')
                Invoke-Dbt @('build', '--selector', 'nightly') -AllowTestFailures
            }
        }
        'unit'      { Invoke-Dbt @('test', '-s', 'test_type:unit') }
        'freshness' { Invoke-Dbt @('source', 'freshness') }
        'docs' {
            Invoke-Dbt @('docs', 'generate')
            Invoke-Dbt @('docs', 'serve')
        }
        'lineage' {
            if (-not (Test-Path 'dbt\target\manifest.json')) { Invoke-Dbt @('parse') }
            Invoke-Script 'build_lineage.py' @()
            $page = Join-Path $RepoRoot 'docs\lineage\dbt_lineage.html'
            if (-not $Yes) { Start-Process $page }   # -Yes = build only, don't open
        }
        'lint' {
            . (Join-Path $RepoRoot 'scripts\load_env.ps1')
            Push-Location (Join-Path $RepoRoot 'dbt')
            try { & (Join-Path $RepoRoot '.venv\Scripts\sqlfluff.exe') lint models } finally { Pop-Location }
        }
        'setup' {
            $a = @('--env', $Environment)
            if ($Capacity) { $a += @('--capacity', $Capacity) }
            Invoke-Script 'provision.py' $a
        }
        'identity' { Write-NotYet 'Phase 3' 'scripts\setup_identity.ps1' }
        'bronze' {
            $a = @('--env', $Environment)
            if ($ReloadAll) { $a += '--reload-all' }
            if ($SkipUpload) { $a += '--skip-upload' }
            Invoke-Script 'load_bronze.py' $a
        }
        'pipeline' {
            $a = @('--env', $Environment)
            if ($Run) { $a += '--run' }
            if ($SkipUpload) { $a += '--skip-upload' }
            Invoke-Script 'pipeline.py' $a
        }
        'dbt-job' {
            $a = @('--env', $Environment)
            if ($Run) { $a += '--run' }
            Invoke-Script 'dbt_job.py' $a
        }
        'powerbi' {
            $a = @('--env', $Environment)
            if ($Export) { $a += '--export' }
            Invoke-Script 'powerbi.py' $a
        }
        'reset' {
            if (-not $Yes) {
                Write-Host @"
reset puts $Environment back to the demo's starting state ("act 1"). It
  1. regenerates data\landing (backfill only, with the planted defects),
  2. replaces lh_bronze Files/landing and reloads every bronze table (scripts\reset_demo.py),
  3. drops every table and view in ref, silver, snapshots, gold and audit,
  4. rebuilds with dbt, unit tests last (dev only).
Run again with -Yes to do it.
"@ -ForegroundColor Yellow
                exit 1
            }
            if ($Environment -eq 'prod') { throw 'reset refuses prod. Run scripts\reset_demo.py --allow-prod deliberately.' }
            $dirty = git status --porcelain -- dbt
            if ($dirty) {
                Write-Host 'Note: dbt\ has uncommitted changes (demo edits?). Restore them with git restore <file>:' -ForegroundColor Yellow
                $dirty | ForEach-Object { Write-Host "  $_" }
            }
            & (Get-Python) 'scripts\generate_data.py' 'backfill' '--force'
            Assert-LastExit 'generate_data backfill'
            Invoke-Script 'reset_demo.py' @('--env', $Environment, '--confirm')
            if ($Environment -eq 'dev') {
                Invoke-Dbt @('build', '--exclude', 'test_type:unit') -AllowTestFailures
                Invoke-Dbt @('test', '-s', 'test_type:unit')
            } else {
                Write-Host "Local dbt targets dev only. Rebuild $Environment with its pipeline or dbt job." -ForegroundColor Yellow
            }
        }
        'deploy'   { Write-NotYet 'Phase 7' 'scripts\deploy.py' }
        'teardown' { Write-NotYet 'Phase 7' 'scripts\teardown.py' }
        default {
            Write-Host "Unknown task '$Task'. Run .\tasks.ps1 help" -ForegroundColor Red
            exit 1
        }
    }
} finally {
    Pop-Location
}
