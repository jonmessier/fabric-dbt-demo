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
    [switch]$WriteSeeds,
    [switch]$Force
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
    param([string[]]$DbtArgs)
    if (-not (Test-Path $VenvDbt)) { throw "dbt not found in .venv. Run: .\tasks.ps1 venv" }
    . (Join-Path $RepoRoot 'scripts\load_env.ps1')
    Push-Location (Join-Path $RepoRoot 'dbt')
    try {
        Write-Host "dbt $($DbtArgs -join ' ')" -ForegroundColor Cyan
        & $VenvDbt @DbtArgs
        Assert-LastExit "dbt $($DbtArgs[0])"
    } finally {
        Pop-Location
    }
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
    unit           dbt test -s test_type:unit
    freshness      dbt source freshness
    docs           dbt docs generate, then serve

  Fabric
    setup          Create workspace, lakehouse, warehouse, schemas; writes FABRIC_SQL_ENDPOINT
                   (-Capacity "<name>" saves the capacity to .env on first run)
    bronze         Upload data\landing to lh_bronze, deploy and run nb_ingest_bronze
                   (-ReloadAll = rebuild every bronze table, -SkipUpload = notebook only)
    dbt-job        Publish dbt\ as the Fabric dbt job dbt_lpb (dbt build, all nodes)
                   (-Run = also run it in Fabric and print its dbt version and results)

  Fabric (not built yet)
    identity       App registration, GitHub OIDC, tenant checks  (Phase 3)
    reset          Put the demo back to its starting state       (Phase 5)
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
            Write-Host "Local files written. Load them with: .	asks.ps1 bronze -Env $Environment, then .	asks.ps1 build" -ForegroundColor Yellow
        }
        'parse'     { Invoke-Dbt @('parse') }
        'debug'     { Invoke-Dbt @('debug') }
        'seed'      { Invoke-Dbt @('seed') }
        'build' {
            # A unit test that overrides is_incremental needs its model's table to exist, so a new
            # environment's first build would fail. --empty creates missing incremental tables with
            # no rows and leaves existing ones unchanged (docs/SPIKE.md item 8).
            Invoke-Dbt @('run', '-s', 'config.materialized:incremental', '--empty')
            Invoke-Dbt @('build', '--selector', 'nightly')
        }
        'unit'      { Invoke-Dbt @('test', '-s', 'test_type:unit') }
        'freshness' { Invoke-Dbt @('source', 'freshness') }
        'docs' {
            Invoke-Dbt @('docs', 'generate')
            Invoke-Dbt @('docs', 'serve')
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
        'dbt-job' {
            $a = @('--env', $Environment)
            if ($Run) { $a += '--run' }
            Invoke-Script 'dbt_job.py' $a
        }
        'reset'    { Write-NotYet 'Phase 5' 'scripts\reset_demo.py' }
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
