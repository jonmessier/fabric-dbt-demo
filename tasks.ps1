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
    [switch]$Clean,
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

  Fabric (not built yet)
    setup          Create workspace, lakehouse, warehouse       (Phase 3)
    identity       App registration, GitHub OIDC, tenant checks  (Phase 3)
    bronze         Upload landing files and run bronze ingest    (Phase 4)
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
            Write-Host "Local files written. Loading them into bronze arrives in Phase 4." -ForegroundColor Yellow
        }
        'parse'     { Invoke-Dbt @('parse') }
        'debug'     { Invoke-Dbt @('debug') }
        'seed'      { Invoke-Dbt @('seed') }
        'build'     { Invoke-Dbt @('build', '--selector', 'nightly') }
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
        'setup'    { Write-NotYet 'Phase 3' 'scripts\provision.py' }
        'identity' { Write-NotYet 'Phase 3' 'scripts\setup_identity.ps1' }
        'bronze'   { Write-NotYet 'Phase 4' 'scripts\load_bronze.py' }
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
