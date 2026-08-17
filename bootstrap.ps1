<#
.SYNOPSIS
    Sets up a VS Code + dbt development environment on Windows.

.DESCRIPTION
    Installs the VS Code extensions listed in .vscode/extensions.json, creates a
    Python virtual environment, and installs dbt-core plus the adapter you choose.

    Safe to re-run - extension installs and pip installs are idempotent.

.PARAMETER Adapter
    Which dbt adapter to install. Defaults to duckdb, which needs no credentials
    and is the easiest way to learn dbt.

.PARAMETER VenvName
    Name of the virtual environment folder. Defaults to .venv.

.PARAMETER SkipExtensions
    Skip the VS Code extension install step.

.EXAMPLE
    .\bootstrap.ps1
    .\bootstrap.ps1 -Adapter fabric -VenvName .venv-gold
    .\bootstrap.ps1 -Adapter fabricspark -VenvName .venv-silver
#>

[CmdletBinding()]
param(
    [ValidateSet('duckdb', 'postgres', 'snowflake', 'fabric', 'fabricspark', 'none')]
    [string]$Adapter = 'duckdb',

    [string]$VenvName = '.venv',

    [switch]$SkipExtensions
)

$ErrorActionPreference = 'Stop'

function Write-Step { param($m) Write-Host "`n=== $m ===" -ForegroundColor Cyan }
function Write-Ok   { param($m) Write-Host "  [ok] $m" -ForegroundColor Green }
function Write-Warn { param($m) Write-Host "  [!!] $m" -ForegroundColor Yellow }

# ---------------------------------------------------------------
# 1. Prerequisites
# ---------------------------------------------------------------
Write-Step "Checking prerequisites"

if (-not (Get-Command code -ErrorAction SilentlyContinue)) {
    Write-Warn "'code' command not found on PATH."
    Write-Warn "In VS Code: Command Palette -> 'Shell Command: Install code command in PATH'"
    if (-not $SkipExtensions) { throw "Cannot install extensions without the 'code' CLI." }
} else {
    Write-Ok "VS Code CLI found"
}

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) { throw "Python not found on PATH. Install Python 3.9-3.12 first." }

$pyVersion = (& python --version 2>&1) -replace 'Python ', ''
Write-Ok "Python $pyVersion"

$pyMajorMinor = ($pyVersion -split '\.')[0..1] -join '.'
if ([version]$pyMajorMinor -gt [version]'3.12') {
    Write-Warn "dbt adapter support for Python 3.13+ is uneven. 3.11 or 3.12 is the safe choice."
}

# ---------------------------------------------------------------
# 2. VS Code extensions
# ---------------------------------------------------------------
if (-not $SkipExtensions) {
    Write-Step "Installing VS Code extensions"

    $extensions = @(
        'innoverio.vscode-dbt-power-user',
        'samuelcolvin.jinjahtml',
        'sqlfluff.vscode-sqlfluff',
        'ms-python.python',
        'ms-python.vscode-pylance',
        'redhat.vscode-yaml',
        'mechatroner.rainbow-csv',
        'ms-mssql.mssql',
        'eamodio.gitlens',
        'github.vscode-pull-request-github',
        'anthropic.claude-code'
    )

    $installed = & code --list-extensions

    foreach ($ext in $extensions) {
        if ($installed -contains $ext) {
            Write-Ok "$ext (already installed)"
        } else {
            Write-Host "  installing $ext ..."
            & code --install-extension $ext --force | Out-Null
            if ($LASTEXITCODE -eq 0) { Write-Ok $ext }
            else { Write-Warn "$ext failed - install manually from the Marketplace" }
        }
    }
}

# ---------------------------------------------------------------
# 3. Virtual environment
# ---------------------------------------------------------------
Write-Step "Setting up virtual environment: $VenvName"

if (Test-Path $VenvName) {
    Write-Ok "$VenvName already exists"
} else {
    & python -m venv $VenvName
    Write-Ok "created $VenvName"
}

$venvPython = Join-Path $VenvName 'Scripts\python.exe'
if (-not (Test-Path $venvPython)) { throw "Virtual environment looks broken: $venvPython not found" }

& $venvPython -m pip install --upgrade pip --quiet
Write-Ok "pip upgraded"

# ---------------------------------------------------------------
# 4. dbt + adapter
# ---------------------------------------------------------------
if ($Adapter -ne 'none') {
    Write-Step "Installing dbt-$Adapter"

    # Installing the adapter pulls dbt-core as a dependency at a compatible version.
    & $venvPython -m pip install "dbt-$Adapter" --quiet
    Write-Ok "dbt-$Adapter installed"

    & $venvPython -m pip install sqlfluff sqlfluff-templater-dbt --quiet
    Write-Ok "sqlfluff installed"

    Write-Host ""
    & (Join-Path $VenvName 'Scripts\dbt.exe') --version
}

# ---------------------------------------------------------------
# 5. Next steps
# ---------------------------------------------------------------
Write-Step "Done"

Write-Host @"

  Next:

  1. Select the interpreter in VS Code
     Command Palette -> 'Python: Select Interpreter' -> $VenvName

  2. Activate the venv in your terminal
     .\$VenvName\Scripts\Activate.ps1

  3. Create a project (if you don't have one yet)
     dbt init my_project

  4. Point dbt at a profile
     dbt debug

  Profiles live in %USERPROFILE%\.dbt\profiles.yml - never commit that file.

"@ -ForegroundColor Gray