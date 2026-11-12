<#
.SYNOPSIS
    Checks that this machine has what the demo needs. Read-only: installs nothing.
    Prints a table, then one line of JSON for scripts and Claude to parse.
#>
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$results = New-Object System.Collections.Generic.List[object]

function Add-Check {
    param([string]$Name, [bool]$Ok, [string]$Detail, [bool]$Required = $true)
    $results.Add([pscustomobject]@{ Check = $Name; Ok = $Ok; Required = $Required; Detail = $Detail })
}

function Test-Tool {
    param([string]$Name, [string]$Command, [string[]]$VersionArgs = @('--version'), [bool]$Required = $true, [string]$Hint = '')
    $cmd = Get-Command $Command -ErrorAction SilentlyContinue
    if (-not $cmd) { Add-Check $Name $false "not found. $Hint" $Required; return }
    $ver = ''
    try { $ver = (& $Command @VersionArgs 2>&1 | Select-Object -First 1) } catch { $ver = 'found' }
    Add-Check $Name $true "$ver" $Required
}

Test-Tool 'Python'      'python' -Hint 'Install Python 3.11 or 3.12.'
Test-Tool 'Git'         'git'
Test-Tool 'Azure CLI'   'az'  @('version', '--query', '"azure-cli"', '-o', 'tsv') -Hint 'winget install Microsoft.AzureCLI'
Test-Tool 'GitHub CLI'  'gh'  -Required $false -Hint 'winget install GitHub.cli (needed for CI/CD setup)'
Test-Tool 'VS Code CLI' 'code' -Required $false

$dbt = Join-Path $root '.venv\Scripts\dbt.exe'
if (Test-Path $dbt) {
    $v = (& $dbt --version 2>&1 | Out-String) -replace '\s+', ' '
    Add-Check 'dbt (in .venv)' $true $v.Trim()
} else {
    Add-Check 'dbt (in .venv)' $false 'not found. Run .\tasks.ps1 venv'
}

$fab = Join-Path $root '.venv\Scripts\fab.exe'
Add-Check 'Fabric CLI (in .venv)' (Test-Path $fab) $(if (Test-Path $fab) { 'found' } else { 'not found. Run .\tasks.ps1 venv' })

Add-Check '.env file' (Test-Path (Join-Path $root '.env')) 'Copy .env.example to .env and fill it in.'

# dbt-fabric 1.11+ ships its own SQL driver; older versions need this one. The spike confirms which applies.
$odbc = $null
try { $odbc = Get-OdbcDriver -Name 'ODBC Driver 18 for SQL Server' -ErrorAction Stop } catch { }
Add-Check 'ODBC Driver 18 for SQL Server' ($null -ne $odbc) 'optional unless the spike shows dbt-fabric needs it' $false

$acct = $null
if (Get-Command az -ErrorAction SilentlyContinue) {
    try { $acct = (& az account show --query 'user.name' -o tsv 2>$null) } catch { }
}
Add-Check 'az login' ([bool]$acct) $(if ($acct) { "signed in as $acct" } else { 'not signed in. Run: az login' })

$results | Format-Table Check, Ok, Required, Detail -AutoSize -Wrap | Out-String | Write-Host
$missing = @($results | Where-Object { $_.Required -and -not $_.Ok } | ForEach-Object { $_.Check })
$summary = [ordered]@{ ok = ($missing.Count -eq 0); missing = $missing }
Write-Output ($summary | ConvertTo-Json -Compress)
if ($missing.Count -gt 0) { exit 1 }
