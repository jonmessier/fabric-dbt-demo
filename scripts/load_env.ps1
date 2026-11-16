<#
.SYNOPSIS
    Loads .env into the current PowerShell session and points dbt at dbt/profiles.yml.
    Dot-source it so the variables stay in your session:

        . .\scripts\load_env.ps1
#>
$__repoRoot = Split-Path -Parent $PSScriptRoot
$__envFile = Join-Path $__repoRoot '.env'
if (Test-Path $__envFile) {
    foreach ($__line in Get-Content $__envFile) {
        $__line = $__line.Trim()
        if ($__line -and -not $__line.StartsWith('#') -and $__line.Contains('=')) {
            $__name, $__value = $__line -split '=', 2
            [Environment]::SetEnvironmentVariable($__name.Trim(), $__value.Trim().Trim('"'), 'Process')
        }
    }
} else {
    Write-Host "  [!!] .env not found. Copy .env.example to .env and fill it in." -ForegroundColor Yellow
}
$env:DBT_PROFILES_DIR = Join-Path $__repoRoot 'dbt'
