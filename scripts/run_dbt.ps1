# Run dbt from project root. Loads .env, then runs dbt with --profiles-dir in dbt/
# Usage: .\scripts\run_dbt.ps1 [dbt args...]
# Examples:
#   .\scripts\run_dbt.ps1 parse
#   .\scripts\run_dbt.ps1 run
#   .\scripts\run_dbt.ps1 run --select pidk_run_totals pidk_shift_totals

$envPath = Join-Path (Get-Location) ".env"
if (Test-Path $envPath) {
    Get-Content $envPath | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $val = $matches[2].Trim().Trim('"')
            [Environment]::SetEnvironmentVariable($matches[1].Trim(), $val, 'Process')
        }
    }
}

$dbtDir = Join-Path (Get-Location) "dbt"
Push-Location $dbtDir
try {
    # Use venv_dbt's dbt if available
    $projectRoot = Split-Path (Get-Location) -Parent
    $dbtExe = Join-Path $projectRoot (Join-Path "venv_dbt" (Join-Path "Scripts" "dbt.exe"))
    if (-not (Test-Path $dbtExe)) { $dbtExe = "dbt" }
    if ($args.Count -gt 0) {
        & $dbtExe @args --profiles-dir .
    } else {
        & $dbtExe run --profiles-dir .
    }
} finally {
    Pop-Location
}
