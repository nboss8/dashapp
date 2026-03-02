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
    # Prefer venv_dbt's Python + dbt module to avoid broken Windows launchers
    $projectRoot = Split-Path (Get-Location) -Parent
    $pythonExe = Join-Path $projectRoot (Join-Path "venv_dbt" (Join-Path "Scripts" "python.exe"))
    if (-not (Test-Path $pythonExe)) { $pythonExe = "python" }

    if ($args.Count -gt 0) {
        & $pythonExe -m dbt.cli.main @args --profiles-dir .
    }
    else {
        & $pythonExe -m dbt.cli.main run --profiles-dir .
    }
} finally {
    Pop-Location
}
