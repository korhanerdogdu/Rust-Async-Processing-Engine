# Runs the full verification: infra up, build, unit tests, cross-process e2e tests.
#   powershell -ExecutionPolicy Bypass -File scripts/test-e2e.ps1
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

docker compose up -d
Write-Host 'waiting for TimescaleDB...'
for ($i = 0; $i -lt 60; $i++) {
    docker compose exec -T timescaledb pg_isready -U engine -d engine *> $null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 1
}

cargo build --workspace
if ($LASTEXITCODE -ne 0) { exit 1 }
cargo test --workspace
if ($LASTEXITCODE -ne 0) { exit 1 }

$env:E2E_OTLP_ENDPOINT = 'http://127.0.0.1:4318'
$env:E2E_JAEGER_URL = 'http://127.0.0.1:16686'
cargo test -p e2e-tests -- --ignored --test-threads=4
exit $LASTEXITCODE
