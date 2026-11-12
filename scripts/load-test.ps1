# Load experiments with engine-cli against a running system.
#   scripts/load-test.ps1                    # 10 / 100 / 1000 concurrent submissions
# Re-run after restarting the worker with different MAX_ACTIVE_JOBS / QUEUE_CAPACITY /
# TOKIO_WORKER_THREADS to compare. Numbers are whatever YOUR machine measures.
param([string]$JobType = 'simulate_delay', [string]$Payload = '{"duration_ms":100}', [switch]$Release)
Set-Location (Join-Path $PSScriptRoot '..')
$profileDir = if ($Release) { 'release' } else { 'debug' }
$cli = ".\target\$profileDir\engine-cli.exe"
foreach ($n in 10, 100, 1000) {
    Write-Host "`n===== $n concurrent submissions of $JobType ====="
    & $cli load --count $n --concurrency $n --job-type $JobType --payload $Payload --wait --wait-timeout-secs 600
}
