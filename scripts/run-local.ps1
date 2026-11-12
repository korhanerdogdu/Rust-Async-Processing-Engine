# Starts infra (Docker) and both Rust processes on the host in separate windows.
#   powershell -ExecutionPolicy Bypass -File scripts/run-local.ps1 [-MaxActiveJobs 4]
param([int]$MaxActiveJobs = 1, [int]$QueueCapacity = 64)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
if (-not (Test-Path .env)) { Copy-Item .env.example .env; Write-Host 'created .env from .env.example' }
docker compose up -d
cargo build -p worker-engine -p api-gateway -p engine-cli
$env:MAX_ACTIVE_JOBS = "$MaxActiveJobs"; $env:QUEUE_CAPACITY = "$QueueCapacity"
Start-Process powershell -ArgumentList '-NoExit', '-Command', '.\target\debug\worker-engine.exe'
Start-Sleep -Seconds 2
Start-Process powershell -ArgumentList '-NoExit', '-Command', '.\target\debug\api-gateway.exe'
Write-Host 'REST/WS: http://127.0.0.1:8080   gRPC: 127.0.0.1:50051   Jaeger: http://127.0.0.1:16686'
