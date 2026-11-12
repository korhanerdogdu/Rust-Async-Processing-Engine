#!/usr/bin/env sh
# Starts infra and both Rust processes on the host (logs in ./data/*.log).
#   MAX_ACTIVE_JOBS=4 scripts/run-local.sh      # stop with: kill $(cat data/*.pid)
set -eu
cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env
mkdir -p data
docker compose up -d
cargo build -p worker-engine -p api-gateway -p engine-cli
./target/debug/worker-engine > data/worker.log 2>&1 & echo $! > data/worker.pid
sleep 2
./target/debug/api-gateway > data/gateway.log 2>&1 & echo $! > data/gateway.pid
echo "REST/WS: http://127.0.0.1:8080   gRPC: 127.0.0.1:50051   Jaeger: http://127.0.0.1:16686"
