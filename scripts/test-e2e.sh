#!/usr/bin/env sh
# Runs the full verification: infra up, build, unit tests, cross-process e2e tests.
set -eu
cd "$(dirname "$0")/.."

docker compose up -d
echo "waiting for TimescaleDB..."
i=0
until docker compose exec -T timescaledb pg_isready -U engine -d engine >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 60 ] && { echo "database not ready"; exit 1; }; sleep 1
done

cargo build --workspace
cargo test --workspace
E2E_OTLP_ENDPOINT=http://127.0.0.1:4318 E2E_JAEGER_URL=http://127.0.0.1:16686 \
  cargo test -p e2e-tests -- --ignored --test-threads=4
