#!/usr/bin/env sh
# Load experiments with engine-cli against a running system (10 / 100 / 1000 concurrent).
#   JOB_TYPE=cpu_compute PAYLOAD='{"from":0,"to":200000}' scripts/load-test.sh
set -eu
cd "$(dirname "$0")/.."
JOB_TYPE=${JOB_TYPE:-simulate_delay}
PAYLOAD=${PAYLOAD:-'{"duration_ms":100}'}
for n in 10 100 1000; do
  echo; echo "===== $n concurrent submissions of $JOB_TYPE ====="
  ./target/debug/engine-cli load --count "$n" --concurrency "$n" --job-type "$JOB_TYPE" --payload "$PAYLOAD" --wait --wait-timeout-secs 600
done
