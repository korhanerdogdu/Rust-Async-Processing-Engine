#!/usr/bin/env sh
# curl walkthrough against a running gateway (scripts/run-local.sh).
set -eu
H=${ENGINE_HTTP:-http://127.0.0.1:8080}

echo "== readiness"; curl -s "$H/api/v1/health/ready"; echo
echo "== submit simulate_delay (202 Accepted)"
ID=$(curl -s -X POST "$H/api/v1/jobs" -H 'content-type: application/json' \
  -d '{"job_type":"simulate_delay","payload":{"seconds":2}}' | sed -E 's/.*"job_id":"([^"]+)".*/\1/')
echo "job $ID"
echo "== result while running (409)"; curl -s -w ' [HTTP %{http_code}]\n' "$H/api/v1/jobs/$ID/result"
sleep 3
echo "== result (200)"; curl -s "$H/api/v1/jobs/$ID/result"; echo
echo "== history"; curl -s "$H/api/v1/jobs/$ID/history"; echo
echo "== validation error (422)"
curl -s -w ' [HTTP %{http_code}]\n' -X POST "$H/api/v1/jobs" -H 'content-type: application/json' \
  -d '{"job_type":"simulate_delay","payload":{"seconds":999}}'
echo "== grpc (needs grpcurl; reflection is enabled)"
command -v grpcurl >/dev/null && grpcurl -plaintext -d '{"job_type":"text_transform","payload_json":"{\"text\":\"hi\",\"operation\":\"uppercase\"}"}' \
  127.0.0.1:50051 engine.v1.JobGateway/SubmitJob || echo "grpcurl not installed; use: cargo run -p engine-cli -- submit ... --grpc"
