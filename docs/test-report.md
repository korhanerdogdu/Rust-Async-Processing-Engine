# Test report

All results below were produced on the development machine on 2026-11-12:
Windows 11 Pro, 10 cores / 16 logical CPUs, Rust 1.99.1, Docker Desktop 28.3 (WSL2 backend),
TimescaleDB 2.17.2 on PostgreSQL 16 in Docker. Nothing here is a guaranteed number – rerun
on your machine.

## Quality gates

| Command | Result |
|---|---|
| `cargo build --workspace` | ✅ no warnings |
| `cargo fmt --all --check` | ✅ |
| `cargo clippy --workspace --all-targets -- -D warnings` | ✅ |
| `cargo test --workspace` (Docker-free) | ✅ 74 passed, 19 ignored (the e2e tests) |
| `cargo test -p e2e-tests -- --ignored --test-threads=4` (with Compose infra, OTLP + Jaeger checks on) | ✅ 19 passed in ~30 s |
| `docker compose --profile apps build` + `up` | ✅ both images build; containerized REST, WebSocket, sequential demo and Chromium PDF verified |

## What the tests cover

### Unit / component tests (no Docker)

| Area | Where | Highlights |
|---|---|---|
| State machine, ids, errors, validation | `crates/core` (15) | allowed/forbidden transitions, error codes, sanitized internal errors, payload bounds, `Send`/`Sync` compile checks |
| Handlers | `crates/handlers` (20 + 4) | JSON formatting/sort/malformed, Unicode upper/lower/grapheme reverse, paused-time delay + cancel, Rayon prime count = sequential, ≥ 2 threads used, cancellation, artifact path traversal, atomic finalize, **real Chromium PDF parsed with lopdf and text extracted**, CSP blocks `<script>` |
| Blocking vs async | `crates/handlers/tests/blocking_vs_async.rs` | blocking sleep stalls a heartbeat ≥ 200 ms; async < 100 ms |
| Channels | `crates/application/tests/channels.rs` (6) | mpsc multi-producer/single-consumer, full-queue policy, reserve-then-send, oneshot reply/drop, broadcast `Lagged` + no replay, watch latest-value |
| Dispatcher | `crates/application/tests/dispatcher.rs` (12) | exactly one processing with `MAX_ACTIVE_JOBS=1`, parallel peak = 4, 40 producers, queue-full 429 without persistence, bounded retries + history, invalid input not retried, timeouts, cancel queued/processing, restart recovery, expired last attempt → failed, DB outage, idempotency, bounded graceful shutdown |
| Metrics / log layer | `crates/observability` (7) | percentiles, bounded window, Prometheus text, field capture + span correlation + redaction, drop-on-full counter, foreign targets ignored |
| IPC mapping | `crates/ipc` (5) | every `EngineError` round-trips through `tonic::Status`, transport errors → unavailable |
| HTTP mapping | `apps/api-gateway` (2) | status code table, `Retry-After` |

### Cross-process e2e tests (`tests/e2e`, real processes + PostgreSQL)

Each test creates its own database and starts real `worker-engine` and `api-gateway` processes.

| Test | Verifies |
|---|---|
| `rest_delay_job_lifecycle_is_persisted` | 202 + Location + links, 409 while processing, 200 after, DB row + history `queued→processing→completed` |
| `single_active_job_shows_queueing` | 4 concurrent submits → 1 processing / 3 queued; no overlapping execution intervals (SQL) |
| `multiple_active_jobs_run_in_parallel` | `MAX_ACTIVE_JOBS=4`: 4×1.5 s finish in < 4 s, all pairs overlap |
| `rest_and_public_grpc_are_equivalent` | identical results over REST and gRPC; `INVALID_ARGUMENT`/`NOT_FOUND` vs 422/404 |
| `websocket_pushes_transitions_from_worker_and_reconnect_gets_truth` | snapshot first, transitions from the worker process, strictly increasing versions, reconnect = snapshot |
| `grpc_watch_job_streams_snapshot_and_events` | public gRPC server stream |
| `html_to_pdf_produces_a_real_pdf` | download via gateway (streamed from worker over gRPC), `%PDF`, lopdf parse, text extracted, only metadata in DB |
| `cpu_compute_uses_rayon_and_is_deterministic` | π(10⁶) = 78 498, ≥ 2 Rayon threads |
| `timescale_hypertables_and_operational_logs_are_queryable` | both hypertables, log rows from **both** services for the job, no payload leakage, metric snapshots |
| `trace_context_survives_gateway_to_worker_ipc` | client `traceparent` → job `trace_id` in the worker's DB row and in both services' log rows; trace with `job.execute` found in **Jaeger** |
| `error_responses_are_consistent` | 400/404/422/409, failed job error code, idempotency replay + conflict |
| `full_queue_returns_429_and_persists_nothing` | `QUEUE_CAPACITY=1`: 429 + Retry-After, DB count unchanged |
| `worker_down_gives_503_and_gateway_recovers_when_it_starts` | 503 + not ready; lazy reconnect once the worker appears |
| `database_down_gives_503_not_ready` | worker keeps retrying the DB; readiness shows `starting`; submit → 503 |
| `crashed_worker_jobs_are_recovered_after_restart` | hard kill mid-job → restart → `lease_recovered`, attempts = 2, queued jobs also complete |
| `graceful_shutdown_releases_running_job_and_restart_completes_it` | stdin-close shutdown: bounded exit 0, job released to `queued` with attempt not counted, completes after restart |
| `timeouts_are_retried_a_bounded_number_of_times` | timeout → retried → failed after `MAX_ATTEMPTS`; transient failure succeeds on attempt 2 |
| `cancelling_a_processing_job_stops_it` | cancel → `cancelled`, slot frees immediately |
| `concurrent_load_smoke` | 300 submissions (100 in flight) all accepted and completed |

## Demonstrations

**Sequential single-active-job demo** (`engine-cli demo-sequential --count 3 --seconds 2`, debug build):

```text
t=  0.04s  job#1 6bf5ad6a  snapshot -> queued
t=  0.04s  job#2 d9dede07  snapshot -> queued
t=  0.04s  job#0 67858834  snapshot -> queued
t=  0.05s  job#0 67858834  status   -> processing
t=  2.08s  job#0 67858834  status   -> completed
t=  2.09s  job#2 d9dede07  status   -> processing
t=  4.10s  job#2 d9dede07  status   -> completed
t=  4.10s  job#1 6bf5ad6a  status   -> processing
t=  6.12s  job#1 6bf5ad6a  status   -> completed
```

**Rayon CPU job over public gRPC** (`count_primes 0..2·10⁶`, debug build): `prime_count=148933`,
`distinct_rayon_threads_used=15` of 16, `parallel_ms=21`, `sequential_ms=154`, measured speedup 7.3×.

Standalone benchmarks (release): see `docs/execution-model.md` (blocking vs async sleep;
Rayon 1/2/4/8/16 threads: ×0.97 / ×2.02 / ×4.07 / ×6.02 / ×7.32).

## Load experiments (release build, host processes, `engine-cli load ... --wait`)

`simulate_delay(100 ms)`, all submissions fired at once (`--concurrency = --count`).

| Config | Submissions | HTTP result | Completion throughput | queue wait p50 / p95 | processing p50 / p99 |
|---|---|---|---|---|---|
| `MAX_ACTIVE_JOBS=1`, `QUEUE_CAPACITY=64` | 100 | **202: 64, 429: 36** | 8.0 jobs/s | 3.9 s / 7.4 s | 113 / 127 ms |
| `MAX_ACTIVE_JOBS=8`, queue 2048, pool 10 | 10 | 202: 10 | 25.9 jobs/s | 31 / 149 ms | 120 / 134 ms |
| same | 100 | 202: 100 | 57.1 jobs/s | 0.70 s / 1.27 s | 114 / 144 ms |
| same | 1000 | **202: 382, 503: 618** | 23.8 jobs/s | 10.4 s / 11.9 s | 118 ms / 4.45 s |
| `MAX_ACTIVE_JOBS=8`, queue 2048, **pool 40** | 1000 | **202: 1000** | 58.2 jobs/s | 8.6 s / 14.8 s | 113 / 410 ms |
| same, `cpu_compute 0..200k` | 200 (100 in flight) | 202: 200 | 130 jobs/s | 306 / 495 ms | 12.5 / 75 ms |

Interpretation:

* With one slot, throughput ≈ 1 / (100 ms + ~25 ms overhead) and the bounded queue admits
  exactly 64 jobs; the rest are **rejected explicitly** (429 + `Retry-After`), never silently
  dropped. With 8 slots throughput is ≈ 8× (57–58 jobs/s).
* At 1000 simultaneous submissions the bottleneck is **PostgreSQL commit latency** (sessions
  wait in `WALWrite`/`WALSync`; each job needs ~3 commits). With a 10-connection pool,
  submissions that cannot get a connection within `acquire_timeout` (3 s) are answered with
  **503** (`database: pool timed out`) – again explicit, and every accepted job completed. A
  40-connection pool absorbs the burst. The default is now 20 (`DB_MAX_CONNECTIONS`).
* Processing p99 rising under DB contention is the completion write waiting for a connection
  (and its retry); the sleep itself stays ~100 ms.

### Environment issue observed during testing

While a `docker compose --profile apps build` (release compile inside the Docker VM) ran in
parallel, and later when the Docker Desktop engine crashed (API returned HTTP 500; C: had
≈ 19 GB free), load runs showed multi-second DB latencies, timeouts and 3 jobs left
non-terminal. After restarting Docker Desktop the same experiments ran cleanly (table above).
Those degraded runs are not included in the table because they measured the broken
environment, not the engine.

## Not verified / limitations

* No Linux/macOS host run (Windows only). The Linux code path is exercised inside the
  containers (Chromium PDF, SIGTERM handling is compiled but signals were not sent manually).
* Graceful shutdown in the e2e test uses the stdin-close trigger (`SHUTDOWN_ON_STDIN_CLOSE=1`)
  because Windows cannot send Ctrl-C to a single child process. The same `CancellationToken`
  path is used by Ctrl-C/SIGTERM.
* No CPU/memory profiles were collected for the load runs (only latency/throughput/error counts).
* `k6`/`hey`/`wrk` were not used; the Rust `engine-cli load` client was.
