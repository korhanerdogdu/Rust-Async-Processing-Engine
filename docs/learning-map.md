# Learning map

The earlier 8-week plan (originally built around an FX domain) mapped to **real code in this
project**. Read the files in this order; every row ends with something you can run.

## Week 1 – Ownership, borrowing, crates and modules

| Concept | Where | Try |
|---|---|---|
| Owned vs borrowed data | `JobCommand` owns `String`s (`crates/core/src/job.rs`); `text::transform(&TextTransformParams)` only borrows | change `TextTransformParams.text` to `&str` and see why the queue can't hold it (`'static`) |
| Moves | `permit.send(JobEnvelope { command, .. })` in `crates/application/src/engine.rs` | use `command` after the send – compile error |
| `Copy` newtypes | `JobId` (`crates/core/src/ids.rs`) | |
| Workspace / crates / modules | root `Cargo.toml`, `docs/architecture.md` | `cargo tree -p worker-engine --depth 1` |
| Library vs binary crate | `crates/*` vs `apps/*` | |

## Week 2 – Traits, generics, errors

| Concept | Where |
|---|---|
| Trait objects (`dyn`) + boxed futures | `JobHandler` (`crates/core/src/handler.rs`), `HandlerRegistry::execute` |
| Generics + `-> impl Future + Send` in traits (static dispatch) | `JobStore` (`crates/core/src/store.rs`), `EngineHandle<S: JobStore>` |
| `async fn` implementing an RPITIT trait | `impl JobStore for InMemoryJobStore` (`crates/application/src/memory.rs`) |
| Typed errors with `thiserror` | `EngineError`, `HandlerError`, `StoreError` |
| Mapping at the boundary only | `rest.rs::http_status`, `ipc/src/status.rs` |
| `From` conversions for `?` | `impl From<StoreError> for EngineError` |

Lab: add a `TextOperation::TitleCase` – compiler errors show every `match` you must update.

## Week 3 – Tokio: runtime, tasks, async/await

| Concept | Where |
|---|---|
| Explicit multi-thread runtime, named threads | `main()` in both `apps/*/src/main.rs` |
| `tokio::spawn` = task, not thread | dispatcher `running.spawn(...)`, `JoinSet` |
| `select!` with `biased` | consumer loop, delay handler, PDF handler |
| Timeouts | `tokio::time::timeout` in `dispatcher.rs`, PDF child wait |
| Paused time in tests | `#[tokio::test(start_paused = true)]` in `handlers/src/delay.rs` |
| Thread hopping across `.await` | `simulate_delay` result `thread_before_await` / `thread_after_await` |

Lab: `cargo run --release -p engine-handlers --example blocking_vs_async`.

## Week 4 – Channels and shared state

| Channel | Where | Test |
|---|---|---|
| `mpsc` bounded, many producers, one consumer | `engine.rs` (producers), `dispatcher.rs::run_consumer` (consumer) | `tests/channels.rs::mpsc_*`, `tests/dispatcher.rs::many_producers_*` |
| `oneshot` | Rayon → async hand-off in `handlers/src/cpu.rs` | `channels.rs::oneshot_request_reply` |
| `broadcast` | `application/src/bus.rs`, `api-gateway/src/hub.rs` | `channels.rs::broadcast_lag_and_reconnect_semantics` |
| `watch` | `EngineState` (engine.rs), `UpstreamState` (hub.rs) | `channels.rs::watch_keeps_only_the_latest_state` |
| `Semaphore` permits | `MAX_ACTIVE_JOBS`, CPU/PDF admission, WebSocket slots | `dispatcher.rs::single_active_job_*` |
| `Arc` + `std::sync::Mutex` (never across `.await`) | `queue.rs::InflightSet`, `metrics.rs` | |
| RAII cleanup | `InflightGuard` (`queue.rs`) | |

Lab: set `MAX_ACTIVE_JOBS=4` and rerun `engine-cli demo-sequential --count 8 --seconds 2`.

## Week 5 – IPC, REST, gRPC, WebSocket

| Concept | Where |
|---|---|
| Protobuf contract | `proto/engine.proto`, `crates/ipc/build.rs` |
| Internal gRPC server (IPC) | `apps/worker-engine/src/grpc_server.rs` |
| gRPC client with deadlines + metadata | `apps/api-gateway/src/service.rs`, `crates/ipc/src/metadata.rs` |
| Server streaming | `SubscribeStatus`, `DownloadArtifact`, `WatchJob` |
| REST with axum, DTOs, 202 + links | `apps/api-gateway/src/rest.rs` |
| WebSocket push | `apps/api-gateway/src/websocket.rs`, `hub.rs::watch_job` |
| One service behind two transports | `GatewayService` used by `rest.rs` and `grpc.rs` |

Lab: `grpcurl -plaintext 127.0.0.1:50051 list` (reflection) and compare with `curl`.

## Week 6 – Async vs sync vs blocking vs CPU parallel

| Kind | Handler | Note |
|---|---|---|
| short sync | `json.rs`, `text.rs` | inline |
| async wait | `delay.rs` | `tokio::time::sleep` |
| external process + blocking I/O | `pdf.rs` | `tokio::process`, kill + reap, `spawn_blocking` |
| CPU parallel | `cpu.rs` | Rayon pool, permit moved into closure, cooperative cancel |

Read `docs/execution-model.md`. Lab:
`cargo run --release -p engine-handlers --example cpu_parallel_vs_sequential -- 20000000 1 2 4 8 16`.

## Week 7 – Reliability: durability, retries, cancellation, shutdown

| Concept | Where | Test |
|---|---|---|
| Reserve → persist → publish | `EngineHandle::submit` | `dispatcher.rs::full_queue_rejects_without_persisting` |
| Atomic, guarded transitions | `crates/persistence/src/lib.rs`, trigger in `migrations/..._jobs.sql` | e2e `single_active_job_shows_queueing` |
| Leases + crash recovery | `reconciler.rs`, `recover_stale_jobs` | e2e `crashed_worker_jobs_are_recovered_after_restart` |
| Bounded retries with backoff | `dispatcher.rs::persist_failure`, `EngineConfig::retry_backoff` | e2e `timeouts_are_retried_*` |
| Cancellation tokens | `JobContext.cancel`, `RunningJobs` | e2e `cancelling_a_processing_job_stops_it` |
| Graceful, bounded shutdown | `EngineHandle::shutdown` | e2e `graceful_shutdown_*` |
| Idempotency keys | `insert_job` `ON CONFLICT`, `Idempotency-Key` header | e2e `error_responses_are_consistent` |

Lab: start a 30 s delay job, `taskkill /F` the worker (or `kill -9`), restart it, watch
`/api/v1/jobs/{id}/history` show `lease_recovered`.

## Week 8 – Tower, observability, testing, load

| Concept | Where |
|---|---|
| `Service` / `poll_ready` / `call` by hand | `middleware.rs::RequestMetrics` |
| Backpressure via readiness | `ConcurrencyLimit` + `LoadShed` + `HandleErrorLayer` |
| HTTP readiness ≠ queue admission | module docs of `middleware.rs` |
| Tracing spans + OTel + propagation | `crates/observability/src/{telemetry,propagation}.rs` |
| Non-blocking log persistence | `db_layer.rs` + `sink.rs` |
| Metrics with real percentiles | `metrics.rs` |
| Process-level tests | `tests/e2e` |
| Load | `engine-cli load`, `scripts/load-test.*`, `docs/test-report.md` |

Lab: set `MAX_INFLIGHT_REQUESTS=2` on the gateway and run `engine-cli load --count 200
--concurrency 200 --job-type text_transform --payload '{"text":"a","operation":"uppercase"}'`
– you will see `503 gateway_overloaded` produced by `LoadShed`, *before* any worker queue
admission happens.

## Short labs for later

1. Separate DB pools for submission and job execution so a submission burst cannot slow
   completions (see the pool-contention row in `docs/test-report.md`).
2. Add a `heartbeat` that extends leases for very long jobs instead of `timeout + grace`.
3. Replace the polling reconciler with PostgreSQL `LISTEN/NOTIFY` as a wake-up hint (keep the
   scan as safety net).
4. Make `MAX_ACTIVE_JOBS` changeable at runtime via a `watch` channel.
