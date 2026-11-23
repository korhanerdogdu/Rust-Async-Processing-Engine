# Rust Async Processing Engine

A domain-agnostic, learning-oriented Rust backend that accepts jobs over **REST, gRPC and
WebSocket**, persists them in **PostgreSQL**, executes them in a separate **worker process**
through a **bounded single-consumer MPSC queue**, and makes every step observable through
**tracing, OpenTelemetry/Jaeger and TimescaleDB**.

```text
CLIENTS ── REST :8080 ─┐
        ── gRPC :50051 ┼─► PROCESS 1 api-gateway ══ gRPC/TCP (IPC) ══► PROCESS 2 worker-engine
        ── WS   :8080 ─┘   axum + tonic + Tower                         tonic server → producer tasks
                           one status stream ◄══════════════════════    → bounded mpsc → ONE consumer
                                                                         → handlers (async / sync /
                                                                           process / Rayon)
                                     PostgreSQL + TimescaleDB ◄──────────┘
             both processes: tracing → OTLP → OTel Collector → Jaeger; logs/metrics → TimescaleDB
```

Job types: `json_format` (sync), `text_transform` (sync, Unicode-aware), `simulate_delay`
(async sleep), `html_to_pdf` (real headless Chromium render), `cpu_compute` (Rayon parallel
prime counting with sequential baseline).

Statuses: `queued → processing → completed | failed`, plus `cancelled`; retries go back to
`queued`. `POST` returns **202** only after the job is durably committed.

The gateway can also be opened to the local network so that several client PCs send jobs to
one server while their requests are told apart in the logs (`client_addr`, `client_name`) –
see [LAN multi-PC test](#lan-multi-pc-test).

## Documentation

| Doc | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | processes vs crates, ports, components, request flow |
| [docs/execution-model.md](docs/execution-model.md) | runtime/threads/tasks, async vs blocking vs Rayon, `Send`/`Sync`, measurements |
| [docs/data-flow.md](docs/data-flow.md) | ownership through channels, job lifecycle, durability gaps, WebSocket bridge |
| [docs/observability.md](docs/observability.md) | logs vs traces vs metrics, propagation, Jaeger, SQL |
| [docs/learning-map.md](docs/learning-map.md) | the 8-week topics mapped to files, tests and labs |
| [docs/test-report.md](docs/test-report.md) | what was run, results, load numbers, limitations |

## Repository layout

```text
apps/api-gateway/        PROCESS 1: REST + WebSocket (axum), public gRPC (tonic), Tower middleware, status hub
apps/worker-engine/      PROCESS 2: internal gRPC server, engine wiring, DB connect/migrate, artifact cleanup
crates/core/             transport-free model: JobCommand, JobStatus state machine, typed errors, JobStore/JobHandler traits
crates/application/      EngineHandle, single-consumer dispatcher, reconciler, status bus, in-memory test store
crates/handlers/         json, text, delay, pdf (headless Chromium), cpu (Rayon), artifact store, diagnostics + examples
crates/persistence/      PgJobStore (sqlx, guarded transitions), embedded migrations
crates/ipc/              generated protobuf/gRPC code, mappings, EngineError <-> Status, trace metadata
crates/observability/    tracing/OTel setup, DB log layer, batched Timescale writers, metrics registry
tools/engine-cli/        sample client (REST, gRPC, WebSocket, demos, load generator)
tests/e2e/               cross-process integration tests (real processes + PostgreSQL)
proto/  migrations/  deploy/  docker/  scripts/  docs/
```

## Prerequisites

* Rust stable with edition 2024 support. Verified with 1.99.1 on the host and 1.91 in the
  Docker build image (older toolchains untested). No `protoc` needed (vendored).
* Docker + Docker Compose (PostgreSQL/TimescaleDB, OTel Collector, Jaeger).
* For `html_to_pdf` on the host: Chrome, Chromium or Edge (auto-detected, or `PDF_BROWSER_PATH`).
  The worker container image ships Chromium.

## Run it

### Option A – infra in Docker, Rust processes on the host (recommended for learning)

```bash
cp .env.example .env              # PowerShell: Copy-Item .env.example .env
docker compose up -d              # TimescaleDB :55432, collector :4317/:4318, Jaeger :16686
cargo run -p worker-engine        # terminal 1 (runs migrations on start)
cargo run -p api-gateway          # terminal 2
```

Helpers: `scripts/run-local.ps1` (Windows) / `scripts/run-local.sh`.
Change concurrency: `MAX_ACTIVE_JOBS=4 cargo run -p worker-engine`.

### Option B – everything in containers

```bash
docker compose --profile apps up -d --build
```

The worker's internal port 50052 is not published; only the gateway (8080/50051) is.

### Stop

Ctrl-C each process (graceful: drain, release running jobs, flush logs and spans), then
`docker compose down` (add `-v` to delete the database volume). SIGTERM does the same on
Unix. For supervisors/tests there is also `SHUTDOWN_ON_STDIN_CLOSE=1`: closing the process's
stdin triggers the same graceful shutdown (used by the e2e tests, since Windows cannot send
Ctrl-C to a single child process).

## Ports

| Port | Service |
|---|---|
| 8080 | REST + WebSocket (gateway) |
| 50051 | public gRPC `engine.v1.JobGateway` (+ server reflection) |
| 50052 | internal gRPC `engine.v1.WorkerEngine` (worker, private) |
| 55432 | PostgreSQL/TimescaleDB (host side; container 5432) |
| 4317 / 4318 | OTel Collector OTLP gRPC / HTTP |
| 16686 | Jaeger UI |

Everything listens on `127.0.0.1` by default. In [LAN mode](#lan-multi-pc-test) only the
gateway ports (8080, 50051) are opened on `0.0.0.0`; the worker, database and Jaeger stay local.

## Try it

### REST (curl)

```bash
H=http://127.0.0.1:8080
curl -s $H/api/v1/health/ready

# submit -> 202 Accepted with job_id and links
curl -s -X POST $H/api/v1/jobs -H 'content-type: application/json' \
  -d '{"job_type":"simulate_delay","payload":{"seconds":5}}'

curl -s $H/api/v1/jobs/<id>            # poll status
curl -s $H/api/v1/jobs/<id>/result     # 409 until completed, then 200
curl -s $H/api/v1/jobs/<id>/history    # status transitions with reasons
curl -s "$H/api/v1/jobs?status=queued&limit=20"
curl -s -X POST $H/api/v1/jobs/<id>/cancel

curl -s -X POST $H/api/v1/jobs -H 'content-type: application/json' \
  -d '{"job_type":"html_to_pdf","payload":{"html":"<h1>Hello</h1>"}}'
curl -s -o out.pdf $H/api/v1/jobs/<id>/artifact

curl -s -X POST $H/api/v1/jobs -H 'content-type: application/json' -H 'idempotency-key: order-1' \
  -d '{"job_type":"cpu_compute","payload":{"from":0,"to":5000000,"compare_sequential":true}}'

curl -s $H/api/v1/engine/stats         # queue depth, active jobs, threads, tasks
curl -s $H/api/v1/metrics              # Prometheus text (gateway + worker)
```

Payloads:

| job_type | payload |
|---|---|
| `json_format` | `{"input": "<json text>", "indent": 2, "sort_keys": false}` |
| `text_transform` | `{"text": "...", "operation": "uppercase|lowercase|reverse|reverse_chars"}` |
| `simulate_delay` | `{"seconds": 5}` or `{"duration_ms": 5000}`, optional `"fail_first_attempts": 1` |
| `html_to_pdf` | `{"html": "<h1>...</h1>"}` (≤ 256 KiB, no scripts/remote resources) |
| `cpu_compute` | `{"from": 0, "to": 5000000, "chunk_size": 50000, "compare_sequential": true}` |

Errors are JSON `{"error":{"code","message","request_id","details"}}`:
400 malformed body / bad id · 422 validation · 404 not found · 409 result not ready /
idempotency conflict / invalid state · 429 queue full (`Retry-After`) · 503 worker or DB
unavailable or gateway overloaded · 504 timeout · 500 internal.

### Postman

Import [postman/rust-async-engine.postman_collection.json](postman/rust-async-engine.postman_collection.json)
(File → Import). Every "Submit" request stores the returned `job_id` in the collection variable
`{{jobId}}`, so "Job durumu", "Job sonucu", "Durum geçmişi" etc. work right after it.
WebSocket and gRPC cannot be stored in a v2.1 collection file; create them in Postman:
* **New → WebSocket**: `ws://127.0.0.1:8080/api/v1/ws/jobs/<job_id>` → Connect.
* **New → gRPC**: `127.0.0.1:50051`, "Use server reflection", method `engine.v1.JobGateway/SubmitJob`,
  message `{"job_type":"simulate_delay","payload_json":"{\"seconds\":5}"}`.

### engine-cli (REST, gRPC, WebSocket, demos, load)

```bash
cargo run -p engine-cli -- submit simulate_delay '{"seconds":2}' --wait     # REST + WebSocket
cargo run -p engine-cli -- submit text_transform '{"text":"héllo","operation":"reverse"}' --grpc
cargo run -p engine-cli -- watch <id>            # WebSocket push
cargo run -p engine-cli -- grpc-watch <id>       # public gRPC WatchJob stream
cargo run -p engine-cli -- pdf --html '<h1>Hi</h1>' --out hi.pdf
cargo run -p engine-cli -- demo-sequential --count 5 --seconds 5
cargo run -p engine-cli -- load --count 100 --concurrency 100 --wait
cargo run -p engine-cli -- stats
```

Targets default to `http://127.0.0.1:8080` / `http://127.0.0.1:50051`; override with
`--http-url` / `--grpc-url` (or `ENGINE_HTTP` / `ENGINE_GRPC`). REST requests carry an
`x-client-name` header (default: the computer name; override with `--client-name` or
`ENGINE_CLIENT_NAME`) so the gateway logs show which machine sent them. A plain curl
walkthrough is in [scripts/demo.sh](scripts/demo.sh).

> Windows PowerShell 5.1 note: `Invoke-RestMethod -Body '<string>'` does not send UTF-8, so
> non-ASCII text is rejected as `malformed_request`. Send `[Text.Encoding]::UTF8.GetBytes(...)`
> or use `engine-cli` / `curl.exe`.

### gRPC with grpcurl (reflection enabled)

```bash
grpcurl -plaintext 127.0.0.1:50051 list engine.v1.JobGateway
grpcurl -plaintext -d '{"job_type":"text_transform","payload_json":"{\"text\":\"hi\",\"operation\":\"uppercase\"}"}' \
  127.0.0.1:50051 engine.v1.JobGateway/SubmitJob
grpcurl -plaintext -d '{"job_id":"<id>"}' 127.0.0.1:50051 engine.v1.JobGateway/WatchJob
```

### WebSocket

`ws://127.0.0.1:8080/api/v1/ws/jobs/<id>` – receives `{"type":"snapshot",...}` immediately,
then `{"type":"status","event":{...}}` per transition (originating in the worker process),
and is closed after a terminal status. Reconnecting yields a fresh snapshot.
(e.g. `websocat ws://127.0.0.1:8080/api/v1/ws/jobs/<id>`)

### The end-to-end demo

1. Five clients submit `simulate_delay(seconds=5)` at once:
   `cargo run -p engine-cli -- demo-sequential --count 5 --seconds 5`.
   With `MAX_ACTIVE_JOBS=1` exactly one is `processing`, the others `queued`; each finishes
   ~5 s after the previous one; transitions arrive over WebSocket.
2. Restart the worker with `MAX_ACTIVE_JOBS=4` and repeat: four run at once.
3. `cargo run -p engine-cli -- submit cpu_compute '{"from":0,"to":20000000,"compare_sequential":true}' --wait`
   – look at `distinct_rayon_threads_used` and `measured_speedup`; `GET /api/v1/health/live`
   stays instant meanwhile.
4. `cargo run -p engine-cli -- pdf --html '<h1>Report</h1>' --out report.pdf` and open the file.
5. Open Jaeger (http://127.0.0.1:16686), pick a trace: spans from `api-gateway` and
   `worker-engine`, including the later `job.execute`.
6. Query TimescaleDB (below).

## LAN multi-PC test

One PC runs the engine; 3–4 other PCs on the same network send requests to it, and the
server's logs show who sent what.

```text
client PCs (same Wi-Fi / hotspot)          server PC
  pc-1  lan-client.ps1 ─┐            ┌──────────────────────────────────────────┐
  pc-2  lan-client.ps1 ─┼── LAN ───► │ api-gateway   0.0.0.0:8080 / :50051      │
  pc-3  Postman ────────┘            │ worker-engine 127.0.0.1:50052 (private)  │
                                     │ TimescaleDB, Jaeger (local only)         │
                                     └──────────────────────────────────────────┘
```

**Server PC** – [scripts/lan-server.ps1](scripts/lan-server.ps1) sets the bind addresses as
environment variables for that process only, so `.env` stays on `127.0.0.1`:

```powershell
.\scripts\lan-server.ps1 -Role firewall          # once, as Administrator: allow inbound TCP 8080/50051
docker compose up -d
.\scripts\lan-server.ps1 -Role worker            # terminal 1 (optional: -MaxActiveJobs 4 -QueueCapacity 100)
.\scripts\lan-server.ps1 -Role gateway           # terminal 2: prints the LAN IP to give to clients
.\scripts\lan-server.ps1 -Role info              # LAN IPs + firewall state
.\scripts\lan-server.ps1 -Role firewall-remove   # afterwards, as Administrator
```

**Client PCs** – copy the single file [scripts/lan-client.ps1](scripts/lan-client.ps1); it needs
nothing but Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\lan-client.ps1 -Server <server-ip> -Name pc-1
powershell -ExecutionPolicy Bypass -File .\lan-client.ps1 -Server <server-ip> -Name pc-2 -Scenario slow -Count 3 -DelaySeconds 5
powershell -ExecutionPolicy Bypass -File .\lan-client.ps1 -Server <server-ip> -Name pc-3 -Scenario burst -Count 150
```

| Scenario | Sends | Shows |
|---|---|---|
| `mixed` (default) | one request every `-IntervalMs`, rotating `simulate_delay`, `text_transform`, `json_format`, `cpu_compute` and one deliberately invalid request | all job types run; invalid input gets 422 |
| `slow` | `simulate_delay` jobs of `-DelaySeconds` each | with `MAX_ACTIVE_JOBS=1` jobs run one after another, the rest wait `queued` |
| `burst` | `-Count` short jobs at the same moment | the bounded queue fills, the excess gets 429, accepted jobs still complete |

The script checks `health/ready`, prints every response, a summary per HTTP status, and then
follows accepted jobs until they are terminal. Postman works too; add the header
`x-client-name: <name>` (Metadata for gRPC).

**What is logged** – every gateway request span (REST, WebSocket session, public gRPC) carries:

* `client_addr` – the caller's TCP address, taken from the connection;
* `client_name` – the value of the optional `x-client-name` header (`-` if absent).

Both appear on the gateway console and in `operational_logs.attributes`, so requests can be
grouped per machine:

```sql
SELECT attributes->>'client_name' AS pc,
       split_part(attributes->>'client_addr', ':', 1) AS ip,
       attributes->>'status' AS http_status, count(*)
FROM operational_logs
WHERE event = 'http.response' AND attributes ? 'client_addr'
GROUP BY 1, 2, 3 ORDER BY 1, 3;
```

The worker does not receive the client identity; join on `job_id` (e.g. the gateway's
`http.job_accepted` event) to attribute worker events such as `job.processing` /
`job.completed` to a client.

## SQL

```bash
docker compose exec timescaledb psql -U engine -d engine
```

```sql
SELECT status, count(*) FROM jobs GROUP BY 1;
SELECT * FROM job_status_history WHERE job_id = '<id>' ORDER BY id;
SELECT time, service, thread_name, event, message, trace_id
  FROM operational_logs WHERE job_id = '<id>' ORDER BY time;
SELECT time_bucket('10 seconds', time), max(value)
  FROM operational_metrics WHERE metric = 'engine_queue_depth' GROUP BY 1 ORDER BY 1 DESC;
```

More in [scripts/queries.sql](scripts/queries.sql). Migrations live in [migrations/](migrations/)
and are applied by the worker at startup (`MIGRATE_ON_START=true`, embedded with `sqlx::migrate!`).

## Configuration

All variables with defaults and comments: [.env.example](.env.example). The important ones:

| Variable | Default | Effect |
|---|---|---|
| `MAX_ACTIVE_JOBS` | 1 | concurrent jobs (semaphore permits); still exactly one queue receiver |
| `QUEUE_CAPACITY` | 64 | bounded MPSC capacity; full → 429 |
| `TOKIO_WORKER_THREADS` | logical CPUs | runtime worker threads per process |
| `RAYON_THREADS` / `CPU_MAX_CONCURRENT_JOBS` | logical CPUs / 1 | CPU pool size / admission |
| `MAX_ATTEMPTS`, `RETRY_BASE_MS`, `RETRY_MAX_MS` | 3, 1000, 30000 | bounded retries with exponential backoff |
| `JOB_TIMEOUT_*_MS`, `PDF_TIMEOUT_MS` | see file | per-type deadlines |
| `DB_MAX_CONNECTIONS` | 20 | worker pool; bounds DB concurrency |
| `MAX_INFLIGHT_REQUESTS` | 1024 | gateway Tower concurrency limit (excess → 503) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP/HTTP base URL for span export |
| `METRICS_TOKEN` | unset | bearer token for `/api/v1/metrics` |

## Tests

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                      # 74 unit/component tests, no Docker needed

docker compose up -d && cargo build --workspace
cargo test -p e2e-tests -- --ignored --test-threads=4   # 19 cross-process tests
# or: scripts/test-e2e.ps1  /  scripts/test-e2e.sh  (also enables the Jaeger assertion)
```

Load experiments: `scripts/load-test.ps1` / `scripts/load-test.sh`; Rust experiments:
`cargo run --release -p engine-handlers --example blocking_vs_async` and
`--example cpu_parallel_vs_sequential`. Results: [docs/test-report.md](docs/test-report.md).

## Security and development caveats

* **Local demo only.** No authentication; everything binds to 127.0.0.1 by default. Do not
  expose the gateway, and never the worker's internal port, to the public internet.
* LAN mode (`lan-server.ps1 -Role gateway`) opens the unauthenticated gateway to everyone on
  the local network. Use it only on a trusted network for the duration of a test, and remove
  the firewall rule afterwards. `x-client-name` is self-reported by the client and only a
  label, not an identity.
* HTML→PDF: the document gets a CSP `<meta>` (`default-src 'none'`: no scripts, no remote or
  `file://` subresources, inline styles and `data:` images only), all browser traffic goes to
  a dead proxy (`127.0.0.1:9`, loopback included), each render uses a throw-away profile, and
  the child is killed on timeout. The Chromium sandbox stays **on** for host runs. The
  Compose worker sets `PDF_NO_SANDBOX=true` because unprivileged containers lack the user
  namespaces the sandbox needs – acceptable only for this local container, with the CSP and
  proxy isolation still in place.
* The dev database password in `.env.example`/`docker-compose.yml` is a placeholder for local
  use; override via environment variables.
* Job payloads (which may contain user HTML/text) are stored in `jobs.payload` because
  recovery needs them; they are never written to logs (redaction + test). Operational tables
  have a 14-day retention policy; there is no automatic cleanup of `jobs` or artifacts.
* Errors returned to clients are sanitized (`internal` errors carry no details).

## Known limitations

* Execution is **at-least-once** (a job can run twice if a worker dies after doing the work
  but before committing). Not exactly-once.
* Single worker instance assumed for fast restart recovery (`WORKER_ID`); multiple workers
  with distinct ids work but only recover each other's jobs after lease expiry.
* Leases are `timeout + grace` instead of heartbeats.
* Under a very large submission burst PostgreSQL commit latency is the bottleneck; excess
  submissions get 503 after the pool's acquire timeout (see test report).
* No durable event replay for WebSocket clients (by design: snapshot on (re)connect).
  NATS/Kafka/event streaming are intentionally **not** part of v1.
* Verified on Windows (host) and Linux (containers) only.
