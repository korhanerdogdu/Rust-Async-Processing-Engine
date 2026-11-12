# Execution model: runtime, threads, tasks, async, blocking, Rayon

## The vocabulary

| Thing | What it is | In this project |
|---|---|---|
| **CPU core / logical CPU** | Hardware. `std::thread::available_parallelism()` reports logical CPUs visible to the process (cores × SMT, minus affinity/cgroup limits). | dev machine: 10 cores / 16 logical |
| **OS thread** | Scheduled by the operating system; has its own stack. Expensive-ish (≈ MBs, µs to create). | Tokio workers `worker-tokio-N`/`gateway-tokio-N`, Rayon `rayon-cpu-N`, Tokio blocking pool, `stdin-watch` |
| **Tokio runtime** | A scheduler + I/O driver + timer driver running on a pool of worker OS threads (`TOKIO_WORKER_THREADS`). | one per process, built explicitly in each `main.rs` |
| **Task** (`tokio::spawn`) | A future the runtime polls to completion. Cheap (≈ hundreds of bytes). Thousands per thread are normal. **Task ≠ thread.** | every RPC/HTTP request, the dispatcher, every job, the reconciler, log writers, WebSocket connections |
| **Future** | A state machine produced by `async fn`. Does nothing until polled. | everything `async` |

The `GET /api/v1/engine/stats` endpoint shows *measured* `tokio_worker_threads` (from
`Handle::metrics().num_workers()`) and `tokio_alive_tasks` next to the configured values –
with 16 threads you will typically see more tasks than threads, and that is the point.

## `async`/`.await` and polling

```text
async fn handle() { let row = db.query().await; ... }
            │
            ▼ compiled into a state machine (a Future)
  poll() ──► query not ready → registers a waker, returns Pending ──► thread runs OTHER tasks
              ...socket becomes readable → waker fires → task rescheduled
  poll() ──► Ready(row) → continues to the next .await
```

* `.await` is a **yield point**: the only place a task can be paused and another task run.
* Between `.await`s a task runs **uninterrupted** on its current thread. Code without
  `.await` that runs for a long time therefore blocks that runtime thread.
* After an `.await` the task may resume on a **different** worker thread (work stealing).
  `simulate_delay` reports `thread_before_await` and `thread_after_await` – run a few and you
  will see them differ. That is why futures that cross `.await` must be `Send`.

## Async is waiting, not speed

Async lets *one* thread wait for *many* things (sockets, timers, DB rows). It does not make
CPU work faster. A CPU loop inside an `async fn` occupies the worker thread just like a
blocking call. See the experiment below.

## The four kinds of work (and where each lives)

| Work | Example here | Mechanism | Why |
|---|---|---|---|
| Short synchronous | validation (`JobCommand::parse`), JSON pretty-print, text transform | plain `fn` called inline in the async task | microseconds; spawning would cost more than it saves |
| Async waiting | gRPC, SQLx queries, channels, `simulate_delay` | `async fn` + `.await`, `tokio::time::sleep` | the thread is free while waiting |
| Blocking / external process | HTML→PDF | `tokio::process::Command` (async wait on the child), `spawn_blocking` for hashing/parsing/fsync/rename | the browser is a separate process; std::fs work is bounded and runs on Tokio's **blocking pool** |
| CPU-parallel | prime counting | dedicated **Rayon** pool (`RAYON_THREADS`), admission semaphore, result via `oneshot` | real parallelism across cores without touching Tokio workers |

### `spawn_blocking` vs Rayon

* `spawn_blocking` moves a closure to Tokio's blocking thread pool (up to 512 threads by
  default). Good for **blocking I/O** or short sync calls you cannot make async. Not for big
  CPU jobs: it does not split work, and many concurrent calls create many threads.
* Rayon is a **work-stealing pool sized to the CPUs** that splits data (`par_iter`) across
  threads. Good for **CPU-bound, divisible** work. Never call Rayon's blocking `install`
  from an async task; this project uses `pool.spawn` + `oneshot`, so the Tokio task just
  awaits a channel.

### Cancellation is not preemption

* Dropping a future (timeout, client gone) stops it at its **next `.await`**.
* `spawn_blocking` closures and Rayon jobs **cannot be cancelled** by dropping anything: they
  run to completion. Therefore:
  * the CPU handler moves its admission permit **into** the Rayon closure (slot stays taken
    until the CPU really stops) and checks a `CancellationToken` before every chunk;
  * the PDF handler explicitly `start_kill()`s and `wait()`s the browser on timeout/cancel,
    with `kill_on_drop(true)` as a backstop.

## Measured: blocking sleep vs async sleep

`cargo run --release -p engine-handlers --example blocking_vs_async` (2 runtime workers,
4 tasks sleeping 500 ms, a heartbeat task ticking every 10 ms). Result on the dev machine:

```text
Async     heartbeat ticks=35   max lag=    6.6ms mean lag=    5.6ms load finished after   547.7ms
Blocking  heartbeat ticks=2    max lag=  991.3ms mean lag=  497.9ms load finished after      1.0s
```

With `std::thread::sleep` both worker threads are parked: the heartbeat (stand-in for
"every other request") stalls for ~1 s and the four sleeps are serialized into two rounds.
The same comparison is an automated test: `crates/handlers/tests/blocking_vs_async.rs`.

## Measured: Rayon parallelism

`cargo run --release -p engine-handlers --example cpu_parallel_vs_sequential -- 20000000 1 2 4 8 16`:

```text
sequential           : 1270607 primes in    2228.9 ms
rayon threads =   1  : 1270607 primes in    2287.0 ms  speedup x0.97
rayon threads =   2  : 1270607 primes in    1105.9 ms  speedup x2.02
rayon threads =   4  : 1270607 primes in     548.3 ms  speedup x4.07
rayon threads =   8  : 1270607 primes in     370.1 ms  speedup x6.02
rayon threads =  16  : 1270607 primes in     304.7 ms  speedup x7.32
```

Near-linear up to 4, then diminishing: the machine has 10 physical cores of mixed types and
16 logical CPUs; SMT siblings share execution units. Numbers are *your* machine's – rerun.

## Observing thread identity

* Every log line has `ThreadId(n)` and the thread name (`worker-tokio-3`, `rayon-cpu-2`, …).
* `operational_logs.thread_id` / `thread_name` store the same, queryable by SQL.
* `cpu_compute` results contain `distinct_rayon_threads_used` and `rayon_thread_indices`.
* Do **not** infer the thread *count* from log thread ids – use the runtime metrics.

## `Send` and `Sync`

* `T: Send` – a value of `T` may be **moved** to another thread. Needed for anything passed to
  `tokio::spawn`, `spawn_blocking`, `rayon::spawn`, or sent through a channel.
* `T: Sync` – `&T` may be **shared** between threads (equivalently `&T: Send`). Needed for
  things inside an `Arc` that many tasks read: `HandlerRegistry`, `MetricsRegistry`, `PgPool`.
* `Rc`, `RefCell`, raw `Cell` are not `Sync`/`Send`; using them in a spawned future is a
  compile error – that is the compiler preventing data races.
* `crates/core/src/lib.rs` has `assert_send_sync::<…>()` compile-time checks;
  `JobHandler: Send + Sync + 'static` and `JobStore: Send + Sync + 'static` make the bound
  part of the trait contract.
* **Locks:** `std::sync::Mutex` is used only for tiny critical sections that never contain an
  `.await` (metrics maps, in-flight set). Holding a `std::sync::MutexGuard` across `.await`
  would make the future `!Send` (compile error in `tokio::spawn`) and could deadlock the
  runtime thread.
