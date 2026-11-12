//! `cpu_compute` – **true CPU parallelism with Rayon**.
//!
//! ```text
//!  Tokio task (async)                         Rayon pool (OS threads "rayon-cpu-N")
//!  ─────────────────                          ─────────────────────────────────────
//!  acquire admission permit  ──move permit──► pool.spawn(closure)
//!  rx.await  (yields, Tokio thread is free)     chunks.par_iter().map(count).sum()
//!     ▲                                          ├─ chunk 0 on rayon-cpu-0
//!     │                                          ├─ chunk 1 on rayon-cpu-3  (work stealing)
//!     └──────────── oneshot::Sender::send ◄──────┴─ ...
//! ```
//!
//! * The heavy loop never runs on a Tokio worker, so HTTP/gRPC stay responsive.
//! * A **dedicated** pool (not Rayon's global one) with a configured size
//!   (`RAYON_THREADS`) – CPU work cannot starve anything else using Rayon.
//! * Admission (`CPU_MAX_CONCURRENT_JOBS`) bounds how many CPU jobs are in the
//!   pool at once. The permit is *moved into* the Rayon closure: if the
//!   awaiting future is dropped (timeout/cancel), the permit is released only
//!   when the CPU work actually stops – dropping a future does not stop
//!   threads.
//! * Cancellation is cooperative: each chunk checks the token first.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use engine_core::{
    BoxFuture, CpuComputeParams, HandlerError, JobCommand, JobContext, JobHandler, JobKind,
    JobOutput,
};
use rayon::prelude::*;
use serde_json::{Value, json};
use tokio::sync::{Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct CpuConfig {
    pub threads: usize,
    pub max_concurrent_jobs: usize,
}

impl Default for CpuConfig {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            max_concurrent_jobs: 1,
        }
    }
}

pub struct CpuComputeHandler {
    pool: Arc<rayon::ThreadPool>,
    admission: Arc<Semaphore>,
    threads: usize,
}

impl CpuComputeHandler {
    pub fn new(cfg: CpuConfig) -> Result<Self, rayon::ThreadPoolBuildError> {
        let threads = cfg.threads.max(1);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("rayon-cpu-{i}"))
            // Without a handler, a panic inside `pool.spawn` aborts the process.
            .panic_handler(|_| {
                eprintln!("[cpu] a Rayon job panicked; the job will be marked failed")
            })
            .build()?;
        Ok(Self {
            pool: Arc::new(pool),
            admission: Arc::new(Semaphore::new(cfg.max_concurrent_jobs.max(1))),
            threads,
        })
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    async fn run(
        &self,
        ctx: JobContext,
        params: CpuComputeParams,
    ) -> Result<JobOutput, HandlerError> {
        let permit = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(HandlerError::Cancelled),
            p = self.admission.clone().acquire_owned() => p.map_err(|_| HandlerError::failed("cpu_pool_closed", "pool closed", false))?,
        };

        let (tx, rx) = oneshot::channel::<Option<Value>>();
        let cancel = ctx.cancel.clone();
        let threads = self.threads;
        self.pool.spawn(move || {
            let _permit = permit; // released when the CPU work really ends
            let result = compute(&params, threads, &cancel);
            // The receiver may be gone (job timed out); that is fine.
            let _ = tx.send(result);
        });

        tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(HandlerError::Cancelled),
            r = rx => match r {
                Ok(Some(value)) => Ok(JobOutput::inline(value)),
                Ok(None) => Err(HandlerError::Cancelled),
                Err(_) => Err(HandlerError::failed("compute_panicked", "CPU computation panicked", false)),
            },
        }
    }
}

impl JobHandler for CpuComputeHandler {
    fn kind(&self) -> JobKind {
        JobKind::CpuCompute
    }

    fn execute<'a>(
        &'a self,
        ctx: JobContext,
        command: JobCommand,
    ) -> BoxFuture<'a, Result<JobOutput, HandlerError>> {
        Box::pin(async move {
            let JobCommand::CpuCompute(params) = command else {
                return Err(HandlerError::Unsupported(command.kind()));
            };
            self.run(ctx, params).await
        })
    }
}

/// Deterministic primality test (6k ± 1 trial division). Pure CPU.
pub fn is_prime(n: u64) -> bool {
    if n < 2 {
        return false;
    }
    if n < 4 {
        return true;
    }
    if n % 2 == 0 || n % 3 == 0 {
        return false;
    }
    let mut i = 5u64;
    while i * i <= n {
        if n % i == 0 || n % (i + 2) == 0 {
            return false;
        }
        i += 6;
    }
    true
}

pub fn count_primes_sequential(from: u64, to: u64) -> u64 {
    (from..to).filter(|&n| is_prime(n)).count() as u64
}

pub fn chunk_ranges(from: u64, to: u64, chunk: u64) -> Vec<(u64, u64)> {
    let chunk = chunk.max(1);
    let mut out = Vec::new();
    let mut start = from;
    while start < to {
        let end = start.saturating_add(chunk).min(to);
        out.push((start, end));
        start = end;
    }
    out
}

/// Parallel count. Must be called from inside the target Rayon pool
/// (`pool.spawn`/`pool.install`) so `par_iter` uses that pool. Returns `None`
/// when cancelled.
pub fn count_primes_parallel(
    from: u64,
    to: u64,
    chunk: u64,
    cancel: &CancellationToken,
) -> Option<(u64, BTreeSet<usize>, usize)> {
    let ranges = chunk_ranges(from, to, chunk);
    let seen = Mutex::new(BTreeSet::new());
    let counts: Option<Vec<u64>> = ranges
        .par_iter()
        .map(|&(a, b)| {
            if cancel.is_cancelled() {
                return None;
            }
            if let Some(idx) = rayon::current_thread_index() {
                seen.lock().unwrap_or_else(|p| p.into_inner()).insert(idx);
            }
            Some(count_primes_sequential(a, b))
        })
        .collect();
    let total = counts?.into_iter().sum();
    Some((total, seen.into_inner().unwrap_or_default(), ranges.len()))
}

fn compute(
    params: &CpuComputeParams,
    pool_threads: usize,
    cancel: &CancellationToken,
) -> Option<Value> {
    let chunk = params.effective_chunk_size();
    let started = Instant::now();
    let (count, threads_used, chunks) =
        count_primes_parallel(params.from, params.to, chunk, cancel)?;
    let parallel_ms = started.elapsed().as_secs_f64() * 1000.0;

    let (sequential_ms, speedup) = if params.compare_sequential && !cancel.is_cancelled() {
        let s = Instant::now();
        let seq = count_primes_sequential(params.from, params.to);
        debug_assert_eq!(seq, count);
        let ms = s.elapsed().as_secs_f64() * 1000.0;
        (Some(round2(ms)), Some(round2(ms / parallel_ms.max(0.001))))
    } else {
        (None, None)
    };

    Some(json!({
        "operation": params.operation,
        "from": params.from,
        "to": params.to,
        "prime_count": count,
        "chunk_size": chunk,
        "chunks": chunks,
        "rayon_pool_threads": pool_threads,
        "distinct_rayon_threads_used": threads_used.len(),
        "rayon_thread_indices": threads_used,
        "available_parallelism": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        "parallel_ms": round2(parallel_ms),
        "sequential_ms": sequential_ms,
        "measured_speedup": speedup,
    }))
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::{CpuOperation, JobId};

    #[test]
    fn primality_basics() {
        let primes: Vec<u64> = (0..30).filter(|&n| is_prime(n)).collect();
        assert_eq!(primes, vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);
        assert!(is_prime(1_000_000_007));
        assert!(!is_prime(1_000_000_007 * 3));
    }

    #[test]
    fn chunking_covers_range_exactly() {
        let r = chunk_ranges(10, 35, 10);
        assert_eq!(r, vec![(10, 20), (20, 30), (30, 35)]);
        assert!(chunk_ranges(5, 5, 10).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rayon_result_matches_sequential_and_uses_several_threads() {
        let handler = CpuComputeHandler::new(CpuConfig {
            threads: 4,
            max_concurrent_jobs: 1,
        })
        .unwrap();
        let params = CpuComputeParams {
            operation: CpuOperation::CountPrimes,
            from: 0,
            to: 1_000_000,
            chunk_size: Some(10_000),
            compare_sequential: true,
        };
        let ctx = JobContext {
            job_id: JobId::generate(),
            attempt: 1,
            cancel: CancellationToken::new(),
        };
        let out = handler
            .execute(ctx, JobCommand::CpuCompute(params))
            .await
            .unwrap();
        // pi(10^6) = 78_498 (well-known value).
        assert_eq!(out.result["prime_count"], 78_498);
        assert_eq!(out.result["chunks"], 100);
        assert_eq!(out.result["rayon_pool_threads"], 4);
        let used = out.result["distinct_rayon_threads_used"].as_u64().unwrap();
        assert!(
            used >= 2,
            "expected work on several Rayon threads, got {used}"
        );
    }

    #[tokio::test]
    async fn cancellation_stops_cpu_work() {
        let handler = CpuComputeHandler::new(CpuConfig {
            threads: 2,
            max_concurrent_jobs: 1,
        })
        .unwrap();
        let ctx = JobContext {
            job_id: JobId::generate(),
            attempt: 1,
            cancel: CancellationToken::new(),
        };
        ctx.cancel.cancel();
        let params = CpuComputeParams {
            operation: CpuOperation::CountPrimes,
            from: 0,
            to: 50_000_000,
            chunk_size: None,
            compare_sequential: false,
        };
        let err = handler
            .execute(ctx, JobCommand::CpuCompute(params))
            .await
            .unwrap_err();
        assert_eq!(err, HandlerError::Cancelled);
    }

    #[test]
    fn parallel_cancel_returns_none() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(count_primes_parallel(0, 1_000_000, 1000, &token).is_none());
    }
}
