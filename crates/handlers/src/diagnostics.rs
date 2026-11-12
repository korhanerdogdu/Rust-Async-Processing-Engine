//! **Diagnostic / test-only**: why `std::thread::sleep` in async code is bad.
//!
//! The experiment runs on a small multi-thread runtime (`worker_threads`
//! threads). A "heartbeat" task wants to tick every `tick`. Meanwhile
//! `load_tasks` tasks each "sleep" for `sleep`:
//!
//! * `SleepKind::Async`    – `tokio::time::sleep`: tasks yield; the heartbeat
//!   keeps ticking on time.
//! * `SleepKind::Blocking` – `std::thread::sleep`: each task *parks its runtime
//!   worker thread*. With load_tasks >= worker_threads every worker is parked,
//!   and the heartbeat (and every HTTP request, gRPC call, DB result...) waits.
//!
//! The production code never uses `std::thread::sleep` in async contexts. Run
//! `cargo run -p engine-handlers --example blocking_vs_async` to see numbers.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepKind {
    Async,
    Blocking,
}

#[derive(Debug, Clone)]
pub struct LagReport {
    pub kind: SleepKind,
    pub ticks: usize,
    pub max_lag: Duration,
    pub mean_lag: Duration,
    pub total: Duration,
}

/// Builds its own runtime, so call it from synchronous code (tests/examples).
pub fn heartbeat_lag_experiment(
    kind: SleepKind,
    worker_threads: usize,
    load_tasks: usize,
    sleep: Duration,
    tick: Duration,
) -> LagReport {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async move {
        let started = Instant::now();
        let heartbeat = tokio::spawn(async move {
            let mut lags = Vec::new();
            let window = sleep + tick * 4;
            let begin = Instant::now();
            while begin.elapsed() < window {
                let expected = Instant::now() + tick;
                tokio::time::sleep(tick).await;
                lags.push(Instant::now().saturating_duration_since(expected));
            }
            lags
        });
        // Give the heartbeat a moment to start before the load arrives.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let load: Vec<_> = (0..load_tasks)
            .map(|_| {
                tokio::spawn(async move {
                    match kind {
                        SleepKind::Async => tokio::time::sleep(sleep).await,
                        // Deliberately WRONG: blocks the runtime worker thread.
                        SleepKind::Blocking => std::thread::sleep(sleep),
                    }
                })
            })
            .collect();
        for t in load {
            let _ = t.await;
        }
        let lags = heartbeat.await.unwrap_or_default();
        let max_lag = lags.iter().copied().max().unwrap_or_default();
        let mean_lag = if lags.is_empty() {
            Duration::ZERO
        } else {
            lags.iter().sum::<Duration>() / lags.len() as u32
        };
        LagReport {
            kind,
            ticks: lags.len(),
            max_lag,
            mean_lag,
            total: started.elapsed(),
        }
    })
}
