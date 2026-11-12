//! `simulate_delay` – **async waiting**.
//!
//! `tokio::time::sleep` registers a timer and *yields*: the runtime thread is
//! free to run other tasks while this job "works". Compare with
//! `std::thread::sleep`, which would park the whole runtime worker thread
//! (see `diagnostics.rs` and `tests/blocking_vs_async.rs`).
//!
//! The result reports the runtime thread before and after the `.await`. They
//! often differ: a task is not pinned to a thread; the multi-thread scheduler
//! may resume it on any worker (work stealing). That is why the future must be
//! `Send`.

use std::time::Instant;

use engine_core::{
    BoxFuture, HandlerError, JobCommand, JobContext, JobHandler, JobKind, JobOutput,
};
use serde_json::json;

pub struct DelayHandler;

fn thread_label() -> String {
    let t = std::thread::current();
    format!("{} {:?}", t.name().unwrap_or("unnamed"), t.id())
}

impl JobHandler for DelayHandler {
    fn kind(&self) -> JobKind {
        JobKind::SimulateDelay
    }

    fn execute<'a>(
        &'a self,
        ctx: JobContext,
        command: JobCommand,
    ) -> BoxFuture<'a, Result<JobOutput, HandlerError>> {
        Box::pin(async move {
            let JobCommand::SimulateDelay(params) = command else {
                return Err(HandlerError::Unsupported(command.kind()));
            };
            let thread_before = thread_label();
            let started = Instant::now();

            tokio::select! {
                // `biased` = check cancellation first, deterministically.
                biased;
                _ = ctx.cancel.cancelled() => return Err(HandlerError::Cancelled),
                _ = tokio::time::sleep(params.duration()) => {}
            }

            if ctx.attempt <= params.fail_first_attempts {
                tracing::warn!(
                    event = "delay.simulated_failure",
                    attempt = ctx.attempt,
                    "simulated transient failure"
                );
                return Err(HandlerError::failed(
                    "simulated_failure",
                    format!("simulated transient failure on attempt {}", ctx.attempt),
                    true,
                ));
            }

            Ok(JobOutput::inline(json!({
                "requested_ms": params.duration_ms,
                "slept_ms": started.elapsed().as_millis() as u64,
                "attempt": ctx.attempt,
                "thread_before_await": thread_before,
                "thread_after_await": thread_label(),
            })))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::{DelayParams, JobId};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn ctx(attempt: u32) -> JobContext {
        JobContext {
            job_id: JobId::generate(),
            attempt,
            cancel: CancellationToken::new(),
        }
    }

    fn cmd(ms: u64, fail: u32) -> JobCommand {
        JobCommand::SimulateDelay(DelayParams {
            duration_ms: ms,
            fail_first_attempts: fail,
        })
    }

    // `start_paused = true`: Tokio's clock is virtual and auto-advances when
    // all tasks are idle, so a "60 second" job finishes instantly in the test.
    #[tokio::test(start_paused = true)]
    async fn sleeps_without_real_waiting() {
        let started = tokio::time::Instant::now();
        let out = DelayHandler.execute(ctx(1), cmd(60_000, 0)).await.unwrap();
        assert_eq!(out.result["requested_ms"], 60_000);
        assert!(started.elapsed() >= Duration::from_secs(60)); // virtual time
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_interrupts_sleep() {
        let c = ctx(1);
        let token = c.cancel.clone();
        let task = tokio::spawn(async move { DelayHandler.execute(c, cmd(60_000, 0)).await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        token.cancel();
        assert_eq!(task.await.unwrap().unwrap_err(), HandlerError::Cancelled);
    }

    #[tokio::test(start_paused = true)]
    async fn simulated_failures_are_retryable_and_bounded() {
        let err = DelayHandler.execute(ctx(1), cmd(10, 2)).await.unwrap_err();
        assert!(err.is_retryable());
        assert!(DelayHandler.execute(ctx(2), cmd(10, 2)).await.is_err());
        assert!(DelayHandler.execute(ctx(3), cmd(10, 2)).await.is_ok());
    }
}
