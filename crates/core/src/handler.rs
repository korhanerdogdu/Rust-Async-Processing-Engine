//! The handler abstraction.
//!
//! ## Why `dyn JobHandler` with a boxed future?
//!
//! The dispatcher keeps handlers in a `HashMap<JobKind, Arc<dyn JobHandler>>`
//! and picks one at runtime. A trait with a native `async fn` (or
//! `-> impl Future`) is **not dyn-compatible** in stable Rust: every
//! implementation returns a different anonymous future type, so a vtable
//! cannot describe it. Returning `Pin<Box<dyn Future + Send>>` erases that
//! type at the cost of one heap allocation per job – negligible next to the
//! work a job does.
//!
//! Contrast with [`crate::store::JobStore`], which uses `-> impl Future + Send`
//! (static dispatch, zero allocation) because there is exactly one store type
//! per process, chosen at compile time via generics.

use std::future::Future;
use std::pin::Pin;

use tokio_util::sync::CancellationToken;

use crate::error::HandlerError;
use crate::ids::JobId;
use crate::job::{JobCommand, JobKind};
use crate::output::JobOutput;

/// A heap-allocated, type-erased future that can be moved across threads.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Per-attempt execution context handed to a handler.
#[derive(Debug, Clone)]
pub struct JobContext {
    pub job_id: JobId,
    /// 1-based attempt number (the value of `jobs.attempts` after the claim).
    pub attempt: u32,
    /// Cancelled on client cancellation, handler timeout or shutdown.
    /// Async handlers `select!` on it; CPU handlers poll `is_cancelled()`.
    pub cancel: CancellationToken,
}

/// A job handler. `Send + Sync + 'static` because one instance is shared (via
/// `Arc`) by every job task, on whichever runtime thread runs them.
pub trait JobHandler: Send + Sync + 'static {
    fn kind(&self) -> JobKind;

    fn execute<'a>(
        &'a self,
        ctx: JobContext,
        command: JobCommand,
    ) -> BoxFuture<'a, Result<JobOutput, HandlerError>>;
}
