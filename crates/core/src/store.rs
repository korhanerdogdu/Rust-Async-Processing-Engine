//! Persistence abstraction for job state.
//!
//! Implemented by `engine-persistence::PgJobStore` (production) and
//! `engine-application::memory::InMemoryJobStore` (fast unit tests). Every
//! state-changing method is *guarded*: it only applies when the row is still in
//! the expected state and owned by the expected lease, and returns `None`
//! otherwise. That is what makes processing idempotent under races
//! (duplicate dispatch, cancellation during processing, expired leases).

use std::future::Future;

use crate::error::EngineError;
use crate::ids::JobId;
use crate::output::JobOutput;
use crate::record::{
    CancelOutcome, InsertOutcome, JobFailure, JobRecord, LeaseInfo, ListFilter, NewJob,
    StatusHistoryEntry,
};
use crate::status::JobStatus;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// Database unreachable, pool exhausted, connection dropped.
    #[error("database unavailable: {0}")]
    Unavailable(String),
    /// Idempotency key reused with a different request.
    #[error("conflict: {0}")]
    Conflict(String),
    /// Anything else (bug, constraint violation, corrupt row).
    #[error("database error: {0}")]
    Internal(String),
}

impl From<StoreError> for EngineError {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::Unavailable(m) => EngineError::Unavailable(format!("database: {m}")),
            StoreError::Conflict(_) => EngineError::IdempotencyConflict,
            StoreError::Internal(m) => EngineError::Internal(m),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Uses `-> impl Future + Send` instead of `async fn` so that the `Send`
/// bound is part of the contract: futures returned by the store are awaited
/// inside `tokio::spawn`ed tasks, which require `Send`.
pub trait JobStore: Send + Sync + 'static {
    /// Insert a new job in `queued` state (and its first history row) in one
    /// transaction. Honors the idempotency key.
    fn insert_job(&self, job: NewJob) -> impl Future<Output = StoreResult<InsertOutcome>> + Send;

    fn get_job(&self, id: JobId) -> impl Future<Output = StoreResult<Option<JobRecord>>> + Send;

    fn list_jobs(
        &self,
        filter: ListFilter,
    ) -> impl Future<Output = StoreResult<Vec<JobRecord>>> + Send;

    /// Atomic `queued -> processing` claim. Returns `None` if the job is not
    /// (or no longer) claimable: already taken, cancelled, not yet due, ...
    fn claim_job(
        &self,
        id: JobId,
        lease: &LeaseInfo,
    ) -> impl Future<Output = StoreResult<Option<JobRecord>>> + Send;

    /// Guarded `processing -> completed` (same owner + attempt).
    fn complete_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
        output: &JobOutput,
    ) -> impl Future<Output = StoreResult<Option<JobRecord>>> + Send;

    /// Guarded `processing -> failed` or `processing -> queued` (retry).
    fn fail_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
        failure: &JobFailure,
    ) -> impl Future<Output = StoreResult<Option<JobRecord>>> + Send;

    /// Guarded `processing -> queued` used on graceful shutdown. The
    /// interrupted attempt is not counted.
    fn release_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
    ) -> impl Future<Output = StoreResult<Option<JobRecord>>> + Send;

    /// `queued|processing -> cancelled`.
    fn cancel_job(&self, id: JobId) -> impl Future<Output = StoreResult<CancelOutcome>> + Send;

    /// Queued jobs whose `next_attempt_at` is due, oldest first.
    fn dispatchable_jobs(
        &self,
        limit: u32,
    ) -> impl Future<Output = StoreResult<Vec<JobRecord>>> + Send;

    /// Requeue (or fail, when attempts are exhausted) jobs stuck in
    /// `processing` whose lease expired, or that are owned by a *previous
    /// incarnation* of this worker (`worker_id` matches, owner differs).
    fn recover_stale_jobs(
        &self,
        worker_id: &str,
        current_owner: &str,
    ) -> impl Future<Output = StoreResult<Vec<JobRecord>>> + Send;

    fn status_history(
        &self,
        id: JobId,
    ) -> impl Future<Output = StoreResult<Vec<StatusHistoryEntry>>> + Send;

    fn count_by_status(&self) -> impl Future<Output = StoreResult<Vec<(JobStatus, i64)>>> + Send;

    fn ping(&self) -> impl Future<Output = StoreResult<()>> + Send;
}
