//! In-memory `JobStore` with the same guarded semantics as the PostgreSQL
//! implementation. Used by fast, Docker-free unit tests of the dispatcher.
//! Not durable – never use it for real jobs.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Utc;
use engine_core::store::StoreResult;
use engine_core::{
    CancelOutcome, InsertOutcome, JobFailure, JobId, JobOutput, JobRecord, JobStatus, JobStore,
    LeaseInfo, ListFilter, NewJob, StatusHistoryEntry, StoreError,
};

#[derive(Default)]
struct State {
    jobs: HashMap<JobId, JobRecord>,
    history: HashMap<JobId, Vec<StatusHistoryEntry>>,
}

#[derive(Default)]
pub struct InMemoryJobStore {
    state: Mutex<State>,
    unavailable: AtomicBool,
}

impl InMemoryJobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulate a database outage.
    pub fn set_unavailable(&self, down: bool) {
        self.unavailable.store(down, Ordering::SeqCst);
    }

    fn check(&self) -> StoreResult<()> {
        if self.unavailable.load(Ordering::SeqCst) {
            Err(StoreError::Unavailable("simulated outage".into()))
        } else {
            Ok(())
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Test helper: force a lease to look expired.
    pub fn expire_lease(&self, id: JobId) {
        if let Some(j) = self.lock().jobs.get_mut(&id) {
            j.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(1));
        }
    }

    /// Test helper: make a queued retry due immediately.
    pub fn make_due(&self, id: JobId) {
        if let Some(j) = self.lock().jobs.get_mut(&id) {
            j.next_attempt_at = None;
        }
    }
}

/// Applies a transition the way the SQL trigger does: validates it, bumps the
/// version and appends a history row.
fn transition(
    state: &mut State,
    id: JobId,
    to: JobStatus,
    reason: &str,
    mutate: impl FnOnce(&mut JobRecord),
) -> JobRecord {
    let job = state.jobs.get_mut(&id).expect("job exists");
    let from = job.status;
    assert!(
        from.can_transition_to(to),
        "illegal transition {from} -> {to}"
    );
    mutate(job);
    job.status = to;
    job.status_version += 1;
    job.updated_at = Utc::now();
    let snapshot = job.clone();
    state
        .history
        .entry(id)
        .or_default()
        .push(StatusHistoryEntry {
            from_status: Some(from),
            to_status: to,
            status_version: snapshot.status_version,
            attempt: snapshot.attempts,
            reason: Some(reason.into()),
            actor: snapshot.lease_owner.clone(),
            error_code: snapshot.error_code.clone(),
            at: snapshot.updated_at,
        });
    snapshot
}

fn owned_by(job: &JobRecord, owner: &str, attempt: u32) -> bool {
    job.status == JobStatus::Processing
        && job.lease_owner.as_deref() == Some(owner)
        && job.attempts == attempt
}

impl JobStore for InMemoryJobStore {
    async fn insert_job(&self, job: NewJob) -> StoreResult<InsertOutcome> {
        self.check()?;
        let mut st = self.lock();
        if let Some(key) = &job.idempotency_key {
            if let Some(existing) = st
                .jobs
                .values()
                .find(|j| j.idempotency_key.as_ref() == Some(key))
            {
                return if existing.kind == job.kind && existing.payload == job.payload {
                    Ok(InsertOutcome::Existing(existing.clone()))
                } else {
                    Err(StoreError::Conflict("idempotency key reused".into()))
                };
            }
        }
        let now = Utc::now();
        let record = JobRecord {
            id: job.id,
            kind: job.kind,
            payload: job.payload,
            status: JobStatus::Queued,
            status_version: 1,
            attempts: 0,
            max_attempts: job.max_attempts,
            created_at: now,
            queued_at: now,
            started_at: None,
            finished_at: None,
            next_attempt_at: None,
            lease_owner: None,
            lease_expires_at: None,
            error_code: None,
            error_message: None,
            result: None,
            artifact: None,
            idempotency_key: job.idempotency_key,
            request_id: job.request_id,
            trace_id: job.trace_id,
            traceparent: job.traceparent,
            updated_at: now,
        };
        st.history
            .entry(record.id)
            .or_default()
            .push(StatusHistoryEntry {
                from_status: None,
                to_status: JobStatus::Queued,
                status_version: 1,
                attempt: 0,
                reason: Some("submitted".into()),
                actor: None,
                error_code: None,
                at: now,
            });
        st.jobs.insert(record.id, record.clone());
        Ok(InsertOutcome::Created(record))
    }

    async fn get_job(&self, id: JobId) -> StoreResult<Option<JobRecord>> {
        self.check()?;
        Ok(self.lock().jobs.get(&id).cloned())
    }

    async fn list_jobs(&self, filter: ListFilter) -> StoreResult<Vec<JobRecord>> {
        self.check()?;
        let st = self.lock();
        let mut v: Vec<_> = st
            .jobs
            .values()
            .filter(|j| filter.status.is_none_or(|s| j.status == s))
            .cloned()
            .collect();
        v.sort_by_key(|j| std::cmp::Reverse(j.created_at));
        v.truncate(filter.limit.max(1) as usize);
        Ok(v)
    }

    async fn claim_job(&self, id: JobId, lease: &LeaseInfo) -> StoreResult<Option<JobRecord>> {
        self.check()?;
        let mut st = self.lock();
        let now = Utc::now();
        let claimable = st.jobs.get(&id).is_some_and(|j| {
            j.status == JobStatus::Queued
                && j.next_attempt_at.is_none_or(|t| t <= now)
                && j.attempts < j.max_attempts
        });
        if !claimable {
            return Ok(None);
        }
        Ok(Some(transition(
            &mut st,
            id,
            JobStatus::Processing,
            "claimed",
            |j| {
                j.attempts += 1;
                j.started_at = Some(now);
                j.finished_at = None;
                j.lease_owner = Some(lease.owner.clone());
                j.lease_expires_at =
                    Some(now + chrono::Duration::from_std(lease.duration).unwrap_or_default());
                j.next_attempt_at = None;
                j.error_code = None;
                j.error_message = None;
            },
        )))
    }

    async fn complete_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
        output: &JobOutput,
    ) -> StoreResult<Option<JobRecord>> {
        self.check()?;
        let mut st = self.lock();
        if !st
            .jobs
            .get(&id)
            .is_some_and(|j| owned_by(j, owner, attempt))
        {
            return Ok(None);
        }
        Ok(Some(transition(
            &mut st,
            id,
            JobStatus::Completed,
            "completed",
            |j| {
                j.finished_at = Some(Utc::now());
                j.result = Some(output.result.clone());
                j.artifact = output.artifact.clone();
                j.lease_expires_at = None;
            },
        )))
    }

    async fn fail_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
        failure: &JobFailure,
    ) -> StoreResult<Option<JobRecord>> {
        self.check()?;
        let mut st = self.lock();
        if !st
            .jobs
            .get(&id)
            .is_some_and(|j| owned_by(j, owner, attempt))
        {
            return Ok(None);
        }
        let (to, reason) = match failure.retry_at {
            Some(_) => (JobStatus::Queued, "retry_scheduled"),
            None => (JobStatus::Failed, "failed"),
        };
        Ok(Some(transition(&mut st, id, to, reason, |j| {
            j.error_code = Some(failure.code.clone());
            j.error_message = Some(failure.message.clone());
            j.lease_expires_at = None;
            match failure.retry_at {
                Some(t) => {
                    j.next_attempt_at = Some(t);
                    j.queued_at = t;
                }
                None => j.finished_at = Some(Utc::now()),
            }
        })))
    }

    async fn release_job(
        &self,
        id: JobId,
        owner: &str,
        attempt: u32,
    ) -> StoreResult<Option<JobRecord>> {
        self.check()?;
        let mut st = self.lock();
        if !st
            .jobs
            .get(&id)
            .is_some_and(|j| owned_by(j, owner, attempt))
        {
            return Ok(None);
        }
        Ok(Some(transition(
            &mut st,
            id,
            JobStatus::Queued,
            "released_on_shutdown",
            |j| {
                j.attempts = j.attempts.saturating_sub(1);
                j.queued_at = Utc::now();
                j.next_attempt_at = None;
                j.lease_expires_at = None;
            },
        )))
    }

    async fn cancel_job(&self, id: JobId) -> StoreResult<CancelOutcome> {
        self.check()?;
        let mut st = self.lock();
        let Some(job) = st.jobs.get(&id) else {
            return Ok(CancelOutcome::NotFound);
        };
        let previous = job.status;
        if previous.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal(job.clone()));
        }
        let record = transition(
            &mut st,
            id,
            JobStatus::Cancelled,
            "cancelled_by_client",
            |j| {
                j.finished_at = Some(Utc::now());
                j.lease_expires_at = None;
            },
        );
        Ok(CancelOutcome::Cancelled { previous, record })
    }

    async fn dispatchable_jobs(&self, limit: u32) -> StoreResult<Vec<JobRecord>> {
        self.check()?;
        let now = Utc::now();
        let st = self.lock();
        let mut v: Vec<_> = st
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Queued && j.next_attempt_at.is_none_or(|t| t <= now))
            .cloned()
            .collect();
        v.sort_by_key(|j| (j.queued_at, j.created_at));
        v.truncate(limit as usize);
        Ok(v)
    }

    async fn recover_stale_jobs(
        &self,
        worker_id: &str,
        current_owner: &str,
    ) -> StoreResult<Vec<JobRecord>> {
        self.check()?;
        let now = Utc::now();
        let mut st = self.lock();
        let stale: Vec<JobId> = st
            .jobs
            .values()
            .filter(|j| {
                j.status == JobStatus::Processing
                    && (j.lease_expires_at.is_some_and(|t| t < now)
                        || j.lease_owner.as_deref().is_some_and(|o| {
                            o.split(':').next() == Some(worker_id) && o != current_owner
                        }))
            })
            .map(|j| j.id)
            .collect();
        let mut out = Vec::new();
        for id in stale {
            let exhausted = st.jobs[&id].attempts >= st.jobs[&id].max_attempts;
            let rec = if exhausted {
                transition(&mut st, id, JobStatus::Failed, "lease_recovered", |j| {
                    j.error_code = Some("attempts_exhausted".into());
                    j.error_message = Some("worker lost during final attempt".into());
                    j.finished_at = Some(now);
                    j.lease_expires_at = None;
                })
            } else {
                transition(&mut st, id, JobStatus::Queued, "lease_recovered", |j| {
                    j.error_code = Some("worker_lost".into());
                    j.error_message =
                        Some("worker lease expired or worker restarted during processing".into());
                    j.queued_at = now;
                    j.next_attempt_at = None;
                    j.lease_expires_at = None;
                })
            };
            out.push(rec);
        }
        Ok(out)
    }

    async fn status_history(&self, id: JobId) -> StoreResult<Vec<StatusHistoryEntry>> {
        self.check()?;
        Ok(self.lock().history.get(&id).cloned().unwrap_or_default())
    }

    async fn count_by_status(&self) -> StoreResult<Vec<(JobStatus, i64)>> {
        self.check()?;
        let st = self.lock();
        Ok(JobStatus::ALL
            .into_iter()
            .map(|s| (s, st.jobs.values().filter(|j| j.status == s).count() as i64))
            .collect())
    }

    async fn ping(&self) -> StoreResult<()> {
        self.check()
    }
}
