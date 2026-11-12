//! DB scanner / reconciler – the bridge between the durable table and the
//! ephemeral channel.
//!
//! Runs at startup and then every `RECONCILE_INTERVAL_MS`:
//!
//! 1. **Recover stale leases**: `processing` rows whose lease expired (worker
//!    crashed mid-job) or that belong to a previous incarnation of *this*
//!    worker id are moved back to `queued` – or to `failed`
//!    (`attempts_exhausted`) once `max_attempts` is reached. Bounded retries,
//!    never silent loss.
//! 2. **Dispatch due queued rows** that are not already in memory: restarts,
//!    retries whose backoff elapsed, or rows whose in-memory envelope was lost.
//!
//! The reconciler is simply *another producer* (it owns a cloned Sender) and
//! uses `try_reserve`: if the channel is full it stops for this round and
//! never blocks.

use std::sync::Arc;

use engine_core::{JobCommand, JobFailure, JobStore, LeaseInfo};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::engine::Shared;
use crate::queue::JobEnvelope;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub recovered: usize,
    pub exhausted: usize,
    pub enqueued: usize,
    pub invalid: usize,
}

pub(crate) async fn run_reconciler<S: JobStore>(
    shared: Arc<Shared<S>>,
    sender: mpsc::Sender<JobEnvelope>,
    stop: CancellationToken,
) {
    let mut ticker = tokio::time::interval(shared.config.reconcile_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        // The first tick completes immediately: startup recovery.
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = ticker.tick() => {}
        }
        let report = reconcile_once(&shared, &sender).await;
        if report != ReconcileReport::default() {
            tracing::info!(
                event = "reconciler.pass",
                recovered = report.recovered,
                exhausted = report.exhausted,
                enqueued = report.enqueued,
                invalid = report.invalid,
                "reconciliation pass"
            );
        }
    }
    // Dropping `sender` here: one less producer.
}

pub(crate) async fn reconcile_once<S: JobStore>(
    shared: &Shared<S>,
    sender: &mpsc::Sender<JobEnvelope>,
) -> ReconcileReport {
    let mut report = ReconcileReport::default();

    match shared
        .store
        .recover_stale_jobs(&shared.config.worker_id, &shared.lease_owner)
        .await
    {
        Ok(recovered) => {
            for r in recovered {
                if r.status.is_terminal() {
                    report.exhausted += 1;
                    shared
                        .metrics
                        .counter(
                            "jobs_failed_total",
                            &[
                                ("job_type", r.kind.as_str()),
                                ("error_code", "attempts_exhausted"),
                            ],
                        )
                        .inc();
                    tracing::warn!(event = "job.recovery_exhausted", job_id = %r.id, attempts = r.attempts, "stale job failed: attempts exhausted");
                } else {
                    report.recovered += 1;
                    shared
                        .metrics
                        .counter("jobs_recovered_total", &[("job_type", r.kind.as_str())])
                        .inc();
                    tracing::warn!(event = "job.recovered", job_id = %r.id, attempts = r.attempts, "stale processing job requeued");
                }
                shared.publish(&r);
            }
        }
        Err(e) => {
            tracing::warn!(event = "reconciler.recover_failed", error = %e, "lease recovery failed (database?)");
            return report;
        }
    }

    let free = sender.capacity();
    if free == 0 {
        return report;
    }
    let want = (free + shared.inflight.len()).min(500) as u32;
    let candidates = match shared.store.dispatchable_jobs(want).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(event = "reconciler.scan_failed", error = %e, "queued-job scan failed");
            return report;
        }
    };

    for record in candidates {
        if shared.inflight.contains(record.id) {
            continue;
        }
        let command = match JobCommand::parse(record.kind, &record.payload, &shared.config.limits) {
            Ok(c) => c,
            Err(e) => {
                // A stored payload that no longer validates (e.g. limits were
                // lowered). Claim it and fail it explicitly – never drop silently.
                report.invalid += 1;
                let lease = LeaseInfo {
                    owner: shared.lease_owner.clone(),
                    duration: shared.config.lease_grace,
                };
                if let Ok(Some(claimed)) = shared.store.claim_job(record.id, &lease).await {
                    let failure = JobFailure {
                        code: "invalid_stored_payload".into(),
                        message: e.to_string(),
                        retry_at: None,
                    };
                    if let Ok(Some(failed)) = shared
                        .store
                        .fail_job(record.id, &shared.lease_owner, claimed.attempts, &failure)
                        .await
                    {
                        shared.publish(&failed);
                    }
                }
                continue;
            }
        };
        match sender.try_reserve() {
            Ok(permit) => {
                shared.inflight.insert(record.id);
                permit.send(JobEnvelope {
                    job_id: record.id,
                    command,
                    traceparent: record.traceparent.clone(),
                    enqueued_at: Instant::now(),
                });
                report.enqueued += 1;
            }
            Err(mpsc::error::TrySendError::Full(())) => break,
            Err(mpsc::error::TrySendError::Closed(())) => break,
        }
    }
    report
}
