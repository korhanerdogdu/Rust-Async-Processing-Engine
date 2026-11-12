//! `EngineHandle` – the worker's transport-independent application service.
//!
//! The internal gRPC server (`apps/worker-engine/src/grpc_server.rs`) is a thin
//! adapter over these methods. Every gRPC request handler runs as its **own
//! Tokio task**, so concurrent `submit` calls are concurrent **producers**:
//! each clones the `mpsc::Sender`, reserves a slot and sends.
//!
//! ## Admission + durability protocol (`submit`)
//!
//! ```text
//! 1. validate (short sync code)                         -> 422 on error
//! 2. reserve a channel slot (try_reserve / timeout)     -> 429 RESOURCE_EXHAUSTED if full
//! 3. INSERT job as 'queued' + history row, COMMIT       -> 503 if DB down (slot released)
//! 4. permit.send(envelope)  (cannot fail: slot is ours)
//! 5. publish 'queued' on the broadcast bus, return 202
//! ```
//!
//! Reserving *before* the insert means we never persist a job we then refuse,
//! and never answer 202 for a job that is not durably stored. There is no
//! atomicity between PostgreSQL and memory: if the process dies between 3 and
//! 4, the row is still `queued` and the reconciler dispatches it after
//! restart. Acceptance is durable; execution is **at-least-once**.

use std::sync::Arc;
use std::time::Duration;

use engine_core::{
    ArtifactRef, CancelOutcome, EngineError, InsertOutcome, JobCommand, JobId, JobKind, JobRecord,
    JobStatus, JobStatusEvent, JobStore, ListFilter, NewJob, StatusHistoryEntry, ValidationError,
};
use engine_observability::MetricsRegistry;
use engine_observability::propagation::{current_trace_id, current_traceparent};
use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, Semaphore, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::bus::StatusBus;
use crate::config::EngineConfig;
use crate::dispatcher::run_consumer;
use crate::queue::{InflightSet, JobEnvelope, RunningJobs};
use crate::reconciler::{ReconcileReport, reconcile_once, run_reconciler};
use crate::registry::HandlerRegistry;

/// Engine lifecycle, published through a `tokio::sync::watch` channel.
///
/// *Why watch?* Readers (health checks, submit admission, the gateway's
/// readiness probe via gRPC) only ever care about the **latest** state, never
/// about intermediate ones. `watch` stores exactly one value; readers call
/// `borrow()` for the current value or `changed().await` to wait for a change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    Starting,
    Ready,
    Draining,
    Stopped,
}

impl EngineState {
    pub fn as_str(self) -> &'static str {
        match self {
            EngineState::Starting => "starting",
            EngineState::Ready => "ready",
            EngineState::Draining => "draining",
            EngineState::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubmitRequest {
    pub kind: JobKind,
    pub payload: Value,
    pub idempotency_key: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SubmitOutcome {
    pub record: JobRecord,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EngineStats {
    pub worker_id: String,
    pub lease_owner: String,
    pub state: EngineState,
    pub queue_capacity: usize,
    /// Slots in use: envelopes waiting in the channel + reserved slots.
    pub queue_depth: usize,
    pub max_active_jobs: usize,
    pub active_jobs: usize,
    pub running_job_ids: Vec<JobId>,
    pub inflight_jobs: usize,
    pub status_subscribers: usize,
}

/// State shared by producers, the consumer, job tasks and the reconciler.
pub(crate) struct Shared<S> {
    pub(crate) store: Arc<S>,
    pub(crate) registry: HandlerRegistry,
    pub(crate) bus: StatusBus,
    pub(crate) config: EngineConfig,
    pub(crate) lease_owner: String,
    pub(crate) inflight: Arc<InflightSet>,
    pub(crate) running: Arc<RunningJobs>,
    /// Permits = MAX_ACTIVE_JOBS. Acquired by the consumer *before* `recv`.
    pub(crate) active: Arc<Semaphore>,
    pub(crate) metrics: Arc<MetricsRegistry>,
    pub(crate) state: watch::Sender<EngineState>,
    /// Parent of every running job's token; cancelled when the shutdown grace
    /// period is over (jobs are then released back to `queued`).
    pub(crate) job_cancel_root: CancellationToken,
    /// Stops intake: consumer loop and reconciler.
    pub(crate) intake_stop: CancellationToken,
    pub(crate) artifact_cleanup: Option<ArtifactCleanup>,
}

/// Callback used to delete an artifact produced for a job that was cancelled
/// while it was rendering. (The application layer does not know where
/// artifacts live; the worker binary provides this.)
pub type ArtifactCleanup = Arc<dyn Fn(&ArtifactRef) + Send + Sync>;

impl<S> Shared<S> {
    pub(crate) fn publish(&self, record: &JobRecord) {
        self.bus.publish(JobStatusEvent::from_record(record));
    }
}

struct Tasks {
    consumer: JoinHandle<tokio::task::JoinSet<()>>,
    reconciler: JoinHandle<()>,
}

/// Cheap to clone (`Arc` inside); hand one to every gRPC handler.
pub struct EngineHandle<S> {
    shared: Arc<Shared<S>>,
    /// The original Sender. Producers *clone* it; the channel closes when the
    /// last clone is dropped (or the receiver closes it).
    sender: mpsc::Sender<JobEnvelope>,
    tasks: Arc<AsyncMutex<Option<Tasks>>>,
}

impl<S> Clone for EngineHandle<S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            sender: self.sender.clone(),
            tasks: self.tasks.clone(),
        }
    }
}

impl<S: JobStore> EngineHandle<S> {
    /// Creates the bounded channel, spawns the **single consumer** task and the
    /// reconciler, and marks the engine `Ready`. Must be called inside a Tokio
    /// runtime.
    pub fn start(
        store: Arc<S>,
        registry: HandlerRegistry,
        config: EngineConfig,
        metrics: Arc<MetricsRegistry>,
        artifact_cleanup: Option<ArtifactCleanup>,
    ) -> Self {
        let lease_owner = format!("{}:{}", config.worker_id, uuid::Uuid::new_v4().simple());
        // THE job queue. Exactly one `Receiver` exists and it is moved into the
        // consumer task below; it can never be cloned or shared.
        let (sender, receiver) = mpsc::channel::<JobEnvelope>(config.queue_capacity.max(1));
        let (state, _) = watch::channel(EngineState::Starting);

        metrics
            .gauge("engine_queue_capacity", &[])
            .set(config.queue_capacity as i64);
        metrics
            .gauge("engine_max_active_jobs", &[])
            .set(config.max_active_jobs as i64);

        let shared = Arc::new(Shared {
            store,
            registry,
            bus: StatusBus::new(config.status_channel_capacity),
            active: Arc::new(Semaphore::new(config.max_active_jobs.max(1))),
            config,
            lease_owner,
            inflight: Arc::new(InflightSet::default()),
            running: Arc::new(RunningJobs::default()),
            metrics,
            state,
            job_cancel_root: CancellationToken::new(),
            intake_stop: CancellationToken::new(),
            artifact_cleanup,
        });

        let consumer = tokio::spawn(run_consumer(
            shared.clone(),
            receiver,
            shared.intake_stop.clone(),
        ));
        let reconciler = tokio::spawn(run_reconciler(
            shared.clone(),
            sender.clone(),
            shared.intake_stop.clone(),
        ));
        shared.state.send_replace(EngineState::Ready);
        tracing::info!(
            event = "engine.started",
            worker_id = %shared.config.worker_id,
            lease_owner = %shared.lease_owner,
            queue_capacity = shared.config.queue_capacity,
            max_active_jobs = shared.config.max_active_jobs,
            "engine started: one bounded MPSC queue, one consumer task"
        );
        Self {
            shared,
            sender,
            tasks: Arc::new(AsyncMutex::new(Some(Tasks {
                consumer,
                reconciler,
            }))),
        }
    }

    pub fn state(&self) -> EngineState {
        *self.shared.state.borrow()
    }

    pub fn watch_state(&self) -> watch::Receiver<EngineState> {
        self.shared.state.subscribe()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<JobStatusEvent> {
        self.shared.bus.subscribe()
    }

    pub fn config(&self) -> &EngineConfig {
        &self.shared.config
    }

    fn ensure_accepting(&self) -> Result<(), EngineError> {
        match self.state() {
            EngineState::Ready => Ok(()),
            s => Err(EngineError::Unavailable(format!(
                "engine is {}",
                s.as_str()
            ))),
        }
    }

    fn reject(&self, reason: &str) {
        self.shared
            .metrics
            .counter("jobs_rejected_total", &[("reason", reason)])
            .inc();
    }

    /// Reserves one slot in the bounded channel *for this producer*.
    async fn reserve_slot(&self) -> Result<mpsc::OwnedPermit<JobEnvelope>, EngineError> {
        // Each producer works with its own clone of the Sender.
        let sender = self.sender.clone();
        let timeout = self.shared.config.enqueue_timeout;
        let retry_after_ms = 1000;
        if timeout.is_zero() {
            sender.try_reserve_owned().map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => EngineError::Overloaded { retry_after_ms },
                mpsc::error::TrySendError::Closed(_) => {
                    EngineError::Unavailable("job queue closed".into())
                }
            })
        } else {
            match tokio::time::timeout(timeout, sender.reserve_owned()).await {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(_closed)) => Err(EngineError::Unavailable("job queue closed".into())),
                Err(_elapsed) => Err(EngineError::Overloaded { retry_after_ms }),
            }
        }
    }

    /// Durable submission. See module docs for the protocol.
    #[tracing::instrument(name = "engine.submit", skip_all, fields(job_type = %req.kind, job_id = tracing::field::Empty))]
    pub async fn submit(&self, req: SubmitRequest) -> Result<SubmitOutcome, EngineError> {
        self.ensure_accepting()
            .inspect_err(|_| self.reject("not_ready"))?;
        let limits = &self.shared.config.limits;
        let command = JobCommand::parse(req.kind, &req.payload, limits)
            .inspect_err(|_| self.reject("validation"))?;
        if let Some(key) = &req.idempotency_key {
            validate_idempotency_key(key, limits.max_idempotency_key_len)
                .inspect_err(|_| self.reject("validation"))?;
        }

        let permit = async { self.reserve_slot().await }
            .instrument(tracing::info_span!("queue.admission"))
            .await
            .inspect_err(|e| {
                self.reject("queue_full");
                tracing::warn!(
                    event = "job.rejected",
                    reason = e.code(),
                    queue_capacity = self.shared.config.queue_capacity,
                    "admission rejected"
                );
            })?;

        let new = NewJob {
            id: JobId::generate(),
            kind: req.kind,
            payload: command.to_payload(),
            max_attempts: self.shared.config.max_attempts,
            idempotency_key: req.idempotency_key,
            request_id: req.request_id,
            trace_id: current_trace_id(),
            traceparent: current_traceparent(),
        };
        let traceparent = new.traceparent.clone();
        let outcome = self
            .shared
            .store
            .insert_job(new)
            .instrument(tracing::info_span!("db.insert_job"))
            .await
            .inspect_err(|e| {
                self.reject("database");
                tracing::error!(event = "job.persist_failed", error = %e, "could not persist job; nothing accepted");
            })?; // `permit` dropped here on error -> slot released

        match outcome {
            InsertOutcome::Existing(record) => {
                drop(permit);
                tracing::Span::current().record("job_id", tracing::field::display(record.id));
                tracing::info!(event = "job.deduplicated", job_id = %record.id, "idempotency key matched an existing job");
                Ok(SubmitOutcome {
                    record,
                    deduplicated: true,
                })
            }
            InsertOutcome::Created(record) => {
                tracing::Span::current().record("job_id", tracing::field::display(record.id));
                self.shared.inflight.insert(record.id);
                // Ownership: `command` is *moved* into the envelope, the
                // envelope is *moved* into the channel. No clone of the payload.
                permit.send(JobEnvelope {
                    job_id: record.id,
                    command,
                    traceparent,
                    enqueued_at: Instant::now(),
                });
                self.shared.publish(&record);
                self.shared
                    .metrics
                    .counter("jobs_accepted_total", &[("job_type", req.kind.as_str())])
                    .inc();
                self.update_queue_gauge();
                tracing::info!(event = "job.accepted", job_id = %record.id, status = "queued", "job durably accepted and enqueued");
                Ok(SubmitOutcome {
                    record,
                    deduplicated: false,
                })
            }
        }
    }

    fn update_queue_gauge(&self) {
        let depth = self
            .shared
            .config
            .queue_capacity
            .saturating_sub(self.sender.capacity());
        self.shared
            .metrics
            .gauge("engine_queue_depth", &[])
            .set(depth as i64);
    }

    pub async fn get(&self, id: JobId) -> Result<JobRecord, EngineError> {
        self.shared
            .store
            .get_job(id)
            .await?
            .ok_or(EngineError::NotFound(id))
    }

    /// Only `completed` jobs have a result; anything else is `ResultNotReady`
    /// (carrying the status and, for failures, the error).
    pub async fn result(&self, id: JobId) -> Result<JobRecord, EngineError> {
        let record = self.get(id).await?;
        match record.status {
            JobStatus::Completed => Ok(record),
            status => Err(EngineError::ResultNotReady {
                status,
                error_code: record.error_code,
                error_message: record.error_message,
            }),
        }
    }

    pub async fn list(&self, filter: ListFilter) -> Result<Vec<JobRecord>, EngineError> {
        Ok(self.shared.store.list_jobs(filter).await?)
    }

    pub async fn history(&self, id: JobId) -> Result<Vec<StatusHistoryEntry>, EngineError> {
        let _ = self.get(id).await?;
        Ok(self.shared.store.status_history(id).await?)
    }

    #[tracing::instrument(name = "engine.cancel", skip(self), fields(job_id = %id))]
    pub async fn cancel(&self, id: JobId) -> Result<JobRecord, EngineError> {
        match self.shared.store.cancel_job(id).await? {
            CancelOutcome::Cancelled { previous, record } => {
                if previous == JobStatus::Processing {
                    let signalled = self.shared.running.cancel(id);
                    tracing::info!(
                        event = "job.cancel_signalled",
                        signalled,
                        "running job asked to stop"
                    );
                }
                self.shared.publish(&record);
                self.shared
                    .metrics
                    .counter(
                        "jobs_cancelled_total",
                        &[("job_type", record.kind.as_str())],
                    )
                    .inc();
                tracing::info!(event = "job.cancelled", previous = %previous, "job cancelled");
                Ok(record)
            }
            CancelOutcome::AlreadyTerminal(record) => Err(EngineError::InvalidState {
                status: record.status,
            }),
            CancelOutcome::NotFound => Err(EngineError::NotFound(id)),
        }
    }

    pub async fn counts_by_status(&self) -> Result<Vec<(JobStatus, i64)>, EngineError> {
        Ok(self.shared.store.count_by_status().await?)
    }

    pub async fn ping_store(&self) -> Result<(), EngineError> {
        Ok(self.shared.store.ping().await?)
    }

    pub fn stats(&self) -> EngineStats {
        self.update_queue_gauge();
        let max = self.shared.config.max_active_jobs.max(1);
        // NOT `max - available_permits()`: the consumer acquires a permit
        // *before* waiting in `recv()`, so while idle it holds one permit
        // without any job running. Count jobs that are really executing.
        let running = self.shared.running.ids();
        EngineStats {
            worker_id: self.shared.config.worker_id.clone(),
            lease_owner: self.shared.lease_owner.clone(),
            state: self.state(),
            queue_capacity: self.shared.config.queue_capacity,
            queue_depth: self
                .shared
                .config
                .queue_capacity
                .saturating_sub(self.sender.capacity()),
            max_active_jobs: max,
            active_jobs: running.len(),
            running_job_ids: running,
            inflight_jobs: self.shared.inflight.len(),
            status_subscribers: self.shared.bus.subscriber_count(),
        }
    }

    /// One reconciliation pass (also run periodically). Exposed for tests.
    pub async fn reconcile_now(&self) -> ReconcileReport {
        reconcile_once(&self.shared, &self.sender).await
    }

    /// Graceful shutdown:
    /// 1. `Draining`: new submissions get UNAVAILABLE (503).
    /// 2. stop the consumer + reconciler; envelopes still in the channel are
    ///    dropped – their rows stay `queued` and are picked up after restart.
    /// 3. wait up to `shutdown_grace` for running jobs to finish;
    /// 4. then cancel them; each releases its row back to `queued`.
    pub async fn shutdown(&self) {
        self.shared.state.send_replace(EngineState::Draining);
        tracing::info!(event = "engine.draining", "engine draining: intake stopped");
        self.shared.intake_stop.cancel();
        let Some(tasks) = self.tasks.lock().await.take() else {
            return;
        };
        let _ = tasks.reconciler.await;
        let mut running = match tasks.consumer.await {
            Ok(set) => set,
            Err(e) => {
                tracing::error!(event = "engine.consumer_panicked", error = %e, "consumer task failed");
                tokio::task::JoinSet::new()
            }
        };
        let in_progress = running.len();
        let graceful = tokio::time::timeout(self.shared.config.shutdown_grace, async {
            while running.join_next().await.is_some() {}
        })
        .await
        .is_ok();
        if !graceful {
            tracing::warn!(
                event = "engine.shutdown_grace_exceeded",
                remaining = running.len(),
                "cancelling unfinished jobs; they will be released to queued"
            );
            self.shared.job_cancel_root.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(5), async {
                while running.join_next().await.is_some() {}
            })
            .await;
            running.abort_all();
        }
        self.shared.state.send_replace(EngineState::Stopped);
        tracing::info!(
            event = "engine.stopped",
            in_progress_at_shutdown = in_progress,
            graceful,
            "engine stopped"
        );
    }
}

fn validate_idempotency_key(key: &str, max: usize) -> Result<(), ValidationError> {
    let ok = !key.is_empty()
        && key.len() <= max
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'));
    if ok {
        Ok(())
    } else {
        Err(ValidationError::new(
            "invalid_idempotency_key",
            format!("idempotency key must be 1..={max} chars of [A-Za-z0-9-_.:]"),
        )
        .with_field("idempotency_key"))
    }
}
