use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::JobId;
use crate::job::JobKind;
use crate::output::ArtifactRef;
use crate::status::JobStatus;

/// A job row as persisted in PostgreSQL (the source of truth).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: JobId,
    pub kind: JobKind,
    /// Canonical, validated payload (may contain user content: never log it).
    pub payload: Value,
    pub status: JobStatus,
    /// Incremented by the database on every status change. Lets clients
    /// order/deduplicate notifications that arrive via different paths.
    pub status_version: i64,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    /// When the job (most recently) became eligible for dispatch.
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub result: Option<Value>,
    pub artifact: Option<ArtifactRef>,
    pub idempotency_key: Option<String>,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub traceparent: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl JobRecord {
    pub fn summary(&self) -> JobSummary {
        JobSummary {
            id: self.id,
            kind: self.kind,
            status: self.status,
            status_version: self.status_version,
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            created_at: self.created_at,
            queued_at: self.queued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            next_attempt_at: self.next_attempt_at,
            error_code: self.error_code.clone(),
            error_message: self.error_message.clone(),
            trace_id: self.trace_id.clone(),
        }
    }
}

/// Public view of a job (no payload, no lease internals). This is what REST,
/// public gRPC and WebSocket snapshots expose.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSummary {
    #[serde(rename = "job_id")]
    pub id: JobId,
    #[serde(rename = "job_type")]
    pub kind: JobKind,
    pub status: JobStatus,
    pub status_version: i64,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub trace_id: Option<String>,
}

/// Input for inserting a new job.
#[derive(Debug, Clone, PartialEq)]
pub struct NewJob {
    pub id: JobId,
    pub kind: JobKind,
    pub payload: Value,
    pub max_attempts: u32,
    pub idempotency_key: Option<String>,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub traceparent: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertOutcome {
    /// A new row was committed.
    Created(JobRecord),
    /// The idempotency key matched an existing identical request.
    Existing(JobRecord),
}

/// Identifies *which* worker incarnation owns a processing attempt.
/// Format: `<worker_id>:<boot_uuid>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseInfo {
    pub owner: String,
    pub duration: Duration,
}

/// How a failed attempt should be recorded.
#[derive(Debug, Clone, PartialEq)]
pub struct JobFailure {
    pub code: String,
    pub message: String,
    /// `Some(t)`: put the job back to `queued`, eligible again at `t`.
    /// `None`: the failure is final.
    pub retry_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CancelOutcome {
    Cancelled {
        previous: JobStatus,
        record: JobRecord,
    },
    AlreadyTerminal(JobRecord),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListFilter {
    pub status: Option<JobStatus>,
    pub limit: u32,
}

/// One row of `job_status_history`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusHistoryEntry {
    pub from_status: Option<JobStatus>,
    pub to_status: JobStatus,
    pub status_version: i64,
    pub attempt: u32,
    pub reason: Option<String>,
    pub actor: Option<String>,
    pub error_code: Option<String>,
    pub at: DateTime<Utc>,
}
