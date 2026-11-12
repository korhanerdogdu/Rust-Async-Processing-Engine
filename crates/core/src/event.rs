use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::JobId;
use crate::record::JobRecord;
use crate::status::JobStatus;

/// A committed status transition, published *after* the database commit.
///
/// Small and `Clone` because `tokio::sync::broadcast` clones every message
/// once per receiver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobStatusEvent {
    pub job_id: JobId,
    pub status: JobStatus,
    pub status_version: i64,
    pub attempts: u32,
    pub error_code: Option<String>,
    pub at: DateTime<Utc>,
}

impl JobStatusEvent {
    pub fn from_record(record: &JobRecord) -> Self {
        Self {
            job_id: record.id,
            status: record.status,
            status_version: record.status_version,
            attempts: record.attempts,
            error_code: record.error_code.clone(),
            at: record.updated_at,
        }
    }
}
