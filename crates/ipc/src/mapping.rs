//! Conversions between `engine-core` types and protobuf messages.

use chrono::{DateTime, Utc};
use engine_core::{
    ArtifactRef, EngineError, JobId, JobStatus, JobStatusEvent, JobSummary, StatusHistoryEntry,
};
use prost_types::Timestamp;

use crate::pb;

pub fn ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

pub fn opt_ts(dt: Option<DateTime<Utc>>) -> Option<Timestamp> {
    dt.map(ts)
}

pub fn from_ts(t: &Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(t.seconds, t.nanos.max(0) as u32)
}

pub fn status_to_pb(s: JobStatus) -> pb::JobStatus {
    match s {
        JobStatus::Queued => pb::JobStatus::Queued,
        JobStatus::Processing => pb::JobStatus::Processing,
        JobStatus::Completed => pb::JobStatus::Completed,
        JobStatus::Failed => pb::JobStatus::Failed,
        JobStatus::Cancelled => pb::JobStatus::Cancelled,
    }
}

pub fn status_from_pb(v: i32) -> Option<JobStatus> {
    match pb::JobStatus::try_from(v).ok()? {
        pb::JobStatus::Unspecified => None,
        pb::JobStatus::Queued => Some(JobStatus::Queued),
        pb::JobStatus::Processing => Some(JobStatus::Processing),
        pb::JobStatus::Completed => Some(JobStatus::Completed),
        pb::JobStatus::Failed => Some(JobStatus::Failed),
        pb::JobStatus::Cancelled => Some(JobStatus::Cancelled),
    }
}

pub fn job_to_pb(s: &JobSummary) -> pb::Job {
    pb::Job {
        job_id: s.id.to_string(),
        job_type: s.kind.as_str().to_string(),
        status: status_to_pb(s.status) as i32,
        status_version: s.status_version,
        attempts: s.attempts,
        max_attempts: s.max_attempts,
        created_at: Some(ts(s.created_at)),
        queued_at: Some(ts(s.queued_at)),
        started_at: opt_ts(s.started_at),
        finished_at: opt_ts(s.finished_at),
        next_attempt_at: opt_ts(s.next_attempt_at),
        error_code: s.error_code.clone(),
        error_message: s.error_message.clone(),
        trace_id: s.trace_id.clone(),
    }
}

fn bad(msg: &str) -> EngineError {
    EngineError::Internal(format!("malformed message from worker: {msg}"))
}

pub fn job_from_pb(j: &pb::Job) -> Result<JobSummary, EngineError> {
    Ok(JobSummary {
        id: j.job_id.parse::<JobId>().map_err(|_| bad("job_id"))?,
        kind: j.job_type.parse().map_err(|_| bad("job_type"))?,
        status: status_from_pb(j.status).ok_or_else(|| bad("status"))?,
        status_version: j.status_version,
        attempts: j.attempts,
        max_attempts: j.max_attempts,
        created_at: j
            .created_at
            .as_ref()
            .and_then(from_ts)
            .ok_or_else(|| bad("created_at"))?,
        queued_at: j
            .queued_at
            .as_ref()
            .and_then(from_ts)
            .ok_or_else(|| bad("queued_at"))?,
        started_at: j.started_at.as_ref().and_then(from_ts),
        finished_at: j.finished_at.as_ref().and_then(from_ts),
        next_attempt_at: j.next_attempt_at.as_ref().and_then(from_ts),
        error_code: j.error_code.clone(),
        error_message: j.error_message.clone(),
        trace_id: j.trace_id.clone(),
    })
}

pub fn event_to_pb(e: &JobStatusEvent) -> pb::StatusEvent {
    pb::StatusEvent {
        job_id: e.job_id.to_string(),
        status: status_to_pb(e.status) as i32,
        status_version: e.status_version,
        attempts: e.attempts,
        error_code: e.error_code.clone(),
        at: Some(ts(e.at)),
    }
}

pub fn event_from_pb(e: &pb::StatusEvent) -> Result<JobStatusEvent, EngineError> {
    Ok(JobStatusEvent {
        job_id: e.job_id.parse().map_err(|_| bad("event.job_id"))?,
        status: status_from_pb(e.status).ok_or_else(|| bad("event.status"))?,
        status_version: e.status_version,
        attempts: e.attempts,
        error_code: e.error_code.clone(),
        at: e.at.as_ref().and_then(from_ts).unwrap_or_else(Utc::now),
    })
}

pub fn artifact_to_pb(a: &ArtifactRef) -> pb::Artifact {
    pb::Artifact {
        file_name: a.relative_path.clone(),
        content_type: a.content_type.clone(),
        size_bytes: a.size_bytes,
        sha256: a.sha256.clone(),
    }
}

pub fn history_to_pb(h: &StatusHistoryEntry) -> pb::HistoryEntry {
    pb::HistoryEntry {
        from_status: h.from_status.map(|s| status_to_pb(s) as i32).unwrap_or(0),
        to_status: status_to_pb(h.to_status) as i32,
        status_version: h.status_version,
        attempt: h.attempt,
        reason: h.reason.clone(),
        actor: h.actor.clone(),
        error_code: h.error_code.clone(),
        at: Some(ts(h.at)),
    }
}

pub fn history_from_pb(h: &pb::HistoryEntry) -> Result<StatusHistoryEntry, EngineError> {
    Ok(StatusHistoryEntry {
        from_status: status_from_pb(h.from_status),
        to_status: status_from_pb(h.to_status).ok_or_else(|| bad("history.to_status"))?,
        status_version: h.status_version,
        attempt: h.attempt,
        reason: h.reason.clone(),
        actor: h.actor.clone(),
        error_code: h.error_code.clone(),
        at: h.at.as_ref().and_then(from_ts).unwrap_or_else(Utc::now),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::JobKind;

    #[test]
    fn job_round_trip() {
        let now = DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap();
        let s = JobSummary {
            id: JobId::generate(),
            kind: JobKind::HtmlToPdf,
            status: JobStatus::Processing,
            status_version: 2,
            attempts: 1,
            max_attempts: 3,
            created_at: now,
            queued_at: now,
            started_at: Some(now),
            finished_at: None,
            next_attempt_at: None,
            error_code: None,
            error_message: None,
            trace_id: Some("abc".into()),
        };
        assert_eq!(job_from_pb(&job_to_pb(&s)).unwrap(), s);
    }

    #[test]
    fn statuses_round_trip() {
        for s in JobStatus::ALL {
            assert_eq!(status_from_pb(status_to_pb(s) as i32), Some(s));
        }
        assert_eq!(status_from_pb(0), None);
        assert_eq!(status_from_pb(99), None);
    }
}
