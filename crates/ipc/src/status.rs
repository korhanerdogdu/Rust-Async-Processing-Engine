//! `EngineError` <-> `tonic::Status`.
//!
//! The gRPC code says *what kind* of failure it is (clients and proxies
//! understand it); the `x-engine-error-code` metadata carries our stable code so
//! the gateway can reconstruct the exact `EngineError` on the other side of
//! the process boundary – and then map it to HTTP for REST clients.
//!
//! | EngineError          | gRPC code            | HTTP (gateway) |
//! |----------------------|----------------------|----------------|
//! | Validation           | INVALID_ARGUMENT     | 422            |
//! | NotFound             | NOT_FOUND            | 404            |
//! | ResultNotReady       | FAILED_PRECONDITION  | 409            |
//! | IdempotencyConflict  | ALREADY_EXISTS       | 409            |
//! | InvalidState         | FAILED_PRECONDITION  | 409            |
//! | Overloaded           | RESOURCE_EXHAUSTED   | 429            |
//! | Unavailable          | UNAVAILABLE          | 503            |
//! | Timeout              | DEADLINE_EXCEEDED    | 504            |
//! | Internal             | INTERNAL             | 500            |

use engine_core::{EngineError, JobStatus, ValidationError};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

pub const ERROR_CODE_KEY: &str = "x-engine-error-code";
const JOB_ID_KEY: &str = "x-job-id";
const JOB_STATUS_KEY: &str = "x-job-status";
const JOB_ERROR_CODE_KEY: &str = "x-job-error-code";
const VALIDATION_CODE_KEY: &str = "x-validation-code";
const VALIDATION_FIELD_KEY: &str = "x-validation-field";
const RETRY_AFTER_KEY: &str = "retry-after-ms";

fn put(md: &mut MetadataMap, key: &'static str, value: &str) {
    // Metadata values must be visible ASCII; skip anything else.
    if let Ok(v) = MetadataValue::try_from(value) {
        md.insert(key, v);
    }
}

pub fn error_to_status(err: &EngineError) -> Status {
    let mut md = MetadataMap::new();
    put(&mut md, ERROR_CODE_KEY, err.code());
    let (code, message) = match err {
        EngineError::Validation(v) => {
            put(&mut md, VALIDATION_CODE_KEY, &v.code);
            if let Some(f) = &v.field {
                put(&mut md, VALIDATION_FIELD_KEY, f);
            }
            (Code::InvalidArgument, v.message.clone())
        }
        EngineError::NotFound(id) => {
            put(&mut md, JOB_ID_KEY, &id.to_string());
            (Code::NotFound, err.to_string())
        }
        EngineError::ResultNotReady {
            status,
            error_code,
            error_message,
        } => {
            put(&mut md, JOB_STATUS_KEY, status.as_str());
            if let Some(c) = error_code {
                put(&mut md, JOB_ERROR_CODE_KEY, c);
            }
            (
                Code::FailedPrecondition,
                error_message.clone().unwrap_or_else(|| err.to_string()),
            )
        }
        EngineError::IdempotencyConflict => (Code::AlreadyExists, err.to_string()),
        EngineError::InvalidState { status } => {
            put(&mut md, JOB_STATUS_KEY, status.as_str());
            (Code::FailedPrecondition, err.to_string())
        }
        EngineError::Overloaded { retry_after_ms } => {
            put(&mut md, RETRY_AFTER_KEY, &retry_after_ms.to_string());
            (Code::ResourceExhausted, err.to_string())
        }
        EngineError::Unavailable(_) => (Code::Unavailable, err.to_string()),
        EngineError::Timeout(_) => (Code::DeadlineExceeded, err.to_string()),
        EngineError::Internal(_) => (Code::Internal, err.public_message()),
    };
    Status::with_metadata(code, message, md)
}

fn get<'a>(md: &'a MetadataMap, key: &str) -> Option<&'a str> {
    md.get(key).and_then(|v| v.to_str().ok())
}

fn job_status(md: &MetadataMap) -> JobStatus {
    get(md, JOB_STATUS_KEY)
        .and_then(|s| s.parse().ok())
        .unwrap_or(JobStatus::Queued)
}

/// Rebuilds the `EngineError` from a Status received from the worker. A
/// Status *without* our metadata was produced by the transport itself
/// (connection refused, deadline, h2 reset): the worker is unreachable.
pub fn status_to_error(status: &Status) -> EngineError {
    let md = status.metadata();
    let msg = status.message().to_string();
    match get(md, ERROR_CODE_KEY) {
        Some("validation_error") => EngineError::Validation(ValidationError {
            code: get(md, VALIDATION_CODE_KEY)
                .unwrap_or("invalid_payload")
                .to_string(),
            message: msg,
            field: get(md, VALIDATION_FIELD_KEY).map(str::to_string),
        }),
        Some("not_found") => match get(md, JOB_ID_KEY).and_then(|s| s.parse().ok()) {
            Some(id) => EngineError::NotFound(id),
            None => EngineError::Internal("not_found without job id".into()),
        },
        Some("result_not_ready") => {
            let error_code = get(md, JOB_ERROR_CODE_KEY).map(str::to_string);
            let error_message = error_code.is_some().then_some(msg);
            EngineError::ResultNotReady {
                status: job_status(md),
                error_code,
                error_message,
            }
        }
        Some("idempotency_conflict") => EngineError::IdempotencyConflict,
        Some("invalid_state") => EngineError::InvalidState {
            status: job_status(md),
        },
        Some("overloaded") => EngineError::Overloaded {
            retry_after_ms: get(md, RETRY_AFTER_KEY)
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
        },
        Some("unavailable") => {
            EngineError::Unavailable(strip_prefix(&msg, "service unavailable: "))
        }
        Some("timeout") => EngineError::Timeout(strip_prefix(&msg, "deadline exceeded: ")),
        Some(_) => EngineError::Internal(msg),
        None => match status.code() {
            Code::Unavailable | Code::Cancelled | Code::Unknown => {
                EngineError::Unavailable(format!("worker engine unreachable: {msg}"))
            }
            Code::DeadlineExceeded => EngineError::Timeout(format!("worker engine call: {msg}")),
            Code::ResourceExhausted => EngineError::Overloaded {
                retry_after_ms: 1000,
            },
            _ => EngineError::Internal(format!("{:?}: {msg}", status.code())),
        },
    }
}

fn strip_prefix(s: &str, prefix: &str) -> String {
    s.strip_prefix(prefix).unwrap_or(s).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::JobId;

    #[test]
    fn every_error_survives_the_process_boundary() {
        let id = JobId::generate();
        let cases = vec![
            EngineError::Validation(
                ValidationError::new("out_of_range", "duration_ms must be <= 60000")
                    .with_field("duration_ms"),
            ),
            EngineError::NotFound(id),
            EngineError::ResultNotReady {
                status: JobStatus::Processing,
                error_code: None,
                error_message: None,
            },
            EngineError::ResultNotReady {
                status: JobStatus::Failed,
                error_code: Some("invalid_json".into()),
                error_message: Some("bad json".into()),
            },
            EngineError::IdempotencyConflict,
            EngineError::InvalidState {
                status: JobStatus::Completed,
            },
            EngineError::Overloaded {
                retry_after_ms: 750,
            },
            EngineError::Unavailable("engine is draining".into()),
            EngineError::Timeout("x".into()),
        ];
        for err in cases {
            let status = error_to_status(&err);
            assert_eq!(
                status_to_error(&status),
                err,
                "round trip of {err:?} via {:?}",
                status.code()
            );
        }
    }

    #[test]
    fn grpc_codes() {
        assert_eq!(
            error_to_status(&EngineError::Overloaded { retry_after_ms: 1 }).code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            error_to_status(&EngineError::NotFound(JobId::generate())).code(),
            Code::NotFound
        );
        let internal = error_to_status(&EngineError::Internal("secret detail".into()));
        assert_eq!(internal.code(), Code::Internal);
        assert!(!internal.message().contains("secret"));
    }

    #[test]
    fn bare_transport_errors_mean_worker_unavailable() {
        let err = status_to_error(&Status::unavailable("tcp connect error"));
        assert!(matches!(err, EngineError::Unavailable(m) if m.contains("unreachable")));
        assert!(matches!(
            status_to_error(&Status::deadline_exceeded("t")),
            EngineError::Timeout(_)
        ));
    }
}
