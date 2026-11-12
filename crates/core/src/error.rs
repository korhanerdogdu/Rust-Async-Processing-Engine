//! Typed errors.
//!
//! * [`ValidationError`] – the request is malformed / out of bounds (client fault).
//! * [`EngineError`]     – what an *API operation* (submit, get, cancel, ...) can fail with.
//!   Transports map it: REST -> HTTP status (`api-gateway/src/rest.rs`),
//!   gRPC -> `tonic::Code` (`crates/ipc/src/status.rs`).
//! * [`HandlerError`]    – what *executing a job* can fail with. These end up as
//!   `error_code`/`error_message` on a failed job, never as HTTP errors.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ids::JobId;
use crate::job::JobKind;
use crate::status::JobStatus;

/// A client-side validation problem. `code` is a stable machine-readable
/// identifier; `message` is safe to show to the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationError {
    pub code: String,
    pub message: String,
    pub field: Option<String>,
}

impl ValidationError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            field: None,
        }
    }

    pub fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.field {
            Some(field) => write!(f, "{} ({field}): {}", self.code, self.message),
            None => write!(f, "{}: {}", self.code, self.message),
        }
    }
}

impl std::error::Error for ValidationError {}

/// Errors returned by the job *API* (submission, queries, cancellation).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EngineError {
    #[error("validation failed: {0}")]
    Validation(ValidationError),

    #[error("job {0} not found")]
    NotFound(JobId),

    /// The job exists but has no result yet (or never will: failed/cancelled).
    #[error("result not available: job is {status}")]
    ResultNotReady {
        status: JobStatus,
        error_code: Option<String>,
        error_message: Option<String>,
    },

    /// The same idempotency key was reused with a different request.
    #[error("idempotency key was already used for a different request")]
    IdempotencyConflict,

    /// e.g. cancelling a job that already completed.
    #[error("job is {status}; operation not allowed")]
    InvalidState { status: JobStatus },

    /// Admission control rejected the job (bounded queue full).
    #[error("job queue is full; retry later")]
    Overloaded { retry_after_ms: u64 },

    /// Engine not ready, draining, database down, or worker unreachable.
    #[error("service unavailable: {0}")]
    Unavailable(String),

    #[error("deadline exceeded: {0}")]
    Timeout(String),

    /// Unexpected failure. The message is for logs; clients get a sanitized text.
    #[error("internal error: {0}")]
    Internal(String),
}

impl EngineError {
    /// Stable, documented error code used in REST bodies and gRPC metadata.
    pub fn code(&self) -> &'static str {
        match self {
            EngineError::Validation(_) => "validation_error",
            EngineError::NotFound(_) => "not_found",
            EngineError::ResultNotReady { .. } => "result_not_ready",
            EngineError::IdempotencyConflict => "idempotency_conflict",
            EngineError::InvalidState { .. } => "invalid_state",
            EngineError::Overloaded { .. } => "overloaded",
            EngineError::Unavailable(_) => "unavailable",
            EngineError::Timeout(_) => "timeout",
            EngineError::Internal(_) => "internal",
        }
    }

    /// Message that is safe to return to clients. Internal details are kept
    /// for logs only.
    pub fn public_message(&self) -> String {
        match self {
            EngineError::Internal(_) => "internal server error".to_string(),
            other => other.to_string(),
        }
    }
}

impl From<ValidationError> for EngineError {
    fn from(value: ValidationError) -> Self {
        EngineError::Validation(value)
    }
}

/// Errors produced while *executing* a job.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum HandlerError {
    /// The payload passed schema validation but its content is unusable
    /// (e.g. malformed JSON text inside a `json_format` job). Never retried.
    #[error("invalid input: {message}")]
    InvalidInput { code: String, message: String },

    /// The handler exceeded its configured deadline. Retryable (bounded).
    #[error("job exceeded its time limit")]
    Timeout,

    /// Cancelled by a client request or by graceful shutdown.
    #[error("job was cancelled")]
    Cancelled,

    /// Generic failure with an explicit retry classification.
    #[error("{code}: {message}")]
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },

    #[error("no handler registered for job type {0}")]
    Unsupported(JobKind),
}

impl HandlerError {
    pub fn invalid_input(code: impl Into<String>, message: impl Into<String>) -> Self {
        HandlerError::InvalidInput {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn failed(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        HandlerError::Failed {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    pub fn code(&self) -> String {
        match self {
            HandlerError::InvalidInput { code, .. } => code.clone(),
            HandlerError::Timeout => "timeout".into(),
            HandlerError::Cancelled => "cancelled".into(),
            HandlerError::Failed { code, .. } => code.clone(),
            HandlerError::Unsupported(_) => "unsupported_job_type".into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            HandlerError::Timeout => true,
            HandlerError::Failed { retryable, .. } => *retryable,
            HandlerError::InvalidInput { .. }
            | HandlerError::Cancelled
            | HandlerError::Unsupported(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_errors_are_sanitized() {
        let err = EngineError::Internal("password=hunter2 at db.rs:42".into());
        assert_eq!(err.code(), "internal");
        assert!(!err.public_message().contains("hunter2"));
    }

    #[test]
    fn codes_are_stable() {
        assert_eq!(
            EngineError::Overloaded { retry_after_ms: 1 }.code(),
            "overloaded"
        );
        assert_eq!(EngineError::NotFound(JobId::generate()).code(), "not_found");
        assert_eq!(
            EngineError::from(ValidationError::new("x", "y")).code(),
            "validation_error"
        );
    }

    #[test]
    fn retry_classification() {
        assert!(HandlerError::Timeout.is_retryable());
        assert!(!HandlerError::Cancelled.is_retryable());
        assert!(!HandlerError::invalid_input("invalid_json", "bad").is_retryable());
        assert!(HandlerError::failed("renderer_crashed", "x", true).is_retryable());
        assert!(!HandlerError::failed("renderer_missing", "x", false).is_retryable());
    }
}
