use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ValidationError;

/// Strongly typed job identifier.
///
/// A newtype around `Uuid` so a job id can never be confused with some other
/// UUID (request id, trace id, ...) at compile time. It is `Copy` because it
/// is 16 bytes: cheaper to copy than to reference-count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Generates a fresh random (v4) id. Only the worker engine generates job
    /// ids; clients never choose them (they may supply an idempotency key).
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for JobId {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s.trim()).map(Self).map_err(|_| {
            ValidationError::new("invalid_job_id", "job id must be a UUID").with_field("job_id")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_displays_round_trip() {
        let id = JobId::generate();
        let parsed: JobId = id.to_string().parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn rejects_garbage() {
        let err = "not-a-uuid".parse::<JobId>().unwrap_err();
        assert_eq!(err.code, "invalid_job_id");
    }
}
