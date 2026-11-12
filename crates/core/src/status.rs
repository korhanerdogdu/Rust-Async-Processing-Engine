use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::ValidationError;

/// User-visible job status.
///
/// ```text
///   queued ──► processing ──► completed
///     │  ▲          │ ├────► failed
///     │  └──────────┘ └────► cancelled
///     │   (retry / lease recovery / graceful release)
///     ├────► cancelled
///     └────► failed   (only if a stored payload can no longer be decoded)
/// ```
///
/// The same rules are enforced twice: here (documented, unit-tested contract)
/// and inside PostgreSQL by the `jobs_before_update` trigger, so even a buggy
/// SQL statement cannot perform an illegal transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Processing,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub const ALL: [JobStatus; 5] = [
        JobStatus::Queued,
        JobStatus::Processing,
        JobStatus::Completed,
        JobStatus::Failed,
        JobStatus::Cancelled,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Processing => "processing",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        }
    }

    /// Terminal states never change again.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
        )
    }

    /// The single source of truth (in Rust) for the job state machine.
    pub const fn can_transition_to(self, next: JobStatus) -> bool {
        use JobStatus::*;
        matches!(
            (self, next),
            (Queued, Processing)
                | (Queued, Cancelled)
                | (Queued, Failed)
                | (Processing, Completed)
                | (Processing, Failed)
                | (Processing, Cancelled)
                | (Processing, Queued)
        )
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JobStatus {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(Self::Queued),
            "processing" => Ok(Self::Processing),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(ValidationError::new(
                "invalid_status",
                format!(
                    "unknown status '{other}' (expected queued|processing|completed|failed|cancelled)"
                ),
            )
            .with_field("status")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobStatus::{self, *};

    #[test]
    fn allowed_transitions() {
        for (from, to) in [
            (Queued, Processing),
            (Queued, Cancelled),
            (Processing, Completed),
            (Processing, Failed),
            (Processing, Cancelled),
            (Processing, Queued),
        ] {
            assert!(from.can_transition_to(to), "{from} -> {to} must be allowed");
        }
    }

    #[test]
    fn forbidden_transitions() {
        // Terminal states are final.
        for terminal in [Completed, Failed, Cancelled] {
            for next in JobStatus::ALL {
                assert!(
                    !terminal.can_transition_to(next),
                    "{terminal} -> {next} must be forbidden"
                );
            }
        }
        // A queued job can never jump straight to completed: the result must be
        // produced by a processing attempt first.
        assert!(!Queued.can_transition_to(Completed));
        // Self-transitions are not transitions.
        assert!(!Queued.can_transition_to(Queued));
        assert!(!Processing.can_transition_to(Processing));
    }

    #[test]
    fn string_round_trip() {
        for s in JobStatus::ALL {
            assert_eq!(s.as_str().parse::<JobStatus>().unwrap(), s);
            assert_eq!(serde_json::to_string(&s).unwrap(), format!("\"{s}\""));
        }
        assert!("done".parse::<JobStatus>().is_err());
    }
}
