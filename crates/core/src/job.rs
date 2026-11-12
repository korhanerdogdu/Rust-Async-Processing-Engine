//! The normalized, transport-independent job command.
//!
//! REST bodies and gRPC messages are both mapped into
//! `(JobKind, serde_json::Value)` and then parsed by [`JobCommand::parse`],
//! so validation rules are written exactly once.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ValidationError;
use crate::limits::JobLimits;

/// Public job type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    JsonFormat,
    TextTransform,
    SimulateDelay,
    HtmlToPdf,
    CpuCompute,
}

impl JobKind {
    pub const ALL: [JobKind; 5] = [
        JobKind::JsonFormat,
        JobKind::TextTransform,
        JobKind::SimulateDelay,
        JobKind::HtmlToPdf,
        JobKind::CpuCompute,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            JobKind::JsonFormat => "json_format",
            JobKind::TextTransform => "text_transform",
            JobKind::SimulateDelay => "simulate_delay",
            JobKind::HtmlToPdf => "html_to_pdf",
            JobKind::CpuCompute => "cpu_compute",
        }
    }
}

impl fmt::Display for JobKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JobKind {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JobKind::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| {
                ValidationError::new(
                    "unknown_job_type",
                    format!(
                        "unknown job_type '{s}' (expected one of: json_format, text_transform, simulate_delay, html_to_pdf, cpu_compute)"
                    ),
                )
                .with_field("job_type")
            })
    }
}

// ---------------------------------------------------------------------------
// Per-kind parameters. `deny_unknown_fields` makes typos a validation error
// instead of being silently ignored.
// ---------------------------------------------------------------------------

/// `json_format`: validate and pretty-print JSON *text*.
///
/// The input is a string on purpose: if it were a JSON value, the transport
/// layer would already have rejected malformed JSON and the handler could
/// never demonstrate a typed "invalid JSON" job failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonFormatParams {
    pub input: String,
    #[serde(default = "default_indent")]
    pub indent: u8,
    #[serde(default)]
    pub sort_keys: bool,
}

fn default_indent() -> u8 {
    2
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextOperation {
    Uppercase,
    Lowercase,
    /// Unicode-correct reversal by *extended grapheme clusters*.
    Reverse,
    /// Naive reversal by `char` (Unicode scalar values). Kept on purpose to
    /// demonstrate why it breaks combining marks and emoji sequences.
    ReverseChars,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextTransformParams {
    pub text: String,
    pub operation: TextOperation,
}

/// Wire form of `simulate_delay`: accepts `duration_ms` *or* `seconds`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct DelayWire {
    duration_ms: Option<u64>,
    seconds: Option<f64>,
    #[serde(default)]
    fail_first_attempts: u32,
}

/// Normalized `simulate_delay` parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DelayParams {
    pub duration_ms: u64,
    /// Test/learning hook: fail (retryably) while `attempt <= fail_first_attempts`.
    /// Lets you watch bounded retries without breaking anything for real.
    pub fail_first_attempts: u32,
}

impl DelayParams {
    pub fn duration(&self) -> Duration {
        Duration::from_millis(self.duration_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HtmlToPdfParams {
    pub html: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CpuOperation {
    /// Count primes in `[from, to)` by trial division, in parallel chunks.
    CountPrimes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CpuComputeParams {
    #[serde(default = "default_cpu_op")]
    pub operation: CpuOperation,
    pub from: u64,
    pub to: u64,
    /// Numbers per Rayon work item. Defaults to `span / 64` (min 1_000).
    pub chunk_size: Option<u64>,
    /// Also run a single-threaded baseline and report the measured speedup.
    #[serde(default)]
    pub compare_sequential: bool,
}

fn default_cpu_op() -> CpuOperation {
    CpuOperation::CountPrimes
}

impl CpuComputeParams {
    pub fn effective_chunk_size(&self) -> u64 {
        let span = self.to.saturating_sub(self.from);
        self.chunk_size.unwrap_or((span / 64).max(1_000)).max(1)
    }
}

/// The normalized command executed by the worker.
///
/// Every variant owns its data (`String`, not `&str`): a command is *moved*
/// into the MPSC channel and later *moved* into the handler task, so it must
/// not borrow from the request that created it ('static + Send).
#[derive(Debug, Clone, PartialEq)]
pub enum JobCommand {
    JsonFormat(JsonFormatParams),
    TextTransform(TextTransformParams),
    SimulateDelay(DelayParams),
    HtmlToPdf(HtmlToPdfParams),
    CpuCompute(CpuComputeParams),
}

impl JobCommand {
    pub fn kind(&self) -> JobKind {
        match self {
            JobCommand::JsonFormat(_) => JobKind::JsonFormat,
            JobCommand::TextTransform(_) => JobKind::TextTransform,
            JobCommand::SimulateDelay(_) => JobKind::SimulateDelay,
            JobCommand::HtmlToPdf(_) => JobKind::HtmlToPdf,
            JobCommand::CpuCompute(_) => JobKind::CpuCompute,
        }
    }

    /// Parses and validates a payload for the given job kind.
    ///
    /// This is ordinary short synchronous code: it runs inline inside async
    /// request handlers because it is bounded (payload size is checked first)
    /// and never waits on I/O.
    pub fn parse(
        kind: JobKind,
        payload: &Value,
        limits: &JobLimits,
    ) -> Result<Self, ValidationError> {
        let size = serde_json::to_vec(payload)
            .map(|v| v.len())
            .unwrap_or(usize::MAX);
        if size > limits.max_payload_bytes {
            return Err(ValidationError::new(
                "payload_too_large",
                format!(
                    "payload is {size} bytes; limit is {}",
                    limits.max_payload_bytes
                ),
            )
            .with_field("payload"));
        }

        match kind {
            JobKind::JsonFormat => {
                let p: JsonFormatParams = from_payload(payload)?;
                check_len("input", p.input.len(), limits.max_json_input_bytes)?;
                if p.indent > 8 {
                    return Err(out_of_range("indent", "indent must be between 0 and 8"));
                }
                Ok(JobCommand::JsonFormat(p))
            }
            JobKind::TextTransform => {
                let p: TextTransformParams = from_payload(payload)?;
                check_len("text", p.text.len(), limits.max_text_bytes)?;
                Ok(JobCommand::TextTransform(p))
            }
            JobKind::SimulateDelay => {
                let w: DelayWire = from_payload(payload)?;
                let duration_ms = match (w.duration_ms, w.seconds) {
                    (Some(ms), None) => ms,
                    (None, Some(s)) if s.is_finite() && s >= 0.0 => (s * 1000.0).round() as u64,
                    (None, Some(_)) => {
                        return Err(out_of_range(
                            "seconds",
                            "seconds must be a non-negative number",
                        ));
                    }
                    (Some(_), Some(_)) => {
                        return Err(ValidationError::new(
                            "invalid_payload",
                            "specify either duration_ms or seconds, not both",
                        ));
                    }
                    (None, None) => {
                        return Err(ValidationError::new(
                            "invalid_payload",
                            "missing duration_ms (or seconds)",
                        )
                        .with_field("duration_ms"));
                    }
                };
                let max_ms = limits.max_delay.as_millis() as u64;
                if duration_ms > max_ms {
                    return Err(out_of_range(
                        "duration_ms",
                        format!("duration_ms must be <= {max_ms}"),
                    ));
                }
                if w.fail_first_attempts > 10 {
                    return Err(out_of_range("fail_first_attempts", "must be <= 10"));
                }
                Ok(JobCommand::SimulateDelay(DelayParams {
                    duration_ms,
                    fail_first_attempts: w.fail_first_attempts,
                }))
            }
            JobKind::HtmlToPdf => {
                let p: HtmlToPdfParams = from_payload(payload)?;
                if p.html.trim().is_empty() {
                    return Err(
                        ValidationError::new("invalid_payload", "html must not be empty")
                            .with_field("html"),
                    );
                }
                check_len("html", p.html.len(), limits.max_html_bytes)?;
                Ok(JobCommand::HtmlToPdf(p))
            }
            JobKind::CpuCompute => {
                let p: CpuComputeParams = from_payload(payload)?;
                if p.from >= p.to {
                    return Err(out_of_range("to", "`to` must be greater than `from`"));
                }
                if p.to > limits.max_cpu_value {
                    return Err(out_of_range(
                        "to",
                        format!("`to` must be <= {}", limits.max_cpu_value),
                    ));
                }
                let span = p.to - p.from;
                if span > limits.max_cpu_span {
                    return Err(out_of_range(
                        "to",
                        format!("range span {span} exceeds limit {}", limits.max_cpu_span),
                    ));
                }
                if p.chunk_size == Some(0) {
                    return Err(out_of_range("chunk_size", "chunk_size must be > 0"));
                }
                let chunks = span.div_ceil(p.effective_chunk_size());
                if chunks > limits.max_cpu_chunks {
                    return Err(out_of_range(
                        "chunk_size",
                        format!(
                            "{chunks} chunks exceeds limit {}; use a larger chunk_size",
                            limits.max_cpu_chunks
                        ),
                    ));
                }
                Ok(JobCommand::CpuCompute(p))
            }
        }
    }

    /// Canonical JSON form persisted in `jobs.payload`. Recovery re-parses
    /// exactly this, so what is stored is always the *validated* command.
    pub fn to_payload(&self) -> Value {
        let result = match self {
            JobCommand::JsonFormat(p) => serde_json::to_value(p),
            JobCommand::TextTransform(p) => serde_json::to_value(p),
            JobCommand::SimulateDelay(p) => serde_json::to_value(p),
            JobCommand::HtmlToPdf(p) => serde_json::to_value(p),
            JobCommand::CpuCompute(p) => serde_json::to_value(p),
        };
        // Serializing plain structs of strings/numbers cannot fail.
        result.unwrap_or(Value::Null)
    }
}

fn from_payload<T: serde::de::DeserializeOwned>(payload: &Value) -> Result<T, ValidationError> {
    T::deserialize(payload)
        .map_err(|e| ValidationError::new("invalid_payload", e.to_string()).with_field("payload"))
}

fn check_len(field: &str, len: usize, max: usize) -> Result<(), ValidationError> {
    if len > max {
        Err(ValidationError::new(
            "payload_too_large",
            format!("{field} is {len} bytes; limit is {max}"),
        )
        .with_field(field))
    } else {
        Ok(())
    }
}

fn out_of_range(field: &str, message: impl Into<String>) -> ValidationError {
    ValidationError::new("out_of_range", message).with_field(field)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn limits() -> JobLimits {
        JobLimits::default()
    }

    #[test]
    fn kind_round_trip() {
        for k in JobKind::ALL {
            assert_eq!(k.as_str().parse::<JobKind>().unwrap(), k);
        }
        assert_eq!(
            "fx_rate".parse::<JobKind>().unwrap_err().code,
            "unknown_job_type"
        );
    }

    #[test]
    fn parses_each_kind() {
        let cases = [
            (JobKind::JsonFormat, json!({"input": "{\"a\":1}"})),
            (
                JobKind::TextTransform,
                json!({"text": "hi", "operation": "uppercase"}),
            ),
            (JobKind::SimulateDelay, json!({"seconds": 1.5})),
            (JobKind::HtmlToPdf, json!({"html": "<h1>x</h1>"})),
            (JobKind::CpuCompute, json!({"from": 0, "to": 1000})),
        ];
        for (kind, payload) in cases {
            let cmd = JobCommand::parse(kind, &payload, &limits()).unwrap();
            assert_eq!(cmd.kind(), kind);
            // Canonical payload re-parses to the identical command.
            let again = JobCommand::parse(kind, &cmd.to_payload(), &limits()).unwrap();
            assert_eq!(cmd, again);
        }
    }

    #[test]
    fn delay_seconds_normalized_to_ms() {
        let cmd =
            JobCommand::parse(JobKind::SimulateDelay, &json!({"seconds": 5}), &limits()).unwrap();
        assert_eq!(
            cmd,
            JobCommand::SimulateDelay(DelayParams {
                duration_ms: 5000,
                fail_first_attempts: 0
            })
        );
    }

    #[test]
    fn rejects_unknown_fields_and_bad_bounds() {
        let l = limits();
        let unknown = JobCommand::parse(
            JobKind::TextTransform,
            &json!({"text":"a","operation":"uppercase","x":1}),
            &l,
        );
        assert_eq!(unknown.unwrap_err().code, "invalid_payload");

        let too_long =
            JobCommand::parse(JobKind::SimulateDelay, &json!({"duration_ms": 61_000}), &l);
        assert_eq!(too_long.unwrap_err().code, "out_of_range");

        let both = JobCommand::parse(
            JobKind::SimulateDelay,
            &json!({"duration_ms": 1, "seconds": 1}),
            &l,
        );
        assert_eq!(both.unwrap_err().code, "invalid_payload");

        let inverted = JobCommand::parse(JobKind::CpuCompute, &json!({"from": 10, "to": 5}), &l);
        assert_eq!(inverted.unwrap_err().code, "out_of_range");

        let huge = JobCommand::parse(
            JobKind::CpuCompute,
            &json!({"from": 0, "to": 900_000_000}),
            &l,
        );
        assert_eq!(huge.unwrap_err().code, "out_of_range");

        let empty_html = JobCommand::parse(JobKind::HtmlToPdf, &json!({"html": "  "}), &l);
        assert_eq!(empty_html.unwrap_err().code, "invalid_payload");

        let bad_op = JobCommand::parse(
            JobKind::TextTransform,
            &json!({"text":"a","operation":"rot13"}),
            &l,
        );
        assert_eq!(bad_op.unwrap_err().code, "invalid_payload");
    }

    #[test]
    fn enforces_payload_size() {
        let l = JobLimits {
            max_html_bytes: 10,
            ..JobLimits::default()
        };
        let err = JobCommand::parse(
            JobKind::HtmlToPdf,
            &json!({"html": "<p>0123456789</p>"}),
            &l,
        )
        .unwrap_err();
        assert_eq!(err.code, "payload_too_large");
        assert_eq!(err.field.as_deref(), Some("html"));
    }

    #[test]
    fn malformed_json_text_is_accepted_at_submission() {
        // Schema-valid (input is a string) -> accepted; the *handler* reports
        // invalid JSON as a typed job failure. See handlers/src/json.rs.
        let cmd = JobCommand::parse(JobKind::JsonFormat, &json!({"input": "{oops"}), &limits());
        assert!(cmd.is_ok());
    }
}
