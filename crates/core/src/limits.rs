use std::time::Duration;

/// Hard bounds applied during validation. Both processes load the same values
/// from the environment (see `.env.example`); the worker's copy is
/// authoritative because a client could bypass the gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobLimits {
    /// Max serialized size of a job payload (bytes).
    pub max_payload_bytes: usize,
    /// Max length of the JSON text in a `json_format` job (bytes).
    pub max_json_input_bytes: usize,
    /// Max length of text in a `text_transform` job (bytes).
    pub max_text_bytes: usize,
    /// Max duration of a `simulate_delay` job.
    pub max_delay: Duration,
    /// Max HTML size for `html_to_pdf` (bytes).
    pub max_html_bytes: usize,
    /// Max `to - from` for `cpu_compute`.
    pub max_cpu_span: u64,
    /// Max upper bound value for `cpu_compute` (keeps trial division bounded).
    pub max_cpu_value: u64,
    /// Max number of Rayon chunks a single CPU job may be split into.
    pub max_cpu_chunks: u64,
    /// Max length of a client idempotency key.
    pub max_idempotency_key_len: usize,
}

impl JobLimits {
    /// Builds limits from a key lookup (both binaries pass `std::env::var`),
    /// falling back to defaults. Pure: this crate never reads the
    /// environment itself.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let d = Self::default();
        fn num<T: std::str::FromStr>(
            get: &impl Fn(&str) -> Option<String>,
            key: &str,
            default: T,
        ) -> Result<T, String> {
            match get(key) {
                Some(v) if !v.trim().is_empty() => v
                    .trim()
                    .parse()
                    .map_err(|_| format!("{key}: invalid number '{v}'")),
                _ => Ok(default),
            }
        }
        Ok(Self {
            max_payload_bytes: num(&get, "MAX_PAYLOAD_BYTES", d.max_payload_bytes)?,
            max_json_input_bytes: num(&get, "MAX_JSON_INPUT_BYTES", d.max_json_input_bytes)?,
            max_text_bytes: num(&get, "MAX_TEXT_BYTES", d.max_text_bytes)?,
            max_delay: Duration::from_millis(num(
                &get,
                "MAX_DELAY_MS",
                d.max_delay.as_millis() as u64,
            )?),
            max_html_bytes: num(&get, "MAX_HTML_BYTES", d.max_html_bytes)?,
            max_cpu_span: num(&get, "MAX_CPU_SPAN", d.max_cpu_span)?,
            max_cpu_value: num(&get, "MAX_CPU_VALUE", d.max_cpu_value)?,
            max_cpu_chunks: num(&get, "MAX_CPU_CHUNKS", d.max_cpu_chunks)?,
            max_idempotency_key_len: d.max_idempotency_key_len,
        })
    }
}

impl Default for JobLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 512 * 1024,
            max_json_input_bytes: 256 * 1024,
            max_text_bytes: 64 * 1024,
            max_delay: Duration::from_secs(60),
            max_html_bytes: 256 * 1024,
            max_cpu_span: 50_000_000,
            max_cpu_value: 10_000_000_000,
            max_cpu_chunks: 4_096,
            max_idempotency_key_len: 128,
        }
    }
}
