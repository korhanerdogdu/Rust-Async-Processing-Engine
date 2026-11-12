use std::time::Duration;

use engine_core::{JobKind, JobLimits};

/// Per-job-type execution deadlines enforced by the dispatcher.
#[derive(Debug, Clone)]
pub struct KindTimeouts {
    pub json_format: Duration,
    pub text_transform: Duration,
    pub simulate_delay: Duration,
    pub html_to_pdf: Duration,
    pub cpu_compute: Duration,
}

impl Default for KindTimeouts {
    fn default() -> Self {
        Self {
            json_format: Duration::from_secs(5),
            text_transform: Duration::from_secs(5),
            simulate_delay: Duration::from_secs(65),
            html_to_pdf: Duration::from_secs(45),
            cpu_compute: Duration::from_secs(120),
        }
    }
}

impl KindTimeouts {
    pub fn get(&self, kind: JobKind) -> Duration {
        match kind {
            JobKind::JsonFormat => self.json_format,
            JobKind::TextTransform => self.text_transform,
            JobKind::SimulateDelay => self.simulate_delay,
            JobKind::HtmlToPdf => self.html_to_pdf,
            JobKind::CpuCompute => self.cpu_compute,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Stable id of this worker (e.g. `worker-1`). Lease owner is
    /// `<worker_id>:<boot uuid>` so a restarted worker recognizes its own
    /// abandoned leases immediately.
    pub worker_id: String,
    /// Capacity of the bounded job MPSC channel.
    pub queue_capacity: usize,
    /// Semaphore permits: how many jobs may execute at once (default 1).
    pub max_active_jobs: usize,
    /// How long a submission may wait for queue capacity. Zero = fail fast.
    pub enqueue_timeout: Duration,
    pub max_attempts: u32,
    pub retry_base: Duration,
    pub retry_max: Duration,
    /// Added to the job timeout to form the lease duration.
    pub lease_grace: Duration,
    /// Period of the DB scanner/reconciler.
    pub reconcile_interval: Duration,
    /// Capacity of the status `broadcast` channel (per-receiver ring buffer).
    pub status_channel_capacity: usize,
    /// How long shutdown waits for running jobs before cancelling them.
    pub shutdown_grace: Duration,
    pub timeouts: KindTimeouts,
    pub limits: JobLimits,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            worker_id: "worker-1".into(),
            queue_capacity: 64,
            max_active_jobs: 1,
            enqueue_timeout: Duration::ZERO,
            max_attempts: 3,
            retry_base: Duration::from_millis(1000),
            retry_max: Duration::from_secs(30),
            lease_grace: Duration::from_secs(15),
            reconcile_interval: Duration::from_secs(2),
            status_channel_capacity: 1024,
            shutdown_grace: Duration::from_secs(10),
            timeouts: KindTimeouts::default(),
            limits: JobLimits::default(),
        }
    }
}

impl EngineConfig {
    /// Exponential backoff: base * 2^(attempt-1), capped.
    pub fn retry_backoff(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
        self.retry_base.saturating_mul(factor).min(self.retry_max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_exponential_and_capped() {
        let c = EngineConfig {
            retry_base: Duration::from_millis(100),
            retry_max: Duration::from_millis(1000),
            ..Default::default()
        };
        assert_eq!(c.retry_backoff(1), Duration::from_millis(100));
        assert_eq!(c.retry_backoff(2), Duration::from_millis(200));
        assert_eq!(c.retry_backoff(3), Duration::from_millis(400));
        assert_eq!(c.retry_backoff(10), Duration::from_millis(1000));
        assert_eq!(c.retry_backoff(u32::MAX), Duration::from_millis(1000));
    }
}
