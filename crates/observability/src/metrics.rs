//! A deliberately small, dependency-free metrics registry.
//!
//! * **Counters** and **gauges** are atomics: recording never blocks and never
//!   awaits.
//! * **Histograms** keep the last `WINDOW` observations (a sliding window)
//!   plus a lifetime count/sum. p50/p95/p99 are computed from *actual
//!   observations* in that window (nearest-rank), not estimated.
//!
//! The registry is shared via `Arc` and rendered in Prometheus text format at
//! `GET /api/v1/metrics`; a background task also snapshots it into the
//! TimescaleDB `operational_metrics` hypertable (see [`crate::sink`]).
//!
//! The `std::sync::Mutex` locks below are held only for a map lookup or a
//! `VecDeque` push – never across an `.await` – so a blocking mutex is the
//! right (and faster) choice than `tokio::sync::Mutex`.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const WINDOW: usize = 2048;

type Labels = Vec<(String, String)>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    name: String,
    labels: Labels,
}

impl Key {
    fn new(name: &str, labels: &[(&str, &str)]) -> Self {
        let mut labels: Labels = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        labels.sort();
        Self {
            name: name.to_string(),
            labels,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub struct Gauge(Arc<AtomicI64>);

impl Gauge {
    pub fn set(&self, v: i64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
struct HistogramInner {
    window: VecDeque<f64>,
    count: u64,
    sum: f64,
}

#[derive(Debug, Clone, Default)]
pub struct Histogram(Arc<Mutex<HistogramInner>>);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum: f64,
    pub window_len: usize,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

impl Histogram {
    pub fn observe(&self, value: f64) {
        let mut inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if inner.window.len() == WINDOW {
            inner.window.pop_front();
        }
        inner.window.push_back(value);
        inner.count += 1;
        inner.sum += value;
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let (mut values, count, sum) = {
            let inner = self.0.lock().unwrap_or_else(|p| p.into_inner());
            (
                inner.window.iter().copied().collect::<Vec<_>>(),
                inner.count,
                inner.sum,
            )
        };
        values.sort_by(|a, b| a.total_cmp(b));
        HistogramSnapshot {
            count,
            sum,
            window_len: values.len(),
            p50: percentile(&values, 0.50),
            p95: percentile(&values, 0.95),
            p99: percentile(&values, 0.99),
            max: values.last().copied().unwrap_or(0.0),
        }
    }
}

/// Nearest-rank percentile over sorted values.
pub fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

#[derive(Default)]
struct Inner {
    counters: BTreeMap<Key, Counter>,
    gauges: BTreeMap<Key, Gauge>,
    histograms: BTreeMap<Key, Histogram>,
}

/// One flattened metric value (used for the TimescaleDB snapshot).
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

#[derive(Default)]
pub struct MetricsRegistry {
    inner: Mutex<Inner>,
}

impl MetricsRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Counter {
        self.lock()
            .counters
            .entry(Key::new(name, labels))
            .or_insert_with(|| Counter(Arc::new(AtomicU64::new(0))))
            .clone()
    }

    pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Gauge {
        self.lock()
            .gauges
            .entry(Key::new(name, labels))
            .or_insert_with(|| Gauge(Arc::new(AtomicI64::new(0))))
            .clone()
    }

    pub fn histogram(&self, name: &str, labels: &[(&str, &str)]) -> Histogram {
        self.lock()
            .histograms
            .entry(Key::new(name, labels))
            .or_default()
            .clone()
    }

    /// Flatten into samples. Histograms become `<name>_count`, `_p50`, `_p95`,
    /// `_p99`, `_max`.
    pub fn samples(&self) -> Vec<MetricSample> {
        let inner = self.lock();
        let mut out = Vec::new();
        for (k, c) in &inner.counters {
            out.push(MetricSample {
                name: k.name.clone(),
                labels: k.labels.clone(),
                value: c.get() as f64,
            });
        }
        for (k, g) in &inner.gauges {
            out.push(MetricSample {
                name: k.name.clone(),
                labels: k.labels.clone(),
                value: g.get() as f64,
            });
        }
        for (k, h) in &inner.histograms {
            let s = h.snapshot();
            for (suffix, value) in [
                ("count", s.count as f64),
                ("p50", s.p50),
                ("p95", s.p95),
                ("p99", s.p99),
                ("max", s.max),
            ] {
                out.push(MetricSample {
                    name: format!("{}_{suffix}", k.name),
                    labels: k.labels.clone(),
                    value,
                });
            }
        }
        out
    }

    /// Prometheus text exposition format. Histograms are exposed as
    /// `summary` type with quantiles over the sliding window.
    pub fn render_prometheus(&self) -> String {
        let inner = self.lock();
        let mut out = String::new();
        let mut last_type: Option<String> = None;
        let mut type_line = |out: &mut String, name: &str, kind: &str| {
            if last_type.as_deref() != Some(name) {
                let _ = writeln!(out, "# TYPE {name} {kind}");
                last_type = Some(name.to_string());
            }
        };
        for (k, c) in &inner.counters {
            type_line(&mut out, &k.name, "counter");
            let _ = writeln!(out, "{}{} {}", k.name, fmt_labels(&k.labels, None), c.get());
        }
        for (k, g) in &inner.gauges {
            type_line(&mut out, &k.name, "gauge");
            let _ = writeln!(out, "{}{} {}", k.name, fmt_labels(&k.labels, None), g.get());
        }
        for (k, h) in &inner.histograms {
            type_line(&mut out, &k.name, "summary");
            let s = h.snapshot();
            for (q, v) in [("0.5", s.p50), ("0.95", s.p95), ("0.99", s.p99)] {
                let _ = writeln!(out, "{}{} {v}", k.name, fmt_labels(&k.labels, Some(q)));
            }
            let _ = writeln!(
                out,
                "{}_sum{} {}",
                k.name,
                fmt_labels(&k.labels, None),
                s.sum
            );
            let _ = writeln!(
                out,
                "{}_count{} {}",
                k.name,
                fmt_labels(&k.labels, None),
                s.count
            );
        }
        out
    }
}

fn fmt_labels(labels: &Labels, quantile: Option<&str>) -> String {
    let mut parts: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    if let Some(q) = quantile {
        parts.push(format!("quantile=\"{q}\""));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", parts.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_are_shared_handles() {
        let reg = MetricsRegistry::new();
        reg.counter("jobs_total", &[("kind", "a")]).inc();
        reg.counter("jobs_total", &[("kind", "a")]).add(2);
        assert_eq!(reg.counter("jobs_total", &[("kind", "a")]).get(), 3);
        let g = reg.gauge("depth", &[]);
        g.set(5);
        g.dec();
        assert_eq!(reg.gauge("depth", &[]).get(), 4);
    }

    #[test]
    fn percentiles_are_nearest_rank() {
        let values: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&values, 0.50), 50.0);
        assert_eq!(percentile(&values, 0.95), 95.0);
        assert_eq!(percentile(&values, 0.99), 99.0);
        assert_eq!(percentile(&[], 0.5), 0.0);
        assert_eq!(percentile(&[7.0], 0.99), 7.0);
    }

    #[test]
    fn histogram_window_is_bounded() {
        let h = Histogram::default();
        for i in 0..(WINDOW + 100) {
            h.observe(i as f64);
        }
        let s = h.snapshot();
        assert_eq!(s.count, (WINDOW + 100) as u64);
        assert_eq!(s.window_len, WINDOW);
        assert_eq!(s.max, (WINDOW + 99) as f64);
    }

    #[test]
    fn prometheus_rendering() {
        let reg = MetricsRegistry::new();
        reg.counter("requests_total", &[("method", "GET")]).inc();
        reg.histogram("latency_ms", &[]).observe(12.0);
        let text = reg.render_prometheus();
        assert!(text.contains("# TYPE requests_total counter"));
        assert!(text.contains("requests_total{method=\"GET\"} 1"));
        assert!(text.contains("latency_ms{quantile=\"0.99\"} 12"));
        assert!(text.contains("latency_ms_count 1"));
    }
}
