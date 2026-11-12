//! The in-memory job queue: what travels through the bounded MPSC channel.
//!
//! ```text
//!  producer task A ─┐  (each holds its own cloned Sender)
//!  producer task B ─┼──► mpsc::channel::<JobEnvelope>(QUEUE_CAPACITY) ──► ONE Receiver (dispatcher task)
//!  reconciler task ─┘
//! ```
//!
//! The channel is **ephemeral**: it is lost on crash. Every envelope refers
//! to a row already committed as `queued` in PostgreSQL, so losing the channel
//! loses no work – the reconciler re-discovers queued rows.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use engine_core::{JobCommand, JobId};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// One unit of work in the channel. Fully owned: it is *moved* into the
/// channel by `send` and *moved* out by `recv` – no copies, no borrows, no
/// locks. That is why `JobCommand` owns its `String`s.
#[derive(Debug)]
pub struct JobEnvelope {
    pub job_id: JobId,
    pub command: JobCommand,
    /// W3C trace context of the submission, so execution joins the same trace.
    pub traceparent: Option<String>,
    /// When it entered the channel (in-memory queue wait).
    pub enqueued_at: Instant,
}

/// Job ids currently sitting in the channel or executing in this process.
/// The reconciler skips these to avoid enqueueing duplicates. (Duplicates
/// would still be harmless – the DB claim is atomic – just wasteful.)
#[derive(Default)]
pub struct InflightSet {
    // std Mutex: tiny critical sections, never held across `.await`.
    ids: Mutex<HashSet<JobId>>,
}

impl InflightSet {
    pub fn insert(&self, id: JobId) -> bool {
        self.ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id)
    }
    pub fn remove(&self, id: JobId) {
        self.ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
    }
    pub fn contains(&self, id: JobId) -> bool {
        self.ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&id)
    }
    pub fn len(&self) -> usize {
        self.ids.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Cancellation tokens of jobs currently executing, so `CancelJob` can stop a
/// running handler.
#[derive(Default)]
pub struct RunningJobs {
    tokens: Mutex<HashMap<JobId, CancellationToken>>,
}

impl RunningJobs {
    pub fn insert(&self, id: JobId, token: CancellationToken) {
        self.tokens
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, token);
    }
    pub fn remove(&self, id: JobId) {
        self.tokens
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
    }
    /// Returns true if a running job was signalled.
    pub fn cancel(&self, id: JobId) -> bool {
        match self
            .tokens
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
        {
            Some(t) => {
                t.cancel();
                true
            }
            None => false,
        }
    }
    pub fn ids(&self) -> Vec<JobId> {
        let mut v: Vec<_> = self
            .tokens
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .copied()
            .collect();
        v.sort();
        v
    }
}

/// RAII guard: removes a job from the in-flight bookkeeping when the job task
/// ends – including when it panics or its future is dropped.
pub(crate) struct InflightGuard {
    pub(crate) id: JobId,
    pub(crate) inflight: Arc<InflightSet>,
    pub(crate) running: Arc<RunningJobs>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.running.remove(self.id);
        self.inflight.remove(self.id);
    }
}
