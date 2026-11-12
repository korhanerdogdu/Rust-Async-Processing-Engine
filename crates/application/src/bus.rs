//! In-process status fan-out with `tokio::sync::broadcast`.
//!
//! *Why broadcast?* Many independent receivers (one per gateway status stream)
//! each need **every** transition. `broadcast` gives each receiver its own
//! cursor into a bounded ring buffer. A slow receiver does not slow the
//! publisher: it falls behind and gets `RecvError::Lagged(n)` – it then knows
//! it missed `n` events and must **re-read the truth from PostgreSQL** (the
//! gateway does exactly that). Events are hints; the database is the truth.
//!
//! This bus is process-local. It never crosses the process boundary: the
//! gateway receives events through the internal gRPC `SubscribeStatus` stream.

use engine_core::JobStatusEvent;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct StatusBus {
    tx: broadcast::Sender<JobStatusEvent>,
}

impl StatusBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(1));
        Self { tx }
    }

    /// Publishes a committed transition. `send` only fails when nobody is
    /// subscribed, which is fine – the state is already durable.
    pub fn publish(&self, event: JobStatusEvent) {
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<JobStatusEvent> {
        self.tx.subscribe()
    }

    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}
