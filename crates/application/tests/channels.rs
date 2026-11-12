//! Focused, executable documentation of the four Tokio channel types and how
//! they differ. Each test states the property the engine relies on.

use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot, watch};

/// mpsc: many cloned Senders, exactly ONE Receiver, bounded capacity.
#[tokio::test]
async fn mpsc_bounded_multi_producer_single_consumer() {
    let (tx, mut rx) = mpsc::channel::<(usize, usize)>(4);

    // 5 producer *tasks*, each with its own Sender clone, 10 messages each.
    let mut producers = Vec::new();
    for p in 0..5 {
        let tx = tx.clone();
        producers.push(tokio::spawn(async move {
            for i in 0..10 {
                // `send().await` waits while the channel is full: backpressure.
                tx.send((p, i)).await.unwrap();
            }
        }));
    }
    drop(tx); // keep only the producers' clones alive

    // The single consumer. `recv()` returns None once every Sender is gone.
    let mut received = Vec::new();
    while let Some(msg) = rx.recv().await {
        received.push(msg);
    }
    for p in producers {
        p.await.unwrap();
    }
    assert_eq!(received.len(), 50);
    // Per-producer FIFO order is preserved (no global order across producers).
    for p in 0..5 {
        let seq: Vec<usize> = received
            .iter()
            .filter(|(q, _)| *q == p)
            .map(|(_, i)| *i)
            .collect();
        assert_eq!(seq, (0..10).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn mpsc_full_queue_policy_is_explicit() {
    let (tx, mut rx) = mpsc::channel::<u32>(2);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    // Third message: rejected immediately instead of waiting -> this is what
    // admission control turns into 429 RESOURCE_EXHAUSTED.
    assert!(matches!(
        tx.try_send(3),
        Err(mpsc::error::TrySendError::Full(3))
    ));

    // Reserve-then-send: capacity is claimed *before* doing other work.
    assert!(tx.try_reserve().is_err());
    assert_eq!(rx.recv().await, Some(1));
    let permit = tx.try_reserve().expect("one slot free now");
    permit.send(3); // cannot fail: the slot is ours
    assert_eq!(rx.recv().await, Some(2));
    assert_eq!(rx.recv().await, Some(3));

    // Receiver gone -> producers learn it (the engine maps this to UNAVAILABLE).
    drop(rx);
    assert!(matches!(
        tx.try_send(4),
        Err(mpsc::error::TrySendError::Closed(4))
    ));
}

#[test]
fn mpsc_receiver_cannot_be_cloned() {
    // This is a compile-time property; documented here:
    //   let (_tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
    //   let rx2 = rx.clone(); // error[E0599]: no method named `clone`
    // `Receiver` is not `Clone`, so a second consumer cannot exist by accident.
    fn is_clone<T: Clone>() {}
    is_clone::<mpsc::Sender<u8>>();
}

/// oneshot: exactly one value, exactly once – a reply slot.
#[tokio::test]
async fn oneshot_request_reply() {
    let (req_tx, mut req_rx) = mpsc::channel::<(u64, oneshot::Sender<u64>)>(8);
    let server = tokio::spawn(async move {
        while let Some((n, reply)) = req_rx.recv().await {
            let _ = reply.send(n * n);
        }
    });
    let (tx, rx) = oneshot::channel();
    req_tx.send((12, tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 144);

    // If the responder drops the Sender without replying, the waiter is told.
    let (tx, rx) = oneshot::channel::<u64>();
    drop(tx);
    assert!(rx.await.is_err());

    drop(req_tx);
    server.await.unwrap();
}

/// broadcast: every receiver gets every message – unless it lags behind.
#[tokio::test]
async fn broadcast_lag_and_reconnect_semantics() {
    let (tx, mut slow) = broadcast::channel::<u32>(2);
    let mut fast = tx.subscribe();
    for i in 1..=5 {
        tx.send(i).unwrap();
        assert_eq!(fast.recv().await.unwrap(), i);
    }
    // `slow` never read: the ring buffer kept only the last 2 messages.
    assert_eq!(
        slow.recv().await,
        Err(broadcast::error::RecvError::Lagged(3))
    );
    assert_eq!(slow.recv().await.unwrap(), 4);
    assert_eq!(slow.recv().await.unwrap(), 5);
    // On Lagged the gateway re-reads the current job state from PostgreSQL.

    // A new subscriber ("reconnect") sees only *future* messages – there is no
    // replay. That is why WebSocket clients get a DB snapshot first.
    let mut late = tx.subscribe();
    tx.send(6).unwrap();
    assert_eq!(late.recv().await.unwrap(), 6);
    assert!(matches!(
        late.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
}

/// watch: a single "latest value" cell; intermediate values may be skipped.
#[tokio::test]
async fn watch_keeps_only_the_latest_state() {
    let (tx, mut rx) = watch::channel("starting");
    assert_eq!(*rx.borrow(), "starting");

    tx.send("ready").unwrap();
    tx.send("draining").unwrap(); // overwrites "ready" before anyone looked
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow_and_update(), "draining");

    // No new value -> changed() would wait.
    let waited = tokio::time::timeout(Duration::from_millis(20), rx.changed()).await;
    assert!(waited.is_err());

    // Many readers, cheap reads, no queue: perfect for readiness/shutdown state.
    let rx2 = tx.subscribe();
    assert_eq!(*rx2.borrow(), "draining");
}
