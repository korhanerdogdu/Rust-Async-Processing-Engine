//! Dispatcher behavior with the in-memory store (no Docker needed).

use std::sync::Arc;
use std::time::Duration;

use engine_application::{
    EngineConfig, EngineHandle, HandlerRegistry, InMemoryJobStore, KindTimeouts, SubmitRequest,
};
use engine_core::{
    EngineError, JobHandler, JobId, JobKind, JobStatus, JobStore, LeaseInfo, NewJob,
};
use engine_handlers::{DelayHandler, JsonFormatHandler, TextTransformHandler};
use engine_observability::MetricsRegistry;
use serde_json::{Value, json};

fn registry() -> HandlerRegistry {
    HandlerRegistry::new([
        Arc::new(DelayHandler) as Arc<dyn JobHandler>,
        Arc::new(TextTransformHandler),
        Arc::new(JsonFormatHandler),
    ])
}

fn config() -> EngineConfig {
    EngineConfig {
        queue_capacity: 16,
        max_active_jobs: 1,
        retry_base: Duration::from_millis(10),
        retry_max: Duration::from_millis(20),
        reconcile_interval: Duration::from_millis(25),
        shutdown_grace: Duration::from_secs(2),
        ..EngineConfig::default()
    }
}

fn start(cfg: EngineConfig) -> (EngineHandle<InMemoryJobStore>, Arc<InMemoryJobStore>) {
    let store = Arc::new(InMemoryJobStore::new());
    (
        EngineHandle::start(store.clone(), registry(), cfg, MetricsRegistry::new(), None),
        store,
    )
}

fn req(kind: JobKind, payload: Value) -> SubmitRequest {
    SubmitRequest {
        kind,
        payload,
        idempotency_key: None,
        request_id: None,
    }
}

fn delay(ms: u64) -> SubmitRequest {
    req(JobKind::SimulateDelay, json!({ "duration_ms": ms }))
}

async fn wait_status(
    store: &InMemoryJobStore,
    id: JobId,
    want: JobStatus,
    within: Duration,
) -> engine_core::JobRecord {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let rec = store.get_job(id).await.unwrap().unwrap();
        if rec.status == want {
            return rec;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {id} still {} (wanted {want})",
            rec.status
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_active_job_processes_strictly_one_at_a_time() {
    let (engine, store) = start(config());
    let mut events = engine.subscribe();

    // 4 concurrent producer tasks.
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            e.submit(delay(80)).await.unwrap().record.id
        }));
    }
    let mut ids = Vec::new();
    for t in tasks {
        ids.push(t.await.unwrap());
    }

    // Shortly after: exactly one processing, the rest queued.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let stats = engine.stats();
    assert_eq!(stats.active_jobs, 1);
    let mut statuses = Vec::new();
    for id in &ids {
        statuses.push(store.get_job(*id).await.unwrap().unwrap().status);
    }
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == JobStatus::Processing)
            .count(),
        1,
        "{statuses:?}"
    );
    assert_eq!(
        statuses.iter().filter(|s| **s == JobStatus::Queued).count(),
        3,
        "{statuses:?}"
    );

    for id in &ids {
        wait_status(&store, *id, JobStatus::Completed, Duration::from_secs(5)).await;
    }

    // From the event stream: concurrency never exceeded 1.
    let (mut current, mut peak) = (0i32, 0i32);
    while let Ok(ev) = events.try_recv() {
        match ev.status {
            JobStatus::Processing => current += 1,
            JobStatus::Completed | JobStatus::Failed => current -= 1,
            _ => {}
        }
        peak = peak.max(current);
    }
    assert_eq!(peak, 1);

    // Execution intervals do not overlap.
    let mut recs = Vec::new();
    for id in &ids {
        recs.push(store.get_job(*id).await.unwrap().unwrap());
    }
    recs.sort_by_key(|r| r.started_at);
    for w in recs.windows(2) {
        assert!(w[1].started_at.unwrap() >= w[0].finished_at.unwrap());
    }
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_active_jobs_run_in_parallel_with_one_receiver() {
    let (engine, store) = start(EngineConfig {
        max_active_jobs: 4,
        ..config()
    });
    let mut events = engine.subscribe();
    let started = tokio::time::Instant::now();
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(engine.submit(delay(300)).await.unwrap().record.id);
    }
    for id in &ids {
        wait_status(&store, *id, JobStatus::Completed, Duration::from_secs(5)).await;
    }
    let elapsed = started.elapsed();
    // Sequential would take >= 1200 ms.
    assert!(elapsed < Duration::from_millis(900), "took {elapsed:?}");

    let (mut current, mut peak) = (0i32, 0i32);
    while let Ok(ev) = events.try_recv() {
        match ev.status {
            JobStatus::Processing => current += 1,
            JobStatus::Completed => current -= 1,
            _ => {}
        }
        peak = peak.max(current);
    }
    assert_eq!(peak, 4);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_producers_all_jobs_complete() {
    let (engine, store) = start(EngineConfig {
        queue_capacity: 64,
        max_active_jobs: 8,
        ..config()
    });
    let mut tasks = Vec::new();
    for i in 0..40 {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            e.submit(req(
                JobKind::TextTransform,
                json!({"text": format!("job {i}"), "operation": "uppercase"}),
            ))
            .await
            .unwrap()
            .record
            .id
        }));
    }
    for t in tasks {
        let id = t.await.unwrap();
        let rec = wait_status(&store, id, JobStatus::Completed, Duration::from_secs(5)).await;
        assert!(
            rec.result.unwrap()["output"]
                .as_str()
                .unwrap()
                .starts_with("JOB ")
        );
    }
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_queue_rejects_without_persisting() {
    let (engine, store) = start(EngineConfig {
        queue_capacity: 1,
        max_active_jobs: 1,
        ..config()
    });
    let a = engine.submit(delay(500)).await.unwrap().record.id;
    wait_status(&store, a, JobStatus::Processing, Duration::from_secs(2)).await;
    // The consumer now waits for a permit, so B stays in the channel...
    let b = engine.submit(delay(10)).await.unwrap().record.id;
    // ...and C finds the bounded channel full.
    let err = engine.submit(delay(10)).await.unwrap_err();
    assert!(matches!(err, EngineError::Overloaded { .. }), "{err:?}");
    let all = store
        .list_jobs(engine_core::ListFilter {
            status: None,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 2, "rejected job must not be persisted");
    wait_status(&store, b, JobStatus::Completed, Duration::from_secs(3)).await;
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_are_bounded() {
    let (engine, store) = start(EngineConfig {
        max_attempts: 3,
        ..config()
    });
    // Fails once, then succeeds on attempt 2.
    let ok = engine
        .submit(req(
            JobKind::SimulateDelay,
            json!({"duration_ms": 5, "fail_first_attempts": 1}),
        ))
        .await
        .unwrap()
        .record
        .id;
    let rec = wait_status(&store, ok, JobStatus::Completed, Duration::from_secs(5)).await;
    assert_eq!(rec.attempts, 2);

    // Always fails: gives up after max_attempts.
    let bad = engine
        .submit(req(
            JobKind::SimulateDelay,
            json!({"duration_ms": 5, "fail_first_attempts": 9}),
        ))
        .await
        .unwrap()
        .record
        .id;
    let rec = wait_status(&store, bad, JobStatus::Failed, Duration::from_secs(5)).await;
    assert_eq!(rec.attempts, 3);
    assert_eq!(rec.error_code.as_deref(), Some("simulated_failure"));

    let history = engine.history(bad).await.unwrap();
    let path: Vec<_> = history.iter().map(|h| h.to_status.as_str()).collect();
    assert_eq!(
        path,
        [
            "queued",
            "processing",
            "queued",
            "processing",
            "queued",
            "processing",
            "failed"
        ]
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_input_fails_without_retry_and_timeouts_are_enforced() {
    let timeouts = KindTimeouts {
        simulate_delay: Duration::from_millis(50),
        ..KindTimeouts::default()
    };
    let (engine, store) = start(EngineConfig {
        max_attempts: 2,
        timeouts,
        ..config()
    });

    let bad_json = engine
        .submit(req(JobKind::JsonFormat, json!({"input": "{broken"})))
        .await
        .unwrap()
        .record
        .id;
    let rec = wait_status(&store, bad_json, JobStatus::Failed, Duration::from_secs(3)).await;
    assert_eq!(rec.error_code.as_deref(), Some("invalid_json"));
    assert_eq!(rec.attempts, 1, "invalid input is not retried");

    let slow = engine.submit(delay(5_000)).await.unwrap().record.id;
    let rec = wait_status(&store, slow, JobStatus::Failed, Duration::from_secs(3)).await;
    assert_eq!(rec.error_code.as_deref(), Some("timeout"));
    assert_eq!(
        rec.attempts, 2,
        "timeouts are retried, but only max_attempts times"
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_queued_and_processing_jobs() {
    let (engine, store) = start(config());
    let running = engine.submit(delay(10_000)).await.unwrap().record.id;
    let queued = engine.submit(delay(10)).await.unwrap().record.id;
    wait_status(
        &store,
        running,
        JobStatus::Processing,
        Duration::from_secs(2),
    )
    .await;

    assert_eq!(
        engine.cancel(queued).await.unwrap().status,
        JobStatus::Cancelled
    );
    let started = tokio::time::Instant::now();
    assert_eq!(
        engine.cancel(running).await.unwrap().status,
        JobStatus::Cancelled
    );
    // The running handler is actually stopped: the slot frees up quickly.
    while engine.stats().active_jobs > 0 {
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "handler did not stop"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Cancelling again is an invalid state; unknown ids are not found.
    assert!(matches!(
        engine.cancel(running).await,
        Err(EngineError::InvalidState { .. })
    ));
    assert!(matches!(
        engine.cancel(JobId::generate()).await,
        Err(EngineError::NotFound(_))
    ));
    // The queued envelope is skipped by the claim; status stays cancelled.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        store.get_job(queued).await.unwrap().unwrap().status,
        JobStatus::Cancelled
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_recovers_jobs_abandoned_by_previous_incarnation() {
    let store = Arc::new(InMemoryJobStore::new());
    // Simulate the previous worker process: it accepted two jobs and was
    // processing one of them when it crashed.
    let mk = |ms: u64| NewJob {
        id: JobId::generate(),
        kind: JobKind::SimulateDelay,
        payload: json!({"duration_ms": ms, "fail_first_attempts": 0}),
        max_attempts: 3,
        idempotency_key: None,
        request_id: None,
        trace_id: None,
        traceparent: None,
    };
    let (j1, j2) = (mk(10), mk(10));
    let (id1, id2) = (j1.id, j2.id);
    store.insert_job(j1).await.unwrap();
    store.insert_job(j2).await.unwrap();
    let old_lease = LeaseInfo {
        owner: "worker-1:previousboot".into(),
        duration: Duration::from_secs(3600),
    };
    store.claim_job(id1, &old_lease).await.unwrap().unwrap();

    // New incarnation with the same worker id.
    let engine = EngineHandle::start(
        store.clone(),
        registry(),
        config(),
        MetricsRegistry::new(),
        None,
    );
    let r1 = wait_status(&store, id1, JobStatus::Completed, Duration::from_secs(3)).await;
    let r2 = wait_status(&store, id2, JobStatus::Completed, Duration::from_secs(3)).await;
    assert_eq!(r1.attempts, 2, "interrupted attempt counted, then re-run");
    assert_eq!(r2.attempts, 1);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_lease_of_last_attempt_fails_instead_of_looping() {
    let store = Arc::new(InMemoryJobStore::new());
    let job = NewJob {
        id: JobId::generate(),
        kind: JobKind::SimulateDelay,
        payload: json!({"duration_ms": 10, "fail_first_attempts": 0}),
        max_attempts: 1,
        idempotency_key: None,
        request_id: None,
        trace_id: None,
        traceparent: None,
    };
    let id = job.id;
    store.insert_job(job).await.unwrap();
    let lease = LeaseInfo {
        owner: "other-worker:x".into(),
        duration: Duration::from_secs(60),
    };
    store.claim_job(id, &lease).await.unwrap().unwrap();
    store.expire_lease(id);

    let engine = EngineHandle::start(
        store.clone(),
        registry(),
        config(),
        MetricsRegistry::new(),
        None,
    );
    let rec = wait_status(&store, id, JobStatus::Failed, Duration::from_secs(3)).await;
    assert_eq!(rec.error_code.as_deref(), Some("attempts_exhausted"));
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_outage_rejects_submission_cleanly() {
    let (engine, store) = start(config());
    store.set_unavailable(true);
    let err = engine.submit(delay(1)).await.unwrap_err();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    // The reserved queue slot was released.
    assert_eq!(engine.stats().queue_depth, 0);
    store.set_unavailable(false);
    let id = engine.submit(delay(1)).await.unwrap().record.id;
    wait_status(&store, id, JobStatus::Completed, Duration::from_secs(2)).await;
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_and_idempotency() {
    let (engine, _store) = start(config());
    let err = engine
        .submit(req(JobKind::SimulateDelay, json!({"duration_ms": "soon"})))
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Validation(_)));

    let with_key = |payload: Value| SubmitRequest {
        kind: JobKind::TextTransform,
        payload,
        idempotency_key: Some("order-42".into()),
        request_id: None,
    };
    let first = engine
        .submit(with_key(json!({"text": "a", "operation": "uppercase"})))
        .await
        .unwrap();
    let again = engine
        .submit(with_key(json!({"text": "a", "operation": "uppercase"})))
        .await
        .unwrap();
    assert!(!first.deduplicated);
    assert!(again.deduplicated);
    assert_eq!(first.record.id, again.record.id);
    let conflict = engine
        .submit(with_key(json!({"text": "b", "operation": "uppercase"})))
        .await
        .unwrap_err();
    assert_eq!(conflict, EngineError::IdempotencyConflict);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_releases_unfinished_jobs() {
    let (engine, store) = start(EngineConfig {
        shutdown_grace: Duration::from_millis(100),
        ..config()
    });
    let long = engine.submit(delay(30_000)).await.unwrap().record.id;
    let waiting = engine.submit(delay(10)).await.unwrap().record.id;
    wait_status(&store, long, JobStatus::Processing, Duration::from_secs(2)).await;

    let started = tokio::time::Instant::now();
    engine.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "shutdown must be bounded"
    );

    let rec = store.get_job(long).await.unwrap().unwrap();
    assert_eq!(
        rec.status,
        JobStatus::Queued,
        "released for the next incarnation"
    );
    assert_eq!(rec.attempts, 0, "the interrupted attempt is not counted");
    assert_eq!(
        store.get_job(waiting).await.unwrap().unwrap().status,
        JobStatus::Queued
    );
    // New submissions are refused while/after draining.
    assert!(matches!(
        engine.submit(delay(1)).await,
        Err(EngineError::Unavailable(_))
    ));
}
