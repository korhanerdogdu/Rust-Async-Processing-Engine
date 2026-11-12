//! Cross-process integration tests: real `api-gateway` + `worker-engine`
//! processes, real gRPC over TCP, real PostgreSQL/TimescaleDB.
//!
//! Run: `docker compose up -d; cargo build --workspace;
//!       cargo test -p e2e-tests -- --ignored --test-threads=4`
//! (or `scripts/test-e2e.ps1` / `scripts/test-e2e.sh`).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use e2e_tests::{EnvOptions, TestEnv};
use engine_ipc::pb;
use engine_ipc::pb::job_gateway_client::JobGatewayClient;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

async fn start(name: &str, worker_env: &[(&str, &str)]) -> TestEnv {
    TestEnv::start(
        name,
        EnvOptions {
            worker_env,
            ..EnvOptions::default()
        },
    )
    .await
}

fn secs(s: u64) -> Duration {
    Duration::from_secs(s)
}

async fn ws_messages(env: &TestEnv, id: &str) -> Vec<Value> {
    let (mut ws, _) = tokio_tungstenite::connect_async(env.ws_url(id))
        .await
        .expect("ws connect");
    let mut out = Vec::new();
    let deadline = Instant::now() + secs(30);
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline.into(), ws.next()).await {
        match msg.expect("ws frame") {
            Message::Text(t) => out.push(serde_json::from_str(&t).unwrap()),
            Message::Close(_) => break,
            _ => {}
        }
    }
    out
}

fn ws_status(m: &Value) -> &str {
    m["job"]["status"]
        .as_str()
        .or(m["event"]["status"].as_str())
        .unwrap_or("")
}

// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn rest_delay_job_lifecycle_is_persisted() {
    let env = start("lifecycle", &[]).await;
    let resp = env
        .client
        .post(format!("{}/api/v1/jobs", env.http))
        .json(&json!({"job_type": "simulate_delay", "payload": {"duration_ms": 700}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 202);
    let location = resp.headers()["location"].to_str().unwrap().to_string();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["job"]["status"], "queued");
    let id = body["job"]["job_id"].as_str().unwrap().to_string();
    assert_eq!(location, format!("/api/v1/jobs/{id}"));
    assert_eq!(
        body["job"]["links"]["result"],
        format!("/api/v1/jobs/{id}/result")
    );

    env.wait_status(&id, "processing", secs(5)).await;
    let (code, pending) = env.get_json(&format!("/api/v1/jobs/{id}/result")).await;
    assert_eq!(
        code, 409,
        "result must not be available while processing: {pending}"
    );
    assert_eq!(pending["error"]["details"]["job_status"], "processing");

    let done = env.wait_status(&id, "completed", secs(10)).await;
    assert_eq!(done["attempts"], 1);
    let (code, result) = env.get_json(&format!("/api/v1/jobs/{id}/result")).await;
    assert_eq!(code, 200);
    assert_eq!(result["result"]["requested_ms"], 700);

    // Source of truth: the database row and its audit trail.
    let (status, res): (String, Value) =
        sqlx::query_as("SELECT status, result FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&env.pool)
            .await
            .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(res["requested_ms"], 700);
    let (_, history) = env.get_json(&format!("/api/v1/jobs/{id}/history")).await;
    let path: Vec<&str> = history["transitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["to_status"].as_str().unwrap())
        .collect();
    assert_eq!(path, ["queued", "processing", "completed"]);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn single_active_job_shows_queueing() {
    let env = start("sequential", &[("MAX_ACTIVE_JOBS", "1")]).await;
    let mut handles = Vec::new();
    for _ in 0..4 {
        let (client, url) = (env.client.clone(), format!("{}/api/v1/jobs", env.http));
        handles.push(tokio::spawn(async move {
            let v: Value = client
                .post(url)
                .json(&json!({"job_type": "simulate_delay", "payload": {"duration_ms": 600}}))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v["job"]["job_id"].as_str().unwrap().to_string()
        }));
    }
    let mut ids = Vec::new();
    for h in handles {
        ids.push(h.await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut statuses = Vec::new();
    for id in &ids {
        statuses.push(env.job(id).await["status"].as_str().unwrap().to_string());
    }
    assert_eq!(
        statuses.iter().filter(|s| *s == "processing").count(),
        1,
        "{statuses:?}"
    );
    assert_eq!(
        statuses.iter().filter(|s| *s == "queued").count(),
        3,
        "{statuses:?}"
    );
    for id in &ids {
        env.wait_status(id, "completed", secs(15)).await;
    }
    // Execution intervals must not overlap.
    let overlaps: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs a JOIN jobs b ON a.id < b.id \
         WHERE a.started_at < b.finished_at AND b.started_at < a.finished_at",
    )
    .fetch_one(&env.pool)
    .await
    .unwrap();
    assert_eq!(
        overlaps, 0,
        "with MAX_ACTIVE_JOBS=1 no two jobs may run at the same time"
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn multiple_active_jobs_run_in_parallel() {
    let env = start("parallel", &[("MAX_ACTIVE_JOBS", "4")]).await;
    let started = Instant::now();
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(
            env.submit_ok("simulate_delay", json!({"duration_ms": 1500}))
                .await,
        );
    }
    for id in &ids {
        env.wait_status(id, "completed", secs(10)).await;
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(4000),
        "4 x 1.5 s with 4 slots took {elapsed:?} (sequential would be >= 6 s)"
    );
    let overlaps: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs a JOIN jobs b ON a.id < b.id \
         WHERE a.started_at < b.finished_at AND b.started_at < a.finished_at",
    )
    .fetch_one(&env.pool)
    .await
    .unwrap();
    assert_eq!(
        overlaps, 6,
        "all 4 jobs should overlap pairwise (C(4,2) = 6)"
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn rest_and_public_grpc_are_equivalent() {
    let env = start("rest_grpc", &[]).await;
    let payload = json!({"text": "Straße ünïcödé", "operation": "uppercase"});
    let rest_id = env.submit_ok("text_transform", payload.clone()).await;
    env.wait_status(&rest_id, "completed", secs(5)).await;
    let (_, rest_result) = env
        .get_json(&format!("/api/v1/jobs/{rest_id}/result"))
        .await;

    let mut grpc = JobGatewayClient::connect(env.grpc_public.clone())
        .await
        .unwrap();
    let resp = grpc
        .submit_job(pb::SubmitJobRequest {
            job_type: "text_transform".into(),
            payload_json: payload.to_string(),
            idempotency_key: None,
        })
        .await
        .unwrap()
        .into_inner();
    let grpc_id = resp.job.unwrap().job_id;
    env.wait_status(&grpc_id, "completed", secs(5)).await;
    let r = grpc
        .get_job_result(pb::GetJobResultRequest {
            job_id: grpc_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let grpc_result: Value = serde_json::from_str(&r.result_json).unwrap();
    assert_eq!(grpc_result, rest_result["result"]);
    assert_eq!(grpc_result["output"], "STRASSE ÜNÏCÖDÉ");

    // Same error semantics, different encodings.
    let bad = grpc
        .submit_job(pb::SubmitJobRequest {
            job_type: "simulate_delay".into(),
            payload_json: r#"{"duration_ms":999999}"#.into(),
            idempotency_key: None,
        })
        .await
        .unwrap_err();
    assert_eq!(bad.code(), tonic::Code::InvalidArgument);
    let missing = grpc
        .get_job(pb::GetJobRequest {
            job_id: uuid::Uuid::new_v4().to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);
    let (code, _) = env
        .submit("simulate_delay", json!({"duration_ms": 999999}))
        .await;
    assert_eq!(code, 422);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn websocket_pushes_transitions_from_worker_and_reconnect_gets_truth() {
    let env = start("websocket", &[]).await;
    let id = env
        .submit_ok("simulate_delay", json!({"duration_ms": 800}))
        .await;
    let msgs = ws_messages(&env, &id).await;
    assert_eq!(
        msgs[0]["type"], "snapshot",
        "first message must be the snapshot: {msgs:?}"
    );
    let statuses: Vec<&str> = msgs.iter().map(ws_status).collect();
    assert_eq!(*statuses.last().unwrap(), "completed", "{statuses:?}");
    assert!(
        statuses.contains(&"processing") || statuses[0] == "processing",
        "{statuses:?}"
    );
    // Versions strictly increase: no duplicates, no reordering.
    let versions: Vec<i64> = msgs
        .iter()
        .map(|m| {
            m["job"]["status_version"]
                .as_i64()
                .or(m["event"]["status_version"].as_i64())
                .unwrap()
        })
        .collect();
    assert!(versions.windows(2).all(|w| w[0] < w[1]), "{versions:?}");

    // Reconnect after completion: immediate snapshot of the current truth, then close.
    let again = ws_messages(&env, &id).await;
    assert_eq!(again.len(), 1);
    assert_eq!(again[0]["type"], "snapshot");
    assert_eq!(ws_status(&again[0]), "completed");
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn grpc_watch_job_streams_snapshot_and_events() {
    let env = start("grpc_watch", &[]).await;
    let id = env
        .submit_ok("simulate_delay", json!({"duration_ms": 500}))
        .await;
    let mut grpc = JobGatewayClient::connect(env.grpc_public.clone())
        .await
        .unwrap();
    let mut stream = grpc
        .watch_job(pb::WatchJobRequest { job_id: id.clone() })
        .await
        .unwrap()
        .into_inner();
    let mut kinds = Vec::new();
    let mut last_status = 0;
    while let Some(msg) = stream.message().await.unwrap() {
        match msg.kind.unwrap() {
            pb::watch_job_message::Kind::Snapshot(j) => {
                kinds.push("snapshot");
                last_status = j.status;
            }
            pb::watch_job_message::Kind::Event(e) => {
                kinds.push("event");
                last_status = e.status;
            }
        }
    }
    assert_eq!(kinds[0], "snapshot");
    assert_eq!(last_status, pb::JobStatus::Completed as i32);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn html_to_pdf_produces_a_real_pdf() {
    let env = start("pdf", &[]).await;
    let html = "<html><body><h1>Quarterly Report</h1><p>Generated by the Rust Async Processing Engine 4242</p></body></html>";
    let id = env.submit_ok("html_to_pdf", json!({"html": html})).await;
    let done = env.wait_status(&id, "completed", secs(60)).await;
    assert_eq!(
        done["links"]["artifact"],
        format!("/api/v1/jobs/{id}/artifact")
    );
    let (_, result) = env.get_json(&format!("/api/v1/jobs/{id}/result")).await;
    assert_eq!(result["artifact"]["content_type"], "application/pdf");

    let resp = env
        .client
        .get(format!("{}/api/v1/jobs/{id}/artifact", env.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.headers()["content-type"], "application/pdf");
    let bytes = resp.bytes().await.unwrap();
    assert!(bytes.starts_with(b"%PDF-"), "not a PDF");
    assert_eq!(
        bytes.len() as u64,
        result["artifact"]["size_bytes"].as_u64().unwrap()
    );
    let doc = lopdf::Document::load_mem(&bytes).expect("parsable PDF");
    assert!(!doc.get_pages().is_empty());
    let text = doc.extract_text(&[1]).unwrap_or_default();
    assert!(
        text.contains("Quarterly Report") && text.contains("4242"),
        "rendered text missing: {text:?}"
    );

    // Only metadata in PostgreSQL, bytes on disk.
    let (path, size): (String, i64) =
        sqlx::query_as("SELECT artifact_path, artifact_size_bytes FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&env.pool)
            .await
            .unwrap();
    assert_eq!(path, format!("{id}.pdf"));
    assert_eq!(size as usize, bytes.len());
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn cpu_compute_uses_rayon_and_is_deterministic() {
    let env = start("cpu", &[("RAYON_THREADS", "4")]).await;
    let id = env
        .submit_ok(
            "cpu_compute",
            json!({"from": 0, "to": 1_000_000, "chunk_size": 10_000}),
        )
        .await;
    env.wait_status(&id, "completed", secs(60)).await;
    let (_, r) = env.get_json(&format!("/api/v1/jobs/{id}/result")).await;
    assert_eq!(r["result"]["prime_count"], 78_498);
    assert_eq!(r["result"]["rayon_pool_threads"], 4);
    assert!(
        r["result"]["distinct_rayon_threads_used"].as_u64().unwrap() >= 2,
        "{}",
        r["result"]
    );
    // The gateway stayed responsive while CPU work ran in the worker.
    let (code, _) = env.get_json("/api/v1/health/live").await;
    assert_eq!(code, 200);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn timescale_hypertables_and_operational_logs_are_queryable() {
    let env = start("timescale", &[]).await;
    let id = env
        .submit_ok(
            "json_format",
            json!({"input": "{\"b\":1,\"a\":2}", "sort_keys": true}),
        )
        .await;
    env.wait_status(&id, "completed", secs(5)).await;

    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT hypertable_name::text FROM timescaledb_information.hypertables ORDER BY 1",
    )
    .fetch_all(&env.pool)
    .await
    .unwrap();
    assert_eq!(tables, ["operational_logs", "operational_metrics"]);

    // Both processes write log events for this job through their batch writers.
    let deadline = Instant::now() + secs(10);
    loop {
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT service, event FROM operational_logs WHERE job_id = $1::uuid ORDER BY time",
        )
        .bind(&id)
        .fetch_all(&env.pool)
        .await
        .unwrap();
        let services: HashSet<&str> = rows.iter().map(|(s, _)| s.as_str()).collect();
        let events: HashSet<&str> = rows.iter().filter_map(|(_, e)| e.as_deref()).collect();
        if services.contains("api-gateway")
            && services.contains("worker-engine")
            && events.contains("job.completed")
        {
            assert!(
                events.contains("job.accepted") && events.contains("job.processing"),
                "{events:?}"
            );
            break;
        }
        assert!(Instant::now() < deadline, "log rows not found: {rows:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // No user content leaks into logs.
    let leaked: i64 = sqlx::query_scalar("SELECT count(*) FROM operational_logs WHERE message LIKE '%\"b\":1%' OR attributes::text LIKE '%\"b\":1%'")
        .fetch_one(&env.pool)
        .await
        .unwrap();
    assert_eq!(leaked, 0);
    // Metric snapshots: queue depth + processing duration percentiles.
    let deadline = Instant::now() + secs(10);
    loop {
        let metrics: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT metric FROM operational_metrics WHERE service = 'worker-engine'",
        )
        .fetch_all(&env.pool)
        .await
        .unwrap();
        if metrics.iter().any(|m| m == "engine_queue_depth")
            && metrics.iter().any(|m| m == "job_processing_ms_p95")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "metric rows not found: {metrics:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn trace_context_survives_gateway_to_worker_ipc() {
    let env = start("tracing", &[]).await;
    let trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    let resp = env
        .client
        .post(format!("{}/api/v1/jobs", env.http))
        .header("traceparent", format!("00-{trace_id}-00f067aa0ba902b7-01"))
        .header("x-request-id", "e2e-trace-req-1")
        .json(&json!({"job_type": "text_transform", "payload": {"text": "abc", "operation": "reverse"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 202);
    assert_eq!(resp.headers()["x-request-id"], "e2e-trace-req-1");
    let body: Value = resp.json().await.unwrap();
    let id = body["job"]["job_id"].as_str().unwrap().to_string();
    // The worker (other process) recorded the client's trace id.
    assert_eq!(body["job"]["trace_id"], trace_id);
    env.wait_status(&id, "completed", secs(5)).await;
    let (db_trace, db_req): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT trace_id, request_id FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&env.pool)
            .await
            .unwrap();
    assert_eq!(db_trace.as_deref(), Some(trace_id));
    assert_eq!(db_req.as_deref(), Some("e2e-trace-req-1"));

    // Log rows of both processes carry the same trace id (incl. async execution).
    let deadline = Instant::now() + secs(10);
    loop {
        let services: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT service FROM operational_logs WHERE trace_id = $1")
                .bind(trace_id)
                .fetch_all(&env.pool)
                .await
                .unwrap();
        let exec: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM operational_logs WHERE trace_id = $1 AND event = 'job.completed'",
        )
        .bind(trace_id)
        .fetch_one(&env.pool)
        .await
        .unwrap();
        if services.len() == 2 && exec == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "trace-correlated logs missing: {services:?} exec={exec}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Optional: the trace is visible in Jaeger with spans from both processes.
    if let (Ok(jaeger), Ok(_)) = (
        std::env::var("E2E_JAEGER_URL"),
        std::env::var("E2E_OTLP_ENDPOINT"),
    ) {
        let deadline = Instant::now() + secs(20);
        loop {
            let v: Value = env
                .client
                .get(format!("{jaeger}/api/traces/{trace_id}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap_or_default();
            let names: HashSet<String> = v["data"][0]["processes"]
                .as_object()
                .map(|p| {
                    p.values()
                        .filter_map(|x| x["serviceName"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let ops: HashSet<String> = v["data"][0]["spans"]
                .as_array()
                .map(|s| {
                    s.iter()
                        .filter_map(|x| x["operationName"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if names.contains("api-gateway")
                && names.contains("worker-engine")
                && ops.iter().any(|o| o.starts_with("job.execute"))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "trace not in Jaeger: services={names:?} ops={ops:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn error_responses_are_consistent() {
    let env = start("errors", &[]).await;
    let post = |body: String| {
        let (c, u) = (env.client.clone(), format!("{}/api/v1/jobs", env.http));
        async move {
            c.post(u)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    };
    assert_eq!(post("{not json".into()).await.status().as_u16(), 400);
    let r = post(json!({"job_type": "fx_rates", "payload": {}}).to_string()).await;
    assert_eq!(r.status().as_u16(), 422);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["error"]["details"]["validation_code"], "unknown_job_type");
    assert_eq!(env.get_json("/api/v1/jobs/not-a-uuid").await.0, 400);
    assert_eq!(
        env.get_json(&format!("/api/v1/jobs/{}", uuid::Uuid::new_v4()))
            .await
            .0,
        404
    );
    assert_eq!(env.get_json("/api/v1/jobs?status=done").await.0, 422);

    // A failed job: typed error code, result endpoint explains why.
    let bad = env
        .submit_ok("json_format", json!({"input": "{\"a\": }"}))
        .await;
    let failed = env.wait_status(&bad, "failed", secs(5)).await;
    assert_eq!(failed["error_code"], "invalid_json");
    assert_eq!(failed["attempts"], 1, "invalid input is not retried");
    let (code, v) = env.get_json(&format!("/api/v1/jobs/{bad}/result")).await;
    assert_eq!(code, 409);
    assert_eq!(v["error"]["details"]["job_error_code"], "invalid_json");

    // Cancelling a terminal job is a conflict.
    let r = env
        .client
        .post(format!("{}/api/v1/jobs/{bad}/cancel", env.http))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 409);

    // Idempotency: same key + same body -> same job; different body -> 409.
    let submit_key = |text: &'static str| {
        let (c, u) = (env.client.clone(), format!("{}/api/v1/jobs", env.http));
        async move {
            c.post(u)
                .header("idempotency-key", "e2e-key-1")
                .json(&json!({"job_type": "text_transform", "payload": {"text": text, "operation": "lowercase"}}))
                .send()
                .await
                .unwrap()
        }
    };
    let a: Value = submit_key("ABC").await.json().await.unwrap();
    let b = submit_key("ABC").await;
    assert_eq!(
        b.headers()
            .get("idempotent-replayed")
            .map(|v| v.to_str().unwrap()),
        Some("true")
    );
    let b: Value = b.json().await.unwrap();
    assert_eq!(a["job"]["job_id"], b["job"]["job_id"]);
    assert_eq!(submit_key("XYZ").await.status().as_u16(), 409);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn full_queue_returns_429_and_persists_nothing() {
    let env = start(
        "queue_full",
        &[("QUEUE_CAPACITY", "1"), ("MAX_ACTIVE_JOBS", "1")],
    )
    .await;
    let first = env
        .submit_ok("simulate_delay", json!({"duration_ms": 3000}))
        .await;
    env.wait_status(&first, "processing", secs(5)).await;
    let _second = env
        .submit_ok("simulate_delay", json!({"duration_ms": 10}))
        .await; // occupies the only slot
    let resp = env
        .client
        .post(format!("{}/api/v1/jobs", env.http))
        .json(&json!({"job_type": "simulate_delay", "payload": {"duration_ms": 10}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 429);
    assert!(resp.headers().contains_key("retry-after"));
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["error"]["code"], "overloaded");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&env.pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "a rejected submission must not be persisted");
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn worker_down_gives_503_and_gateway_recovers_when_it_starts() {
    let mut env = TestEnv::start(
        "worker_down",
        EnvOptions {
            start_worker: false,
            ..EnvOptions::default()
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (code, v) = env
        .submit("simulate_delay", json!({"duration_ms": 1}))
        .await;
    assert_eq!(code, 503, "{v}");
    assert_eq!(v["error"]["code"], "unavailable");
    assert_eq!(env.get_json("/api/v1/health/ready").await.0, 503);
    assert_eq!(env.get_json("/api/v1/health/live").await.0, 200);

    // Start the worker: lazy channel reconnects, status stream re-subscribes.
    env.spawn_worker();
    env.wait_ready(secs(30)).await;
    let id = env
        .submit_ok("simulate_delay", json!({"duration_ms": 1}))
        .await;
    env.wait_status(&id, "completed", secs(5)).await;
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn database_down_gives_503_not_ready() {
    let dead_port = e2e_tests::free_port();
    let env = TestEnv::start(
        "db_down",
        EnvOptions {
            worker_database_url: Some(format!("postgres://engine:x@127.0.0.1:{dead_port}/none")),
            wait_ready: false,
            ..EnvOptions::default()
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (code, ready) = env.get_json("/api/v1/health/ready").await;
    assert_eq!(code, 503);
    assert_eq!(ready["worker"]["state"], "starting");
    let (code, v) = env
        .submit("simulate_delay", json!({"duration_ms": 1}))
        .await;
    assert_eq!(code, 503, "{v}");
    // A refused connect + pool acquire timeout can take a few seconds per try.
    let deadline = Instant::now() + secs(15);
    while !env.worker_log().contains("db.connect_failed") {
        assert!(
            Instant::now() < deadline,
            "worker should keep retrying the database:\n{}",
            env.worker_log()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn crashed_worker_jobs_are_recovered_after_restart() {
    let mut env = start("crash", &[("MAX_ACTIVE_JOBS", "1")]).await;
    let long = env
        .submit_ok("simulate_delay", json!({"duration_ms": 4000}))
        .await;
    let short1 = env
        .submit_ok("simulate_delay", json!({"duration_ms": 50}))
        .await;
    let short2 = env
        .submit_ok(
            "text_transform",
            json!({"text": "x", "operation": "uppercase"}),
        )
        .await;
    env.wait_status(&long, "processing", secs(5)).await;

    env.kill_worker(); // crash: no cleanup, the lease is left behind
    let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1::uuid")
        .bind(&long)
        .fetch_one(&env.pool)
        .await
        .unwrap();
    assert_eq!(
        status, "processing",
        "a crashed worker leaves the row in processing"
    );

    env.spawn_worker(); // same WORKER_ID -> recognizes its previous incarnation's lease
    env.wait_ready(secs(30)).await;
    let done = env.wait_status(&long, "completed", secs(20)).await;
    assert_eq!(
        done["attempts"], 2,
        "interrupted attempt counted, then re-executed (at-least-once)"
    );
    env.wait_status(&short1, "completed", secs(10)).await;
    env.wait_status(&short2, "completed", secs(10)).await;
    let reasons: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT reason FROM job_status_history WHERE job_id = $1::uuid ORDER BY id",
    )
    .bind(&long)
    .fetch_all(&env.pool)
    .await
    .unwrap();
    assert!(
        reasons
            .iter()
            .any(|r| r.as_deref() == Some("lease_recovered")),
        "{reasons:?}"
    );
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn graceful_shutdown_releases_running_job_and_restart_completes_it() {
    let mut env = start("graceful", &[("SHUTDOWN_GRACE_MS", "300")]).await;
    let id = env
        .submit_ok("simulate_delay", json!({"duration_ms": 6000}))
        .await;
    env.wait_status(&id, "processing", secs(5)).await;

    let worker = env.worker.take().unwrap();
    let log_path = worker.log.clone();
    let started = Instant::now();
    let status = worker
        .stop_gracefully(secs(15))
        .expect("worker must exit within the bounded shutdown");
    assert!(status.success(), "graceful exit code: {status:?}");
    assert!(started.elapsed() < secs(10));
    let log = std::fs::read_to_string(log_path).unwrap();
    assert!(
        log.contains("engine.shutdown_grace_exceeded") && log.contains("job.released"),
        "{}",
        e2e_tests::tail(&log, 30)
    );

    let (status, attempts): (String, i32) =
        sqlx::query_as("SELECT status, attempts FROM jobs WHERE id = $1::uuid")
            .bind(&id)
            .fetch_one(&env.pool)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), attempts),
        ("queued", 0),
        "released, attempt not counted"
    );

    env.spawn_worker();
    env.wait_ready(secs(30)).await;
    let done = env.wait_status(&id, "completed", secs(20)).await;
    assert_eq!(done["attempts"], 1);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn timeouts_are_retried_a_bounded_number_of_times() {
    let env = start(
        "timeout",
        &[("JOB_TIMEOUT_DELAY_MS", "200"), ("MAX_ATTEMPTS", "2")],
    )
    .await;
    let id = env
        .submit_ok("simulate_delay", json!({"duration_ms": 3000}))
        .await;
    let failed = env.wait_status(&id, "failed", secs(15)).await;
    assert_eq!(failed["error_code"], "timeout");
    assert_eq!(failed["attempts"], 2);
    // Transient failure that succeeds on retry.
    let flaky = env
        .submit_ok(
            "simulate_delay",
            json!({"duration_ms": 10, "fail_first_attempts": 1}),
        )
        .await;
    let ok = env.wait_status(&flaky, "completed", secs(15)).await;
    assert_eq!(ok["attempts"], 2);
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn cancelling_a_processing_job_stops_it() {
    let env = start("cancel", &[]).await;
    let id = env
        .submit_ok("simulate_delay", json!({"duration_ms": 20000}))
        .await;
    env.wait_status(&id, "processing", secs(5)).await;
    let r = env
        .client
        .post(format!("{}/api/v1/jobs/{id}/cancel", env.http))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["status"], "cancelled");
    // The slot frees up: a new job runs right away.
    let next = env
        .submit_ok(
            "text_transform",
            json!({"text": "go", "operation": "uppercase"}),
        )
        .await;
    env.wait_status(&next, "completed", secs(3)).await;
    env.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires `docker compose up -d` and `cargo build --workspace`"]
async fn concurrent_load_smoke() {
    let env = start(
        "load",
        &[("MAX_ACTIVE_JOBS", "8"), ("QUEUE_CAPACITY", "512")],
    )
    .await;
    let n = 300;
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(100));
    let started = Instant::now();
    let mut tasks = Vec::new();
    for i in 0..n {
        let (client, url, gate) = (
            env.client.clone(),
            format!("{}/api/v1/jobs", env.http),
            gate.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let _p = gate.acquire_owned().await.unwrap();
            let r = client
                .post(url)
                .json(&json!({"job_type": "text_transform", "payload": {"text": format!("item {i}"), "operation": "reverse"}}))
                .send()
                .await
                .unwrap();
            r.status().as_u16()
        }));
    }
    let mut accepted = 0;
    for t in tasks {
        if t.await.unwrap() == 202 {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, n,
        "all submissions accepted with a 512-slot queue"
    );
    let deadline = Instant::now() + secs(60);
    loop {
        let done: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status = 'completed'")
            .fetch_one(&env.pool)
            .await
            .unwrap();
        if done == n as i64 {
            break;
        }
        assert!(Instant::now() < deadline, "only {done}/{n} completed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!(
        "{n} jobs submitted and completed in {:?}",
        started.elapsed()
    );
    env.finish().await;
}
