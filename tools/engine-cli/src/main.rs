//! engine-cli – sample client for every public interface.
//!
//!   engine-cli submit simulate_delay '{"seconds":2}' [--grpc] [--wait]
//!   engine-cli get <job_id>
//!   engine-cli watch <job_id>                 # WebSocket push
//!   engine-cli grpc-watch <job_id>            # public gRPC server stream
//!   engine-cli pdf --html '<h1>Hi</h1>' --out out.pdf
//!   engine-cli demo-sequential --count 5 --seconds 5
//!   engine-cli load --count 100 --concurrency 100 --job-type simulate_delay --payload '{"duration_ms":100}' --wait
//!   engine-cli stats

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use engine_ipc::pb;
use engine_ipc::pb::job_gateway_client::JobGatewayClient;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
#[command(
    name = "engine-cli",
    about = "Client for the Rust Async Processing Engine"
)]
struct Cli {
    #[arg(
        long = "http-url",
        env = "ENGINE_HTTP",
        default_value = "http://127.0.0.1:8080",
        global = true
    )]
    http_url: String,
    #[arg(
        long = "grpc-url",
        env = "ENGINE_GRPC",
        default_value = "http://127.0.0.1:50051",
        global = true
    )]
    grpc_url: String,
    /// Label sent as `x-client-name` on REST requests so the server logs show
    /// which machine sent what. Defaults to this computer's name.
    #[arg(long = "client-name", env = "ENGINE_CLIENT_NAME", global = true)]
    client_name: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Submit one job (REST by default, --grpc for the public gRPC API).
    Submit {
        job_type: String,
        payload: String,
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long)]
        grpc: bool,
        /// Watch over WebSocket until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Fetch a job and (if completed) its result.
    Get { job_id: String },
    /// Follow a job over WebSocket.
    Watch { job_id: String },
    /// Follow a job over the public gRPC WatchJob stream.
    GrpcWatch { job_id: String },
    /// Render HTML to PDF and download it.
    Pdf {
        #[arg(long)]
        html: String,
        #[arg(long, default_value = "out.pdf")]
        out: String,
    },
    /// N concurrent delay jobs; prints a status timeline (shows queueing).
    DemoSequential {
        #[arg(long, default_value_t = 5)]
        count: usize,
        #[arg(long, default_value_t = 5.0)]
        seconds: f64,
    },
    /// Load test: submit `count` jobs with `concurrency` in flight.
    Load {
        #[arg(long, default_value_t = 100)]
        count: usize,
        #[arg(long, default_value_t = 100)]
        concurrency: usize,
        #[arg(long, default_value = "simulate_delay")]
        job_type: String,
        #[arg(long, default_value = r#"{"duration_ms":100}"#)]
        payload: String,
        /// Poll until every accepted job is terminal and report end-to-end stats.
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300)]
        wait_timeout_secs: u64,
    },
    /// Worker queue/runtime statistics.
    Stats,
}

type AnyResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> AnyResult<()> {
    let client_name = cli
        .client_name
        .clone()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .filter(|s| !s.trim().is_empty());
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(name) = &client_name {
        if let Ok(v) = reqwest::header::HeaderValue::from_str(name.trim()) {
            headers.insert("x-client-name", v);
        }
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .default_headers(headers)
        .build()?;
    match cli.cmd {
        Cmd::Submit {
            job_type,
            payload,
            idempotency_key,
            grpc,
            wait,
        } => {
            let payload: Value = serde_json::from_str(&payload)?;
            let id = if grpc {
                let mut client = JobGatewayClient::connect(cli.grpc_url.clone()).await?;
                let resp = client
                    .submit_job(pb::SubmitJobRequest {
                        job_type,
                        payload_json: payload.to_string(),
                        idempotency_key,
                    })
                    .await?
                    .into_inner();
                let job = resp.job.ok_or("no job")?;
                println!(
                    "gRPC SubmitJob -> job_id={} status={:?} deduplicated={}",
                    job.job_id,
                    pb::JobStatus::try_from(job.status),
                    resp.deduplicated
                );
                job.job_id
            } else {
                let mut body = json!({"job_type": job_type, "payload": payload});
                if let Some(k) = idempotency_key {
                    body["idempotency_key"] = json!(k);
                }
                let resp = http
                    .post(format!("{}/api/v1/jobs", cli.http_url))
                    .json(&body)
                    .send()
                    .await?;
                let status = resp.status();
                let v: Value = resp.json().await?;
                println!("HTTP {status}\n{}", serde_json::to_string_pretty(&v)?);
                if !status.is_success() {
                    return Ok(());
                }
                v["job"]["job_id"].as_str().unwrap_or_default().to_string()
            };
            if wait {
                watch_ws(&cli.http_url, &id, true).await?;
                get(&http, &cli.http_url, &id).await?;
            }
        }
        Cmd::Get { job_id } => get(&http, &cli.http_url, &job_id).await?,
        Cmd::Watch { job_id } => {
            watch_ws(&cli.http_url, &job_id, true).await?;
        }
        Cmd::GrpcWatch { job_id } => {
            let mut client = JobGatewayClient::connect(cli.grpc_url.clone()).await?;
            let mut stream = client
                .watch_job(pb::WatchJobRequest { job_id })
                .await?
                .into_inner();
            while let Some(msg) = stream.message().await? {
                println!(
                    "{}  {:?}",
                    chrono::Utc::now().format("%H:%M:%S%.3f"),
                    msg.kind
                );
            }
            println!("stream ended");
        }
        Cmd::Pdf { html, out } => {
            let html = if std::path::Path::new(&html).is_file() {
                std::fs::read_to_string(&html)?
            } else {
                html
            };
            let v: Value = http
                .post(format!("{}/api/v1/jobs", cli.http_url))
                .json(&json!({"job_type": "html_to_pdf", "payload": {"html": html}}))
                .send()
                .await?
                .json()
                .await?;
            let id = v["job"]["job_id"]
                .as_str()
                .ok_or_else(|| format!("submission failed: {v}"))?
                .to_string();
            println!("submitted {id}");
            watch_ws(&cli.http_url, &id, true).await?;
            let resp = http
                .get(format!("{}/api/v1/jobs/{id}/artifact", cli.http_url))
                .send()
                .await?;
            if !resp.status().is_success() {
                return Err(format!(
                    "download failed: HTTP {} {}",
                    resp.status(),
                    resp.text().await?
                )
                .into());
            }
            let bytes = resp.bytes().await?;
            std::fs::write(&out, &bytes)?;
            println!(
                "wrote {} bytes to {out} (header: {:?})",
                bytes.len(),
                String::from_utf8_lossy(&bytes[..bytes.len().min(8)])
            );
        }
        Cmd::DemoSequential { count, seconds } => {
            demo_sequential(&http, &cli.http_url, count, seconds).await?
        }
        Cmd::Load {
            count,
            concurrency,
            job_type,
            payload,
            wait,
            wait_timeout_secs,
        } => {
            load(
                &http,
                &cli.http_url,
                count,
                concurrency,
                &job_type,
                &payload,
                wait,
                Duration::from_secs(wait_timeout_secs),
            )
            .await?
        }
        Cmd::Stats => {
            let v: Value = http
                .get(format!("{}/api/v1/engine/stats", cli.http_url))
                .send()
                .await?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
    }
    Ok(())
}

async fn get(http: &reqwest::Client, base: &str, id: &str) -> AnyResult<()> {
    let job: Value = http
        .get(format!("{base}/api/v1/jobs/{id}"))
        .send()
        .await?
        .json()
        .await?;
    println!("job:\n{}", serde_json::to_string_pretty(&job)?);
    let resp = http
        .get(format!("{base}/api/v1/jobs/{id}/result"))
        .send()
        .await?;
    let status = resp.status();
    let v: Value = resp.json().await?;
    println!(
        "result (HTTP {status}):\n{}",
        serde_json::to_string_pretty(&v)?
    );
    Ok(())
}

fn ws_url(http_base: &str, id: &str) -> String {
    let base = http_base
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    format!("{base}/api/v1/ws/jobs/{id}")
}

async fn watch_ws(http_base: &str, id: &str, print: bool) -> AnyResult<Vec<Value>> {
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url(http_base, id)).await?;
    let mut msgs = Vec::new();
    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t)?;
                if print {
                    let status = v["job"]["status"]
                        .as_str()
                        .or(v["event"]["status"].as_str())
                        .unwrap_or("?");
                    println!(
                        "{}  ws {:<8} status={status}",
                        chrono::Utc::now().format("%H:%M:%S%.3f"),
                        v["type"].as_str().unwrap_or("?")
                    );
                }
                msgs.push(v);
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(msgs)
}

async fn demo_sequential(
    http: &reqwest::Client,
    base: &str,
    count: usize,
    seconds: f64,
) -> AnyResult<()> {
    println!("submitting {count} x simulate_delay({seconds}s) concurrently...");
    let t0 = Instant::now();
    let mut submits = Vec::new();
    for _ in 0..count {
        let http = http.clone();
        let base = base.to_string();
        submits.push(tokio::spawn(async move {
            let v: Value = http
                .post(format!("{base}/api/v1/jobs"))
                .json(&json!({"job_type": "simulate_delay", "payload": {"seconds": seconds}}))
                .send()
                .await?
                .json()
                .await?;
            v["job"]["job_id"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("rejected: {v}").into())
        }));
    }
    let mut ids = Vec::new();
    for s in submits {
        let r: AnyResult<String> = s.await?;
        ids.push(r?);
    }
    println!(
        "all {} accepted (202) after {:?}; watching over WebSocket:\n",
        ids.len(),
        t0.elapsed()
    );
    let mut watchers = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        let base = base.to_string();
        let id = id.clone();
        watchers.push(tokio::spawn(async move {
            let (mut ws, _) = tokio_tungstenite::connect_async(ws_url(&base, &id)).await?;
            while let Some(msg) = ws.next().await {
                if let Message::Text(t) = msg? {
                    let v: Value = serde_json::from_str(&t)?;
                    let status = v["job"]["status"]
                        .as_str()
                        .or(v["event"]["status"].as_str())
                        .unwrap_or("?")
                        .to_string();
                    println!(
                        "t={:>6.2}s  job#{i} {}  {:<8} -> {status}",
                        t0.elapsed().as_secs_f64(),
                        &id[..8],
                        v["type"].as_str().unwrap_or("")
                    );
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }));
    }
    for w in watchers {
        w.await??;
    }
    println!("\nall done after {:.2}s", t0.elapsed().as_secs_f64());
    Ok(())
}

fn pct(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn summary(name: &str, mut v: Vec<f64>) {
    v.sort_by(|a, b| a.total_cmp(b));
    println!(
        "{name:<22} n={:<6} p50={:>9.1}ms p95={:>9.1}ms p99={:>9.1}ms max={:>9.1}ms",
        v.len(),
        pct(&v, 0.5),
        pct(&v, 0.95),
        pct(&v, 0.99),
        v.last().copied().unwrap_or(0.0)
    );
}

#[allow(clippy::too_many_arguments)]
async fn load(
    http: &reqwest::Client,
    base: &str,
    count: usize,
    concurrency: usize,
    job_type: &str,
    payload: &str,
    wait: bool,
    wait_timeout: Duration,
) -> AnyResult<()> {
    let payload: Value = serde_json::from_str(payload)?;
    let gate = Arc::new(Semaphore::new(concurrency.max(1)));
    let t0 = Instant::now();
    let mut tasks = Vec::with_capacity(count);
    for _ in 0..count {
        let (http, base, gate) = (http.clone(), base.to_string(), gate.clone());
        let body = json!({"job_type": job_type, "payload": payload});
        tasks.push(tokio::spawn(async move {
            let _p = gate.acquire_owned().await;
            let s = Instant::now();
            let res = http
                .post(format!("{base}/api/v1/jobs"))
                .json(&body)
                .send()
                .await;
            let ms = s.elapsed().as_secs_f64() * 1000.0;
            match res {
                Ok(r) => {
                    let code = r.status().as_u16();
                    let v: Value = r.json().await.unwrap_or(Value::Null);
                    (code, ms, v["job"]["job_id"].as_str().map(str::to_string))
                }
                Err(_) => (0u16, ms, None),
            }
        }));
    }
    let mut codes: BTreeMap<u16, usize> = BTreeMap::new();
    let mut lat = Vec::new();
    let mut ids = Vec::new();
    for t in tasks {
        let (code, ms, id) = t.await?;
        *codes.entry(code).or_default() += 1;
        lat.push(ms);
        if let Some(id) = id {
            ids.push(id);
        }
    }
    let submit_secs = t0.elapsed().as_secs_f64();
    println!(
        "submitted {count} {job_type} jobs, concurrency {concurrency}, in {submit_secs:.2}s ({:.1} req/s)",
        count as f64 / submit_secs
    );
    println!("HTTP status codes: {codes:?}   (0 = transport error)");
    summary("submit latency", lat);
    if !wait || ids.is_empty() {
        return Ok(());
    }
    let deadline = Instant::now() + wait_timeout;
    let mut pending: Vec<String> = ids.clone();
    let mut finished: Vec<Value> = Vec::new();
    while !pending.is_empty() && Instant::now() < deadline {
        let mut still = Vec::new();
        for id in pending {
            let v: Value = http
                .get(format!("{base}/api/v1/jobs/{id}"))
                .send()
                .await?
                .json()
                .await?;
            match v["status"].as_str() {
                Some("completed" | "failed" | "cancelled") => finished.push(v),
                _ => still.push(id),
            }
        }
        pending = still;
        if !pending.is_empty() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let total_secs = t0.elapsed().as_secs_f64();
    let ts = |v: &Value, k: &str| {
        v[k].as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
    };
    let mut by_status: BTreeMap<String, usize> = BTreeMap::new();
    let (mut e2e, mut qwait, mut proc_ms) = (Vec::new(), Vec::new(), Vec::new());
    for v in &finished {
        *by_status
            .entry(v["status"].as_str().unwrap_or("?").to_string())
            .or_default() += 1;
        if let (Some(c), Some(q), Some(s), Some(f)) = (
            ts(v, "created_at"),
            ts(v, "queued_at"),
            ts(v, "started_at"),
            ts(v, "finished_at"),
        ) {
            e2e.push((f - c).num_microseconds().unwrap_or(0) as f64 / 1000.0);
            qwait.push((s - q).num_microseconds().unwrap_or(0) as f64 / 1000.0);
            proc_ms.push((f - s).num_microseconds().unwrap_or(0) as f64 / 1000.0);
        }
    }
    println!(
        "terminal: {by_status:?}, still pending after timeout: {}; wall clock {total_secs:.2}s, completion throughput {:.1} jobs/s",
        pending.len(),
        finished.len() as f64 / total_secs
    );
    summary("queue wait", qwait);
    summary("processing", proc_ms);
    summary("end-to-end (DB)", e2e);
    Ok(())
}
