//! Harness for cross-process integration tests.
//!
//! Every test gets:
//! * its **own PostgreSQL database** (`e2e_<name>_<rand>`) on the Compose
//!   TimescaleDB instance – created here, migrated by the worker on start;
//! * free TCP ports;
//! * two **real OS processes** – `worker-engine` and `api-gateway` – started
//!   from `target/debug`, talking gRPC over TCP, exactly as in production.
//!
//! Requirements: `docker compose up -d` and `cargo build --workspace` first
//! (the `scripts/test-e2e.*` helpers do both). Tests are `#[ignore]`d so a
//! plain `cargo test --workspace` stays Docker-free.
//!
//! Environment overrides:
//! * `E2E_ADMIN_DATABASE_URL` (default `postgres://engine:engine_dev_password@127.0.0.1:55432/postgres`)
//! * `E2E_OTLP_ENDPOINT` – export spans from the test processes (e.g. `http://127.0.0.1:4318`)
//! * `E2E_JAEGER_URL` – enables the Jaeger query assertion (e.g. `http://127.0.0.1:16686`)

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use sqlx::{Connection, PgConnection, PgPool};

pub fn admin_url() -> String {
    std::env::var("E2E_ADMIN_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://engine:engine_dev_password@127.0.0.1:55432/postgres".into())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

pub fn binary(name: &str) -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"));
    let path = target
        .join("debug")
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        path.is_file(),
        "{} not found – run `cargo build --workspace` before the e2e tests (scripts/test-e2e.ps1 does this)",
        path.display()
    );
    path
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A child process that is killed when dropped (tests never leak processes).
pub struct Proc {
    pub name: String,
    child: Child,
    pub log: PathBuf,
}

impl Proc {
    /// Graceful stop: close stdin (`SHUTDOWN_ON_STDIN_CLOSE=1`), wait for exit.
    pub fn stop_gracefully(mut self, within: Duration) -> Option<std::process::ExitStatus> {
        drop(self.child.stdin.take());
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    }

    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill(); // hard kill (TerminateProcess / SIGKILL)
        let _ = self.child.wait();
    }
}

pub struct TestEnv {
    pub name: String,
    pub db_name: String,
    pub db_url: String,
    pub http: String,
    pub grpc_public: String,
    pub worker_addr: String,
    pub client: reqwest::Client,
    pub pool: PgPool,
    worker_env: HashMap<String, String>,
    gateway_env: HashMap<String, String>,
    pub worker: Option<Proc>,
    pub gateway: Option<Proc>,
    workdir: PathBuf,
    worker_spawns: usize,
}

pub struct EnvOptions<'a> {
    pub worker_env: &'a [(&'a str, &'a str)],
    pub gateway_env: &'a [(&'a str, &'a str)],
    pub start_worker: bool,
    /// Override the worker's DATABASE_URL (e.g. an unreachable one).
    pub worker_database_url: Option<String>,
    pub wait_ready: bool,
}

impl Default for EnvOptions<'_> {
    fn default() -> Self {
        Self {
            worker_env: &[],
            gateway_env: &[],
            start_worker: true,
            worker_database_url: None,
            wait_ready: true,
        }
    }
}

impl TestEnv {
    pub async fn start(name: &str, opts: EnvOptions<'_>) -> Self {
        let db_name = format!(
            "e2e_{}_{}",
            name,
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        let admin = admin_url();
        let mut conn = PgConnection::connect(&admin).await.unwrap_or_else(|e| {
            panic!("cannot reach PostgreSQL at {admin}: {e} – is `docker compose up -d` running?")
        });
        sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.ok();
        let db_url = replace_db(&admin, &db_name);

        let workdir = workspace_root().join("target").join("e2e").join(&db_name);
        std::fs::create_dir_all(&workdir).unwrap();

        let (wport, hport, gport) = (free_port(), free_port(), free_port());
        let worker_addr = format!("127.0.0.1:{wport}");

        let mut worker_env: HashMap<String, String> = [
            (
                "DATABASE_URL",
                opts.worker_database_url
                    .clone()
                    .unwrap_or_else(|| db_url.clone()),
            ),
            ("WORKER_GRPC_ADDR", worker_addr.clone()),
            ("WORKER_ID", "worker-1".into()),
            (
                "ARTIFACT_DIR",
                workdir.join("artifacts").display().to_string(),
            ),
            ("RECONCILE_INTERVAL_MS", "200".into()),
            ("RETRY_BASE_MS", "100".into()),
            ("RETRY_MAX_MS", "500".into()),
            ("METRICS_FLUSH_MS", "500".into()),
            ("TOKIO_WORKER_THREADS", "4".into()),
            ("RAYON_THREADS", "4".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        for (k, v) in opts.worker_env {
            worker_env.insert(k.to_string(), v.to_string());
        }
        let mut gateway_env: HashMap<String, String> = [
            ("DATABASE_URL", db_url.clone()),
            ("GATEWAY_HTTP_ADDR", format!("127.0.0.1:{hport}")),
            ("GATEWAY_GRPC_ADDR", format!("127.0.0.1:{gport}")),
            ("WORKER_ENDPOINT", format!("http://{worker_addr}")),
            ("METRICS_FLUSH_MS", "500".into()),
            ("TOKIO_WORKER_THREADS", "4".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        for (k, v) in opts.gateway_env {
            gateway_env.insert(k.to_string(), v.to_string());
        }

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(&db_url)
            .await
            .unwrap();
        let mut env = TestEnv {
            name: name.to_string(),
            db_name,
            db_url,
            http: format!("http://127.0.0.1:{hport}"),
            grpc_public: format!("http://127.0.0.1:{gport}"),
            worker_addr,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
            pool,
            worker_env,
            gateway_env,
            worker: None,
            gateway: None,
            workdir,
            worker_spawns: 0,
        };
        if opts.start_worker {
            env.spawn_worker();
        }
        env.spawn_gateway();
        if opts.start_worker && opts.wait_ready {
            env.wait_ready(Duration::from_secs(30)).await;
        }
        env
    }

    fn spawn(&self, bin: &str, vars: &HashMap<String, String>, suffix: &str) -> Proc {
        let log = self.workdir.join(format!("{bin}{suffix}.log"));
        let out = std::fs::File::create(&log).unwrap();
        let err = out.try_clone().unwrap();
        let mut cmd = Command::new(binary(bin));
        cmd.current_dir(&self.workdir) // no stray .env from the repo root
            .env("RUST_LOG", "info,sqlx=warn")
            .env("SHUTDOWN_ON_STDIN_CLOSE", "1")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .stdin(Stdio::piped())
            .stdout(out)
            .stderr(err);
        if let Ok(otlp) = std::env::var("E2E_OTLP_ENDPOINT") {
            cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", otlp);
        }
        cmd.envs(vars);
        let child = cmd.spawn().unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
        Proc {
            name: bin.to_string(),
            child,
            log,
        }
    }

    pub fn spawn_worker(&mut self) {
        self.worker_spawns += 1;
        let suffix = format!("-{}", self.worker_spawns);
        self.worker = Some(self.spawn("worker-engine", &self.worker_env, &suffix));
    }

    pub fn spawn_gateway(&mut self) {
        self.gateway = Some(self.spawn("api-gateway", &self.gateway_env, ""));
    }

    /// Hard kill: simulates a crash (no graceful shutdown, no lease release).
    pub fn kill_worker(&mut self) {
        self.worker.take();
    }

    pub async fn wait_ready(&self, within: Duration) {
        let deadline = Instant::now() + within;
        loop {
            if let Ok(r) = self
                .client
                .get(format!("{}/api/v1/health/ready", self.http))
                .send()
                .await
            {
                if r.status().is_success() {
                    let v: Value = r.json().await.unwrap_or_default();
                    if v["status_stream"] == "connected" {
                        return;
                    }
                }
            }
            if Instant::now() > deadline {
                panic!(
                    "[{}] not ready within {within:?}\n--- worker log ---\n{}\n--- gateway log ---\n{}",
                    self.name,
                    self.worker_log(),
                    self.gateway_log()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn worker_log(&self) -> String {
        self.worker
            .as_ref()
            .map(|p| p.log_text())
            .unwrap_or_default()
    }

    pub fn gateway_log(&self) -> String {
        self.gateway
            .as_ref()
            .map(|p| p.log_text())
            .unwrap_or_default()
    }

    pub async fn submit(&self, job_type: &str, payload: Value) -> (u16, Value) {
        let r = self
            .client
            .post(format!("{}/api/v1/jobs", self.http))
            .json(&serde_json::json!({"job_type": job_type, "payload": payload}))
            .send()
            .await
            .expect("submit request");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// Submits and asserts 202; returns the job id.
    pub async fn submit_ok(&self, job_type: &str, payload: Value) -> String {
        let (status, body) = self.submit(job_type, payload).await;
        assert_eq!(status, 202, "submission failed: {body}");
        body["job"]["job_id"].as_str().unwrap().to_string()
    }

    pub async fn get_json(&self, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.http))
            .send()
            .await
            .expect("GET");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    pub async fn job(&self, id: &str) -> Value {
        let (status, v) = self.get_json(&format!("/api/v1/jobs/{id}")).await;
        assert_eq!(status, 200, "GET job: {v}");
        v
    }

    pub async fn wait_status(&self, id: &str, want: &str, within: Duration) -> Value {
        let deadline = Instant::now() + within;
        loop {
            let v = self.job(id).await;
            if v["status"] == want {
                return v;
            }
            if Instant::now() > deadline {
                panic!(
                    "[{}] job {id} is {} (wanted {want}) after {within:?}: {v}\n--- worker log tail ---\n{}",
                    self.name,
                    v["status"],
                    tail(&self.worker_log(), 40)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn ws_url(&self, id: &str) -> String {
        format!(
            "{}/api/v1/ws/jobs/{id}",
            self.http.replacen("http://", "ws://", 1)
        )
    }

    /// Stops processes and drops the test database.
    pub async fn finish(mut self) {
        self.gateway.take();
        self.worker.take();
        self.pool.close().await;
        if let Ok(mut conn) = PgConnection::connect(&admin_url()).await {
            let _ = sqlx::query(&format!(
                "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
                self.db_name
            ))
            .execute(&mut conn)
            .await;
        }
    }
}

fn replace_db(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let base = match base.rfind('/') {
        Some(i) if base[..i].contains("://") && i > base.find("://").unwrap() + 2 => &base[..i],
        _ => base,
    };
    match query {
        Some(q) => format!("{base}/{db}?{q}"),
        None => format!("{base}/{db}"),
    }
}

pub fn tail(s: &str, lines: usize) -> String {
    let v: Vec<&str> = s.lines().collect();
    v[v.len().saturating_sub(lines)..].join("\n")
}

#[cfg(test)]
mod tests {
    #[test]
    fn replaces_database_name() {
        assert_eq!(
            super::replace_db("postgres://u:p@h:1/postgres", "x"),
            "postgres://u:p@h:1/x"
        );
        assert_eq!(
            super::replace_db("postgres://u:p@h:1/postgres?sslmode=disable", "x"),
            "postgres://u:p@h:1/x?sslmode=disable"
        );
    }
}
