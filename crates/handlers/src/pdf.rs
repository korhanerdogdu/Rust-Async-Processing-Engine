//! `html_to_pdf` – **external process + blocking file work**.
//!
//! Rendering is done by a real HTML engine: headless Chromium/Chrome/Edge
//! (`--print-to-pdf`). The browser runs as a child process managed with
//! `tokio::process`, so *waiting* for it is async (no Tokio worker is parked).
//!
//! Lifecycle guarantees:
//! * admission: a semaphore bounds concurrent browser processes (`PDF_MAX_CONCURRENCY`)
//! * timeout:   on deadline or cancellation the child is **killed and reaped**
//!   (`start_kill` + `wait`). `kill_on_drop(true)` is the backstop
//!   if this future is dropped (e.g. by the dispatcher's outer timeout).
//! * blocking:  hashing, PDF parsing (lopdf), `fsync` and `rename` use std::fs
//!   and run in `spawn_blocking` – bounded work, isolated from the
//!   async worker threads.
//!
//! Security (local demo defaults – see README "Security"):
//! * the HTML is prefixed with a CSP `<meta>` (`default-src 'none'`): no
//!   scripts, no remote/`file://` subresources, no iframes; only inline styles
//!   and `data:` images/fonts.
//! * all browser network traffic is routed to a dead proxy (`127.0.0.1:9`),
//!   including loopback (`<-loopback>`), so even a CSP bypass cannot fetch.
//! * a fresh throw-away profile per render; JS/extensions/sync disabled.
//! * the Chromium sandbox stays **enabled** unless `PDF_NO_SANDBOX=true` is set
//!   explicitly (needed only inside unprivileged containers; documented).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engine_core::{
    BoxFuture, HandlerError, JobCommand, JobContext, JobHandler, JobKind, JobOutput,
};
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::Semaphore;

use crate::artifacts::ArtifactStore;

pub const CSP_PREAMBLE: &str = concat!(
    "<!DOCTYPE html><meta charset=\"utf-8\">",
    "<meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; ",
    "style-src 'unsafe-inline'; img-src data:; font-src data:; frame-src 'none'; ",
    "form-action 'none'; base-uri 'none'\">"
);

#[derive(Debug, Clone)]
pub struct PdfConfig {
    /// Explicit browser path; auto-detected when `None`.
    pub browser: Option<PathBuf>,
    pub timeout: Duration,
    pub max_concurrency: usize,
    pub no_sandbox: bool,
    pub max_pdf_bytes: u64,
}

impl Default for PdfConfig {
    fn default() -> Self {
        Self {
            browser: None,
            timeout: Duration::from_secs(30),
            max_concurrency: 2,
            no_sandbox: false,
            max_pdf_bytes: 20 * 1024 * 1024,
        }
    }
}

/// Finds a Chromium-family browser in conventional locations.
pub fn detect_browser() -> Option<PathBuf> {
    let candidates: &[&str] = if cfg!(windows) {
        &[
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        ]
    } else if cfg!(target_os = "macos") {
        &[
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
        ]
    } else {
        &[
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/snap/bin/chromium",
        ]
    };
    candidates.iter().map(PathBuf::from).find(|p| p.is_file())
}

pub struct HtmlToPdfHandler {
    cfg: PdfConfig,
    browser: Option<PathBuf>,
    artifacts: ArtifactStore,
    admission: Arc<Semaphore>,
}

impl HtmlToPdfHandler {
    pub fn new(cfg: PdfConfig, artifacts: ArtifactStore) -> Self {
        let browser = cfg.browser.clone().or_else(detect_browser);
        let admission = Arc::new(Semaphore::new(cfg.max_concurrency.max(1)));
        Self {
            cfg,
            browser,
            artifacts,
            admission,
        }
    }

    pub fn browser(&self) -> Option<&Path> {
        self.browser.as_deref()
    }

    async fn render(&self, ctx: &JobContext, html: &str) -> Result<JobOutput, HandlerError> {
        let browser = self.browser.clone().ok_or_else(|| {
            HandlerError::failed(
                "renderer_unavailable",
                "no Chromium/Chrome/Edge executable found; set PDF_BROWSER_PATH",
                false,
            )
        })?;

        // Admission: wait for a renderer slot, but give up if cancelled.
        let _permit = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(HandlerError::Cancelled),
            p = self.admission.acquire() => p.map_err(|_| HandlerError::failed("renderer_closed", "renderer shut down", false))?,
        };

        let unique = format!(
            "{}-{}-{}",
            ctx.job_id,
            ctx.attempt,
            uuid::Uuid::new_v4().simple()
        );
        let workdir = self.artifacts.tmp_dir().join(format!("{unique}.work"));
        let tmp_pdf = self.artifacts.tmp_dir().join(format!("{unique}.part"));
        let input = workdir.join("input.html");

        let result = self
            .render_in(ctx, &browser, &workdir, &input, &tmp_pdf, html)
            .await;
        // Best-effort cleanup of the profile/workdir (Chrome may still hold a
        // file for a moment on Windows; leftovers are removed at next startup).
        let _ = tokio::fs::remove_dir_all(&workdir).await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp_pdf).await;
        }
        result
    }

    async fn render_in(
        &self,
        ctx: &JobContext,
        browser: &Path,
        workdir: &Path,
        input: &Path,
        tmp_pdf: &Path,
        html: &str,
    ) -> Result<JobOutput, HandlerError> {
        let io_err = |e: std::io::Error| HandlerError::failed("artifact_io", e.to_string(), true);
        tokio::fs::create_dir_all(workdir.join("profile"))
            .await
            .map_err(io_err)?;
        let mut document = String::with_capacity(CSP_PREAMBLE.len() + html.len());
        document.push_str(CSP_PREAMBLE);
        document.push_str(html);
        tokio::fs::write(input, document.as_bytes())
            .await
            .map_err(io_err)?;

        let mut cmd = Command::new(browser);
        cmd.arg("--headless")
            .arg("--disable-gpu")
            .arg("--no-first-run")
            .arg("--no-default-browser-check")
            .arg("--disable-extensions")
            .arg("--disable-background-networking")
            .arg("--disable-component-update")
            .arg("--disable-default-apps")
            .arg("--disable-sync")
            .arg("--disable-dev-shm-usage")
            .arg("--mute-audio")
            .arg("--hide-scrollbars")
            .arg("--no-pdf-header-footer")
            .arg("--proxy-server=http://127.0.0.1:9")
            .arg("--proxy-bypass-list=<-loopback>")
            .arg(format!(
                "--user-data-dir={}",
                workdir.join("profile").display()
            ))
            .arg(format!("--print-to-pdf={}", tmp_pdf.display()));
        if self.cfg.no_sandbox {
            cmd.arg("--no-sandbox");
        }
        cmd.arg(file_url(input))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let started = Instant::now();
        let mut child = cmd
            .spawn()
            .map_err(|e| HandlerError::failed("renderer_spawn_failed", e.to_string(), true))?;
        tracing::info!(
            event = "pdf.render_started",
            pid = child.id().unwrap_or(0),
            "browser process started"
        );

        // Drain stderr concurrently (bounded) so the child can never block on a
        // full pipe; keep only the tail for diagnostics.
        let mut stderr = child.stderr.take();
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(s) = stderr.as_mut() {
                let _ = s.take(64 * 1024).read_to_end(&mut buf).await;
            }
            String::from_utf8_lossy(&buf).into_owned()
        });

        let outcome = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => Err(HandlerError::Cancelled),
            r = tokio::time::timeout(self.cfg.timeout, child.wait()) => match r {
                Ok(Ok(status)) => Ok(status),
                Ok(Err(e)) => Err(HandlerError::failed("renderer_wait_failed", e.to_string(), true)),
                Err(_) => Err(HandlerError::Timeout),
            },
        };

        let status = match outcome {
            Ok(status) => status,
            Err(e) => {
                // Explicit kill + reap: a timed-out future does NOT stop a child
                // process by itself.
                let _ = child.start_kill();
                let _ = child.wait().await;
                stderr_task.abort();
                tracing::warn!(event = "pdf.render_aborted", reason = %e.code(), "browser process killed");
                return Err(e);
            }
        };
        let stderr_text = stderr_task.await.unwrap_or_default();
        let render_ms = started.elapsed().as_millis() as u64;

        if !status.success() {
            let tail: String = stderr_text
                .lines()
                .rev()
                .take(3)
                .collect::<Vec<_>>()
                .join(" | ");
            return Err(HandlerError::failed(
                "renderer_failed",
                format!("browser exited with {status}: {}", truncate(&tail, 300)),
                true,
            ));
        }

        // Blocking section: validate, parse, hash, fsync, rename.
        let artifacts = self.artifacts.clone();
        let tmp = tmp_pdf.to_path_buf();
        let max = self.cfg.max_pdf_bytes;
        let job_id = ctx.job_id;
        let (artifact, pages) = tokio::task::spawn_blocking(move || -> Result<_, HandlerError> {
            let bytes = std::fs::read(&tmp).map_err(|e| {
                HandlerError::failed("renderer_no_output", format!("no PDF produced: {e}"), true)
            })?;
            if bytes.len() as u64 > max {
                return Err(HandlerError::failed(
                    "artifact_too_large",
                    format!("PDF is {} bytes; limit {max}", bytes.len()),
                    false,
                ));
            }
            if !bytes.starts_with(b"%PDF-") {
                return Err(HandlerError::failed(
                    "renderer_invalid_output",
                    "output is not a PDF",
                    true,
                ));
            }
            let doc = lopdf::Document::load_mem(&bytes).map_err(|e| {
                HandlerError::failed(
                    "renderer_invalid_output",
                    format!("unparsable PDF: {e}"),
                    true,
                )
            })?;
            let pages = doc.get_pages().len();
            let artifact = artifacts
                .finalize_blocking(&tmp, job_id, "pdf", "application/pdf")
                .map_err(|e| HandlerError::failed("artifact_io", e.to_string(), true))?;
            Ok((artifact, pages))
        })
        .await
        .map_err(|e| HandlerError::failed("artifact_task_failed", e.to_string(), true))??;

        tracing::info!(
            event = "pdf.render_finished",
            render_ms,
            pages,
            size_bytes = artifact.size_bytes,
            "PDF rendered"
        );
        Ok(JobOutput {
            result: json!({
                "pages": pages,
                "size_bytes": artifact.size_bytes,
                "sha256": artifact.sha256,
                "render_ms": render_ms,
                "renderer": browser.file_name().map(|f| f.to_string_lossy().into_owned()),
                "content_type": "application/pdf",
            }),
            artifact: Some(artifact),
        })
    }
}

fn truncate(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

/// `file:` URL for a local absolute path (Windows drive paths included).
fn file_url(path: &Path) -> String {
    let s = path.display().to_string().replace('\\', "/");
    let s = s.trim_start_matches("//?/");
    let encoded: String = s
        .chars()
        .map(|c| match c {
            ' ' => "%20".to_string(),
            '#' => "%23".to_string(),
            '%' => "%25".to_string(),
            c => c.to_string(),
        })
        .collect();
    if encoded.starts_with('/') {
        format!("file://{encoded}")
    } else {
        format!("file:///{encoded}")
    }
}

impl JobHandler for HtmlToPdfHandler {
    fn kind(&self) -> JobKind {
        JobKind::HtmlToPdf
    }

    fn execute<'a>(
        &'a self,
        ctx: JobContext,
        command: JobCommand,
    ) -> BoxFuture<'a, Result<JobOutput, HandlerError>> {
        Box::pin(async move {
            let JobCommand::HtmlToPdf(params) = command else {
                return Err(HandlerError::Unsupported(command.kind()));
            };
            self.render(&ctx, &params.html).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_urls() {
        if cfg!(windows) {
            assert_eq!(
                file_url(Path::new(r"C:\a b\x.html")),
                "file:///C:/a%20b/x.html"
            );
        } else {
            assert_eq!(
                file_url(Path::new("/tmp/a b/x.html")),
                "file:///tmp/a%20b/x.html"
            );
        }
    }

    #[test]
    fn csp_blocks_everything_but_inline_styles_and_data() {
        assert!(CSP_PREAMBLE.contains("default-src 'none'"));
        assert!(!CSP_PREAMBLE.contains("script-src"));
    }
}
