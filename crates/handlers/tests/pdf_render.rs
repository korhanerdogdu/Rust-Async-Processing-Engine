//! Renders real HTML with the local Chromium-family browser.
//!
//! If no browser is installed the test prints a notice and returns early
//! (it cannot prove anything without a renderer). Set `PDF_BROWSER_PATH` to
//! point at a specific executable.

use std::path::PathBuf;
use std::time::Duration;

use engine_core::{HandlerError, HtmlToPdfParams, JobCommand, JobContext, JobHandler, JobId};
use engine_handlers::pdf::detect_browser;
use engine_handlers::{ArtifactStore, HtmlToPdfHandler, PdfConfig};
use tokio_util::sync::CancellationToken;

fn browser() -> Option<PathBuf> {
    std::env::var_os("PDF_BROWSER_PATH")
        .map(PathBuf::from)
        .or_else(detect_browser)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renders_a_real_parsable_pdf() {
    let Some(browser) = browser() else {
        eprintln!("SKIPPED: no Chromium/Chrome/Edge found (set PDF_BROWSER_PATH)");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = ArtifactStore::new(dir.path()).unwrap();
    let handler = HtmlToPdfHandler::new(
        PdfConfig {
            browser: Some(browser),
            timeout: Duration::from_secs(60),
            ..PdfConfig::default()
        },
        store.clone(),
    );
    let ctx = JobContext {
        job_id: JobId::generate(),
        attempt: 1,
        cancel: CancellationToken::new(),
    };
    let html = "<html><body><h1>Rust Async Processing Engine</h1><p>Hello PDF 12345</p>\
                <img src=\"http://example.com/tracker.png\"><script>document.body.innerHTML='pwned'</script></body></html>";
    let out = handler
        .execute(
            ctx,
            JobCommand::HtmlToPdf(HtmlToPdfParams { html: html.into() }),
        )
        .await
        .expect("render succeeds");

    let artifact = out.artifact.expect("artifact");
    let path = store.resolve(&artifact.relative_path).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.starts_with(b"%PDF-"), "missing %PDF header");
    assert_eq!(bytes.len() as u64, artifact.size_bytes);
    let doc = lopdf::Document::load_mem(&bytes).expect("parsable PDF");
    assert_eq!(doc.get_pages().len(), 1);
    assert_eq!(out.result["pages"], 1);
    // Chrome embeds the text with a ToUnicode map, so it is extractable.
    let text = doc.extract_text(&[1]).unwrap_or_default();
    println!("extracted text: {text:?}");
    assert!(
        text.contains("Hello PDF 12345"),
        "rendered text not found in PDF: {text:?}"
    );
    // The script must not have run (CSP), so the original heading survives.
    assert!(!text.contains("pwned"));
    // No temp leftovers.
    assert!(
        std::fs::read_dir(store.tmp_dir()).unwrap().all(|e| !e
            .unwrap()
            .path()
            .extension()
            .is_some_and(|x| x == "part"))
    );
}

#[tokio::test]
async fn cancelled_render_returns_cancelled() {
    let Some(browser) = browser() else {
        eprintln!("SKIPPED: no browser");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = ArtifactStore::new(dir.path()).unwrap();
    let handler = HtmlToPdfHandler::new(
        PdfConfig {
            browser: Some(browser),
            ..PdfConfig::default()
        },
        store,
    );
    let ctx = JobContext {
        job_id: JobId::generate(),
        attempt: 1,
        cancel: CancellationToken::new(),
    };
    ctx.cancel.cancel();
    let err = handler
        .execute(
            ctx,
            JobCommand::HtmlToPdf(HtmlToPdfParams {
                html: "<p>x</p>".into(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(err, HandlerError::Cancelled);
}

#[tokio::test]
async fn missing_browser_is_a_non_retryable_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = ArtifactStore::new(dir.path()).unwrap();
    let handler = HtmlToPdfHandler::new(
        PdfConfig {
            browser: Some(PathBuf::from("definitely-not-a-browser-xyz")),
            ..PdfConfig::default()
        },
        store,
    );
    let ctx = JobContext {
        job_id: JobId::generate(),
        attempt: 1,
        cancel: CancellationToken::new(),
    };
    let err = handler
        .execute(
            ctx,
            JobCommand::HtmlToPdf(HtmlToPdfParams {
                html: "<p>x</p>".into(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "renderer_spawn_failed");
}
