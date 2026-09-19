//! HTTP surface, split by role.
//!
//! * [`worker_router`] — the collector process: self `/metrics`, hot range
//!   queries, the cold-file surface, and the `GET /api/v1/status` contract.
//!   It owns the Fjall store (via the engine) and is the only writer.
//! * [`dashboard_router`] — the read-only frontend: serves `dist/` and the
//!   cold-file surface from the shared cold directory, and reverse-proxies
//!   the hot APIs + status to `COLLECTOR_UPSTREAM`. It never touches Fjall.
//!
//! The cold-file surface is identical on both roles: `serve` reads
//! `metrics_cold_*.parquet` locally so history keeps working while the
//! worker is down. `worker_router` keeps it too (direct access / backward
//! compatibility); the primary dashboard path is the local read.
//!
//! NOTE: deliberately *no* `track` middleware on either role. The worker
//! mirrors scraped samples into the same registry with an extra
//! `scrape_target` label, so self `http_*` series (3 labels) would collide
//! with scraped ones (4 labels) and panic the registry. Self-observability
//! is the `collector_*` outcome series instead.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::{Path as UrlPath, Query as UrlQuery, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use tower::ServiceExt as _;

use crate::query::{self, QueryError, RecentBuffer};
use crate::status::{StatusSnapshot, StatusTracker};

/// Cold export filename shape (`tonggeret::storage` convention).
const COLD_PREFIX: &str = "metrics_cold_";

/// Worker router: hot APIs, `/metrics`, cold surface, status contract.
///
/// `tracker` is updated on the scrape path; this router only reads it.
pub fn worker_router(
    cold_dir: PathBuf,
    buffer: Arc<RecentBuffer>,
    tracker: Arc<StatusTracker>,
) -> axum::Router {
    let status_ctx = StatusCtx {
        tracker,
        buffer: buffer.clone(),
        cold_dir: Arc::new(cold_dir.clone()),
    };
    axum::Router::new()
        .route("/api/files", files_route(cold_dir.clone()))
        .route(
            "/api/v1/query_range",
            get(query_range).with_state(buffer.clone()),
        )
        .route("/api/v1/labels", get(label_names).with_state(buffer))
        .route(
            "/api/v1/status",
            get(collector_status).with_state(status_ctx),
        )
        .route(
            "/telemetry/cold/{file}",
            get(serve_cold_file).with_state(Arc::new(cold_dir.clone())),
        )
        .route(
            "/metrics",
            get(tonggeret::middleware::axum::prometheus_handler),
        )
        .route("/healthz", get(healthz))
        .merge(tonggeret::middleware::axum::parquet_route(cold_dir))
}

/// Read-only dashboard router: `dist/` + local cold files, hot API/status
/// reverse-proxied to `upstream`. Never opens Fjall. Serves the cold surface
/// locally so history keeps working while the worker is unreachable.
pub fn dashboard_router(static_dir: PathBuf, cold_dir: PathBuf, upstream: &str) -> axum::Router {
    let proxy = ProxyCtx {
        client: reqwest::Client::new(),
        upstream: Arc::new(upstream.trim_end_matches('/').to_string()),
    };
    axum::Router::new()
        .route("/api/files", files_route(cold_dir.clone()))
        .route(
            "/api/v1/query_range",
            get(proxy_upstream).with_state(proxy.clone()),
        )
        .route(
            "/api/v1/labels",
            get(proxy_upstream).with_state(proxy.clone()),
        )
        .route("/api/v1/status", get(dashboard_status).with_state(proxy))
        .route(
            "/telemetry/cold/{file}",
            get(serve_cold_file).with_state(Arc::new(cold_dir.clone())),
        )
        .route("/healthz", get(healthz))
        .merge(tonggeret::middleware::axum::parquet_route(cold_dir))
        .fallback_service(tower_http::services::ServeDir::new(static_dir))
}

/// `/api/files` manifest from a cold directory (shared by both roles).
fn files_route(cold_dir: PathBuf) -> axum::routing::MethodRouter {
    let dir = Arc::new(cold_dir);
    get(move || {
        let dir = dir.clone();
        async move { Json(list_cold_files(&dir)) }
    })
}

/// Process-liveness endpoint for both roles (systemd / reverse-proxy health).
async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// State for the worker status handler: tracker + buffer occupancy + cold dir.
#[derive(Clone)]
struct StatusCtx {
    tracker: Arc<StatusTracker>,
    buffer: Arc<RecentBuffer>,
    cold_dir: Arc<PathBuf>,
}

/// `GET /api/v1/status` — worker health snapshot (see [`crate::status`]).
async fn collector_status(State(ctx): State<StatusCtx>) -> Json<StatusSnapshot> {
    let (samples, cap) = ctx.buffer.occupancy();
    let cold_files = list_cold_files(ctx.cold_dir.as_path()).len();
    Json(ctx.tracker.snapshot(samples, cap, cold_files))
}

/// Dashboard proxy state: HTTP client + worker base URL.
#[derive(Clone)]
struct ProxyCtx {
    client: reqwest::Client,
    upstream: Arc<String>,
}

/// Reverse-proxy `query_range` / `labels` to the worker. On any upstream
/// failure the dashboard answers 503 (Prometheus-shaped error body) instead
/// of hanging — the UI then falls back to cold Parquet.
async fn proxy_upstream(State(ctx): State<ProxyCtx>, req: Request) -> Response {
    let path_query = req.uri().path_and_query().map_or("", |p| p.as_str());
    let url = format!("{}{}", ctx.upstream, path_query);
    match ctx.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => passthrough(resp).await,
        Ok(resp) => gateway_error(&format!("upstream returned {}", resp.status())),
        Err(e) => gateway_error(&format!("collector offline: {e}")),
    }
}

/// Proxy the status endpoint, but always answer 200: when the worker is
/// unreachable the dashboard returns `{"status":"offline", ...}` so the UI
/// can render a status indicator instead of an error.
async fn dashboard_status(State(ctx): State<ProxyCtx>) -> Response {
    let url = format!("{}/api/v1/status", ctx.upstream);
    match ctx.client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.bytes().await {
            Ok(bytes) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(body) => Json(body).into_response(),
                Err(e) => offline_status(&format!("bad upstream status body: {e}")),
            },
            Err(e) => offline_status(&format!("upstream read failed: {e}")),
        },
        Ok(resp) => offline_status(&format!("upstream returned {}", resp.status())),
        Err(e) => offline_status(&format!("collector offline: {e}")),
    }
}

/// Copy an upstream response through (status + content-type + body).
async fn passthrough(resp: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp.headers().get(CONTENT_TYPE).cloned();
    match resp.bytes().await {
        Ok(bytes) => {
            let mut builder = Response::builder().status(status);
            if let Some(ct) = content_type {
                builder = builder.header(CONTENT_TYPE, ct);
            }
            builder
                .body(axum::body::Body::from(bytes))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err(e) => gateway_error(&format!("upstream read failed: {e}")),
    }
}

/// Prometheus-shaped 503 for a failed hot-query proxy.
fn gateway_error(detail: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "status": "error",
            "errorType": "unavailable",
            "error": detail,
        })),
    )
        .into_response()
}

/// Always-200 offline status body for the dashboard status endpoint.
fn offline_status(detail: &str) -> Response {
    Json(serde_json::json!({
        "status": "offline",
        "error": detail,
    }))
    .into_response()
}

/// Prometheus-shaped range query over recent samples:
/// `GET /api/v1/query_range?query=<name|{...}>&start=<unix>&end=<unix>&step=<dur>`.
/// Subset contract lives on [`crate::query`]; violations are 400, never truncation.
async fn query_range(
    State(buffer): State<Arc<RecentBuffer>>,
    UrlQuery(params): UrlQuery<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, QueryError> {
    if params.contains_key("match[]") {
        return Err(QueryError::bad_data(
            "match[] is not supported; put exact matchers in query={...}",
        ));
    }
    let get = |k: &str| params.get(k).map_or("", String::as_str);
    let rq = query::parse_range_query(
        get("query"),
        get("start"),
        get("end"),
        get("step"),
        query::now_micros(),
    )?;
    Ok(Json(query::render_matrix(&buffer.query(&rq)?)))
}

/// Distinct buffered metric names: `GET /api/v1/labels` →
/// `{"status":"success","data":[...]}`. Hot-mode metric picker source.
async fn label_names(State(buffer): State<Arc<RecentBuffer>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "success", "data": buffer.names() }))
}

/// Serve one cold file by name (Range-capable). The filename is
/// allowlisted to `metrics_cold_*.parquet` with no path separators, so a
/// crafted `:file` segment cannot escape the cold directory.
async fn serve_cold_file(
    State(dir): State<Arc<PathBuf>>,
    UrlPath(file): UrlPath<String>,
    req: Request,
) -> Response {
    if !is_cold_filename(&file) {
        return (StatusCode::NOT_FOUND, "unknown file").into_response();
    }
    tower_http::services::ServeFile::new(dir.join(file))
        .oneshot(req)
        .await
        .into_response()
}

/// Strict cold-filename check (used by the route above + purge/manifest).
#[must_use]
pub fn is_cold_filename(name: &str) -> bool {
    !name.is_empty()
        && name.starts_with(COLD_PREFIX)
        && name.ends_with(".parquet")
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
}

/// Absolute URLs of cold Parquet files (oldest first), for `/api/files`.
/// Mirrors the mock server's manifest shape.
#[must_use]
pub fn list_cold_files(cold_dir: &Path) -> Vec<String> {
    let mut files: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(cold_dir) {
        files = entries
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(COLD_PREFIX) && n.ends_with(".parquet"))
            .collect();
    }
    files.sort();
    files
        .iter()
        .filter(|n| is_cold_filename(n))
        .map(|n| format!("/telemetry/cold/{n}"))
        .collect()
}

/// Delete cold files older than `purge_days`; returns removals.
/// Non-parquet files are never touched.
pub fn purge_cold_files(cold_dir: &Path, purge_days: u64) -> usize {
    let cutoff = SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(
            purge_days.saturating_mul(24 * 3600),
        ))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    let mut entries = Vec::new();
    if let Ok(rd) = std::fs::read_dir(cold_dir) {
        entries = rd.filter_map(Result::ok).collect::<Vec<_>>();
    }
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_cold_filename(&name) {
            continue;
        }
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .is_ok_and(|t| t < cutoff);
        if old && std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    if removed > 0 {
        tracing::info!(removed, "purged expired cold parquet files");
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(dir: &Path, name: &str, age_secs: u64) {
        let path = dir.join(name);
        std::fs::write(&path, b"parquet-ish").unwrap();
        let old = SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(age_secs))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        filetime_set(&path, old);
    }

    // `std` cannot set mtimes; shell out to `touch -d` (tests run on unix).
    fn filetime_set(path: &Path, t: SystemTime) {
        let secs = t
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let status = std::process::Command::new("touch")
            .arg("-d")
            .arg(format!("@{secs}"))
            .arg(path)
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn manifest_lists_only_cold_parquet_sorted() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "metrics_cold_b.parquet", 10);
        touch(dir.path(), "metrics_cold_a.parquet", 10);
        touch(dir.path(), "notes.txt", 10);
        touch(dir.path(), "metrics_cold_tmp.part", 10);
        let listed = list_cold_files(dir.path());
        assert_eq!(
            listed,
            vec![
                "/telemetry/cold/metrics_cold_a.parquet".to_string(),
                "/telemetry/cold/metrics_cold_b.parquet".to_string(),
            ]
        );
        assert!(list_cold_files(Path::new("./does-not-exist")).is_empty());
    }

    #[test]
    fn cold_filename_allowlist() {
        assert!(is_cold_filename("metrics_cold_20240101T000000.parquet"));
        assert!(!is_cold_filename("metrics_cold_20240101T000000.parquet/.."));
        assert!(!is_cold_filename("../metrics_cold_x.parquet"));
        assert!(!is_cold_filename("metrics_cold_x.parquet%00"));
        assert!(!is_cold_filename("notes.txt"));
        assert!(!is_cold_filename(""));
        assert!(!is_cold_filename("metrics_cold_tmp.part"));
    }

    #[test]
    fn purge_removes_only_expired_cold_files() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "metrics_cold_old.parquet", 40 * 24 * 3600);
        touch(dir.path(), "metrics_cold_new.parquet", 10);
        touch(dir.path(), "keep.txt", 40 * 24 * 3600);
        assert_eq!(purge_cold_files(dir.path(), 32), 1);
        assert!(!dir.path().join("metrics_cold_old.parquet").exists());
        assert!(dir.path().join("metrics_cold_new.parquet").exists());
        assert!(dir.path().join("keep.txt").exists());
    }

    use crate::config::TargetConfig;
    use crate::query::{BufferedSample, RecentBuffer};
    use crate::scrape::ScrapeStatus;

    fn tracker_for(names: &[&str]) -> Arc<StatusTracker> {
        let targets: Vec<TargetConfig> = names
            .iter()
            .map(|n| TargetConfig {
                name: (*n).to_string(),
                url: format!("http://localhost/{n}/metrics"),
                allow: vec!["http_".to_string()],
            })
            .collect();
        Arc::new(StatusTracker::new(&targets, 15, 30))
    }

    fn seeded_buffer() -> Arc<RecentBuffer> {
        let buffer = Arc::new(RecentBuffer::new(100));
        buffer.push_batch(
            "app",
            &[BufferedSample {
                ts_micros: 1_700_000_000_000_000,
                name: "http_x".to_string(),
                value: 2.0,
                labels: vec![("scrape_target".to_string(), "app".to_string())],
            }],
        );
        buffer
    }

    fn worker_app(cold: &Path, buffer: Arc<RecentBuffer>) -> axum::Router {
        super::worker_router(cold.to_path_buf(), buffer, tracker_for(&["app"]))
    }

    async fn get(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let request = axum::http::Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn query_range_serves_buffered_matrix() {
        let dir = tempfile::tempdir().unwrap();
        let app = worker_app(dir.path(), seeded_buffer());
        let (status, body) = get(
            app,
            "/api/v1/query_range?query=http_x&start=1699999990&end=1700000010&step=60",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "matrix");
        assert_eq!(body["data"]["result"][0]["metric"]["__name__"], "http_x");
        assert_eq!(body["data"]["result"][0]["metric"]["scrape_target"], "app");
        assert_eq!(body["data"]["result"][0]["values"][0][1], "2");
    }

    #[tokio::test]
    async fn query_range_rejects_bad_params_as_prometheus_error() {
        let dir = tempfile::tempdir().unwrap();
        let app = worker_app(dir.path(), seeded_buffer());
        let (status, body) = get(
            app,
            "/api/v1/query_range?query=http_x&start=1&end=2&step=0s",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], "error");
    }

    #[tokio::test]
    async fn labels_lists_buffered_names() {
        let dir = tempfile::tempdir().unwrap();
        let app = worker_app(dir.path(), seeded_buffer());
        let (status, body) = get(app, "/api/v1/labels").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            serde_json::json!({"status": "success", "data": ["http_x"]})
        );
    }

    #[tokio::test]
    async fn status_endpoint_reports_collector_health() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "metrics_cold_20240101T000000.parquet", 10);
        let tracker = tracker_for(&["app"]);
        tracker.record("app", ScrapeStatus::Ok, 3, 9);

        let app = super::worker_router(dir.path().to_path_buf(), seeded_buffer(), tracker);
        let (status, body) = get(app, "/api/v1/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["interval_secs"], 15);
        assert_eq!(body["retention_days"], 30);
        assert_eq!(body["buffer"]["samples"], 1);
        assert_eq!(body["buffer"]["cap"], 100);
        assert_eq!(body["cold_files"], 1);
        assert_eq!(body["targets"][0]["name"], "app");
        assert_eq!(body["targets"][0]["last_status"], "ok");
        assert_eq!(body["targets"][0]["last_samples"], 3);
        assert_eq!(body["targets"][0]["consecutive_failures"], 0);
    }

    #[tokio::test]
    async fn healthz_answers_ok() {
        let dir = tempfile::tempdir().unwrap();
        let app = worker_app(dir.path(), seeded_buffer());
        let (status, body) = get(app, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    /// Spin a mock worker exposing `/api/v1/labels` + `/api/v1/query_range`.
    async fn spawn_mock_worker() -> String {
        async fn labels() -> Json<serde_json::Value> {
            Json(serde_json::json!({"status": "success", "data": ["http_x"]}))
        }
        let app = axum::Router::new().route("/api/v1/labels", axum::routing::get(labels));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn dashboard_proxies_hot_api_to_worker() {
        let dir = tempfile::tempdir().unwrap();
        let upstream = spawn_mock_worker().await;
        let app = super::dashboard_router(
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
            &upstream,
        );
        let (status, body) = get(app, "/api/v1/labels").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            serde_json::json!({"status": "success", "data": ["http_x"]})
        );
    }

    #[tokio::test]
    async fn dashboard_serves_cold_and_reports_offline_without_worker() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path(), "metrics_cold_20240101T000000.parquet", 10);
        // Port 1 is unreachable: the worker is "down".
        let app = super::dashboard_router(
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
            "http://127.0.0.1:1",
        );

        // Cold history keeps working locally.
        let (status, body) = get(app.clone(), "/api/files").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body[0],
            "/telemetry/cold/metrics_cold_20240101T000000.parquet"
        );

        // Status always answers 200, flagged offline.
        let (status, body) = get(app.clone(), "/api/v1/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "offline");

        // Hot queries surface 503 instead of hanging.
        let (status, body) = get(
            app,
            "/api/v1/query_range?query=http_x&start=1&end=2&step=60",
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "error");
    }
}
