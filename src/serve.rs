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
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{any, delete, get, post};
use serde::Deserialize;
use tower::ServiceExt as _;

use crate::query::{self, QueryError, RecentBuffer};
use crate::queue::{JobQueue, QueueError, QueueSummary};
use crate::status::{QueueInfo, StatusSnapshot, StatusTracker};
use crate::targets::{Mode, Target, TargetError, TargetRegistry};

/// Cold export filename shape (`tonggeret::storage` convention).
const COLD_PREFIX: &str = "metrics_cold_";

/// Worker router: hot APIs, `/metrics`, cold surface, status contract, and
/// the mutating control API (`/api/v1/targets`, `/api/v1/queue`).
///
/// `tracker` / `registry` / `queue` are updated by the executor; this router
/// reads and mutates them. `control_token`, when set, is required on every
/// mutating endpoint via the `x-control-token` header.
pub fn worker_router(
    cold_dir: PathBuf,
    buffer: Arc<RecentBuffer>,
    tracker: Arc<StatusTracker>,
    registry: Arc<TargetRegistry>,
    queue: Arc<JobQueue>,
    control_token: Option<String>,
) -> axum::Router {
    let status_ctx = StatusCtx {
        tracker,
        buffer: buffer.clone(),
        cold_dir: Arc::new(cold_dir.clone()),
        queue: queue.clone(),
    };
    let ctrl = ControlCtx {
        registry,
        queue,
        tracker: status_ctx.tracker.clone(),
        token: control_token.map(Arc::new),
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
        // Control API: targets.
        .route(
            "/api/v1/targets",
            get(list_targets).post(add_targets).with_state(ctrl.clone()),
        )
        .route(
            "/api/v1/targets/{id}/enable",
            post(enable_target).with_state(ctrl.clone()),
        )
        .route(
            "/api/v1/targets/{id}/disable",
            post(disable_target).with_state(ctrl.clone()),
        )
        .route(
            "/api/v1/targets/{id}",
            delete(remove_target).with_state(ctrl.clone()),
        )
        // Control API: worker-wide freeze (halts scheduled + manual).
        .route(
            "/api/v1/worker",
            get(worker_status).with_state(ctrl.clone()),
        )
        .route(
            "/api/v1/worker/freeze",
            post(freeze_worker).with_state(ctrl.clone()),
        )
        .route(
            "/api/v1/worker/resume",
            post(resume_worker).with_state(ctrl.clone()),
        )
        // Control API: queue (pause/resume before the `{id}` wildcard).
        .route(
            "/api/v1/queue",
            get(queue_status)
                .post(enqueue_jobs)
                .delete(clear_queue)
                .with_state(ctrl.clone()),
        )
        .route(
            "/api/v1/queue/pause",
            post(pause_queue).with_state(ctrl.clone()),
        )
        .route(
            "/api/v1/queue/resume",
            post(resume_queue).with_state(ctrl.clone()),
        )
        .route("/api/v1/queue/{id}", delete(cancel_job).with_state(ctrl))
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
/// and the control API reverse-proxied to `upstream`. Never opens Fjall.
/// Serves the cold surface locally so history keeps working while the worker
/// is unreachable.
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
        .route(
            "/api/v1/status",
            get(dashboard_status).with_state(proxy.clone()),
        )
        // Control API proxies: method/body/token forwarded to the worker.
        .route(
            "/api/v1/targets",
            any(proxy_upstream).with_state(proxy.clone()),
        )
        .route(
            "/api/v1/targets/{*rest}",
            any(proxy_upstream).with_state(proxy.clone()),
        )
        .route(
            "/api/v1/queue",
            any(proxy_upstream).with_state(proxy.clone()),
        )
        .route(
            "/api/v1/queue/{*rest}",
            any(proxy_upstream).with_state(proxy.clone()),
        )
        .route(
            "/api/v1/worker",
            any(proxy_upstream).with_state(proxy.clone()),
        )
        .route(
            "/api/v1/worker/{*rest}",
            any(proxy_upstream).with_state(proxy),
        )
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
    queue: Arc<JobQueue>,
}

/// `GET /api/v1/status` — worker health snapshot (see [`crate::status`]).
async fn collector_status(State(ctx): State<StatusCtx>) -> Json<StatusSnapshot> {
    let (samples, cap) = ctx.buffer.occupancy();
    let cold_files = list_cold_files(ctx.cold_dir.as_path()).len();
    let q = ctx.queue.snapshot();
    Json(ctx.tracker.snapshot(
        samples,
        cap,
        cold_files,
        QueueInfo {
            depth: q.depth,
            cap: q.cap,
            paused: q.paused,
            frozen: q.frozen,
            running: q.running.map(|j| j.target),
            done: q.done,
            failed: q.failed,
        },
    ))
}

/// Shared state for the mutating control API.
#[derive(Clone)]
struct ControlCtx {
    registry: Arc<TargetRegistry>,
    queue: Arc<JobQueue>,
    tracker: Arc<StatusTracker>,
    token: Option<Arc<String>>,
}

/// Control-API failures → `{"detail": ...}` with a matching status code.
#[derive(Debug)]
enum ApiError {
    /// 422: malformed/duplicate input.
    Invalid(String),
    /// 404: unknown target/job.
    NotFound(String),
    /// 429: a bounded resource is full.
    Full(String),
    /// 401: control token missing/wrong.
    Unauthorized,
    /// 500: persistence/other internal failure.
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, detail) = match self {
            ApiError::Invalid(m) => (StatusCode::UNPROCESSABLE_ENTITY, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Full(m) => (StatusCode::TOO_MANY_REQUESTS, m),
            ApiError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "missing or invalid x-control-token".to_string(),
            ),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, Json(serde_json::json!({ "detail": detail }))).into_response()
    }
}

impl From<TargetError> for ApiError {
    fn from(e: TargetError) -> Self {
        match e {
            TargetError::Invalid(m) => Self::Invalid(m),
            TargetError::NotFound(m) => Self::NotFound(m),
            TargetError::Limit(m) => Self::Full(m),
            TargetError::Persist(m) => Self::Internal(m),
        }
    }
}

impl From<QueueError> for ApiError {
    fn from(e: QueueError) -> Self {
        match e {
            QueueError::Full(_) => Self::Full(e.to_string()),
            QueueError::NotFound(m) => Self::NotFound(m),
        }
    }
}

/// Enforce the optional control token on mutating endpoints.
fn authorize(ctx: &ControlCtx, headers: &HeaderMap) -> Result<(), ApiError> {
    match &ctx.token {
        None => Ok(()),
        Some(expected) => {
            let got = headers.get("x-control-token").and_then(|v| v.to_str().ok());
            if got.is_some_and(|g| g == expected.as_str()) {
                Ok(())
            } else {
                Err(ApiError::Unauthorized)
            }
        }
    }
}

/// `POST /api/v1/targets` body. `urls` (or `url`) is required; empty `allow`
/// uses the default prefixes; `mode` defaults to recurring.
#[derive(Debug, Deserialize)]
struct AddTargetsRequest {
    /// One or more `/metrics` URLs.
    #[serde(default)]
    urls: Vec<String>,
    /// Single-URL convenience alias.
    #[serde(default)]
    url: Option<String>,
    /// Optional explicit name (only valid for a single URL).
    #[serde(default)]
    name: Option<String>,
    /// Optional allow prefixes (defaults when empty/absent).
    #[serde(default)]
    allow: Option<Vec<String>>,
    /// `recurring` (swept) or `once` (queued immediately).
    #[serde(default)]
    mode: Mode,
}

/// `POST /api/v1/queue` body.
#[derive(Debug, Deserialize)]
struct EnqueueRequest {
    /// URLs to enqueue as one-shot manual jobs (targets auto-created).
    #[serde(default)]
    urls: Vec<String>,
}

/// `GET /api/v1/targets` — registry contents.
async fn list_targets(State(ctx): State<ControlCtx>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "targets": ctx.registry.list() }))
}

/// `POST /api/v1/targets` — add one or more dynamic targets; `once` targets
/// are also enqueued as manual jobs.
async fn add_targets(
    State(ctx): State<ControlCtx>,
    headers: HeaderMap,
    Json(req): Json<AddTargetsRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&ctx, &headers)?;
    let mut urls = req.urls;
    if let Some(u) = req.url {
        urls.push(u);
    }
    urls.retain(|u| !u.trim().is_empty());
    if urls.is_empty() {
        return Err(ApiError::Invalid("urls must not be empty".to_string()));
    }
    if urls.len() > 1 && req.name.is_some() {
        return Err(ApiError::Invalid(
            "name is only valid with a single url".to_string(),
        ));
    }

    let mut created = Vec::new();
    let mut jobs = Vec::new();
    for url in urls {
        let target =
            ctx.registry
                .add_dynamic(req.name.as_deref(), &url, req.allow.as_deref(), req.mode)?;
        ctx.tracker.add_target(&target);
        if req.mode == Mode::Once {
            jobs.push(
                ctx.queue
                    .enqueue_manual(&target.name, &target.url, target.allow.clone())?,
            );
        }
        created.push(target);
    }
    Ok(Json(
        serde_json::json!({ "targets": created, "jobs": jobs }),
    ))
}

/// `POST /api/v1/targets/{id}/enable`.
async fn enable_target(
    State(ctx): State<ControlCtx>,
    UrlPath(id): UrlPath<String>,
    headers: HeaderMap,
) -> Result<Json<Target>, ApiError> {
    authorize(&ctx, &headers)?;
    let target = ctx.registry.set_enabled(&id, true)?;
    ctx.tracker.set_enabled(&id, true);
    Ok(Json(target))
}

/// `POST /api/v1/targets/{id}/disable`.
async fn disable_target(
    State(ctx): State<ControlCtx>,
    UrlPath(id): UrlPath<String>,
    headers: HeaderMap,
) -> Result<Json<Target>, ApiError> {
    authorize(&ctx, &headers)?;
    let target = ctx.registry.set_enabled(&id, false)?;
    ctx.tracker.set_enabled(&id, false);
    Ok(Json(target))
}

/// `DELETE /api/v1/targets/{id}`.
async fn remove_target(
    State(ctx): State<ControlCtx>,
    UrlPath(id): UrlPath<String>,
    headers: HeaderMap,
) -> Result<Json<Target>, ApiError> {
    authorize(&ctx, &headers)?;
    let target = ctx.registry.remove(&id)?;
    ctx.tracker.remove_target(&id);
    Ok(Json(target))
}

/// `GET /api/v1/queue` — queue state for the UI.
async fn queue_status(State(ctx): State<ControlCtx>) -> Json<QueueSummary> {
    Json(ctx.queue.snapshot())
}

/// `POST /api/v1/queue` — enqueue one-shot manual jobs (targets auto-created).
async fn enqueue_jobs(
    State(ctx): State<ControlCtx>,
    headers: HeaderMap,
    Json(req): Json<EnqueueRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&ctx, &headers)?;
    let mut jobs = Vec::new();
    for url in req.urls {
        let url = url.trim();
        if url.is_empty() {
            continue;
        }
        let target = if let Some(t) = ctx.registry.find_by_url(url) {
            t
        } else {
            let t = ctx.registry.add_dynamic(None, url, None, Mode::Once)?;
            ctx.tracker.add_target(&t);
            t
        };
        jobs.push(
            ctx.queue
                .enqueue_manual(&target.name, &target.url, target.allow.clone())?,
        );
    }
    if jobs.is_empty() {
        return Err(ApiError::Invalid("urls must not be empty".to_string()));
    }
    Ok(Json(serde_json::json!({ "jobs": jobs })))
}

/// `POST /api/v1/queue/pause` — hold manual jobs (scheduled keep running).
async fn pause_queue(
    State(ctx): State<ControlCtx>,
    headers: HeaderMap,
) -> Result<Json<QueueSummary>, ApiError> {
    authorize(&ctx, &headers)?;
    ctx.queue.pause();
    Ok(Json(ctx.queue.snapshot()))
}

/// `POST /api/v1/queue/resume` — release manual jobs.
async fn resume_queue(
    State(ctx): State<ControlCtx>,
    headers: HeaderMap,
) -> Result<Json<QueueSummary>, ApiError> {
    authorize(&ctx, &headers)?;
    ctx.queue.resume();
    Ok(Json(ctx.queue.snapshot()))
}

/// `DELETE /api/v1/queue/{id}` — cancel one pending job.
async fn cancel_job(
    State(ctx): State<ControlCtx>,
    UrlPath(id): UrlPath<String>,
    headers: HeaderMap,
) -> Result<Json<crate::queue::Job>, ApiError> {
    authorize(&ctx, &headers)?;
    Ok(Json(ctx.queue.cancel(&id)?))
}

/// `DELETE /api/v1/queue` — drop every pending job.
async fn clear_queue(
    State(ctx): State<ControlCtx>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&ctx, &headers)?;
    let cancelled = ctx.queue.clear_pending();
    Ok(Json(serde_json::json!({ "cancelled": cancelled })))
}

/// `GET /api/v1/worker` — worker-wide control state (`frozen`).
async fn worker_status(State(ctx): State<ControlCtx>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "frozen": ctx.queue.is_frozen() }))
}

/// `POST /api/v1/worker/freeze` — halt the executor (scheduled + manual).
async fn freeze_worker(
    State(ctx): State<ControlCtx>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&ctx, &headers)?;
    ctx.queue.freeze();
    Ok(Json(serde_json::json!({ "frozen": true })))
}

/// `POST /api/v1/worker/resume` — lift the freeze and drain pending jobs.
async fn resume_worker(
    State(ctx): State<ControlCtx>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&ctx, &headers)?;
    ctx.queue.unfreeze();
    Ok(Json(serde_json::json!({ "frozen": false })))
}

/// Dashboard proxy state: HTTP client + worker base URL.
#[derive(Clone)]
struct ProxyCtx {
    client: reqwest::Client,
    upstream: Arc<String>,
}

/// Reverse-proxy a request to the worker, forwarding method, query, body,
/// content-type, and the `x-control-token` header. Used for the GET hot
/// APIs and the mutating control API. On any upstream failure the dashboard
/// answers 503 (Prometheus-shaped error body) instead of hanging.
async fn proxy_upstream(State(ctx): State<ProxyCtx>, req: Request) -> Response {
    let method = req.method().clone();
    let path_query = req.uri().path_and_query().map_or("", |p| p.as_str());
    let url = format!("{}{}", ctx.upstream, path_query);

    let mut builder = ctx.client.request(method, &url);
    if let Some(ct) = req.headers().get(CONTENT_TYPE) {
        builder = builder.header(CONTENT_TYPE, ct);
    }
    if let Some(token) = req.headers().get("x-control-token") {
        builder = builder.header("x-control-token", token);
    }
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(e) => return gateway_error(&format!("request body too large: {e}")),
    };
    if !body.is_empty() {
        builder = builder.body(body);
    }
    match builder.send().await {
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
    use crate::targets::{Mode, Origin, Target};

    fn targets_for(names: &[&str]) -> Vec<Target> {
        names
            .iter()
            .map(|n| Target {
                id: format!("cfg:{n}"),
                name: (*n).to_string(),
                url: format!("http://localhost/{n}/metrics"),
                allow: vec!["http_".to_string()],
                enabled: true,
                origin: Origin::Config,
                mode: Mode::Recurring,
                created_at: 0,
            })
            .collect()
    }

    fn tracker_for(names: &[&str]) -> Arc<StatusTracker> {
        Arc::new(StatusTracker::new(&targets_for(names), 15, 30))
    }

    fn registry_for(dir: &Path, names: &[&str]) -> Arc<TargetRegistry> {
        let cfg: Vec<TargetConfig> = names
            .iter()
            .map(|n| TargetConfig {
                name: (*n).to_string(),
                url: format!("http://localhost/{n}/metrics"),
                allow: vec!["http_".to_string()],
            })
            .collect();
        Arc::new(TargetRegistry::load(&cfg, &dir.join("targets.json"), 64).unwrap())
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
        super::worker_router(
            cold.to_path_buf(),
            buffer,
            tracker_for(&["app"]),
            registry_for(cold, &["app"]),
            Arc::new(JobQueue::new(16)),
            None,
        )
    }

    async fn request(
        app: axum::Router,
        method: &str,
        uri: &str,
        body: &str,
        headers: &[(&str, &str)],
    ) -> (StatusCode, serde_json::Value) {
        let mut builder = axum::http::Request::builder().uri(uri).method(method);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let request = builder
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn get(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
        request(app, "GET", uri, "", &[]).await
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

        let app = super::worker_router(
            dir.path().to_path_buf(),
            seeded_buffer(),
            tracker,
            registry_for(dir.path(), &["app"]),
            Arc::new(JobQueue::new(16)),
            None,
        );
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

    fn control_app(
        dir: &Path,
        registry: Arc<TargetRegistry>,
        tracker: Arc<StatusTracker>,
        queue: Arc<JobQueue>,
        token: Option<String>,
    ) -> axum::Router {
        super::worker_router(
            dir.to_path_buf(),
            seeded_buffer(),
            tracker,
            registry,
            queue,
            token,
        )
    }

    #[tokio::test]
    async fn control_api_adds_targets_and_enqueues_once() {
        let dir = tempfile::tempdir().unwrap();
        let registry = registry_for(dir.path(), &["app"]);
        let tracker = tracker_for(&["app"]);
        let queue = Arc::new(JobQueue::new(16));
        let app = control_app(dir.path(), registry.clone(), tracker, queue.clone(), None);

        let (status, body) = request(
            app.clone(),
            "POST",
            "/api/v1/targets",
            r#"{"urls":["https://a.example/metrics"],"mode":"recurring"}"#,
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["targets"][0]["mode"], "recurring");
        assert_eq!(body["targets"][0]["enabled"], true);
        assert_eq!(registry.list().len(), 2);
        assert!(dir.path().join("targets.json").exists());

        let (status, body) = request(
            app.clone(),
            "POST",
            "/api/v1/targets",
            r#"{"urls":["https://b.example/metrics"],"mode":"once"}"#,
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
        assert_eq!(queue.snapshot().depth, 1);
        assert_eq!(registry.list().len(), 3);

        let (status, body) = get(app, "/api/v1/targets").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["targets"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn control_api_pause_cancel_resume() {
        let dir = tempfile::tempdir().unwrap();
        let registry = registry_for(dir.path(), &["app"]);
        let tracker = tracker_for(&["app"]);
        let queue = Arc::new(JobQueue::new(16));
        let app = control_app(dir.path(), registry, tracker, queue.clone(), None);

        let (status, body) = request(
            app.clone(),
            "POST",
            "/api/v1/queue",
            r#"{"urls":["https://c.example/metrics"]}"#,
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);

        let (status, body) = request(app.clone(), "POST", "/api/v1/queue/pause", "", &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["paused"], true);

        let (status, body) = get(app.clone(), "/api/v1/queue").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["paused"], true);
        assert_eq!(body["depth"], 1);
        let id = body["pending"][0]["id"].as_str().unwrap().to_string();

        let (status, _) = request(
            app.clone(),
            "DELETE",
            &format!("/api/v1/queue/{id}"),
            "",
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(queue.snapshot().depth, 0);

        let (status, body) = request(app, "POST", "/api/v1/queue/resume", "", &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["paused"], false);
    }

    #[tokio::test]
    async fn control_api_validates_and_maps_errors() {
        let dir = tempfile::tempdir().unwrap();
        let registry = registry_for(dir.path(), &["app"]);
        let tracker = tracker_for(&["app"]);
        let queue = Arc::new(JobQueue::new(1));
        let app = control_app(dir.path(), registry, tracker, queue, None);

        let (status, body) = request(
            app.clone(),
            "POST",
            "/api/v1/targets",
            r#"{"urls":["ftp://nope"]}"#,
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(body["detail"].as_str().unwrap().contains("http"));

        let (status, _) = request(app.clone(), "DELETE", "/api/v1/targets/cfg:app", "", &[]).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let (status, _) = request(app.clone(), "DELETE", "/api/v1/targets/nope", "", &[]).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Fill the size-1 queue, then overflow → 429.
        let body = r#"{"urls":["https://q1.example/metrics"]}"#;
        let _ = request(
            app.clone(),
            "POST",
            "/api/v1/queue",
            body,
            &[("content-type", "application/json")],
        )
        .await;
        let (status, _) = request(
            app,
            "POST",
            "/api/v1/queue",
            r#"{"urls":["https://q2.example/metrics"]}"#,
            &[("content-type", "application/json")],
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn control_token_is_enforced_on_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let registry = registry_for(dir.path(), &["app"]);
        let tracker = tracker_for(&["app"]);
        let queue = Arc::new(JobQueue::new(16));
        let app = control_app(dir.path(), registry, tracker, queue, Some("s3cret".into()));

        let (status, body) = request(app.clone(), "POST", "/api/v1/queue/pause", "", &[]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body["detail"].as_str().unwrap().contains("token"));

        let (status, _) = request(
            app.clone(),
            "POST",
            "/api/v1/queue/pause",
            "",
            &[("x-control-token", "s3cret")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Reads stay open.
        let (status, _) = get(app, "/api/v1/queue").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn worker_freeze_and_resume_control() {
        let dir = tempfile::tempdir().unwrap();
        let registry = registry_for(dir.path(), &["app"]);
        let tracker = tracker_for(&["app"]);
        let queue = Arc::new(JobQueue::new(16));
        let app = control_app(dir.path(), registry, tracker, queue.clone(), None);

        let (status, body) = request(app.clone(), "POST", "/api/v1/worker/freeze", "", &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["frozen"], true);
        assert!(queue.is_frozen());

        let (status, body) = get(app.clone(), "/api/v1/worker").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["frozen"], true);

        // Status surfaces the freeze for the chip.
        let (_, body) = get(app.clone(), "/api/v1/status").await;
        assert_eq!(body["queue"]["frozen"], true);

        // Frozen: even a queued manual job does not run.
        queue
            .enqueue_manual("app", "http://localhost/app/metrics", vec![])
            .unwrap();
        assert!(queue.pop_next().is_none());
        assert_eq!(queue.snapshot().depth, 1);

        let (status, body) = request(app, "POST", "/api/v1/worker/resume", "", &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["frozen"], false);
        assert!(!queue.is_frozen());
        assert!(queue.pop_next().is_some(), "resume lets pending work run");
    }

    #[tokio::test]
    async fn worker_freeze_requires_token() {
        let dir = tempfile::tempdir().unwrap();
        let registry = registry_for(dir.path(), &["app"]);
        let tracker = tracker_for(&["app"]);
        let queue = Arc::new(JobQueue::new(16));
        let app = control_app(dir.path(), registry, tracker, queue, Some("s3cret".into()));

        let (status, _) = request(app.clone(), "POST", "/api/v1/worker/freeze", "", &[]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = request(
            app,
            "POST",
            "/api/v1/worker/freeze",
            "",
            &[("x-control-token", "s3cret")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn dashboard_proxies_worker_freeze() {
        async fn frozen() -> Json<serde_json::Value> {
            Json(serde_json::json!({ "frozen": true }))
        }
        let upstream =
            axum::Router::new().route("/api/v1/worker/freeze", axum::routing::post(frozen));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let app = super::dashboard_router(
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
            &format!("http://{addr}"),
        );
        let (status, body) = request(app, "POST", "/api/v1/worker/freeze", "", &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["frozen"], true);
    }

    #[tokio::test]
    async fn dashboard_proxies_control_method_body_and_token() {
        async fn echo(req: Request) -> Json<serde_json::Value> {
            let method = req.method().to_string();
            let token = req
                .headers()
                .get("x-control-token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let bytes = axum::body::to_bytes(req.into_body(), 4096).await.unwrap();
            Json(serde_json::json!({
                "method": method,
                "token": token,
                "body": String::from_utf8_lossy(&bytes),
            }))
        }
        let upstream = axum::Router::new().route("/api/v1/targets", axum::routing::any(echo));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let app = super::dashboard_router(
            dir.path().to_path_buf(),
            dir.path().to_path_buf(),
            &format!("http://{addr}"),
        );
        let (status, body) = request(
            app,
            "POST",
            "/api/v1/targets",
            r#"{"urls":["https://x.example/metrics"]}"#,
            &[
                ("content-type", "application/json"),
                ("x-control-token", "tok"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["method"], "POST");
        assert_eq!(body["token"], "tok");
        assert!(body["body"].as_str().unwrap().contains("urls"));
    }
}
