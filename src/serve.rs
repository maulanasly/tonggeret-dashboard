//! HTTP surface: prebuilt UI + self `/metrics` + cold Parquet + manifest.
//!
//! The UI bundle (`dist/`, served at `/`) is the same DuckDB-Wasm app the
//! mock server hosts: it resolves `<base>/telemetry/parquet` (newest cold
//! file, Range-capable via tonggeret) or the `/api/files` manifest below.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::{Path as UrlPath, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use tower::ServiceExt as _;

/// Cold export filename shape (`tonggeret::storage` convention).
const COLD_PREFIX: &str = "metrics_cold_";

/// Build the full router: UI, scrape-compat endpoints, telemetry.
///
/// NOTE: deliberately *no* `track` middleware here. The collector mirrors
/// scraped samples into this same registry with an extra `scrape_target`
/// label, so self `http_*` series (3 labels) would collide with scraped
/// ones (4 labels) and panic the registry. Self-observability is the
/// `collector_*` outcome series instead.
pub fn router(static_dir: PathBuf, cold_dir: PathBuf) -> axum::Router {
    let files_dir = Arc::new(cold_dir.clone());
    let serve_dir = files_dir.clone();
    axum::Router::new()
        .route(
            "/api/files",
            axum::routing::get(move || {
                let dir = files_dir.clone();
                async move { Json(list_cold_files(&dir)) }
            }),
        )
        .route(
            "/telemetry/cold/{file}",
            axum::routing::get(serve_cold_file).with_state(serve_dir),
        )
        .route(
            "/metrics",
            axum::routing::get(tonggeret::middleware::axum::prometheus_handler),
        )
        .merge(tonggeret::middleware::axum::parquet_route(cold_dir))
        .fallback_service(tower_http::services::ServeDir::new(static_dir))
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
}
