//! `collector`: Prometheus scrape → Fjall hot store → Parquet history.
//!
//! Two roles share one binary:
//!
//! * `collector worker [config]` (default) — scrape loop + storage + hot
//!   APIs + `GET /api/v1/status`. The only process that opens Fjall.
//! * `collector serve [config]` — read-only dashboard: serves `dist/` and
//!   the cold Parquet locally and reverse-proxies the hot APIs + status to
//!   `COLLECTOR_UPSTREAM` (`COLLECTOR_LISTEN` / `COLLECTOR_FJALL_DIR` /
//!   `COLLECTOR_UPSTREAM` env overrides apply to both).
//!
//! `collector [config]` stays worker, so the single-binary deployment is
//! unchanged.

use std::sync::Arc;
use std::time::Duration;

use tonggeret_dashboard::{config, query, scrape, serve, status};

/// Hourly cold-file purge check (cheap readdir; no extra task).
const PURGE_CHECK_SECS: u64 = 3600;

/// Default config path when no explicit argument is given.
const DEFAULT_CONFIG: &str = "collector.toml";

enum Role {
    /// Scrape + store + hot APIs. Opens Fjall; never serves `dist/`.
    Worker,
    /// Read-only dashboard. Never opens Fjall.
    Serve,
}

fn parse_args() -> (Role, String) {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("serve") => (
            Role::Serve,
            args.next().unwrap_or_else(|| DEFAULT_CONFIG.to_string()),
        ),
        Some("worker") => (
            Role::Worker,
            args.next().unwrap_or_else(|| DEFAULT_CONFIG.to_string()),
        ),
        // Backward compatible: a bare path (or nothing) means worker.
        Some(path) => (Role::Worker, path.to_string()),
        None => (Role::Worker, DEFAULT_CONFIG.to_string()),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let (role, config_path) = parse_args();
    let cfg = match config::load(&config_path) {
        Ok(cfg) => Arc::new(cfg),
        Err(e) => {
            eprintln!("collector: {e}");
            std::process::exit(1);
        }
    };
    match role {
        Role::Worker => run_worker(cfg).await,
        Role::Serve => run_serve(cfg).await,
    }
}

/// Worker: own the store, run the scrape loop, serve the hot API + status.
async fn run_worker(cfg: Arc<config::Config>) {
    let mut fjall = tonggeret::FjallConfig::new(&cfg.fjall.dir);
    fjall.cold_storage_dir = Some(cfg.fjall.cold_dir.clone());
    fjall.retention = Duration::from_secs(cfg.fjall.retention_days.saturating_mul(24 * 3600));
    let cold_dir = fjall.cold_dir();
    if let Err(e) = std::fs::create_dir_all(&cold_dir) {
        eprintln!(
            "collector: cannot create cold dir {}: {e}",
            cold_dir.display()
        );
        std::process::exit(1);
    }
    // Storage misconfiguration is fatal here: collecting without a store
    // silently drops history, which defeats this binary's sole purpose.
    if let Err(e) =
        tonggeret::init(tonggeret::Config::default_full(&cfg.fjall.dir).with_fjall(fjall))
    {
        eprintln!("collector: tonggeret init failed: {e}");
        std::process::exit(1);
    }
    tracing::info!(
        budget_bytes = tonggeret::FjallConfig::new(&cfg.fjall.dir).memory_budget_bytes(),
        "collector worker initialized"
    );

    // Recent-samples ring for /api/v1/query_range. Memory budget note:
    // `recent_buffer_samples` (default 20k ≈ ≤6 MiB) drop-oldest, written
    // inline on the scrape path and read under a short mutex hold — no new
    // background tasks, no second Fjall open (the engine forbids that).
    let buffer = Arc::new(query::RecentBuffer::new(cfg.recent_buffer_samples));
    // Per-target status for /api/v1/status; plain shared memory, no task.
    let tracker = Arc::new(status::StatusTracker::new(
        &cfg.targets,
        cfg.interval_secs,
        cfg.fjall.retention_days,
    ));

    // Purge once at startup (crash leftovers), then hourly inside the loop.
    serve::purge_cold_files(&cold_dir, cfg.fjall.cold_purge_days);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let loop_cfg = cfg.clone();
    let loop_cold = cold_dir.clone();
    let loop_buffer = buffer.clone();
    let loop_tracker = tracker.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(loop_cfg.interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_purge = std::time::Instant::now();
        loop {
            tick.tick().await;
            for target in &loop_cfg.targets {
                scrape::scrape_once(
                    &client,
                    target,
                    loop_cfg.max_samples_per_scrape,
                    loop_cfg.max_body_bytes,
                    &loop_buffer,
                    &loop_tracker,
                )
                .await;
            }
            if last_purge.elapsed() >= Duration::from_secs(PURGE_CHECK_SECS) {
                serve::purge_cold_files(&loop_cold, loop_cfg.fjall.cold_purge_days);
                last_purge = std::time::Instant::now();
            }
        }
    });

    let app = serve::worker_router(cold_dir, buffer, tracker);
    serve_until_shutdown(app, &cfg.listen, "collector worker").await;
    let _ = tonggeret::shutdown();
}

/// Dashboard: serve the UI + cold history, proxy hot API/status to the worker.
async fn run_serve(cfg: Arc<config::Config>) {
    tracing::info!(
        upstream = %cfg.upstream,
        cold_dir = %cfg.fjall.cold_dir.display(),
        "collector dashboard (read-only)"
    );
    let app = serve::dashboard_router(
        cfg.static_dir.clone(),
        cfg.fjall.cold_dir.clone(),
        &cfg.upstream,
    );
    serve_until_shutdown(app, &cfg.listen, "collector dashboard").await;
}

/// Bind `listen`, serve `app`, and shut down cleanly on Ctrl-C.
async fn serve_until_shutdown(app: axum::Router, listen: &str, what: &str) {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .unwrap_or_else(|e| {
            eprintln!("collector: cannot bind {listen}: {e}");
            std::process::exit(1);
        });
    tracing::info!("{what} listening on {listen}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap_or_else(|e| {
            eprintln!("collector: serve failed: {e}");
            std::process::exit(1);
        });
}
