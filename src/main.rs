//! `collector`: Prometheus scrape → Fjall hot store → Parquet history.
//!
//! Roles:
//!
//! * `collector [config]` (no role) — **single binary**: worker + dashboard
//!   UI on one port. Pre-split behaviour, kept for backward compatibility.
//! * `collector worker [config]` — worker only (no UI): scrape loop +
//!   storage + hot APIs + `GET /api/v1/status` + control API. The only
//!   process that opens Fjall.
//! * `collector serve [config]` — read-only dashboard: serves `dist/` and
//!   the cold Parquet locally and reverse-proxies the hot APIs + status +
//!   control API to `COLLECTOR_UPSTREAM` (`COLLECTOR_LISTEN` /
//!   `COLLECTOR_FJALL_DIR` / `COLLECTOR_UPSTREAM` /
//!   `COLLECTOR_CONTROL_TOKEN` env overrides apply to every role).
//!
//! The worker runs **one executor task**: an interval tick enqueues a
//! `Scheduled` job per enabled recurring target, and the same task drains
//! the queue (manual jobs first, gated by pause). One task ⇒ scrapes never
//! overlap.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tonggeret_dashboard::config::TargetConfig;
use tonggeret_dashboard::queue::JobQueue;
use tonggeret_dashboard::scrape::{self, ScrapeStatus};
use tonggeret_dashboard::targets::TargetRegistry;
use tonggeret_dashboard::{config, query, serve, status};

/// Hourly cold-file purge check (cheap readdir; no extra task).
const PURGE_CHECK_SECS: u64 = 3600;

/// Default config path when no explicit argument is given.
const DEFAULT_CONFIG: &str = "collector.toml";

enum Role {
    /// Scrape + store + hot APIs + control API. Opens Fjall. `serve_ui` is
    /// true for the legacy single-binary form (`collector <config>`), which
    /// also serves `dist/`; the explicit `worker` split role does not.
    Worker { serve_ui: bool },
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
            Role::Worker { serve_ui: false },
            args.next().unwrap_or_else(|| DEFAULT_CONFIG.to_string()),
        ),
        // Backward compatible: a bare path (or nothing) is the single-binary
        // form — worker + embedded UI on one port.
        Some(path) => (Role::Worker { serve_ui: true }, path.to_string()),
        None => (Role::Worker { serve_ui: true }, DEFAULT_CONFIG.to_string()),
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
        Role::Worker { serve_ui } => run_worker(cfg, serve_ui).await,
        Role::Serve => run_serve(cfg).await,
    }
}

/// Worker: own the store, run the unified executor, serve the APIs. When
/// `serve_ui` is set (single-binary form) it also serves `static_dir` at `/`,
/// so `collector <config>` keeps the pre-split behaviour.
async fn run_worker(cfg: Arc<config::Config>, serve_ui: bool) {
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

    // Runtime target registry (config + persisted dynamic). A corrupt state
    // file is fatal, like storage init: better to fail than silently drop
    // targets the operator added.
    let registry = match TargetRegistry::load(
        &cfg.targets,
        &cfg.state.targets_file,
        cfg.state.max_dynamic_targets,
    ) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("collector: {e}");
            std::process::exit(1);
        }
    };

    // Recent-samples ring for /api/v1/query_range. Memory budget note:
    // `recent_buffer_samples` (default 20k ≈ ≤6 MiB) drop-oldest, written
    // inline on the scrape path and read under a short mutex hold — no
    // second Fjall open (the engine forbids that).
    let buffer = Arc::new(query::RecentBuffer::new(cfg.recent_buffer_samples));
    // Per-target status + job queue. Queue holds ≤ `queue_capacity` pending
    // jobs (default 256 × ≤300 B ≈ 77 KiB) plus a 100-entry history; the
    // executor is the one extra task, documented in AGENTS.md.
    let tracker = Arc::new(status::StatusTracker::new(
        &registry.list(),
        cfg.interval_secs,
        cfg.fjall.retention_days,
    ));
    let queue = Arc::new(JobQueue::new(cfg.queue_capacity));

    // Purge once at startup (crash leftovers), then hourly inside the loop.
    serve::purge_cold_files(&cold_dir, cfg.fjall.cold_purge_days);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    let executor = Executor {
        cfg: cfg.clone(),
        client,
        buffer: buffer.clone(),
        tracker: tracker.clone(),
        registry: registry.clone(),
        queue: queue.clone(),
        cold_dir: cold_dir.clone(),
    };
    tokio::spawn(executor.run());

    let mut app = serve::worker_router(
        cold_dir,
        buffer,
        tracker,
        registry,
        queue,
        cfg.control_token.clone(),
    );
    // Single-binary form serves the dashboard itself (pre-split behaviour);
    // the explicit `worker` split role leaves UI serving to `serve`.
    if serve_ui {
        app = serve::serve_ui(app, cfg.static_dir.clone());
        tracing::info!(static_dir = %cfg.static_dir.display(), "serving dashboard UI");
    }
    let what = if serve_ui {
        "collector (single binary)"
    } else {
        "collector worker"
    };
    serve_until_shutdown(app, &cfg.listen, what).await;
    let _ = tonggeret::shutdown();
}

/// Unified scrape executor: scheduled sweep + queue drain in one task.
struct Executor {
    cfg: Arc<config::Config>,
    client: reqwest::Client,
    buffer: Arc<query::RecentBuffer>,
    tracker: Arc<status::StatusTracker>,
    registry: Arc<TargetRegistry>,
    queue: Arc<JobQueue>,
    cold_dir: PathBuf,
}

impl Executor {
    /// Run forever: on each tick enqueue scheduled jobs; after every wake
    /// drain the queue until empty (manual first while running).
    async fn run(self) {
        let mut tick = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs.max(1)));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_purge = std::time::Instant::now();
        loop {
            tokio::select! {
                () = self.queue.notified() => {}
                _ = tick.tick() => {
                    if !self.queue.is_frozen() {
                        self.enqueue_scheduled();
                    }
                }
            }
            self.drain().await;
            if last_purge.elapsed() >= Duration::from_secs(PURGE_CHECK_SECS) {
                serve::purge_cold_files(&self.cold_dir, self.cfg.fjall.cold_purge_days);
                last_purge = std::time::Instant::now();
            }
        }
    }

    /// Queue one `Scheduled` job per enabled recurring target (deduped).
    fn enqueue_scheduled(&self) {
        for target in self.registry.enabled_recurring() {
            let _ = self
                .queue
                .enqueue_scheduled(&target.name, &target.url, target.allow.clone());
        }
    }

    /// Run queued jobs until none are eligible. Manual jobs run first while
    /// running; while paused only scheduled jobs are eligible; while frozen
    /// (a superset of paused) nothing runs until unfreeze.
    async fn drain(&self) {
        while let Some(job) = self.queue.pop_next() {
            let target = TargetConfig {
                name: job.target.clone(),
                url: job.url.clone(),
                allow: job.allow.clone(),
            };
            let outcome = scrape::scrape_once(
                &self.client,
                &target,
                self.cfg.max_samples_per_scrape,
                self.cfg.max_body_bytes,
                &self.buffer,
                &self.tracker,
            )
            .await;
            let error =
                (outcome.status != ScrapeStatus::Ok).then(|| outcome.status.as_str().to_string());
            self.queue
                .complete(job, outcome.status, outcome.samples, outcome.dropped, error);
        }
    }
}

/// Dashboard: serve the UI + cold history, proxy hot API/status/control.
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
