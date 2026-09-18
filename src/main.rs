//! `collector`: Prometheus scrape → Fjall hot store → Parquet history.
//!
//! One Tokio task scrapes every target sequentially each `interval_secs`,
//! maps exposition to storable samples (`scrape`), and records them through
//! the tonggeret engine (Prometheus mirror + bounded channel → Fjall writer
//! thread). Hourly compaction exports cold Parquet; expired cold files are
//! purged. The same binary serves the dashboard UI, self `/metrics`,
//! `/telemetry/parquet` (newest export), and `/api/files` (manifest).

use tonggeret_dashboard::{config, scrape, serve};

use std::sync::Arc;
use std::time::Duration;

/// Hourly cold-file purge check (cheap readdir; no extra task).
const PURGE_CHECK_SECS: u64 = 3600;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "collector.toml".to_string());
    let cfg = match config::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("collector: {e}");
            std::process::exit(1);
        }
    };
    let cfg = Arc::new(cfg);

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
        "collector initialized"
    );

    // Purge once at startup (crash leftovers), then hourly inside the loop.
    serve::purge_cold_files(&cold_dir, cfg.fjall.cold_purge_days);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let loop_cfg = cfg.clone();
    let loop_cold = cold_dir.clone();
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
                )
                .await;
            }
            if last_purge.elapsed() >= Duration::from_secs(PURGE_CHECK_SECS) {
                serve::purge_cold_files(&loop_cold, loop_cfg.fjall.cold_purge_days);
                last_purge = std::time::Instant::now();
            }
        }
    });

    let app = serve::router(cfg.static_dir.clone(), cold_dir);
    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .unwrap_or_else(|e| {
            eprintln!("collector: cannot bind {}: {e}", cfg.listen);
            std::process::exit(1);
        });
    tracing::info!("collector listening on {}", cfg.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap_or_else(|e| {
            eprintln!("collector: serve failed: {e}");
            std::process::exit(1);
        });
    let _ = tonggeret::shutdown();
}
