//! Collector status snapshot for `GET /api/v1/status`.
//!
//! The worker updates per-target state inline on the scrape path; the
//! dashboard polls this endpoint to show collector health without parsing
//! `/metrics` text. State is plain shared memory (`Mutex<Vec<TargetStatus>>`,
//! one entry per configured target) — no new background task, consistent
//! with the `AGENTS.md` memory-budget rule.
//!
//! `last_*` / `consecutive_failures` extend the aggregate
//! `collector_scrape_total{target,status}` counter with per-target recency,
//! which the Prometheus registry cannot express.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::config::TargetConfig;
use crate::scrape::ScrapeStatus;

/// Overall collector health: all targets healthy.
pub const STATUS_OK: &str = "ok";
/// Overall collector health: at least one target failing.
pub const STATUS_DEGRADED: &str = "degraded";
/// No target has been scraped yet (startup window).
pub const STATUS_STARTING: &str = "starting";

/// Current time, whole seconds since the Unix epoch (0 on clock skew).
#[must_use]
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Recent-samples ring occupancy.
#[derive(Debug, Clone, Serialize)]
pub struct BufferInfo {
    /// Samples currently buffered.
    pub samples: usize,
    /// Ring capacity (`recent_buffer_samples`).
    pub cap: usize,
}

/// Per-status scrape totals (mirrors `collector_scrape_total{status}`).
#[derive(Debug, Clone, Default, Serialize)]
pub struct ScrapeTotals {
    /// Successful scrapes.
    pub ok: u64,
    /// Transport / non-2xx / oversize / non-UTF8 scrapes.
    pub fetch_error: u64,
    /// Unparsable exposition scrapes.
    pub parse_error: u64,
}

/// One target's status.
#[derive(Debug, Clone, Serialize)]
pub struct TargetStatus {
    /// Target slug (`scrape_target` label).
    pub name: String,
    /// `/metrics` URL.
    pub url: String,
    /// Unix seconds of the last attempt (`null` before the first scrape).
    pub last_scrape_ts: Option<u64>,
    /// Outcome of the last attempt (`ok` / `fetch_error` / `parse_error`).
    pub last_status: Option<String>,
    /// Samples stored by the last successful attempt.
    pub last_samples: usize,
    /// Wall time of the last attempt, milliseconds.
    pub last_duration_ms: u64,
    /// Consecutive non-`ok` attempts (0 after a success).
    pub consecutive_failures: u64,
    /// Lifetime outcome counts.
    pub totals: ScrapeTotals,
}

/// Full worker status snapshot (the `/api/v1/status` JSON body).
#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    /// `ok` / `degraded` / `starting` — see the module consts.
    pub status: &'static str,
    /// Seconds since collector start.
    pub uptime_secs: u64,
    /// Configured scrape interval.
    pub interval_secs: u64,
    /// Hot ring occupancy.
    pub buffer: BufferInfo,
    /// Cold `metrics_cold_*.parquet` files currently on disk.
    pub cold_files: usize,
    /// Hot age after which keys compact to Parquet.
    pub retention_days: u64,
    /// One entry per configured target, in config order.
    pub targets: Vec<TargetStatus>,
}

impl TargetStatus {
    fn new(target: &TargetConfig) -> Self {
        Self {
            name: target.name.clone(),
            url: target.url.clone(),
            last_scrape_ts: None,
            last_status: None,
            last_samples: 0,
            last_duration_ms: 0,
            consecutive_failures: 0,
            totals: ScrapeTotals::default(),
        }
    }
}

/// Process-global-ish status tracker shared by the scrape loop (writer) and
/// the `/api/v1/status` handler (reader). Cheap `Arc`-clonable.
#[derive(Debug)]
pub struct StatusTracker {
    interval_secs: u64,
    retention_days: u64,
    started_unix: u64,
    targets: Mutex<Vec<TargetStatus>>,
}

impl StatusTracker {
    /// Build a tracker with one entry per configured target, in config order.
    #[must_use]
    pub fn new(targets: &[TargetConfig], interval_secs: u64, retention_days: u64) -> Self {
        Self {
            interval_secs,
            retention_days,
            started_unix: now_unix(),
            targets: Mutex::new(targets.iter().map(TargetStatus::new).collect()),
        }
    }

    /// Record one finished scrape attempt for `target`. Unknown names are
    /// ignored (the target set is fixed by config). Never blocks the scrape
    /// path for long: one short mutex hold, like the recent buffer.
    pub fn record(&self, target: &str, status: ScrapeStatus, samples: usize, duration_ms: u64) {
        let Ok(mut targets) = self.targets.lock() else {
            return;
        };
        let Some(entry) = targets.iter_mut().find(|t| t.name == target) else {
            return;
        };
        entry.last_scrape_ts = Some(now_unix());
        entry.last_status = Some(status.as_str().to_string());
        entry.last_samples = samples;
        entry.last_duration_ms = duration_ms;
        match status {
            ScrapeStatus::Ok => {
                entry.consecutive_failures = 0;
                entry.totals.ok = entry.totals.ok.saturating_add(1);
            }
            ScrapeStatus::FetchError => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.totals.fetch_error = entry.totals.fetch_error.saturating_add(1);
            }
            ScrapeStatus::ParseError => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.totals.parse_error = entry.totals.parse_error.saturating_add(1);
            }
        }
    }

    /// Snapshot with the worker-owned figures the scrape path cannot know
    /// (buffer occupancy, cold-file count).
    #[must_use]
    pub fn snapshot(
        &self,
        buffer_samples: usize,
        buffer_cap: usize,
        cold_files: usize,
    ) -> StatusSnapshot {
        let (targets, status) = match self.targets.lock() {
            Ok(t) => {
                let any_failing = t.iter().any(|s| s.consecutive_failures > 0);
                let any_unscraped = t.iter().any(|s| s.last_scrape_ts.is_none());
                let status = if any_failing {
                    STATUS_DEGRADED
                } else if any_unscraped {
                    STATUS_STARTING
                } else {
                    STATUS_OK
                };
                (t.clone(), status)
            }
            Err(_) => (Vec::new(), STATUS_STARTING),
        };
        StatusSnapshot {
            status,
            uptime_secs: now_unix().saturating_sub(self.started_unix),
            interval_secs: self.interval_secs,
            buffer: BufferInfo {
                samples: buffer_samples,
                cap: buffer_cap,
            },
            cold_files,
            retention_days: self.retention_days,
            targets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets() -> Vec<TargetConfig> {
        vec![
            TargetConfig {
                name: "a".to_string(),
                url: "http://localhost:1/metrics".to_string(),
                allow: vec!["http_".to_string()],
            },
            TargetConfig {
                name: "b".to_string(),
                url: "http://localhost:2/metrics".to_string(),
                allow: vec!["http_".to_string()],
            },
        ]
    }

    #[test]
    fn starts_as_starting_then_ok() {
        let t = StatusTracker::new(&targets(), 15, 30);
        let snap = t.snapshot(0, 20_000, 0);
        assert_eq!(snap.status, STATUS_STARTING);
        assert_eq!(snap.targets.len(), 2);
        assert!(snap.targets[0].last_scrape_ts.is_none());

        t.record("a", ScrapeStatus::Ok, 5, 12);
        let snap = t.snapshot(5, 20_000, 1);
        assert_eq!(snap.status, STATUS_STARTING, "b still unscraped");
        assert_eq!(snap.targets[0].last_status.as_deref(), Some("ok"));
        assert_eq!(snap.targets[0].last_samples, 5);
        assert_eq!(snap.targets[0].totals.ok, 1);
        assert_eq!(snap.buffer.cap, 20_000);
        assert_eq!(snap.cold_files, 1);

        t.record("b", ScrapeStatus::Ok, 3, 7);
        assert_eq!(t.snapshot(0, 1, 0).status, STATUS_OK);
    }

    #[test]
    fn failures_degrade_and_streaks_reset_on_success() {
        let t = StatusTracker::new(&targets(), 15, 30);
        t.record("a", ScrapeStatus::Ok, 1, 1);
        t.record("b", ScrapeStatus::Ok, 1, 1);

        t.record("a", ScrapeStatus::FetchError, 0, 5);
        let snap = t.snapshot(0, 1, 0);
        assert_eq!(snap.status, STATUS_DEGRADED);
        assert_eq!(snap.targets[0].consecutive_failures, 1);
        assert_eq!(snap.targets[0].totals.fetch_error, 1);

        t.record("a", ScrapeStatus::ParseError, 0, 5);
        assert_eq!(t.snapshot(0, 1, 0).targets[0].consecutive_failures, 2);

        t.record("a", ScrapeStatus::Ok, 2, 5);
        let snap = t.snapshot(0, 1, 0);
        assert_eq!(snap.status, STATUS_OK);
        assert_eq!(snap.targets[0].consecutive_failures, 0);
        assert_eq!(snap.targets[0].totals.ok, 2);
        assert_eq!(snap.targets[0].totals.parse_error, 1);
    }

    #[test]
    fn unknown_target_is_ignored() {
        let t = StatusTracker::new(&targets(), 15, 30);
        t.record("nope", ScrapeStatus::Ok, 1, 1);
        let snap = t.snapshot(0, 1, 0);
        assert!(snap.targets.iter().all(|s| s.last_scrape_ts.is_none()));
    }
}
