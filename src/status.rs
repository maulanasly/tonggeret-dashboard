//! Collector status snapshot for `GET /api/v1/status`.
//!
//! The worker updates per-target state inline on the scrape path; the
//! dashboard polls this endpoint to show collector health without parsing
//! `/metrics` text. State is plain shared memory (`Mutex<Vec<TargetStatus>>`,
//! one entry per registered target) — no new background task, consistent
//! with the `AGENTS.md` memory-budget rule.
//!
//! `last_*` / `consecutive_failures` extend the aggregate
//! `collector_scrape_total{target,status}` counter with per-target recency,
//! which the Prometheus registry cannot express. The registry (`crate::targets`)
//! calls `add_target` / `set_enabled` / `remove_target` to keep entries in
//! sync when targets change at runtime.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::scrape::ScrapeStatus;
use crate::targets::{Mode, Origin, Target};

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
    /// Unix seconds of the oldest buffered sample (`null` when empty).
    pub oldest_ts: Option<u64>,
    /// Unix seconds of the newest buffered sample (`null` when empty).
    pub newest_ts: Option<u64>,
}

impl BufferInfo {
    /// Occupancy without timestamps (tests / callers that lack a span).
    #[must_use]
    pub fn new(samples: usize, cap: usize) -> Self {
        Self {
            samples,
            cap,
            oldest_ts: None,
            newest_ts: None,
        }
    }
}

/// Job-queue health for the status payload.
#[derive(Debug, Clone, Serialize)]
pub struct QueueInfo {
    /// Pending job count.
    pub depth: usize,
    /// Queue capacity.
    pub cap: usize,
    /// True when manual jobs are held.
    pub paused: bool,
    /// True when the whole executor is frozen (nothing runs).
    pub frozen: bool,
    /// Name of the currently running job's target, if any.
    pub running: Option<String>,
    /// Lifetime completed jobs.
    pub done: u64,
    /// Lifetime failed jobs.
    pub failed: u64,
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
    /// Target id (registry key).
    pub id: String,
    /// Target slug (`scrape_target` label).
    pub name: String,
    /// `/metrics` URL.
    pub url: String,
    /// Config or dynamic.
    pub origin: Origin,
    /// Recurring or once.
    pub mode: Mode,
    /// Included in the periodic sweep.
    pub enabled: bool,
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
    /// Job-queue health.
    pub queue: QueueInfo,
    /// One entry per registered target.
    pub targets: Vec<TargetStatus>,
}

impl TargetStatus {
    fn new(target: &Target) -> Self {
        Self {
            id: target.id.clone(),
            name: target.name.clone(),
            url: target.url.clone(),
            origin: target.origin,
            mode: target.mode,
            enabled: target.enabled,
            last_scrape_ts: None,
            last_status: None,
            last_samples: 0,
            last_duration_ms: 0,
            consecutive_failures: 0,
            totals: ScrapeTotals::default(),
        }
    }
}

/// Status tracker shared by the scrape loop (writer), the control API
/// (target add/remove), and the `/api/v1/status` handler (reader).
#[derive(Debug)]
pub struct StatusTracker {
    interval_secs: u64,
    retention_days: u64,
    started_unix: u64,
    targets: Mutex<Vec<TargetStatus>>,
}

impl StatusTracker {
    /// Build a tracker with one entry per registered target.
    #[must_use]
    pub fn new(targets: &[Target], interval_secs: u64, retention_days: u64) -> Self {
        Self {
            interval_secs,
            retention_days,
            started_unix: now_unix(),
            targets: Mutex::new(targets.iter().map(TargetStatus::new).collect()),
        }
    }

    /// Record one finished scrape attempt for `target`. Unknown names are
    /// ignored (a removed target may still finish in flight). Never blocks
    /// the scrape path for long: one short mutex hold.
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

    /// Add a newly registered target to the status list.
    pub fn add_target(&self, target: &Target) {
        if let Ok(mut targets) = self.targets.lock() {
            if !targets.iter().any(|t| t.id == target.id) {
                targets.push(TargetStatus::new(target));
            }
        }
    }

    /// Reflect an enable/disable change.
    pub fn set_enabled(&self, id: &str, enabled: bool) {
        if let Ok(mut targets) = self.targets.lock() {
            if let Some(entry) = targets.iter_mut().find(|t| t.id == id) {
                entry.enabled = enabled;
            }
        }
    }

    /// Drop a removed target's status entry.
    pub fn remove_target(&self, id: &str) {
        if let Ok(mut targets) = self.targets.lock() {
            targets.retain(|t| t.id != id);
        }
    }

    /// Snapshot with the worker-owned figures the scrape path cannot know
    /// (buffer occupancy + span, cold-file count, queue health).
    #[must_use]
    pub fn snapshot(
        &self,
        buffer: BufferInfo,
        cold_files: usize,
        queue: QueueInfo,
    ) -> StatusSnapshot {
        let (targets, status) = match self.targets.lock() {
            Ok(t) => {
                let any_failing = t.iter().any(|s| s.consecutive_failures > 0);
                let any_unscraped = t
                    .iter()
                    .any(|s| s.enabled && s.mode == Mode::Recurring && s.last_scrape_ts.is_none());
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
            buffer,
            cold_files,
            retention_days: self.retention_days,
            queue,
            targets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(id: &str, name: &str, mode: Mode) -> Target {
        Target {
            id: id.to_string(),
            name: name.to_string(),
            url: format!("http://localhost/{name}/metrics"),
            allow: vec!["http_".to_string()],
            enabled: mode == Mode::Recurring,
            origin: Origin::Dynamic,
            mode,
            created_at: 0,
        }
    }

    fn queue_info() -> QueueInfo {
        QueueInfo {
            depth: 0,
            cap: 256,
            paused: false,
            frozen: false,
            running: None,
            done: 0,
            failed: 0,
        }
    }

    #[test]
    fn starts_as_starting_then_ok() {
        let targets = vec![
            target("a", "a", Mode::Recurring),
            target("b", "b", Mode::Recurring),
        ];
        let t = StatusTracker::new(&targets, 15, 30);
        let snap = t.snapshot(BufferInfo::new(0, 20_000), 0, queue_info());
        assert_eq!(snap.status, STATUS_STARTING);
        assert_eq!(snap.targets.len(), 2);

        t.record("a", ScrapeStatus::Ok, 5, 12);
        let snap = t.snapshot(BufferInfo::new(5, 20_000), 1, queue_info());
        assert_eq!(snap.status, STATUS_STARTING, "b still unscraped");
        assert_eq!(snap.targets[0].last_status.as_deref(), Some("ok"));
        assert_eq!(snap.targets[0].last_samples, 5);
        assert_eq!(snap.targets[0].totals.ok, 1);
        assert_eq!(snap.buffer.cap, 20_000);
        assert_eq!(snap.cold_files, 1);

        t.record("b", ScrapeStatus::Ok, 3, 7);
        assert_eq!(
            t.snapshot(BufferInfo::new(0, 1), 0, queue_info()).status,
            STATUS_OK
        );
    }

    #[test]
    fn once_targets_do_not_block_starting() {
        let targets = vec![
            target("a", "a", Mode::Recurring),
            target("o", "one", Mode::Once),
        ];
        let t = StatusTracker::new(&targets, 15, 30);
        t.record("a", ScrapeStatus::Ok, 1, 1);
        assert_eq!(
            t.snapshot(BufferInfo::new(0, 1), 0, queue_info()).status,
            STATUS_OK,
            "unscraped once targets are not part of the sweep"
        );
    }

    #[test]
    fn failures_degrade_and_streaks_reset_on_success() {
        let targets = vec![
            target("a", "a", Mode::Recurring),
            target("b", "b", Mode::Recurring),
        ];
        let t = StatusTracker::new(&targets, 15, 30);
        t.record("a", ScrapeStatus::Ok, 1, 1);
        t.record("b", ScrapeStatus::Ok, 1, 1);

        t.record("a", ScrapeStatus::FetchError, 0, 5);
        let snap = t.snapshot(BufferInfo::new(0, 1), 0, queue_info());
        assert_eq!(snap.status, STATUS_DEGRADED);
        assert_eq!(snap.targets[0].consecutive_failures, 1);
        assert_eq!(snap.targets[0].totals.fetch_error, 1);

        t.record("a", ScrapeStatus::ParseError, 0, 5);
        assert_eq!(
            t.snapshot(BufferInfo::new(0, 1), 0, queue_info()).targets[0].consecutive_failures,
            2
        );

        t.record("a", ScrapeStatus::Ok, 2, 5);
        let snap = t.snapshot(BufferInfo::new(0, 1), 0, queue_info());
        assert_eq!(snap.status, STATUS_OK);
        assert_eq!(snap.targets[0].consecutive_failures, 0);
        assert_eq!(snap.targets[0].totals.ok, 2);
        assert_eq!(snap.targets[0].totals.parse_error, 1);
    }

    #[test]
    fn unknown_target_is_ignored() {
        let targets = vec![target("a", "a", Mode::Recurring)];
        let t = StatusTracker::new(&targets, 15, 30);
        t.record("nope", ScrapeStatus::Ok, 1, 1);
        let snap = t.snapshot(BufferInfo::new(0, 1), 0, queue_info());
        assert!(snap.targets.iter().all(|s| s.last_scrape_ts.is_none()));
    }

    #[test]
    fn runtime_add_enable_remove() {
        let targets = vec![target("a", "a", Mode::Recurring)];
        let t = StatusTracker::new(&targets, 15, 30);
        let b = target("b", "b", Mode::Recurring);
        t.add_target(&b);
        assert_eq!(
            t.snapshot(BufferInfo::new(0, 1), 0, queue_info())
                .targets
                .len(),
            2
        );
        t.set_enabled("b", false);
        assert!(!t.snapshot(BufferInfo::new(0, 1), 0, queue_info()).targets[1].enabled);
        t.remove_target("b");
        assert_eq!(
            t.snapshot(BufferInfo::new(0, 1), 0, queue_info())
                .targets
                .len(),
            1
        );
    }
}
