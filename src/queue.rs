//! Bounded job queue for the unified executor.
//!
//! Holds two job kinds: `Manual` (user-triggered, gated by pause) and
//! `Scheduled` (periodic sweep, never paused). `pop_next` gives manual jobs
//! priority while running, and only scheduled jobs while paused — that is
//! the "pause queue only" contract. One executor task consumes the queue, so
//! entries never run concurrently.
//!
//! A separate **freeze** flag halts the whole executor (scheduled + manual);
//! unlike pause, nothing runs until resume. The API stays up so the worker
//! can be unfrozen.
//!
//! Finished jobs move to a bounded `recent` history for the UI; lifetime
//! `done`/`failed` counters feed `GET /api/v1/status`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use serde::Serialize;
use tokio::sync::Notify;

use crate::scrape::ScrapeStatus;

/// How many finished jobs to keep for display.
const HISTORY_CAP: usize = 100;

/// Job origin/scheduling class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    /// User-triggered; gated by pause.
    Manual,
    /// Periodic sweep; runs even while paused.
    Scheduled,
}

/// Job lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    /// Waiting to be picked up.
    Pending,
    /// Currently being scraped.
    Running,
    /// Finished successfully.
    Done,
    /// Finished with a scrape error.
    Failed,
    /// Removed before it ran.
    Cancelled,
}

/// One queued scrape.
#[derive(Debug, Clone, Serialize)]
pub struct Job {
    /// Unique id.
    pub id: String,
    /// Target name (`scrape_target` label).
    pub target: String,
    /// `/metrics` URL.
    pub url: String,
    /// Accepted series prefixes (carried so the executor can scrape directly).
    pub allow: Vec<String>,
    /// Manual or scheduled.
    pub kind: JobKind,
    /// Current state.
    pub state: JobState,
    /// Unix seconds when queued.
    pub enqueued_at: u64,
    /// Unix seconds when started.
    pub started_at: Option<u64>,
    /// Unix seconds when finished.
    pub finished_at: Option<u64>,
    /// Samples stored (on success).
    pub samples: usize,
    /// Over-cap samples dropped.
    pub dropped: usize,
    /// Failure detail (on failure).
    pub error: Option<String>,
}

/// Queue mutation failures.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// Bounded queue is full.
    #[error("queue full ({0})")]
    Full(usize),
    /// No pending job with that id.
    #[error("unknown or non-pending job {0}")]
    NotFound(String),
}

/// Display snapshot of the queue.
#[derive(Debug, Clone, Serialize)]
pub struct QueueSummary {
    /// Number of pending jobs.
    pub depth: usize,
    /// Queue capacity.
    pub cap: usize,
    /// True when manual jobs are held.
    pub paused: bool,
    /// True when the whole executor is frozen (nothing runs).
    pub frozen: bool,
    /// Currently running job, if any.
    pub running: Option<Job>,
    /// Pending jobs (FIFO order).
    pub pending: Vec<Job>,
    /// Most recent finished jobs, newest first.
    pub recent: Vec<Job>,
    /// Lifetime completed job count.
    pub done: u64,
    /// Lifetime failed job count.
    pub failed: u64,
}

#[derive(Debug)]
struct Inner {
    pending: VecDeque<Job>,
    running: Option<Job>,
    recent: VecDeque<Job>,
    done: u64,
    failed: u64,
}

/// Bounded, pausable job queue shared by the executor and control API.
#[derive(Debug)]
pub struct JobQueue {
    cap: usize,
    paused: AtomicBool,
    frozen: AtomicBool,
    inner: Mutex<Inner>,
    notify: Notify,
}

impl JobQueue {
    /// New queue holding at most `cap` pending jobs.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            paused: AtomicBool::new(false),
            frozen: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                pending: VecDeque::new(),
                running: None,
                recent: VecDeque::new(),
                done: 0,
                failed: 0,
            }),
            notify: Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A poisoned lock only means a prior panic; recovering the data is
        // safe here (plain counters + queue) and keeps the worker serving.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Enqueue a manual (user-triggered) job.
    pub fn enqueue_manual(
        &self,
        target: &str,
        url: &str,
        allow: Vec<String>,
    ) -> Result<Job, QueueError> {
        self.enqueue(target, url, allow, JobKind::Manual)
            .map(|j| j.expect("manual enqueue always yields a job"))
    }

    /// Enqueue a scheduled job unless one for `target` is already pending or
    /// running (dedupe so a slow target can't pile up). Returns the job when
    /// enqueued, `None` when deduped.
    pub fn enqueue_scheduled(
        &self,
        target: &str,
        url: &str,
        allow: Vec<String>,
    ) -> Result<Option<Job>, QueueError> {
        let inner = self.lock();
        let dup = inner
            .pending
            .iter()
            .chain(inner.running.as_ref())
            .any(|j| j.kind == JobKind::Scheduled && j.target == target);
        if dup {
            return Ok(None);
        }
        drop(inner);
        self.enqueue(target, url, allow, JobKind::Scheduled)
    }

    fn enqueue(
        &self,
        target: &str,
        url: &str,
        allow: Vec<String>,
        kind: JobKind,
    ) -> Result<Option<Job>, QueueError> {
        let mut inner = self.lock();
        if inner.pending.len() >= self.cap {
            return Err(QueueError::Full(self.cap));
        }
        let job = Job {
            id: next_job_id(),
            target: target.to_string(),
            url: url.to_string(),
            allow,
            kind,
            state: JobState::Pending,
            enqueued_at: crate::targets::now_unix(),
            started_at: None,
            finished_at: None,
            samples: 0,
            dropped: 0,
            error: None,
        };
        inner.pending.push_back(job.clone());
        drop(inner);
        self.notify.notify_one();
        Ok(Some(job))
    }

    /// Pause manual-job pickup (scheduled jobs keep running).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Resume manual-job pickup and wake the executor.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Whether manual jobs are held.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Freeze the executor: nothing runs (scheduled included) until resume.
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Unfreeze the executor and wake it to drain pending work.
    pub fn unfreeze(&self) {
        self.frozen.store(false, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Whether the executor is frozen.
    #[must_use]
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// Async wake-up future for the executor `select!`.
    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    /// Next job to run: manual first while running, scheduled-only while
    /// paused. Marks it `Running` and returns it. Returns `None` while
    /// frozen (nothing runs).
    pub fn pop_next(&self) -> Option<Job> {
        if self.is_frozen() {
            return None;
        }
        let mut inner = self.lock();
        let paused = self.is_paused();
        let idx = inner
            .pending
            .iter()
            .position(|j| !paused && j.kind == JobKind::Manual)
            .or_else(|| {
                inner
                    .pending
                    .iter()
                    .position(|j| j.kind == JobKind::Scheduled)
            })?;
        let mut job = inner.pending.remove(idx)?;
        job.state = JobState::Running;
        job.started_at = Some(crate::targets::now_unix());
        inner.running = Some(job.clone());
        Some(job)
    }

    /// Record a finished job: `Done` on `Ok`, else `Failed` with `error`.
    pub fn complete(
        &self,
        mut job: Job,
        status: ScrapeStatus,
        samples: usize,
        dropped: usize,
        error: Option<String>,
    ) {
        let mut inner = self.lock();
        job.state = if status == ScrapeStatus::Ok {
            inner.done = inner.done.saturating_add(1);
            JobState::Done
        } else {
            inner.failed = inner.failed.saturating_add(1);
            JobState::Failed
        };
        job.samples = samples;
        job.dropped = dropped;
        job.error = error;
        job.finished_at = Some(crate::targets::now_unix());
        inner.running = None;
        push_recent(&mut inner.recent, job);
    }

    /// Cancel one pending job.
    pub fn cancel(&self, id: &str) -> Result<Job, QueueError> {
        let mut inner = self.lock();
        let idx = inner
            .pending
            .iter()
            .position(|j| j.id == id)
            .ok_or_else(|| QueueError::NotFound(id.to_string()))?;
        let mut job = inner
            .pending
            .remove(idx)
            .ok_or_else(|| QueueError::NotFound(id.to_string()))?;
        job.state = JobState::Cancelled;
        job.finished_at = Some(crate::targets::now_unix());
        let cancelled = job.clone();
        push_recent(&mut inner.recent, job);
        Ok(cancelled)
    }

    /// Drop every pending job (returns how many were cancelled).
    pub fn clear_pending(&self) -> usize {
        let mut inner = self.lock();
        let now = crate::targets::now_unix();
        let mut n = 0;
        while let Some(mut job) = inner.pending.pop_front() {
            job.state = JobState::Cancelled;
            job.finished_at = Some(now);
            push_recent(&mut inner.recent, job);
            n += 1;
        }
        n
    }

    /// Display snapshot for `/api/v1/queue` and `/api/v1/status`.
    #[must_use]
    pub fn snapshot(&self) -> QueueSummary {
        let inner = self.lock();
        QueueSummary {
            depth: inner.pending.len(),
            cap: self.cap,
            paused: self.paused.load(Ordering::SeqCst),
            frozen: self.frozen.load(Ordering::SeqCst),
            running: inner.running.clone(),
            pending: inner.pending.iter().cloned().collect(),
            recent: inner.recent.iter().cloned().collect(),
            done: inner.done,
            failed: inner.failed,
        }
    }
}

fn push_recent(recent: &mut VecDeque<Job>, job: Job) {
    recent.push_front(job);
    recent.truncate(HISTORY_CAP);
}

fn next_job_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("j{}:{n}", crate::targets::now_unix())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue() -> JobQueue {
        JobQueue::new(4)
    }

    #[test]
    fn manual_preferred_over_scheduled() {
        let q = queue();
        q.enqueue_scheduled("a", "http://a", vec![]).unwrap();
        q.enqueue_manual("b", "http://b", vec![]).unwrap();
        let first = q.pop_next().unwrap();
        assert_eq!(first.target, "b", "manual job runs first");
        assert_eq!(first.state, JobState::Running);
        let second = q.pop_next().unwrap();
        assert_eq!(second.target, "a");
        assert!(q.pop_next().is_none());
    }

    #[test]
    fn pause_holds_manual_but_runs_scheduled() {
        let q = queue();
        q.enqueue_manual("b", "http://b", vec![]).unwrap();
        q.pause();
        assert!(q.pop_next().is_none(), "paused manual job stays pending");

        assert!(q
            .enqueue_scheduled("a", "http://a", vec![])
            .unwrap()
            .is_some());
        let job = q.pop_next().unwrap();
        assert_eq!(job.target, "a", "scheduled runs while paused");

        q.resume();
        let job = q.pop_next().unwrap();
        assert_eq!(job.target, "b", "manual runs after resume");
    }

    #[test]
    fn scheduled_dedupe_skips_pending_and_running() {
        let q = queue();
        assert!(q
            .enqueue_scheduled("a", "http://a", vec![])
            .unwrap()
            .is_some());
        assert!(q
            .enqueue_scheduled("a", "http://a", vec![])
            .unwrap()
            .is_none());
        let job = q.pop_next().unwrap(); // now running
        assert!(q
            .enqueue_scheduled("a", "http://a", vec![])
            .unwrap()
            .is_none());
        q.complete(job, ScrapeStatus::Ok, 1, 0, None);
        assert!(q
            .enqueue_scheduled("a", "http://a", vec![])
            .unwrap()
            .is_some());
    }

    #[test]
    fn bounded_and_clear_and_cancel() {
        let q = JobQueue::new(2);
        q.enqueue_manual("a", "http://a", vec![]).unwrap();
        let b = q.enqueue_manual("b", "http://b", vec![]).unwrap();
        assert!(matches!(
            q.enqueue_manual("c", "http://c", vec![]).unwrap_err(),
            QueueError::Full(2)
        ));
        assert_eq!(q.snapshot().depth, 2);
        q.cancel(&b.id).unwrap();
        assert_eq!(q.snapshot().depth, 1);
        assert!(q.cancel("nope").is_err());
        assert_eq!(q.clear_pending(), 1);
        assert_eq!(q.snapshot().depth, 0);
    }

    #[test]
    fn complete_records_state_and_counters() {
        let q = queue();
        let job = q.enqueue_manual("a", "http://a", vec![]).unwrap();
        let running = q.pop_next().unwrap();
        q.complete(running, ScrapeStatus::Ok, 12, 1, None);
        let snap = q.snapshot();
        assert_eq!(snap.done, 1);
        assert_eq!(snap.failed, 0);
        assert!(snap.running.is_none());
        assert_eq!(snap.recent[0].state, JobState::Done);
        assert_eq!(snap.recent[0].samples, 12);
        assert_eq!(snap.recent[0].dropped, 1);
        assert_eq!(snap.recent[0].target, job.target);

        let running = {
            q.enqueue_manual("b", "http://b", vec![]).unwrap();
            q.pop_next().unwrap()
        };
        q.complete(running, ScrapeStatus::FetchError, 0, 0, Some("boom".into()));
        let snap = q.snapshot();
        assert_eq!(snap.failed, 1);
        assert_eq!(snap.recent[0].state, JobState::Failed);
        assert_eq!(snap.recent[0].error.as_deref(), Some("boom"));
    }

    #[test]
    fn freeze_halts_all_pops_until_unfreeze() {
        let q = queue();
        q.enqueue_manual("m", "http://m", vec![]).unwrap();
        q.enqueue_scheduled("s", "http://s", vec![]).unwrap();
        q.freeze();
        assert!(q.is_frozen());
        assert!(q.snapshot().frozen);
        assert!(q.pop_next().is_none(), "frozen: nothing runs");
        assert_eq!(q.snapshot().depth, 2, "jobs stay queued while frozen");

        q.unfreeze();
        assert!(!q.is_frozen());
        let first = q.pop_next().unwrap();
        assert_eq!(first.target, "m", "manual remains first after unfreeze");
        let second = q.pop_next().unwrap();
        assert_eq!(second.target, "s");
    }
}
