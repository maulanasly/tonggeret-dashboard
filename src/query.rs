//! Hot range-query API: Prometheus `query_range`-shaped JSON over a bounded
//! in-memory buffer of recent scrape samples.
//!
//! Why a buffer instead of reading Fjall: the tonggeret engine opens the
//! keyspace exactly once per process and exposes no read handles ("never
//! open the same directory twice"), so the collector keeps its own recent
//! observations. The buffer is bounded (drop-oldest, drops counted via
//! `collector_buffer_dropped_total`) and covers process lifetime only —
//! cold Parquet remains the history of record.
//!
//! Memory budget: `cap` samples × ~300 B ≈ 6 MiB at the 20k default.
//! No background tasks; pushes happen inline on the existing scrape path,
//! reads are short mutex-held scans in the request handler.
//!
//! Subset contract (documented, not full PromQL):
//! * `query` is an exact metric name or `{__name__="x",k="v",...}` with
//!   `=` matchers only (`=~`, `!=`, `!~` are rejected, not silently run).
//! * `start`/`end` are unix seconds (fractional allowed); `step` is seconds
//!   or a `<n>s|m|h|d|w` duration, minimum 1s.
//! * Per `(series, step-bucket)` the latest sample wins (counters and gauge
//!   estimates alike — never summed here either).
//! * Hard caps: 7-day range, 10k total points. Over-limit → 400, not truncation.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};

/// Default buffer capacity in samples (~6 MiB worst case).
pub const DEFAULT_BUFFER_SAMPLES: usize = 20_000;
/// Hard cap on total points per response (Prometheus defaults to ~11k).
pub const MAX_POINTS: usize = 10_000;
/// Hard cap on the requested time range (bounds scan CPU).
pub const MAX_RANGE_SECS: f64 = 7.0 * 24.0 * 3600.0;
/// Minimum step: sub-second buckets would explode point counts.
pub const MIN_STEP_SECS: f64 = 1.0;

/// One buffered observation (labels already sorted by the scrape pipeline).
#[derive(Debug, Clone, PartialEq)]
pub struct BufferedSample {
    /// Capture time, micros since the Unix epoch.
    pub ts_micros: u64,
    /// Series name.
    pub name: String,
    /// Sample value (`NaN`/`±Inf` pass through to `"NaN"`/`"+Inf"`/`"-Inf"`).
    pub value: f64,
    /// Sorted `(key, value)` label pairs, including `scrape_target`.
    pub labels: Vec<(String, String)>,
}

/// Bounded recent-samples buffer shared between the scrape loop (writer)
/// and the query handler (reader). Drop-oldest on overflow.
#[derive(Debug)]
pub struct RecentBuffer {
    inner: Mutex<VecDeque<BufferedSample>>,
    cap: usize,
}

impl RecentBuffer {
    /// Create an empty buffer holding at most `cap` samples.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            cap: cap.max(1),
        }
    }

    /// Current occupancy and capacity, for the collector status snapshot.
    #[must_use]
    pub fn occupancy(&self) -> (usize, usize) {
        (self.inner.lock().map_or(0, |q| q.len()), self.cap)
    }

    /// Append samples from one scrape of `target`; evict oldest past capacity.
    /// Evictions are counted, never fatal.
    pub fn push_batch(&self, target: &str, samples: &[BufferedSample]) {
        let mut dropped = 0_usize;
        if let Ok(mut q) = self.inner.lock() {
            q.extend(samples.iter().cloned());
            let len = q.len();
            if len > self.cap {
                dropped = len - self.cap;
                q.drain(..dropped);
            }
        }
        if dropped > 0 {
            #[allow(clippy::cast_precision_loss)]
            let dropped_f = dropped as f64;
            tonggeret::counter!(
                "collector_buffer_dropped_total",
                dropped_f,
                target = target,
                reason = "over_cap"
            );
        }
    }

    /// Sorted distinct metric names currently buffered (backs `/api/v1/labels`
    /// so hot-mode clients can offer a metric picker without a query language).
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        if let Ok(q) = self.inner.lock() {
            set.extend(q.iter().map(|s| s.name.clone()));
        }
        set.into_iter().collect()
    }

    /// Run a parsed range query: filter → group by series → latest per
    /// step-bucket. Returns series in first-seen order plus whether the
    /// requested start predates the buffer (partial answer).
    pub fn query(&self, q: &RangeQuery) -> Result<QueryOutcome, QueryError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| QueryError::new("buffer unavailable"))?;
        let oldest = guard.front().map(|s| s.ts_micros);
        let mut groups: Vec<Series> = Vec::new();
        for s in guard.iter() {
            if s.ts_micros < q.start_micros || s.ts_micros > q.end_micros || s.name != q.name {
                continue;
            }
            if !q
                .matchers
                .iter()
                .all(|(k, v)| s.labels.iter().any(|(lk, lv)| lk == k && lv == v))
            {
                continue;
            }
            let gi = if let Some(i) = groups
                .iter()
                .position(|g| g.name == s.name && g.labels == s.labels)
            {
                i
            } else {
                groups.push(Series {
                    name: s.name.clone(),
                    labels: s.labels.clone(),
                    buckets: BTreeMap::new(),
                });
                groups.len() - 1
            };
            let bucket = (s.ts_micros - q.start_micros) / q.step_micros;
            groups[gi]
                .buckets
                .entry(bucket)
                // Latest sample in the bucket wins (entries arrive in time order).
                .and_modify(|e: &mut (u64, f64)| {
                    if s.ts_micros >= e.0 {
                        *e = (s.ts_micros, s.value);
                    }
                })
                .or_insert((s.ts_micros, s.value));
        }
        let points: usize = groups.iter().map(|g| g.buckets.len()).sum();
        if points > MAX_POINTS {
            return Err(QueryError::new(format!(
                "query would return {points} points (limit {MAX_POINTS}); increase step"
            )));
        }
        Ok(QueryOutcome {
            series: groups,
            clamped: oldest.is_some_and(|old| old > q.start_micros),
        })
    }
}

/// One grouped series with per-bucket latest points (`bucket → (ts, value)`).
#[derive(Debug)]
pub struct Series {
    /// Series name.
    pub name: String,
    /// Sorted label pairs.
    pub labels: Vec<(String, String)>,
    /// Step-bucket index → latest `(ts_micros, value)`.
    pub buckets: BTreeMap<u64, (u64, f64)>,
}

/// A validated range query.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeQuery {
    /// Exact metric name.
    pub name: String,
    /// Exact `=` label matchers.
    pub matchers: Vec<(String, String)>,
    /// Range start, micros since epoch.
    pub start_micros: u64,
    /// Range end, micros since epoch.
    pub end_micros: u64,
    /// Step width, micros (≥ 1s).
    pub step_micros: u64,
}

/// Outcome of [`RecentBuffer::query`].
#[derive(Debug)]
pub struct QueryOutcome {
    /// Matched series (possibly empty — empty is success, not error).
    pub series: Vec<Series>,
    /// True when `start` predates the buffer (partial answer; surfaced as a
    /// top-level `warnings` entry like Prometheus).
    pub clamped: bool,
}

/// Query validation failure (always HTTP 400 with a Prometheus error body).
#[derive(Debug)]
pub struct QueryError {
    msg: String,
}

impl QueryError {
    fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }

    /// Public constructor for handler-level rejections.
    #[must_use]
    pub fn bad_data(msg: impl Into<String>) -> Self {
        Self::new(msg)
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "errorType": "bad_data",
                "error": self.msg,
            })),
        )
            .into_response()
    }
}

/// Parse `query`: bare `metric_name` or `{__name__="x",k="v"}` with `=`
/// matchers only. Anything else is rejected explicitly.
pub fn parse_selector(s: &str) -> Result<(String, Vec<(String, String)>), QueryError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(QueryError::new("missing query parameter"));
    }
    if !s.starts_with('{') {
        if s.starts_with('}') || s.contains(['{', '}', '"', '=']) {
            return Err(QueryError::new(format!("invalid metric name {s:?}")));
        }
        return Ok((s.to_string(), Vec::new()));
    }
    let body = s
        .strip_prefix('{')
        .and_then(|b| b.strip_suffix('}'))
        .ok_or_else(|| QueryError::new(format!("malformed selector {s:?}: expected {{...}}")))?;
    let mut name: Option<String> = None;
    let mut matchers = Vec::new();
    if !body.trim().is_empty() {
        for part in body.split(',') {
            if part.contains("=~") || part.contains("!=") {
                return Err(QueryError::new(
                    "only exact = matchers are supported (no =~, !=, !~)",
                ));
            }
            let (k, v) = part.split_once('=').ok_or_else(|| {
                QueryError::new(format!("malformed matcher {part:?}: expected k=\"v\""))
            })?;
            let key = k.trim();
            if key.is_empty() || key.ends_with('!') || v.starts_with('!') {
                return Err(QueryError::new(
                    "only exact = matchers are supported (no =~, !=, !~)",
                ));
            }
            let val = v
                .trim()
                .strip_prefix('"')
                .and_then(|b| b.strip_suffix('"'))
                .ok_or_else(|| {
                    QueryError::new(format!("malformed matcher {part:?}: value must be quoted"))
                })?;
            if key == "__name__" {
                name = Some(val.to_string());
            } else {
                matchers.push((key.to_string(), val.to_string()));
            }
        }
    }
    match name {
        Some(n) if !n.is_empty() => Ok((n, matchers)),
        _ => Err(QueryError::new(
            "selector requires a metric name (bare name or __name__)",
        )),
    }
}

/// Parse `step`: plain seconds (`90`) or `<n>s|m|h|d|w` (`5m`). Minimum 1s.
pub fn parse_step(s: &str) -> Result<f64, QueryError> {
    let s = s.trim();
    let (num, mult) = if let Some(b) = s.strip_suffix(['s', 'm', 'h', 'd', 'w']) {
        // `strip_suffix` with a pattern array strips one char; recover which.
        let unit = s.chars().last().unwrap_or('s');
        let mult = match unit {
            's' => 1.0,
            'm' => 60.0,
            'h' => 3600.0,
            'd' => 86_400.0,
            'w' => 604_800.0,
            _ => unreachable!(),
        };
        (b, mult)
    } else {
        (s, 1.0)
    };
    let n: f64 = num
        .parse()
        .map_err(|_| QueryError::new(format!("invalid step {s:?}")))?;
    let secs = n * mult;
    if !secs.is_finite() || secs < MIN_STEP_SECS {
        return Err(QueryError::new(format!(
            "step must be >= {MIN_STEP_SECS}s, got {s:?}"
        )));
    }
    Ok(secs)
}

/// Parse `start`/`end`: unix seconds (fractional allowed). No keywords.
pub fn parse_time(s: &str, param: &str) -> Result<f64, QueryError> {
    s.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v >= 0.0)
        .ok_or_else(|| QueryError::new(format!("invalid {param} {s:?}: want unix seconds")))
}

/// Validate the four raw params into a [`RangeQuery`], clamping `end` to now.
pub fn parse_range_query(
    query: &str,
    start: &str,
    end: &str,
    step: &str,
    now_micros: u64,
) -> Result<RangeQuery, QueryError> {
    let (name, matchers) = parse_selector(query)?;
    let start_s = parse_time(start, "start")?;
    let mut end_s = parse_time(end, "end")?;
    let step_s = parse_step(step)?;
    #[allow(clippy::cast_precision_loss)]
    let now_s = now_micros as f64 / 1_000_000.0;
    if end_s > now_s {
        end_s = now_s;
    }
    if end_s <= start_s {
        return Err(QueryError::new("end must be after start"));
    }
    if end_s - start_s > MAX_RANGE_SECS {
        return Err(QueryError::new(format!(
            "range exceeds {} days",
            MAX_RANGE_SECS / 86_400.0
        )));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(RangeQuery {
        name,
        matchers,
        start_micros: (start_s * 1_000_000.0) as u64,
        end_micros: (end_s * 1_000_000.0) as u64,
        step_micros: (step_s * 1_000_000.0) as u64,
    })
}

/// Prometheus value spelling as JSON string (`NaN`, `+Inf`, `-Inf`).
#[must_use]
pub fn format_value(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    format!("{v}")
}

/// Render an outcome as Prometheus `query_range` matrix JSON.
#[must_use]
pub fn render_matrix(outcome: &QueryOutcome) -> serde_json::Value {
    let result: Vec<serde_json::Value> = outcome
        .series
        .iter()
        .map(|g| {
            let mut metric = BTreeMap::new();
            metric.insert("__name__".to_string(), g.name.clone());
            for (k, v) in &g.labels {
                metric.insert(k.clone(), v.clone());
            }
            #[allow(clippy::cast_precision_loss)]
            let values: Vec<serde_json::Value> = g
                .buckets
                .values()
                .map(|(ts, v)| serde_json::json!([(*ts as f64) / 1_000_000.0, format_value(*v)]))
                .collect();
            serde_json::json!({ "metric": metric, "values": values })
        })
        .collect();
    let mut body = serde_json::json!({
        "status": "success",
        "data": { "resultType": "matrix", "result": result },
    });
    if outcome.clamped {
        body["warnings"] = serde_json::json!([
            "start predates the in-memory buffer; answer covers buffered data only"
        ]);
    }
    body
}

/// Current time, micros since the Unix epoch (saturates at 0 on clock skew).
#[must_use]
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros().try_into().unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(ts: u64, name: &str, value: f64, labels: &[(&str, &str)]) -> BufferedSample {
        BufferedSample {
            ts_micros: ts,
            name: name.to_string(),
            value,
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    #[test]
    fn selector_bare_name_and_braces() {
        assert_eq!(
            parse_selector("http_x").unwrap(),
            ("http_x".to_string(), vec![])
        );
        assert_eq!(
            parse_selector("{__name__=\"http_x\",method=\"GET\"}").unwrap(),
            (
                "http_x".to_string(),
                vec![("method".to_string(), "GET".to_string())]
            )
        );
        assert_eq!(
            parse_selector("  { __name__ = \"a\" } ").unwrap().0,
            "a".to_string()
        );
    }

    #[test]
    fn selector_rejects_non_exact_matchers() {
        assert!(parse_selector("").is_err());
        for bad in [
            "{method=~\"G.*\"}",
            "{__name__=\"a\",k!=\"v\"}",
            "{__name__=\"a\",k!~\"v\"}",
            "{method=\"GET\"}",
            "{__name__=x}",
            "http{x}",
        ] {
            let err = parse_selector(bad).unwrap_err();
            assert!(
                err.msg.contains("exact")
                    || err.msg.contains("requires a metric name")
                    || err.msg.contains("malformed")
                    || err.msg.contains("invalid"),
                "{bad}: unexpected message {}",
                err.msg
            );
        }
    }

    #[test]
    fn step_units_and_floor() {
        for (input, want) in [
            ("90", 90.0),
            ("30s", 30.0),
            ("5m", 300.0),
            ("2h", 7200.0),
            ("1d", 86_400.0),
            ("1w", 604_800.0),
            ("1.5m", 90.0),
        ] {
            let got = parse_step(input).unwrap();
            assert!(
                (got - want).abs() <= f64::EPSILON * want.max(1.0),
                "{input}: got {got}, want {want}"
            );
        }
        assert!(parse_step("0s").is_err());
        assert!(parse_step("500ms").is_err());
        assert!(parse_step("soon").is_err());
    }

    #[test]
    fn time_accepts_only_finite_nonneg_seconds() {
        let got = parse_time("1700000000", "start").unwrap();
        assert!((got - 1_700_000_000.0).abs() <= 1.0, "got {got}");
        let got = parse_time("1700000000.5", "start").unwrap();
        assert!((got - 1_700_000_000.5).abs() <= 1.0, "got {got}");
        assert!(parse_time("now", "start").is_err());
        assert!(parse_time("-5", "start").is_err());
        assert!(parse_time("NaN", "start").is_err());
    }

    #[test]
    fn range_validation_clamps_end_and_caps_range() {
        let now = 2_000_000_000_000_000u64; // micros
        let q = parse_range_query("m", "1999999990", "9999999999", "60", now).unwrap();
        assert_eq!(q.end_micros, now);
        assert!(parse_range_query("m", "10", "5", "60", now).is_err());
        assert!(parse_range_query("m", "0", "999999999", "60", now).is_err());
    }

    fn seeded() -> RecentBuffer {
        let b = RecentBuffer::new(100);
        b.push_batch(
            "app",
            &[
                sample(1_000_000, "http_x", 1.0, &[("a", "1")]),
                sample(2_000_000, "http_x", 2.0, &[("a", "1")]),
                sample(2_500_000, "http_x", 9.0, &[("a", "2")]),
                sample(3_000_000, "other", 5.0, &[]),
            ],
        );
        b
    }

    fn rq(name: &str, matchers: Vec<(String, String)>) -> RangeQuery {
        RangeQuery {
            name: name.to_string(),
            matchers,
            start_micros: 1_000_000,
            end_micros: 10_000_000,
            step_micros: 1_000_000,
        }
    }

    #[test]
    fn query_groups_and_latest_per_bucket_wins() {
        let b = seeded();
        let out = b.query(&rq("http_x", vec![])).unwrap();
        assert!(!out.clamped);
        assert_eq!(out.series.len(), 2);
        // Series a=1: buckets 1s and 2s → latest per bucket.
        let g1 = out
            .series
            .iter()
            .find(|g| g.labels == [("a".to_string(), "1".to_string())])
            .unwrap();
        assert_eq!(g1.buckets.len(), 2);
        assert_eq!(g1.buckets[&1], (2_000_000, 2.0));
        // Step wider than the data collapses to one bucket.
        let wide = RangeQuery {
            step_micros: 60_000_000,
            ..rq("http_x", vec![])
        };
        let out = b.query(&wide).unwrap();
        let g1 = out
            .series
            .iter()
            .find(|g| g.labels == [("a".to_string(), "1".to_string())])
            .unwrap();
        assert_eq!(g1.buckets.len(), 1);
        assert_eq!(g1.buckets[&0], (2_000_000, 2.0));
    }

    #[test]
    fn query_matchers_filter_and_empty_is_success() {
        let b = seeded();
        let out = b
            .query(&rq("http_x", vec![("a".to_string(), "2".to_string())]))
            .unwrap();
        assert_eq!(out.series.len(), 1);
        let out = b.query(&rq("missing", vec![])).unwrap();
        assert!(out.series.is_empty());
    }

    #[test]
    fn query_clamps_and_caps_points() {
        let b = seeded();
        // Start exactly at the oldest sample → full answer, no warning.
        let out = b
            .query(&RangeQuery {
                start_micros: 1_000_000,
                ..rq("http_x", vec![])
            })
            .unwrap();
        assert!(!out.clamped);
        // Window fully before the buffer → clamped warning, empty success.
        let out = b
            .query(&RangeQuery {
                start_micros: 0,
                end_micros: 500_000,
                ..rq("http_x", vec![])
            })
            .unwrap();
        assert!(out.clamped);
        assert!(out.series.is_empty());
    }

    #[test]
    fn buffer_drops_oldest_past_capacity() {
        let b = RecentBuffer::new(2);
        b.push_batch(
            "app",
            &[
                sample(1, "m", 1.0, &[]),
                sample(2, "m", 2.0, &[]),
                sample(3, "m", 3.0, &[]),
            ],
        );
        assert_eq!(b.occupancy(), (2, 2));
        let out = b
            .query(&RangeQuery {
                start_micros: 0,
                step_micros: 1,
                ..rq("m", vec![])
            })
            .unwrap();
        let vals: Vec<f64> = out.series[0].buckets.values().map(|(_, v)| *v).collect();
        assert_eq!(vals, vec![2.0, 3.0]);
    }

    #[test]
    fn value_spelling_matches_prometheus() {
        assert_eq!(format_value(1.0), "1");
        assert_eq!(format_value(0.5), "0.5");
        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn names_lists_distinct_sorted_series() {
        let b = seeded();
        assert_eq!(b.names(), vec!["http_x".to_string(), "other".to_string()]);
        assert!(RecentBuffer::new(10).names().is_empty());
    }

    #[test]
    fn matrix_shape_and_warnings() {
        let b = seeded();
        let out = b.query(&rq("http_x", vec![])).unwrap();
        let body = render_matrix(&out);
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "matrix");
        assert_eq!(body["data"]["result"].as_array().unwrap().len(), 2);
        let first = &body["data"]["result"][0];
        assert_eq!(first["metric"]["__name__"], "http_x");
        assert!(first["values"][0][0].is_number());
        assert!(first["values"][0][1].is_string());
        assert!(body.get("warnings").is_none());

        let clamped = QueryOutcome {
            series: vec![],
            clamped: true,
        };
        let body = render_matrix(&clamped);
        assert!(body.get("warnings").is_some());
    }
}
