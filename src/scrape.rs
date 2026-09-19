//! Scrape pipeline: fetch exposition → parse → allowlist → store.
//!
//! Mapping contract (see `prometheus-parse 0.2`):
//! * gauge samples → [`tonggeret::MetricType::Gauge`], everything else → Counter.
//! * Histogram `_bucket{le}` lines arrive aggregated as one sample; each
//!   bucket is stored as `<base>_bucket` + `le` label (counter). `_sum` and
//!   `_count` arrive as plain samples and are stored verbatim.
//! * Summary quantiles are stored as `<base>` + `quantile` label (counter).
//! * Every stored sample gains a `scrape_target` label (closed set from
//!   config), so apps share one store without touching them.
//! * Absent series (e.g. visitor metrics on apps without them) simply yield
//!   zero rows — never an error.

use prometheus_parse::{Sample, Scrape, Value};

use crate::config::TargetConfig;
use crate::query::{now_micros, BufferedSample, RecentBuffer};
use crate::status::StatusTracker;

/// Label injected on every stored sample.
pub const SCRAPE_TARGET_LABEL: &str = "scrape_target";

/// Exact series names never stored from scraped input: the source engine's
/// internal drop counter is meaningless aggregated across apps, and it would
/// collide with this collector's own registry entry.
const DENY_EXACT: &[&str] = &["tonggeret_dropped_total"];
/// Series-name prefixes reserved for this collector's own outcome series;
/// never stored from scraped input (same collision + feedback reason).
/// Do not re-add to any allowlist: this deny is checked before the allow
/// check in [`plan_samples`], so an allow entry could never match.
const DENY_PREFIX: &[&str] = &["collector_"];

/// One storable observation derived from exposition.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedSample {
    /// Series name after histogram/summary decomposition.
    pub name: String,
    /// Storage flavour (only Counter/Gauge are ever produced here).
    pub kind: tonggeret::MetricType,
    /// Sample value (`NaN`/`±Inf` pass through: `Float64`-safe).
    pub value: f64,
    /// Original labels plus `scrape_target`.
    pub labels: Vec<(String, String)>,
}

/// Outcome of one target scrape (feeds `collector_*` self series).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrapeStatus {
    /// Fetched, parsed, stored.
    Ok,
    /// Transport / non-2xx / oversize / non-UTF8 body.
    FetchError,
    /// Exposition unparsable.
    ParseError,
}

impl ScrapeStatus {
    /// Stable label value for `collector_scrape_total{status}`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::FetchError => "fetch_error",
            Self::ParseError => "parse_error",
        }
    }
}

/// Fetch failures (counted per target, never fatal to the loop).
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// Transport or non-2xx status.
    #[error("request failed: {0}")]
    Request(String),
    /// Body exceeds the configured cap.
    #[error("body exceeds {0} bytes")]
    TooLarge(usize),
    /// Body is not UTF-8 text.
    #[error("body is not valid UTF-8")]
    InvalidUtf8,
}

/// Parse exposition text. Unknown shapes are skipped by the parser itself,
/// so this only fails on IO-level errors (unreachable for in-memory lines).
pub fn parse_body(body: &str) -> Result<Scrape, String> {
    Scrape::parse(body.lines().map(|line| Ok(line.to_string()))).map_err(|e| e.to_string())
}

/// Map samples to storable observations: allowlist on the original name,
/// inject the target label, decompose histograms/summaries, enforce the cap.
///
/// Returns `(stored, over_cap_dropped)`. Filtered-out (non-allowlisted)
/// samples are neither stored nor counted as dropped.
#[must_use]
pub fn plan_samples(
    target: &str,
    scrape: &Scrape,
    allow: &[String],
    cap: usize,
) -> (Vec<PlannedSample>, usize) {
    let mut out = Vec::new();
    let mut dropped = 0;
    for sample in &scrape.samples {
        if DENY_EXACT.iter().any(|d| *d == sample.metric)
            || DENY_PREFIX.iter().any(|p| sample.metric.starts_with(p))
            || !allow.iter().any(|p| sample.metric.starts_with(p))
        {
            continue;
        }
        for planned in expand_sample(target, sample) {
            if out.len() >= cap {
                dropped += 1;
            } else {
                out.push(planned);
            }
        }
    }
    (out, dropped)
}

/// Record planned samples (non-blocking; silent no-op until engine init).
pub fn store_samples(samples: &[PlannedSample]) {
    use tonggeret::MetricType as Kind;
    for s in samples {
        match s.kind {
            Kind::Counter => tonggeret::record_counter(&s.name, s.value, s.labels.clone()),
            Kind::Gauge => tonggeret::record_gauge(&s.name, s.value, s.labels.clone()),
            // Decomposition never emits distributions: scraped histograms
            // are cumulative bucket counters, not samples. Recording one
            // via `record_histogram` would corrupt the local registry.
            Kind::Histogram => {}
        }
    }
}

/// Fetch one exposition body with size guard.
pub async fn fetch_body(
    client: &reqwest::Client,
    url: &str,
    max_bytes: usize,
) -> Result<String, FetchError> {
    let resp = client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| FetchError::Request(e.to_string()))?;
    let limit = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if resp.content_length().is_some_and(|len| len > limit) {
        return Err(FetchError::TooLarge(max_bytes));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| FetchError::Request(e.to_string()))?;
    if bytes.len() > max_bytes {
        return Err(FetchError::TooLarge(max_bytes));
    }
    String::from_utf8(bytes.to_vec()).map_err(|_| FetchError::InvalidUtf8)
}

/// One full scrape cycle for a target: fetch → parse → plan → store, plus
/// `collector_*` outcome series. Every failure mode is counted, none throw.
/// Planned samples are also mirrored into `buffer` (recent range queries),
/// and the outcome feeds `tracker` for `GET /api/v1/status`.
pub async fn scrape_once(
    client: &reqwest::Client,
    target: &TargetConfig,
    max_samples: usize,
    max_body: usize,
    buffer: &RecentBuffer,
    tracker: &StatusTracker,
) -> ScrapeStatus {
    let now = now_micros();
    let start = std::time::Instant::now();
    let mut samples = 0_usize;
    let status = match fetch_body(client, &target.url, max_body).await {
        Err(e) => {
            tracing::warn!(target = %target.name, error = %e, "scrape fetch failed");
            ScrapeStatus::FetchError
        }
        Ok(body) => match parse_body(&body) {
            Err(e) => {
                tracing::warn!(target = %target.name, error = %e, "scrape parse failed");
                ScrapeStatus::ParseError
            }
            Ok(scrape) => {
                let (planned, dropped) =
                    plan_samples(&target.name, &scrape, &target.allow, max_samples);
                samples = planned.len();
                store_samples(&planned);
                buffer.push_batch(
                    &target.name,
                    &planned
                        .iter()
                        .map(|s| BufferedSample {
                            ts_micros: now,
                            name: s.name.clone(),
                            value: s.value,
                            labels: s.labels.clone(),
                        })
                        .collect::<Vec<_>>(),
                );
                record_outcome(&target.name, planned.len(), dropped);
                tracing::debug!(
                    target = %target.name,
                    stored = planned.len(),
                    dropped,
                    "scrape stored"
                );
                ScrapeStatus::Ok
            }
        },
    };
    tonggeret::counter!(
        "collector_scrape_total",
        1.0,
        target = target.name.as_str(),
        status = status.as_str()
    );
    // Milliseconds fit `u64` for any realistic scrape; saturate on overflow.
    #[allow(clippy::cast_possible_truncation)]
    let duration_ms = start.elapsed().as_millis() as u64;
    tracker.record(&target.name, status, samples, duration_ms);
    status
}

fn record_outcome(target: &str, stored: usize, dropped: usize) {
    // Counts fit `f64` exactly (scrape caps keep them ≪ 2⁵³).
    #[allow(clippy::cast_precision_loss)]
    let stored_f = stored as f64;
    #[allow(clippy::cast_precision_loss)]
    let dropped_f = dropped as f64;
    tonggeret::counter!("collector_samples_stored_total", stored_f, target = target);
    if dropped > 0 {
        tonggeret::counter!(
            "collector_samples_dropped_total",
            dropped_f,
            target = target,
            reason = "over_cap"
        );
    }
}

fn planned(
    name: &str,
    kind: tonggeret::MetricType,
    value: f64,
    labels: Vec<(String, String)>,
) -> PlannedSample {
    PlannedSample {
        name: name.to_string(),
        kind,
        value,
        labels,
    }
}

/// Expand one parsed sample, injecting the `scrape_target` label.
///
/// Label pairs are sorted by key: the mirror registry matches values to
/// names positionally, so every sample sharing a series name must present
/// keys in identical order (parsed label maps iterate randomly).
fn expand_sample(target: &str, sample: &Sample) -> Vec<PlannedSample> {
    use tonggeret::MetricType as Kind;
    let mut base: Vec<(String, String)> = sample
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    base.push((SCRAPE_TARGET_LABEL.to_string(), target.to_string()));
    base.sort_by(|a, b| a.0.cmp(&b.0));
    match &sample.value {
        Value::Gauge(v) => vec![planned(&sample.metric, Kind::Gauge, *v, base)],
        Value::Counter(v) | Value::Untyped(v) => {
            vec![planned(&sample.metric, Kind::Counter, *v, base)]
        }
        Value::Histogram(buckets) => buckets
            .iter()
            .map(|b| {
                let mut labels = base.clone();
                labels.push(("le".to_string(), format_float(b.less_than)));
                planned(
                    &format!("{}_bucket", sample.metric),
                    Kind::Counter,
                    b.count,
                    labels,
                )
            })
            .collect(),
        Value::Summary(quants) => quants
            .iter()
            .map(|q| {
                let mut labels = base.clone();
                labels.push(("quantile".to_string(), format_float(q.quantile)));
                planned(&sample.metric, Kind::Counter, q.count, labels)
            })
            .collect(),
    }
}

/// Shortest round-trip float rendering with Prometheus `±Inf` spelling
/// (`{}` prints `inf`; exposition uses `+Inf`).
fn format_float(v: f64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    const ALLOW: &[&str] = &["http_", "beruang_", "tonggeret_", "visitors_", "unique_"];

    fn allow() -> Vec<String> {
        ALLOW.iter().map(ToString::to_string).collect()
    }

    fn scrape_of(text: &str) -> Scrape {
        parse_body(text).unwrap()
    }

    fn find<'a>(planned: &'a [PlannedSample], name: &str) -> Vec<&'a PlannedSample> {
        planned.iter().filter(|s| s.name == name).collect()
    }

    fn label(sample: &PlannedSample, key: &str) -> Option<String> {
        sample
            .labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }

    #[test]
    fn counter_gauge_untyped_map_with_target_label() {
        let text = "# TYPE a counter\na 1\n# TYPE b gauge\nb{x=\"y\"} 2.5\nc 3\n";
        let (planned, dropped) = plan_samples("app", &scrape_of(text), &allow(), 100);
        assert_eq!(dropped, 0);
        // `a`/`b`/`c` match no allow prefix… so nothing is stored.
        assert!(planned.is_empty());

        let text = "# TYPE http_x counter\nhttp_x 1\n# TYPE beruang_y gauge\nberuang_y 2\n";
        let (planned, dropped) = plan_samples("app", &scrape_of(text), &allow(), 100);
        assert_eq!(dropped, 0);
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].kind, tonggeret::MetricType::Counter);
        assert_eq!(planned[1].kind, tonggeret::MetricType::Gauge);
        for s in &planned {
            assert_eq!(label(s, SCRAPE_TARGET_LABEL).as_deref(), Some("app"));
        }
    }

    #[test]
    fn histogram_buckets_become_le_counters() {
        let text = "# TYPE http_h histogram\nhttp_h_bucket{le=\"0.5\"} 10\nhttp_h_bucket{le=\"+Inf\"} 12\nhttp_h_sum 99\nhttp_h_count 12\n";
        let (planned, dropped) = plan_samples("app", &scrape_of(text), &allow(), 100);
        assert_eq!(dropped, 0);
        let buckets = find(&planned, "http_h_bucket");
        assert_eq!(buckets.len(), 2);
        let les: Vec<String> = buckets.iter().map(|b| label(b, "le").unwrap()).collect();
        assert!(les.contains(&"0.5".to_string()));
        assert!(les.contains(&"+Inf".to_string()));
        assert!(buckets
            .iter()
            .all(|b| b.kind == tonggeret::MetricType::Counter));
        assert_eq!(find(&planned, "http_h_sum").len(), 1);
        assert_eq!(find(&planned, "http_h_count").len(), 1);
    }

    #[test]
    fn summary_quantiles_become_quantile_counters() {
        let text = "# TYPE http_s summary\nhttp_s{quantile=\"0.5\"} 7\nhttp_s_sum 70\n";
        let (planned, _) = plan_samples("app", &scrape_of(text), &allow(), 100);
        let qs = find(&planned, "http_s");
        assert_eq!(qs.len(), 1);
        assert_eq!(label(qs[0], "quantile").as_deref(), Some("0.5"));
        assert_eq!(qs[0].kind, tonggeret::MetricType::Counter);
    }

    #[test]
    fn visitor_series_pass_allowlist_and_absence_is_empty() {
        let text = "# TYPE visitors_total counter\nvisitors_total{region=\"DE\"} 5\n\
                    # TYPE unique_visitors_estimate gauge\nunique_visitors_estimate{region=\"DE\"} 4\n";
        let (planned, _) = plan_samples("app", &scrape_of(text), &allow(), 100);
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].kind, tonggeret::MetricType::Counter);
        assert_eq!(planned[1].kind, tonggeret::MetricType::Gauge);
        assert_eq!(label(&planned[0], "region").as_deref(), Some("DE"));

        // App without visitor instrumentation: no visitor rows, no error.
        let (planned, dropped) = plan_samples(
            "plain",
            &scrape_of("# TYPE http_x counter\nhttp_x 1\n"),
            &allow(),
            100,
        );
        assert_eq!(dropped, 0);
        assert!(
            planned.iter().all(|s| !s.name.starts_with("visitor"))
                && planned.iter().all(|s| !s.name.starts_with("unique_"))
        );
    }

    #[test]
    fn reserved_names_never_stored_from_scrapes() {
        let text = "tonggeret_dropped_total 0\ncollector_scrape_total{target=\"x\",status=\"ok\"} 1\nhttp_x 1\n";
        let (planned, _) = plan_samples("app", &scrape_of(text), &allow(), 100);
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].name, "http_x");
    }

    #[test]
    fn labels_sorted_by_key_for_positional_registry() {
        let text = "http_x{status=\"200\",method=\"GET\",path=\"/\"} 1\n";
        let (planned, _) = plan_samples("app", &scrape_of(text), &allow(), 100);
        assert_eq!(planned.len(), 1);
        let keys: Vec<&str> = planned[0].labels.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["method", "path", "scrape_target", "status"]);
    }

    #[test]
    fn cap_counts_overflow_as_dropped() {
        let text = "http_a 1\nhttp_b 2\nhttp_c 3\n";
        let (planned, dropped) = plan_samples("app", &scrape_of(text), &allow(), 2);
        assert_eq!(planned.len(), 2);
        assert_eq!(dropped, 1);
    }

    #[test]
    fn nan_passthrough_and_float_spelling() {
        assert_eq!(format_float(f64::NAN), "NaN");
        assert_eq!(format_float(f64::INFINITY), "+Inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_float(0.5), "0.5");
        let text = "http_nan Nan\n";
        let (planned, _) = plan_samples("app", &scrape_of(text), &allow(), 100);
        assert_eq!(planned.len(), 1);
        assert!(planned[0].value.is_nan());
    }
}
