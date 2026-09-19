//! Analytical SQL builders for the tonggeret Parquet consumption contract.
//!
//! Cold export schema (see tonggeret `parquet_exporter.rs`):
//! `ts Timestamp(Microsecond)`, `name Utf8`, `value Float64`,
//! `metric_type Utf8`, `labels Utf8` (JSON object string).
//!
//! Dialect notes (DuckDB-WASM):
//! * `labels` is a Utf8 JSON *string*, so label access uses
//!   `json_extract_string(labels, '$.<key>')` — not Postgres `->>`.
//! * Time column is `ts` (not `timestamp`).
//! * Bucketing uses `date_trunc('<part>', ts)` (portable in WASM) instead of
//!   the `time_bucket(INTERVAL …)` extension function.
//! * Bucket output is `epoch_us(…)` microseconds as `bucket_us` so
//!   Arrow/BigInt handling in JS stays unambiguous.
//!
//! This module is DOM-free so it can be `node --check`ed and unit-tested.

/// Time ranges offered by the UI dropdown.
export const Ranges = {
  '1h': { label: 'Last 1 Hour', bucketPart: 'minute', bucketSecs: 60 },
  '24h': { label: 'Last 24 Hours', bucketPart: 'hour', bucketSecs: 3600 },
  all: { label: 'All Time', bucketPart: 'day', bucketSecs: 86400 },
};

/// Escape a string for embedding in a single-quoted SQL literal.
function lit(value) {
  return String(value).replaceAll("'", "''");
}

/// `WHERE`-fragment for a range key. Returns `TRUE` for `all`.
function timeFilter(range) {
  if (range === '1h') return "ts >= (now() - INTERVAL 1 HOUR)";
  if (range === '24h') return "ts >= (now() - INTERVAL 24 HOUR)";
  return 'TRUE';
}

/// Bucket expression for a range key, e.g. `date_trunc('minute', ts)`.
function bucketExpr(range) {
  const part = (Ranges[range] ?? Ranges['24h']).bucketPart;
  return `date_trunc('${part}', ts)`;
}

export const Queries = {
  Ranges,
  timeFilter,
  bucketExpr,

  /// Request throughput + latency breakdown in a single round-trip.
  throughputLatency(range) {
    const filter = timeFilter(range);
    const bucket = bucketExpr(range);
    return (
      `SELECT epoch_us(${bucket}) AS bucket_us,\n` +
      `  count(*) FILTER (WHERE name = 'http_requests_total') AS reqs,\n` +
      `  avg(value) FILTER (WHERE name = 'http_request_duration_ms') AS avg_ms,\n` +
      `  quantile_cont(value, 0.99) FILTER (WHERE name = 'http_request_duration_ms') AS p99_ms\n` +
      `FROM metrics_all\n` +
      `WHERE ${filter}\n` +
      `GROUP BY 1\n` +
      `ORDER BY 1`
    );
  },

  /// HTTP 4xx/5xx breakdown grouped by endpoint path.
  errorDistribution(range) {
    const filter = timeFilter(range);
    return (
      `SELECT COALESCE(NULLIF(json_extract_string(labels, '$.path'), ''), '(unknown)') AS path,\n` +
      `  count(*) FILTER (WHERE json_extract_string(labels, '$.status') LIKE '4%') AS c4xx,\n` +
      `  count(*) FILTER (WHERE json_extract_string(labels, '$.status') LIKE '5%') AS c5xx,\n` +
      `  count(*) AS total\n` +
      `FROM metrics_all\n` +
      `WHERE name = 'http_requests_total' AND ${filter}\n` +
      `GROUP BY 1\n` +
      `HAVING (c4xx + c5xx) > 0\n` +
      `ORDER BY (c4xx + c5xx) DESC\n` +
      `LIMIT 20`
    );
  },

  /// Distinct metric names for the custom visualizer dropdown.
  listMetricNames() {
    return 'SELECT DISTINCT name FROM metrics_all ORDER BY 1 LIMIT 200';
  },

  /// Visitor preset: latest `visitors_total` and `unique_visitors_estimate`
  /// per bucket, grouped by target + region. Both are read as latest
  /// (`arg_max(value, ts)`): the uniques estimate is a gauge and must
  /// never be summed — take latest per `(target, region)`.
  visitorsByRegion(range) {
    const filter = timeFilter(range);
    const bucket = bucketExpr(range);
    return (
      `SELECT epoch_us(${bucket}) AS bucket_us,\n` +
      `  COALESCE(NULLIF(json_extract_string(labels, '$.scrape_target'), ''), '(unknown)') AS target,\n` +
      `  COALESCE(NULLIF(json_extract_string(labels, '$.region'), ''), '(unknown)') AS region,\n` +
      `  arg_max(value, ts) FILTER (WHERE name = 'visitors_total') AS visitors,\n` +
      `  arg_max(value, ts) FILTER (WHERE name = 'unique_visitors_estimate') AS uniques\n` +
      `FROM metrics_all\n` +
      `WHERE name IN ('visitors_total', 'unique_visitors_estimate') AND ${filter}\n` +
      `GROUP BY 1, 2, 3\n` +
      `ORDER BY 1, 2, 3`
    );
  },

  /// General-purpose counter/gauge series builder.
  ///
  /// `opts`: `{ metric, agg, labelKey, labelValue, range }`.
  /// `agg` ∈ `count|sum|avg|min|max|p50|p99`.
  customSeries(opts) {
    const { metric, agg = 'avg', labelKey = '', labelValue = '', range = '24h' } = opts;
    const filter = timeFilter(range);
    const bucket = bucketExpr(range);
    const aggSql = (() => {
      switch (agg) {
        case 'count':
          return 'count(*)';
        case 'sum':
          return 'sum(value)';
        case 'min':
          return 'min(value)';
        case 'max':
          return 'max(value)';
        case 'p50':
          return 'quantile_cont(value, 0.5)';
        case 'p99':
          return 'quantile_cont(value, 0.99)';
        case 'avg':
        default:
          return 'avg(value)';
      }
    })();
    let labelClause = '';
    if (labelKey.trim() !== '') {
      const key = lit(labelKey.trim());
      if (labelValue.trim() !== '') {
        labelClause = ` AND json_extract_string(labels, '$.${key}') = '${lit(labelValue.trim())}'`;
      } else {
        labelClause = ` AND json_extract_string(labels, '$.${key}') IS NOT NULL AND json_extract_string(labels, '$.${key}') <> ''`;
      }
    }
    return (
      `SELECT epoch_us(${bucket}) AS bucket_us,\n` +
      `  ${aggSql} AS v,\n` +
      `  count(*) AS n\n` +
      `FROM metrics_all\n` +
      `WHERE name = '${lit(metric)}' AND ${filter}${labelClause}\n` +
      `GROUP BY 1\n` +
      `ORDER BY 1`
    );
  },
};
