//! Hot-mode reshaping: Prometheus `query_range` matrix JSON → the exact row
//! shapes the charts/cards/controls modules already consume.
//!
//! Display-only reshaping, same contract as the DuckDB builders:
//! * counters are cumulative snapshots → per-bucket counts come from
//!   consecutive-sample diffs (negative diffs from restarts clamp to 0);
//! * gauge estimates are never summed — latest per (series, bucket);
//! * latency `avg` comes from sum/count diffs; `p99` is unavailable hot
//!   (histogram math stays out of the client) and renders as absent.
//!
//! This module is DOM-free so it can be `node --check`ed and unit-tested.

/// Scrape cadence floor for the hot-mode step (seconds): counter diffs need
/// consecutive scrapes in *different* step buckets.
const SCRAPE_CADENCE_SECS = 15;
/// Target number of step buckets across the covered window. Keeps the step
/// small enough for diffs while bounding point counts.
const TARGET_BUCKETS = 240;

function finite(v) {
  const n = typeof v === 'string' ? Number(v) : v;
  return Number.isFinite(n) ? n : null;
}

function labelOf(metric, key, fallback) {
  const v = metric[key];
  return v === undefined || v === null || v === '' ? fallback : String(v);
}

export const HotReshape = {
  /// Least-recent start offset (seconds) per UI range key. `all` is bounded
  /// by the server's 7-day range cap; the buffer clamp warning covers the rest.
  windowFor(range) {
    if (range === '1h') return 3600;
    if (range === '24h') return 86_400;
    return 7 * 86_400;
  },

  /// Hot-mode query plan for one refresh.
  ///
  /// The collector's recent buffer is bounded (often far shorter than the
  /// selected range), so the step must be derived from the **covered span**
  /// rather than the nominal range: a step coarser than the buffer collapses
  /// every series to one bucket and counter diffs (throughput/latency) become
  /// empty. `coverage` is `{ oldest_ts, newest_ts }` (unix seconds) from
  /// `/api/v1/status`, or `null` when unavailable.
  ///
  /// The window zooms to the covered span (clamped to the selected range), so
  /// the x-axis shows the data that actually exists.
  plan(range, coverage, nowSecs) {
    const rangeStart = nowSecs - this.windowFor(range);
    const oldest = Number.isFinite(coverage?.oldest_ts) ? coverage.oldest_ts : null;
    const newest = Number.isFinite(coverage?.newest_ts) ? coverage.newest_ts : null;
    const end = newest ?? nowSecs;
    const start = oldest == null ? rangeStart : Math.max(rangeStart, oldest);
    const span = Math.max(0, end - start);
    const step =
      span > 0 ? Math.max(SCRAPE_CADENCE_SECS, Math.ceil(span / TARGET_BUCKETS)) : SCRAPE_CADENCE_SECS;
    return { start, end, step, coveredSecs: span };
  },

  /// Compact duration text (`45s`, `5m`, `2h`, `3d`) for the coverage hint.
  fmtDuration(secs) {
    const s = Math.max(0, Math.round(Number(secs) || 0));
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.round(s / 60)}m`;
    if (s < 86_400) return `${Math.round(s / 3600)}h`;
    return `${Math.round(s / 86_400)}d`;
  },

  /// Group matrix result entries by series. Returns
  /// `[{ name, labels, points: [{ t, v }] }]` with `t` in seconds, `v` a
  /// finite number or null, points sorted by time.
  groupBySeries(result) {
    const groups = new Map();
    for (const entry of result ?? []) {
      const metric = entry.metric ?? {};
      const name = String(metric.__name__ ?? '(unknown)');
      const labels = Object.entries(metric)
        .filter(([k]) => k !== '__name__')
        .map(([k, v]) => [String(k), String(v)])
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
      const key = `${name}\n${labels.map(([k, v]) => `${k}=${v}`).join('\n')}`;
      if (!groups.has(key)) groups.set(key, { name, labels, points: [] });
      for (const [t, vs] of entry.values ?? []) {
        groups.get(key).points.push({ t: Number(t), v: finite(vs) });
      }
    }
    for (const g of groups.values()) g.points.sort((a, b) => a.t - b.t);
    return [...groups.values()];
  },

  /// Per-series consecutive diffs, negative (restart) clamped to 0.
  /// Returns `[{ t, d }]` attributed to the later sample's bucket.
  diffs(points) {
    const out = [];
    for (let i = 1; i < points.length; i++) {
      const a = points[i - 1].v;
      const b = points[i].v;
      if (a == null || b == null) continue;
      out.push({ t: points[i].t, d: Math.max(0, b - a) });
    }
    return out;
  },

  /// Throughput + latency rows from counter groups. `sumGroups`/`countGroups`
  /// are the `_sum`/`_count` companions of the same histogram family.
  /// Row shape matches the DuckDB path: `{ bucket_us, reqs, avg_ms, p99_ms }`
  /// with `p99_ms` always null (unavailable without histogram math).
  throughputFromGroups(totalGroups, sumGroups, countGroups) {
    const buckets = new Map();
    const add = (groups, field) => {
      for (const g of groups) {
        for (const { t, d } of this.diffs(g.points)) {
          if (!buckets.has(t)) buckets.set(t, { reqs: 0, sum: 0, count: 0 });
          buckets.get(t)[field] += d;
        }
      }
    };
    add(totalGroups, 'reqs');
    add(sumGroups, 'sum');
    add(countGroups, 'count');
    return [...buckets.entries()]
      .sort(([a], [b]) => a - b)
      .map(([t, b]) => ({
        bucket_us: Math.floor(t * 1_000_000),
        reqs: b.reqs,
        avg_ms: b.count > 0 ? b.sum / b.count : null,
        p99_ms: null,
      }));
  },

  /// Error distribution from `http_requests_total` groups. Totals are range
  /// deltas (last − first per series, clamped), matching the DuckDB
  /// `count(*)`-in-range semantics. Only paths with errors are listed.
  errorsFromGroups(totalGroups) {
    const byPath = new Map();
    for (const g of totalGroups) {
      const labels = Object.fromEntries(g.labels);
      const path = labelOf(labels, 'path', '(unknown)');
      const status = labelOf(labels, 'status', '');
      const vals = g.points.map((p) => p.v).filter((v) => v != null);
      if (vals.length === 0) continue;
      const delta = Math.max(0, vals[vals.length - 1] - vals[0]);
      if (!byPath.has(path)) byPath.set(path, { c4xx: 0, c5xx: 0, total: 0 });
      const row = byPath.get(path);
      row.total += delta;
      if (status.startsWith('4')) row.c4xx += delta;
      else if (status.startsWith('5')) row.c5xx += delta;
    }
    return [...byPath.entries()]
      .filter(([, r]) => r.c4xx + r.c5xx > 0)
      .sort(([, a], [, b]) => b.c4xx + b.c5xx - (a.c4xx + a.c5xx))
      .slice(0, 20)
      .map(([path, r]) => ({ path, ...r }));
  },

  /// Visitor rows from the two visitor counter/gauge groups. Row shape
  /// matches the DuckDB preset: `{ bucket_us, target, region, visitors, uniques }`.
  visitorsFromGroups(totalGroups, uniquesGroups) {
    const cells = new Map();
    const put = (groups, field) => {
      for (const g of groups) {
        const labels = Object.fromEntries(g.labels);
        const key = `${labelOf(labels, 'scrape_target', '(unknown)')}\n${labelOf(labels, 'region', '(unknown)')}`;
        for (const p of g.points) {
          if (p.v == null) continue;
          const cell = `${Math.floor(p.t)}\n${key}`;
          if (!cells.has(cell)) {
            const [target, region] = key.split('\n');
            cells.set(cell, {
              bucket_us: Math.floor(p.t * 1_000_000),
              target,
              region,
              visitors: null,
              uniques: null,
            });
          }
          cells.get(cell)[field] = p.v;
        }
      }
    };
    put(totalGroups, 'visitors');
    put(uniquesGroups, 'uniques');
    return [...cells.values()].sort((a, b) => a.bucket_us - b.bucket_us);
  },

  /// Custom visualizer rows: per-bucket mean of contributing series.
  customFromGroups(groups) {
    const buckets = new Map();
    for (const g of groups) {
      for (const p of g.points) {
        if (p.v == null) continue;
        if (!buckets.has(p.t)) buckets.set(p.t, []);
        buckets.get(p.t).push(p.v);
      }
    }
    return [...buckets.entries()]
      .sort(([a], [b]) => a - b)
      .map(([t, vs]) => ({
        bucket_us: Math.floor(t * 1_000_000),
        v: vs.reduce((a, b) => a + b, 0) / vs.length,
        n: vs.length,
      }));
  },

  namesFromLabels(data) {
    return Array.isArray(data) ? data.map(String) : [];
  },

  /// Summary numbers from throughput + error rows (same derivation as the
  /// DuckDB path's `summarize`, kept local so this module stays dependency-free).
  summarize(throughput, errors, bucketSecs) {
    let total = 0;
    let latSum = 0;
    let latN = 0;
    let peak = 0;
    for (const r of throughput) {
      total += r.reqs;
      if (r.reqs > peak) peak = r.reqs;
      if (r.avg_ms != null && Number.isFinite(r.avg_ms)) {
        latSum += r.avg_ms * r.reqs;
        latN += r.reqs;
      }
    }
    let errCount = 0;
    for (const e of errors) errCount += e.c4xx + e.c5xx;
    return {
      total,
      avgMs: latN > 0 ? latSum / latN : null,
      peakPerSec: bucketSecs > 0 ? peak / bucketSecs : peak,
      errorRate: total > 0 ? errCount / total : 0,
      errorCount: errCount,
    };
  },
};
