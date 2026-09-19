//! Main-thread data-source adapter: resolves a user-supplied base URL to
//! concrete Parquet URL(s), owns the `metrics_all` view lifecycle, and
//! turns raw query rows into dashboard-ready datasets + summary numbers.

import { Queries } from './queries.js';
import { WorkerEngine } from './duckdb_worker.js';

const num = (v) => (typeof v === 'bigint' ? Number(v) : v ?? 0);

/// Normalize one throughput row (BigInt-safe).
function normThroughputRow(r) {
  return {
    bucket_us: Number(r.bucket_us),
    reqs: Number(num(r.reqs)),
    avg_ms: r.avg_ms == null ? null : Number(r.avg_ms),
    p99_ms: r.p99_ms == null ? null : Number(r.p99_ms),
  };
}

function normErrorRow(r) {
  return {
    path: String(r.path ?? '(unknown)'),
    c4xx: Number(num(r.c4xx)),
    c5xx: Number(num(r.c5xx)),
    total: Number(num(r.total)),
  };
}

/// Normalize one visitor-preset row (BigInt-safe; nulls stay null so the
/// chart can distinguish "no samples" from zero).
function normVisitorRow(r) {
  return {
    bucket_us: Number(r.bucket_us),
    target: String(r.target ?? '(unknown)'),
    region: String(r.region ?? '(unknown)'),
    visitors: r.visitors == null ? null : Number(num(r.visitors)),
    uniques: r.uniques == null ? null : Number(num(r.uniques)),
  };
}

/// Try `<base>/api/files` manifest (mock-server + future backends).
/// Returns absolute URLs or null when no manifest exists.
async function tryManifest(base) {
  try {
    const url = base.replace(/\/$/, '') + '/api/files';
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json();
    if (!Array.isArray(data) || data.length === 0) return null;
    return data
      .map((e) => (typeof e === 'string' ? e : e.url))
      .filter(Boolean)
      .map((u) => new URL(u, base).toString());
  } catch {
    return null;
  }
}

export const DataSource = {
  baseUrl: '',
  urls: [],
  mode: 'unknown',

  /// Resolve free-form user input to Parquet file URL(s).
  /// - `…/x.parquet` → that file directly.
  /// - otherwise → `<base>/api/files` manifest if present,
  ///   else `<base>/telemetry/parquet` (tonggeret file route).
  async resolveInput(input) {
    const trimmed = input.trim().replace(/\/$/, '');
    if (trimmed === '') throw new Error('empty data source URL');
    if (/\.parquet(\?.*)?$/i.test(trimmed)) return [trimmed];
    const manifest = await tryManifest(trimmed);
    if (manifest) return manifest;
    return [`${trimmed}/telemetry/parquet`];
  },

  async connect(input, onStatus) {
    await WorkerEngine.init(onStatus);
    const urls = await this.resolveInput(input);
    this.baseUrl = input.trim();
    this.urls = urls;
    this.mode = await WorkerEngine.ensureView(urls, onStatus);
    return { urls, mode: this.mode };
  },

  async refreshAll(range, target, onStatus) {
    const throughputSql = Queries.throughputLatency(range, target);
    const errorsSql = Queries.errorDistribution(range, target);
    const namesSql = Queries.listMetricNames();
    const visitorsSql = Queries.visitorsByRegion(range, target);
    const [tRows, eRows, nRows, vRows] = await Promise.all([
      WorkerEngine.query(throughputSql, onStatus, 'aggregating throughput + latency…'),
      WorkerEngine.query(errorsSql, onStatus, 'aggregating error distribution…'),
      WorkerEngine.query(namesSql, onStatus, 'listing metric names…').catch(() => []),
      WorkerEngine.query(visitorsSql, onStatus, 'aggregating visitors…').catch(() => []),
    ]);
    const throughput = tRows.map(normThroughputRow);
    const errors = eRows.map(normErrorRow);
    const names = nRows.map((r) => String(r.name));
    const visitors = vRows.map(normVisitorRow);
    const bucketSecs = Queries.Ranges[range]?.bucketSecs ?? 3600;
    return { throughput, errors, names, visitors, summary: this.summarize(throughput, errors, bucketSecs) };
  },

  /// Distinct `scrape_target` values for the target filter (cold path).
  async listTargets(onStatus) {
    try {
      const rows = await WorkerEngine.query(Queries.listTargets(), onStatus, 'listing targets…');
      return rows.map((r) => String(r.target)).filter(Boolean).sort();
    } catch {
      return [];
    }
  },

  async runCustom(opts, onStatus) {
    const sql = Queries.customSeries(opts);
    const rows = await WorkerEngine.query(sql, onStatus, `plotting ${opts.metric}…`);
    return {
      sql,
      rows: rows.map((r) => ({
        bucket_us: Number(r.bucket_us),
        v: r.v == null ? null : Number(num(r.v)),
        n: Number(num(r.n)),
      })),
    };
  },

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
