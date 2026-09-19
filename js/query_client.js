//! Hot-mode data source: query_range JSON from the collector instead of
//! Parquet-via-DuckDB. Return shapes mirror `duckdb_client.js` exactly
//! (`throughput`, `errors`, `names`, `visitors`, `summary`) so callers
//! render unchanged; only the transport + reshaping differ.
//!
//! `summarize` is intentionally duplicated from `duckdb_client.js` (kept
//! local so this module imports nothing but the DOM-free reshaper —
//! `duckdb_client.js` pulls the vendor WASM import, which Node cannot load).

import { HotReshape } from './hot_reshape.js';

function emitStatus(onStatus, phase, detail) {
  try {
    onStatus?.({ phase, detail: detail ?? '', at: Date.now() });
  } catch {
    // Status subscribers must never break queries.
  }
}

/// Point count from the collector's over-limit error
/// (`"query would return N points (limit 10000); increase step"`), or `null`.
export function parsePointLimit(message) {
  const m = /(\d+)\s+points/.exec(String(message ?? ''));
  return m ? Number(m[1]) : null;
}

/// Server-side point cap (`src/query.rs::MAX_POINTS`).
const MAX_POINTS = 10_000;

export const HotClient = {
  baseUrl: '',
  mode: 'query-range',

  /// Capability probe: cheap, param-less, no time math. False on any
  /// failure (non-2xx, non-JSON, CORS) — the caller falls back to Parquet.
  async probe(input) {
    const base = input.trim().replace(/\/$/, '');
    if (base === '') return false;
    try {
      const res = await fetch(`${base}/api/v1/labels`);
      if (!res.ok) return false;
      const body = await res.json();
      return body?.status === 'success' && Array.isArray(body?.data);
    } catch {
      return false;
    }
  },

  /// Recent-buffer coverage (`{oldest_ts, newest_ts}` unix seconds) from
  /// `/api/v1/status`, or `null` when unavailable/empty. Sizes the hot
  /// window and step so counter diffs survive (see `HotReshape.plan`).
  async coverage() {
    try {
      const res = await fetch(`${this.baseUrl}/api/v1/status`);
      if (!res.ok) return null;
      const body = await res.json();
      const oldest = body?.buffer?.oldest_ts;
      const newest = body?.buffer?.newest_ts;
      if (!Number.isFinite(oldest) || !Number.isFinite(newest)) return null;
      return { oldest_ts: Number(oldest), newest_ts: Number(newest) };
    } catch {
      return null;
    }
  },

  /// Mode badge text: append the covered window when it is shorter than the
  /// selected range (hot mode only holds the recent buffer).
  coverageLabel(coveredSecs, range) {
    const full = HotReshape.windowFor(range);
    if (!Number.isFinite(coveredSecs) || coveredSecs <= 0 || coveredSecs >= full) {
      return this.mode;
    }
    return `${this.mode} · last ${HotReshape.fmtDuration(coveredSecs)}`;
  },

  /// Run one range query under `plan` (`{start,end,step}`). On the server's
  /// point-limit 400, widen `plan.step` (shared object, so later queries see
  /// it) and retry.
  async rangeQuery(selector, plan, onStatus, label) {
    emitStatus(onStatus, 'querying', label ?? `querying ${selector}…`);
    for (let attempt = 0; ; attempt++) {
      const url =
        `${this.baseUrl}/api/v1/query_range?query=${encodeURIComponent(selector)}` +
        `&start=${plan.start}&end=${plan.end}&step=${plan.step}`;
      const res = await fetch(url);
      const body = await res.json().catch(() => null);
      if (res.ok && body?.status === 'success') {
        return { url, result: body?.data?.result ?? [] };
      }
      const points = parsePointLimit(body?.error);
      if (res.status === 400 && points && attempt < 2) {
        plan.step = Math.max(plan.step + 1, Math.ceil((plan.step * points * 1.05) / MAX_POINTS));
        continue;
      }
      throw new Error(body?.error ?? body?.detail ?? `HTTP ${res.status} for ${url}`);
    }
  },

  selector(metric, labelKey, labelValue, target = '') {
    const q = (v) => `"${String(v).replaceAll('"', '')}"`;
    const matchers = [];
    if (labelKey.trim() !== '' && labelValue.trim() !== '') {
      matchers.push(`${labelKey.trim()}=${q(labelValue)}`);
    }
    const t = String(target ?? '').trim();
    if (t !== '' && t !== 'all') matchers.push(`scrape_target=${q(t)}`);
    if (matchers.length === 0) return metric;
    return `{__name__=${q(metric)},${matchers.join(',')}}`;
  },

  /// Selector for a metric optionally narrowed to one `scrape_target`.
  targetSelector(metric, target) {
    return this.selector(metric, '', '', target);
  },

  /// Distinct targets present in the collector's runtime registry. Used to
  /// populate the target filter; empty when unavailable.
  async listTargets() {
    try {
      const res = await fetch(`${this.baseUrl}/api/v1/targets`);
      if (!res.ok) return [];
      const body = await res.json();
      const targets = Array.isArray(body?.targets) ? body.targets : [];
      return [...new Set(targets.map((t) => String(t?.name ?? '')).filter(Boolean))].sort();
    } catch {
      return [];
    }
  },

  async refreshAll(range, target, onStatus) {
    const target0 = String(target ?? '').trim();
    const coverage = await this.coverage();
    const plan = HotReshape.plan(range, coverage, Date.now() / 1000);
    // Throughput first: its query may widen `plan.step` on the point-limit
    // error, and the shared plan object then applies to the rest.
    const total = await this.rangeQuery(
      this.targetSelector('http_requests_total', target0),
      plan,
      onStatus,
      'aggregating throughput…',
    );
    const [sum, count, names, visTotal, visUniques] = await Promise.all([
      this.rangeQuery(
        this.targetSelector('http_request_duration_ms_sum', target0),
        plan,
        onStatus,
        'aggregating latency…',
      ),
      this.rangeQuery(
        this.targetSelector('http_request_duration_ms_count', target0),
        plan,
        onStatus,
        'aggregating latency…',
      ),
      (async () => {
        const res = await fetch(`${this.baseUrl}/api/v1/labels`);
        if (!res.ok) return [];
        const body = await res.json();
        return HotReshape.namesFromLabels(body?.data);
      })().catch(() => []),
      this.rangeQuery(
        this.targetSelector('visitors_total', target0),
        plan,
        onStatus,
        'aggregating visitors…',
      ).catch(() => ({ result: [] })),
      this.rangeQuery(
        this.targetSelector('unique_visitors_estimate', target0),
        plan,
        onStatus,
        'aggregating visitors…',
      ).catch(() => ({ result: [] })),
    ]);
    const totalGroups = HotReshape.groupBySeries(total.result);
    const throughput = HotReshape.throughputFromGroups(
      totalGroups,
      HotReshape.groupBySeries(sum.result),
      HotReshape.groupBySeries(count.result),
    );
    const errors = HotReshape.errorsFromGroups(totalGroups);
    const visitors = HotReshape.visitorsFromGroups(
      HotReshape.groupBySeries(visTotal.result),
      HotReshape.groupBySeries(visUniques.result),
    );
    emitStatus(onStatus, 'idle', `${throughput.length} bucket(s) via query_range`);
    return {
      throughput,
      errors,
      names,
      visitors,
      coveredSecs: plan.coveredSecs,
      summary: HotReshape.summarize(throughput, errors, plan.step),
    };
  },

  async runCustom(opts, onStatus) {
    const coverage = await this.coverage();
    const plan = HotReshape.plan(opts.range, coverage, Date.now() / 1000);
    const { url, result } = await this.rangeQuery(
      this.selector(opts.metric, opts.labelKey ?? '', opts.labelValue ?? '', opts.target ?? ''),
      plan,
      onStatus,
      `plotting ${opts.metric}…`,
    );
    // Hot mode plots latest-per-bucket values; the agg selector applies to
    // the Parquet path only (noted in the SQL preview as the request URL).
    const rows = HotReshape.customFromGroups(HotReshape.groupBySeries(result));
    emitStatus(onStatus, 'idle', `${rows.length} bucket(s) via query_range`);
    return { url, rows };
  },
};
