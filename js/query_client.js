//! Hot-mode data source: query_range JSON from the collector instead of
//! Parquet-via-DuckDB. Return shapes mirror `duckdb_client.js` exactly
//! (`throughput`, `errors`, `names`, `visitors`, `summary`) so callers
//! render unchanged; only the transport + reshaping differ.
//!
//! `summarize` is intentionally duplicated from `duckdb_client.js` (kept
//! local so this module imports nothing but the DOM-free reshaper —
//! `duckdb_client.js` pulls the vendor WASM import, which Node cannot load).

import { HotReshape } from './hot_reshape.js';

function emit(onStatus, phase, detail) {
  try {
    onStatus?.({ phase, detail: detail ?? '', at: Date.now() });
  } catch {
    // Status subscribers must never break queries.
  }
}

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

  async rangeQuery(selector, range, onStatus, label) {
    const step = HotReshape.stepFor(range);
    const end = Date.now() / 1000;
    const start = end - HotReshape.windowFor(range);
    const url =
      `${this.baseUrl}/api/v1/query_range?query=${encodeURIComponent(selector)}` +
      `&start=${start}&end=${end}&step=${step}`;
    emit(onStatus, 'querying', label ?? `querying ${selector}…`);
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    const body = await res.json();
    if (body?.status !== 'success') throw new Error(body?.error ?? 'query failed');
    return { url, result: body?.data?.result ?? [] };
  },

  selector(metric, labelKey, labelValue) {
    if (labelKey.trim() === '') return metric;
    if (labelValue.trim() === '') return metric;
    const q = (v) => `"${String(v).replaceAll('"', '')}"`;
    return `{__name__=${q(metric)},${labelKey.trim()}=${q(labelValue.trim())}}`;
  },

  async refreshAll(range, onStatus) {
    const [total, sum, count, names, visTotal, visUniques] = await Promise.all([
      this.rangeQuery('http_requests_total', range, onStatus, 'aggregating throughput…'),
      this.rangeQuery('http_request_duration_ms_sum', range, onStatus, 'aggregating latency…'),
      this.rangeQuery('http_request_duration_ms_count', range, onStatus, 'aggregating latency…'),
      (async () => {
        const res = await fetch(`${this.baseUrl}/api/v1/labels`);
        if (!res.ok) return [];
        const body = await res.json();
        return HotReshape.namesFromLabels(body?.data);
      })().catch(() => []),
      this.rangeQuery('visitors_total', range, onStatus, 'aggregating visitors…').catch(() => ({ result: [] })),
      this.rangeQuery('unique_visitors_estimate', range, onStatus, 'aggregating visitors…').catch(
        () => ({ result: [] }),
      ),
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
    const bucketSecs = { '1h': 15, '24h': 300, all: 600 }[range] ?? 300;
    emit(onStatus, 'idle', `${throughput.length} bucket(s) via query_range`);
    return {
      throughput,
      errors,
      names,
      visitors,
      summary: HotReshape.summarize(throughput, errors, bucketSecs),
    };
  },

  async runCustom(opts, onStatus) {
    const { url, result } = await this.rangeQuery(
      this.selector(opts.metric, opts.labelKey ?? '', opts.labelValue ?? ''),
      opts.range,
      onStatus,
      `plotting ${opts.metric}…`,
    );
    // Hot mode plots latest-per-bucket values; the agg selector applies to
    // the Parquet path only (noted in the SQL preview as the request URL).
    const rows = HotReshape.customFromGroups(HotReshape.groupBySeries(result));
    emit(onStatus, 'idle', `${rows.length} bucket(s) via query_range`);
    return { url, rows };
  },
};
