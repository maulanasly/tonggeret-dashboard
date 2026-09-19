//! App boot: wires engine status → UI, controls → queries → charts.
//! All DuckDB execution happens in the WASM worker thread; this module
//! only orchestrates state and rendering on the main thread.

import { DataSource } from './duckdb_client.js';
import { HotClient } from './query_client.js';
import { StatusClient } from './status_client.js';
import { TargetsClient } from './targets_client.js';
import { Queries } from './queries.js';
import { WorkerEngine } from './duckdb_worker.js';
import { Charts } from './charts.js';
import { Cards } from './components/cards.js';
import { Controls } from './components/controls.js';

function setStatus(phase, detail) {
  const dot = document.getElementById('statusDot');
  const text = document.getElementById('statusText');
  if (text) text.textContent = detail || phase;
  if (dot) {
    dot.dataset.phase = phase;
    dot.title = phase;
  }
  document.getElementById('main')?.classList.toggle('loading', phase === 'querying' || phase === 'metadata-loading');
}

function showError(message) {
  const banner = document.getElementById('errorBanner');
  if (!banner) return;
  if (!message) {
    banner.classList.add('hidden');
    banner.textContent = '';
    return;
  }
  banner.classList.remove('hidden');
  banner.textContent = message;
}

function corsHint(url) {
  let host = url;
  try {
    host = new URL(url).origin;
  } catch {
    // keep raw
  }
  return (
    ` — if the file exists, this is usually CORS or Range support. ` +
    `The backend must reply with Access-Control-Allow-Origin:* and Accept-Ranges:bytes ` +
    `(check: curl -I ${host}/telemetry/parquet). ` +
    `Same-origin deployments and the mock server (npm run mock-server) work out of the box.`
  );
}

let connectedUrl = '';
let refreshing = false;
// True when the source speaks /api/v1/query_range (probed per connect);
// otherwise the DuckDB-Wasm Parquet path is used.
let useHot = false;

async function refresh({ reconnect }) {
  if (refreshing) return;
  refreshing = true;
  showError('');
  try {
    const source = Controls.getSource();
    const range = Controls.getRange();
    if (reconnect || connectedUrl !== source) {
      setStatus('metadata-loading', `connecting to ${source}…`);
      useHot = await HotClient.probe(source).catch(() => false);
      if (useHot) {
        HotClient.baseUrl = source.trim().replace(/\/$/, '');
        setStatus('idle', 'hot query API available');
      } else {
        await DataSource.connect(source, setStatusCb);
      }
      connectedUrl = source;
      // Poll collector status + targets/queue from the connected base
      // (proxied by the dashboard in split-process mode; same-origin
      // single-binary too).
      StatusClient.watch(connectedUrl);
      TargetsClient.watch(connectedUrl);
    }
    const data = useHot
      ? await HotClient.refreshAll(range, setStatusCb)
      : await DataSource.refreshAll(range, setStatusCb);
    Cards.render(data.summary, useHot ? HotClient.mode : DataSource.mode);
    Charts.renderThroughput(data.throughput);
    Charts.renderLatency(data.throughput);
    Charts.renderErrors(data.errors);
    Charts.renderVisitors(data.visitors ?? []);
    Controls.setNames(data.names);
    Controls.setSql(useHot ? `hot: ${HotClient.baseUrl} (query_range)` : Queries.throughputLatency(range));
  } catch (err) {
    const msg = err?.message ?? String(err);
    const isHttp = /HTTP \d+|Failed to fetch|NetworkError|CORS/i.test(msg);
    showError(`query failed: ${msg}${isHttp ? corsHint(Controls.getSource()) : ''}`);
    setStatus('error', msg);
  } finally {
    refreshing = false;
  }
}

async function runCustom() {
  showError('');
  try {
    if (useHot) {
      const opts = Controls.getCustom();
      const { url, rows } = await HotClient.runCustom(opts, setStatusCb);
      Charts.renderCustom(rows, opts.metric, opts.agg);
      Controls.setSql(`hot query (agg applies to the Parquet path only):\n${url}`);
      return;
    }
    const opts = Controls.getCustom();
    const { sql, rows } = await DataSource.runCustom(opts, setStatusCb);
    Charts.renderCustom(rows, opts.metric, opts.agg);
    Controls.setSql(sql);
  } catch (err) {
    showError(`custom query failed: ${err?.message ?? err}`);
  }
}

function setStatusCb({ phase, detail }) {
  setStatus(phase, detail);
}

function boot() {
  if (window.lucide?.createIcons) window.lucide.createIcons();
  Cards.reset();
  Charts.init();
  StatusClient.stop();
  TargetsClient.init();
  Controls.init({ onRefresh: refresh, onCustom: runCustom });
  Controls.loadPersisted();
  if (!Controls.getSource()) {
    document.getElementById('srcInput').value = 'http://localhost:3000';
  }
  setStatus('idle', 'initializing…');
  // Engine warms up in the background; first refresh connects on demand.
  WorkerEngine.init(setStatusCb)
    .then(() => refresh({ reconnect: true }))
    .catch((err) => {
      showError(`wasm init failed: ${err?.message ?? err}`);
      setStatus('error', 'wasm init failed');
    });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', boot);
} else {
  boot();
}
