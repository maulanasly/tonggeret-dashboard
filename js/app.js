//! App boot: wires engine status → UI, controls → queries → charts.
//! All DuckDB execution happens in the WASM worker thread; this module
//! only orchestrates state and rendering on the main thread.

import { DataSource } from './duckdb_client.js';
import { HotClient } from './query_client.js';
import { StatusClient } from './status_client.js';
import { TargetsClient } from './targets_client.js';
import { Queries } from './queries.js';
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
    // Frozen connection: default to this page's own collector (same origin);
    // the Advanced source input overrides it for static/remote hosting.
    const source = Controls.getBase() || window.location.origin;
    const range = Controls.getRange();
    const target = Controls.getTarget();
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
      // Populate the target filter from the live data source.
      const targetNames = useHot
        ? await HotClient.listTargets().catch(() => [])
        : await DataSource.listTargets(setStatusCb).catch(() => []);
      Controls.setTargets(targetNames);
    }
    const data = useHot
      ? await HotClient.refreshAll(range, target, setStatusCb)
      : await DataSource.refreshAll(range, target, setStatusCb);
    const mode = useHot ? HotClient.coverageLabel(data.coveredSecs, range) : DataSource.mode;
    Cards.render(data.summary, mode);
    Charts.renderThroughput(data.throughput);
    Charts.renderLatency(data.throughput);
    Charts.renderErrors(data.errors);
    Charts.renderVisitors(data.visitors ?? []);
    Controls.setNames(data.names);
    Controls.setSql(useHot ? `hot: ${HotClient.baseUrl} (query_range)` : Queries.throughputLatency(range, target));
  } catch (err) {
    const msg = err?.message ?? String(err);
    const isHttp = /HTTP \d+|Failed to fetch|NetworkError|CORS/i.test(msg);
    showError(`query failed: ${msg}${isHttp ? corsHint(Controls.getBase() || window.location.origin) : ''}`);
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

/// Freeze/unfreeze the worker (halts all scraping) and refresh status.
async function toggleFreeze() {
  try {
    if (TargetsClient.frozen) await TargetsClient.resumeWorker();
    else await TargetsClient.freezeWorker();
    await Promise.all([TargetsClient.refresh(), StatusClient.refreshNow()]);
  } catch (err) {
    showError(`freeze failed: ${err?.message ?? err}`);
  }
}

function boot() {
  if (window.lucide?.createIcons) window.lucide.createIcons();
  Cards.reset();
  Charts.init();
  StatusClient.stop();
  TargetsClient.init();
  Controls.init({ onRefresh: refresh, onCustom: runCustom, onFreeze: toggleFreeze });
  Controls.loadPersisted();
  // Keep the top freeze toggle in sync with the worker's actual state.
  StatusClient.onSnapshot = (snapshot) => Controls.setFreeze(snapshot.frozen);

  // Status + targets are plain HTTP: start polling immediately so the
  // connection chip reflects the collector without waiting for the WASM
  // engine (which is only needed for the cold Parquet path).
  const base = Controls.getBase() || window.location.origin;
  StatusClient.watch(base);
  TargetsClient.watch(base);

  setStatus('idle', 'connecting…');
  // Connect on demand: hot mode (query_range JSON) needs no WASM; cold mode
  // initializes DuckDB-Wasm inside DataSource.connect.
  refresh({ reconnect: true }).catch((err) => {
    showError(`connect failed: ${err?.message ?? err}`);
    setStatus('error', err?.message ?? 'connect failed');
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', boot);
} else {
  boot();
}
