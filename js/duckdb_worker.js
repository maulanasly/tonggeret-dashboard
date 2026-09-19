//! DuckDB-WASM engine: WASM runs inside a Web Worker so SQL never blocks
//! the main thread (60 FPS UI). Hybrid Parquet access:
//!  1. `httpfs` range-requests against the remote URL(s) — only metadata +
//!     needed row-groups travel the wire.
//!  2. Fallback: `fetch` the whole file(s) + `registerFileBuffer`, for
//!     servers without CORS/Range support.
//!
//! All analytical SQL reads the `metrics_all` view installed by
//! `ensureView()`. Status transitions are pushed to the subscriber so the
//! UI can render graceful loading states.

import * as duckdb from '@duckdb/duckdb-wasm';

export const EnginePhases = {
  WASM_LOADING: 'wasm-loading',
  WASM_READY: 'wasm-ready',
  METADATA_LOADING: 'metadata-loading',
  QUERYING: 'querying',
  IDLE: 'idle',
  ERROR: 'error',
};

function emit(onStatus, phase, detail) {
  if (typeof onStatus === 'function') {
    try {
      onStatus({ phase, detail: detail ?? '', at: Date.now() });
    } catch {
      // Status subscribers must never break queries.
    }
  }
}

/// Convert an Arrow result table to plain JSON with BigInt → Number.
function toJsonRows(arrowTable) {
  const rows = arrowTable.toArray().map((r) => r.toJSON());
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      const v = row[key];
      if (typeof v === 'bigint') row[key] = Number(v);
    }
  }
  return rows;
}

export const WorkerEngine = {
  _db: null,
  _conn: null,
  _mode: 'unknown',
  _httpfsReady: false,

  get mode() {
    return this._mode;
  },

  /// Boot the WASM runtime inside a worker thread. Idempotent.
  async init(onStatus) {
    if (this._db) return this._db;
    emit(onStatus, EnginePhases.WASM_LOADING, 'loading duckdb-wasm bundle…');
    // Self-hosted MVP-only bundles (vendor/duckdb). Absolute URLs derived
    // from the page so dev (index.html) and the dist/ bundle resolve
    // identically; the blob-worker importScripts below needs absolute URLs.
    // MVP-only is deliberate: our servers send no COOP/COEP headers, so
    // SharedArrayBuffer is unavailable and selectBundle would pick MVP
    // anyway (verified in the vendored source); eh/coi builds omitted.
    const vendorBase = new URL('vendor/duckdb/', document.baseURI);
    const file = (name) => new URL(name, vendorBase).href;
    const bundles = {
      mvp: {
        mainModule: file('duckdb-mvp.wasm'),
        mainWorker: file('duckdb-browser-mvp.worker.js'),
      },
    };
    const bundle = await duckdb.selectBundle(bundles);
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }),
    );
    try {
      const worker = new Worker(workerUrl);
      const logger = new duckdb.ConsoleLogger();
      this._db = new duckdb.AsyncDuckDB(logger, worker);
      await this._db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    } finally {
      URL.revokeObjectURL(workerUrl);
    }
    emit(onStatus, EnginePhases.WASM_READY, 'duckdb-wasm ready');
    return this._db;
  },

  async _connect() {
    if (this._conn) return this._conn;
    this._conn = await this._db.connect();
    return this._conn;
  },

  async _tryHttpfs() {
    if (this._httpfsReady) return;
    const conn = await this._connect();
    // INSTALL is a no-op when statically linked; LOAD may throw there too.
    // Either way failure is non-fatal — the fallback path covers it.
    try {
      await conn.query('INSTALL httpfs');
    } catch {
      // ignore: bundled or offline
    }
    try {
      await conn.query('LOAD httpfs');
    } catch {
      // ignore: statically linked builds reject LOAD
    }
    this._httpfsReady = true;
  },

  _viewSql(urls) {
    const list = urls.map((u) => `'${u.replaceAll("'", "''")}'`).join(', ');
    return `CREATE OR REPLACE VIEW metrics_all AS SELECT * FROM read_parquet([${list}], union_by_name=true)`;
  },

  /// Point `metrics_all` at remote URL(s). Tries range-requests first,
  /// falls back to full-file fetch on any failure. Returns the access mode.
  async ensureView(urls, onStatus) {
    const conn = await this._connect();
    emit(onStatus, EnginePhases.METADATA_LOADING, `reading parquet metadata (${urls.length} file(s))…`);
    // --- Attempt 1: httpfs range-requests over the remote URL(s). ---
    try {
      await this._tryHttpfs();
      await conn.query(this._viewSql(urls));
      await conn.query('SELECT count(*) AS c FROM (SELECT * FROM metrics_all LIMIT 1)');
      this._mode = 'httpfs-range';
      emit(onStatus, EnginePhases.IDLE, 'metadata loaded via range requests');
      return this._mode;
    } catch (err) {
      // Fall through to full download (CORS / Range / httpfs missing).
      emit(
        onStatus,
        EnginePhases.METADATA_LOADING,
        `range access failed (${err?.message ?? err}); downloading full file(s)…`,
      );
    }
    // --- Attempt 2: fetch whole files into the WASM FS. ---
    const localNames = [];
    for (let i = 0; i < urls.length; i++) {
      const res = await fetch(urls[i]);
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${urls[i]}`);
      const buf = new Uint8Array(await res.arrayBuffer());
      const name = `remote_${i}.parquet`;
      await this._db.registerFileBuffer(name, buf);
      localNames.push(name);
    }
    await conn.query(this._viewSql(localNames));
    this._mode = 'fetch-buffer';
    emit(onStatus, EnginePhases.IDLE, 'parquet downloaded into browser memory');
    return this._mode;
  },

  /// Run analytical SQL against `metrics_all`. Returns plain row objects.
  async query(sql, onStatus, label) {
    const conn = await this._connect();
    emit(onStatus, EnginePhases.QUERYING, label ?? 'executing analytical query…');
    try {
      const arrow = await conn.query(sql);
      const rows = toJsonRows(arrow);
      emit(onStatus, EnginePhases.IDLE, `${rows.length} row(s)`);
      return rows;
    } catch (err) {
      emit(onStatus, EnginePhases.ERROR, err?.message ?? String(err));
      throw err;
    }
  },
};
