# tonggeret-dashboard

Independent, client-side dashboard for [`tonggeret`](https://github.com/maulanasly/tonggeret) Fjall/Parquet exports.
Zero server-side query code: **DuckDB-Wasm runs in a Web Worker inside your browser**
and queries remote Parquet files over HTTP (range requests when the server allows it).

## Quickstart

```sh
cd apps/tonggeret-dashboard
npm run gen-mock      # build a realistic sample export (needs: npm i -D duckdb  OR  pip install duckdb)
npm run mock-server   # http://localhost:8080/  (CORS + Range enabled)
# open http://localhost:8080/ — source defaults to http://localhost:3000
```

Point **Connect** at any host serving `tonggeret` Parquet:

* Rust backend file route: `http://<host>:3000` → dashboard reads `<host>/telemetry/parquet`
  (see `tonggeret::middleware::axum::parquet_route`).
* Direct file: `https://<host>/exports/metrics_cold_20240101T000000.parquet`.
* Mock manifest: the mock server also exposes `/api/files` (auto-used when present).

## Controls

* **Range dropdown** — Last 1 Hour / Last 24 Hours / All Time. Injects
  `WHERE ts >= (now() - INTERVAL …)` and picks the bucket granularity
  (minute / hour / day via `date_trunc`).
* **Summary cards** — Total Requests, Avg Latency, Peak Throughput, Error Rate,
  derived from the throughput + error queries.
* **Custom visualizer** — any `name` × aggregation (`avg|p50|p99|sum|count|min|max`)
  × optional `labels` key/value filter, with the generated SQL shown inline.

## Layout

```text
index.html                  # dev entry (also the dist template)
css/styles.css              # dark theme, responsive grid
js/duckdb_worker.js         # WASM-in-Worker engine + hybrid range/fetch access
js/duckdb_client.js         # source resolution, view lifecycle, summary math
js/queries.js               # DOM-free SQL builders (Parquet contract)
js/charts.js                # ECharts renderers (dark theme, lttb sampling)
js/components/cards.js      # summary cards
js/components/controls.js   # range / source / custom-visualizer wiring
js/app.js                   # boot + orchestration (main thread only renders)
scripts/mock-server.mjs     # npm run mock-server (static + CORS + Range + manifest)
scripts/generate-mock-parquet.mjs  # npm run gen-mock (DuckDB-synthesized sample)
scripts/build-singlefile.mjs       # npm run build → dist/index.html
public/sample/              # sample exports (1 small file checked in)
dist/index.html             # single-file artifact: S3/CDN or Rust embed
```

## Single-file distribution

```sh
npm run build   # → dist/index.html
```

`dist/index.html` inlines all CSS/JS; only CDN URLs stay remote
(`@duckdb/duckdb-wasm`, `apache-arrow`, `echarts`, `lucide-static` — WASM
binaries are MBs and must not be base64-inlined). Host it on any static
S3/CDN, or embed it in Rust:

```rust
const DASHBOARD_HTML: &str = include_str!("../../dashboard/dist/index.html");

async fn dashboard() -> axum::response::Html<&'static str> {
    axum::response::Html(DASHBOARD_HTML)
}
```

## Backend CORS prerequisite

`GET /telemetry/parquet` serves `ServeFile` (Range-capable) but ships **no
CORS headers** today, so cross-origin dashboards are blocked by the browser
(same-origin + mock-server work fine). Add one layer on the Rust side:

```rust
use tower_http::cors::CorsLayer;
let cors = CorsLayer::new()
    .allow_origin(tower_http::cors::Any)
    .allow_methods([axum::http::Method::GET, axum::http::Method::HEAD])
    .allow_headers([axum::http::header::RANGE]);
let app = app.layer(cors);
```

Verify with `curl -I http://<host>:3000/telemetry/parquet`:
expect `Accept-Ranges: bytes` + `Access-Control-Allow-Origin: *`.

## Parquet contract (must stay in sync with backend)

`ts Timestamp(us)`, `name Utf8`, `value Float64`, `metric_type Utf8`,
`labels Utf8` (JSON object string). Labels are queried with
`json_extract_string(labels, '$.path')` / `'$.status'`; buckets with
`date_trunc('<part>', ts)` cast to `bucket_us` (microseconds) to keep
Arrow BigInt handling unambiguous.

## Troubleshooting

* `/telemetry/parquet → 404` — no export yet. Compaction runs hourly and only
  exports keys older than `retention` (default 24h). Use `npm run gen-mock`.
* `query failed … HTTP 4xx/5xx` + CORS hint — file exists but CORS/Range is
  missing (see above), or the URL is wrong.
* `wasm init failed` — CDN blocked (offline). The app needs network for the
  WASM + chart CDNs on first load.
* Empty charts with “no data” — range wider than the export window, or the
  export only contains business metrics. Widen to **All Time**.
