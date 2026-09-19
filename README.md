# tonggeret-dashboard

Scrape collector + history + graphs for [`tonggeret`](https://github.com/maulanasly/tonggeret)
apps. One Rust binary scrapes any Prometheus `/metrics` targets on an
interval, stores samples in embedded Fjall (<10 MiB RAM), compacts 30 days
of history to Parquet, and serves a client-side dashboard (**DuckDB-Wasm
runs in a Web Worker inside your browser** and queries the cold Parquet
over HTTP range requests).

## How it works

Scrape targets → collector (allowlist → dual write: Prometheus mirror +
Fjall hot store + 20k-sample hot ring) → hourly compaction to
`metrics_cold_*.parquet` (30d) → serve UI + `/metrics` + telemetry APIs.
The dashboard has two modes: **hot** (`/api/v1/query_range` JSON, fresh
within seconds, p99 unavailable) and **cold** (DuckDB-Wasm over Parquet,
full history). Full data flow, component map, lifecycle numbers, and
request walkthroughs: [`docs/architecture.md`](docs/architecture.md).
Day-to-day commands: `make help` (`make gate` runs the whole check suite).

## Quickstart (collector)

```sh
cargo run -- collector.toml   # scrapes beruang :8000 → :8080
# open http://localhost:8080/ — graphs read this collector's own history
```

`collector.toml`: targets (`name`/`url`/`allow` prefixes), `interval_secs`,
`listen`, `static_dir`, `max_samples_per_scrape`, `max_body_bytes`,
`[fjall]` (`dir`, `cold_dir`, `retention_days = 30`,
`cold_purge_days = 32`). Env overrides: `COLLECTOR_LISTEN`,
`COLLECTOR_FJALL_DIR`.

| Method | Path | Notes |
|---|---|---|
| GET | `/` | prebuilt UI bundle (`dist/`) |
| GET | `/metrics` | collector + mirrored scraped series (Prometheus text) |
| GET | `/telemetry/parquet` | newest cold export (Range-capable) |
| GET | `/telemetry/cold/:file` | named cold export, allowlisted to `metrics_cold_*.parquet` |
| GET | `/api/files` | JSON manifest of cold files (auto-used by the UI) |
| GET | `/api/v1/query_range` | Prometheus-shaped matrix JSON over recent samples (process lifetime) |

`query_range` params: `query` (exact name or `{__name__="x",k="v"}` with
`=` matchers only), `start`/`end` (unix seconds), `step` (seconds or
`<n>s|m|h|d|w`, min 1s). Caps: 7-day range, 10k points — over-limit is a
400, never silent truncation. Per `(series, step-bucket)` the latest sample
wins; a `start` older than the buffer succeeds with a `warnings` entry.
Example:

```sh
curl -s 'http://localhost:8080/api/v1/query_range?query=http_requests_total&start=1700000000&end=1700003600&step=60' | head -c 400
```

## Dashboard data sources (hot vs cold)

On connect, the dashboard probes `<base>/api/v1/labels`. When the source
is a collector, it uses **hot mode**: preset charts are built from
`query_range` JSON (no DuckDB-Wasm download, works seconds after startup).
Otherwise it falls back to cold Parquet via DuckDB-Wasm (range requests or
full fetch). The mode badge shows `via query-range` or the Parquet access
mode. Hot-mode approximations (documented, display-only):

- steps track the scrape cadence (`1h`→15s, `24h`→5m, all→10m); counter
  charts need two scrapes before the first bucket appears;
- throughput = per-series counter diffs (restarts clamp to 0), latency avg
  = sum/count diffs, **p99 is absent hot** (no histogram math in the client);
- errors group the same `http_requests_total` series by path;
- the custom visualizer plots latest-per-bucket values (its agg selector
  applies to the Parquet path; the preview shows the request URL instead).

## Series catalog

Every stored sample gains a `scrape_target` label (closed set from
config). Gauges stay gauges, everything else is stored as counters;
histogram/summary families are stored as cumulative `*_bucket{le}` /
`*`+`quantile` component counters. Visitor metrics are optional:
`visitors_total{region}` (counter) + `unique_visitors_estimate{region}`
(gauge) flow through the same mapping — apps without them (e.g. beruang
today) store zero visitor rows, no error. Never `sum()` the uniques
estimate; take latest per `(target, region)`.

Self series: `collector_scrape_total{target,status}`,
`collector_samples_stored_total{target}`,
`collector_samples_dropped_total{target,reason}`.

## Retention ops

Hot keys older than `retention_days` compact to hourly
`metrics_cold_*.parquet`; cold files older than `cold_purge_days` are
deleted (must exceed retention). Raw disk ≈ targets × series ×
scrapes/min × ~100 B/day pre-ZSTD — size `data/` for your fleet and back
it up if history matters; `data/` is gitignored and never committed.

## Dashboard (UI dev, no collector)

Independent, client-side dashboard for tonggeret Fjall/Parquet exports.
Zero server-side query code: **DuckDB-Wasm runs in a Web Worker inside your browser**
and queries remote Parquet files over HTTP (range requests when the server allows it).

### UI quickstart

```sh
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

`dist/index.html` inlines all CSS/JS and ships with `dist/vendor/`
(self-hosted DuckDB-Wasm MVP, Arrow, ECharts, Lucide — WASM binaries are
MBs and are copied as files, never inlined). The bundle is fully
offline-capable: `npm run build` fails if any loaded resource still points
remote. Host the `dist/` directory on any static server (it must serve
`index.html` + `vendor/` together), or embed it in Rust:

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
* `wasm init failed` — `dist/vendor/` not served alongside `dist/index.html`
  (the WASM + worker must resolve under `vendor/` relative to the page), or
  the browser blocks WebAssembly. No network is needed: all frontend deps
  are self-hosted (see `vendor/`).
* Empty charts with “no data” — range wider than the export window, or the
  export only contains business metrics. Widen to **All Time**.
