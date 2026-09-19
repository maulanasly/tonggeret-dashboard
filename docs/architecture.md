# How this dashboard works

One Rust binary (`collector`) scrapes Prometheus `/metrics` targets,
keeps history, and serves a client-side dashboard. There is no query
backend: the browser either fetches pre-aggregated JSON (hot mode) or
queries cold Parquet files directly with DuckDB-Wasm (cold mode).

```
scrape targets (/metrics) ──15s──▶ collector ──store──▶ Fjall (hot, <10 MiB)
                                                        │  ▲ registry mirror (/metrics)
                                                        ▼  │ hourly compaction
                                              metrics_cold_*.parquet (30d)
                                                        │
                         ┌──────────────────────────────┼──────────────────┐
                         ▼                              ▼                  ▼
              /api/v1/query_range              /telemetry/parquet     prebuilt UI
              (hot JSON, process               (cold files,           (dist/, same
               lifetime)                        Range-capable)         origin)
                         │                              │
                         ▼                              ▼
                 dashboard HOT mode            dashboard COLD mode
                 (fetch + render)              (DuckDB-Wasm in Worker)
```

## Collector pipeline

1. **Load config** (`src/config.rs:156`): `collector.toml` plus
   `COLLECTOR_LISTEN` / `COLLECTOR_FJALL_DIR` overrides, then `validate()`.
   Storage misconfiguration is fatal (fail fast); everything else degrades.
2. **Scrape loop** (`src/main.rs`): one Tokio task, targets scraped
   sequentially every `interval_secs`. Failures are counted per target in
   `collector_scrape_total{status}`, never fatal.
3. **Map** (`src/scrape.rs:164` `scrape_once`): fetch (1 MiB body cap) →
   parse (`prometheus-parse`) → denylist (`tonggeret_dropped_total`,
   `collector_*`) → allowlist prefixes → 5k samples/scrape cap. Gauges stay
   gauges, everything else becomes counters; histograms/summaries decompose
   to cumulative `*_bucket{le}` / `*`+`quantile` counters. Every sample gains
   a `scrape_target` label; label keys are sorted for the positional registry.
   Visitor metrics (`visitors_total`, `unique_visitors_estimate`) ride the
   generic mapping — apps without them store zero visitor rows, no error.
4. **Dual write**: samples go to the tonggeret engine (in-memory Prometheus
   mirror + bounded channel → Fjall writer thread) and to the collector's own
   bounded `RecentBuffer` (`src/query.rs:55`, 20k samples drop-oldest,
   ~6 MiB) that backs the hot query API. The engine keyspace is opened
   exactly once — the collector never re-opens Fjall for reads.
5. **Compaction + purge**: hot keys older than `retention_days` (30) compact
   to hourly `metrics_cold_*.parquet`; files older than `cold_purge_days`
   (32, must exceed retention) are deleted. `data/` is local-only and
   gitignored.

## Serve surface (`src/serve.rs:33`)

| Method | Path | Source |
|---|---|---|
| GET | `/` | prebuilt UI bundle (`dist/`) |
| GET | `/metrics` | live registry text (self + mirrored scraped series) |
| GET | `/telemetry/parquet` | newest cold export, Range-capable (404 until first compaction) |
| GET | `/telemetry/cold/:file` | named export, allowlisted to `metrics_cold_*.parquet` |
| GET | `/api/files` | JSON manifest of cold files |
| GET | `/api/v1/query_range` | Prometheus matrix JSON over the hot buffer |
| GET | `/api/v1/labels` | distinct buffered metric names (hot metric picker) |

Deliberately no `track` middleware: self `http_*` series would collide with
scraped ones in the shared registry. Self-observability is the
`collector_*` outcome series.

`query_range` is a documented subset (`src/query.rs` header): exact metric
name or `{__name__="x",k="v"}` with `=` matchers only; unix-second
`start`/`end`; `step` in `s|m|h|d|w` (min 1s); 7-day range cap; 10k point
cap. Violations are HTTP 400, never silent truncation. Latest sample wins
per (series, step-bucket); a `start` older than the buffer succeeds with a
`warnings` entry.

## Dashboard modes (`js/app.js`)

On Connect, the dashboard probes `<base>/api/v1/labels`:

- **Hot mode** (probe succeeds): `js/query_client.js:20` fetches
  `query_range` JSON; `js/hot_reshape.js:23` reshapes matrix data into the
  exact row shapes the charts already consume. Works seconds after collector
  startup, no WASM download. Approximations: steps track the scrape cadence
  (`1h`→15s, `24h`→5m, all→10m); throughput = per-series counter diffs
  (restarts clamp to 0); latency avg = sum/count diffs; **p99 is absent hot**
  (no histogram math in the client); the custom visualizer plots
  latest-per-bucket values (its agg selector applies to the cold path; the
  SQL preview shows the request URL instead). Badge reads `via query-range`.
- **Cold mode** (probe fails): `js/duckdb_client.js:59` resolves
  `<base>/api/files` → `/telemetry/parquet`, and `js/duckdb_worker.js:45`
  queries the files with DuckDB-Wasm (range requests first, full-fetch
  fallback). SQL builders live in `js/queries.js:43` (`throughputLatency:49`,
  `errorDistribution:65`, `visitorsByRegion:90`, `customSeries:110`);
  rendering in `js/charts.js:66`; summary cards in `js/components/cards.js:11`;
  controls in `js/components/controls.js:11`. JS stays display-only: SQL
  builders + rendering, no math beyond bucket/summary reshaping.

Empty states are first-class: unknown apps yield zero visitor rows (preset
shows its empty copy), fresh collectors 404 `/telemetry/parquet` until the
first compaction, and every chart has a `no … in range` state.

## Lifecycle numbers

| Knob | Default | Where |
|---|---|---|
| Scrape interval | 15s | `interval_secs`, `collector.toml` |
| Body cap / samples cap | 1 MiB / 5k per scrape | `max_body_bytes`, `max_samples_per_scrape` |
| Hot ring | 20k samples, drop-oldest | `recent_buffer_samples` |
| Retention / cold purge | 30d / 32d | `[fjall]` |
| Query caps | 7d range, 10k points, ≥1s step | `src/query.rs` consts |

## Walkthroughs

**Cold load** (Parquet): open UI → Connect `http://host:8080` → probe
`/api/v1/labels` fails (or host has no collector) → manifest/Parquet URL
resolved → `ensureView` installs `metrics_all` → three parallel DuckDB
queries → cards + charts render; metric names fill the custom-visualizer
dropdown.

**Hot load** (collector): open UI served by the collector itself
(same-origin, no CORS headers are sent) → probe succeeds → six parallel
`query_range` calls (requests, latency sum/count, labels, two visitor
series) → reshape → identical render path, badge `via query-range`.

**Custom visualizer**: pick metric + `avg|p50|p99|sum|count|min|max` + optional
label filter → cold builds `customSeries` SQL (previewed inline), hot builds
a `{__name__,k=v}` selector URL → `renderCustom` plots the series.

## Conventions

Load-bearing rules (shared-registry discipline, memory budget, cold-storage
and JS display-only posture) live in `AGENTS.md` and are not repeated here.
Day-to-day commands live in the `Makefile` (`make help`); CI runs the same
gate (`.github/workflows/ci.yml`).
