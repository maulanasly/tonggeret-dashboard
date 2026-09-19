# How this dashboard works

One Rust binary (`collector`) scrapes Prometheus `/metrics` targets,
keeps history, and serves a client-side dashboard. There is no query
backend: the browser either fetches pre-aggregated JSON (hot mode) or
queries cold Parquet files directly with DuckDB-Wasm (cold mode).

The single-binary mode below is the default and the current production
shape. [Split-process deployment](#split-process-deployment-worker--dashboard)
specifies a target architecture that separates the scrape/storage lifecycle
from the UI lifecycle (worker + read-only dashboard) for hosts where the
dashboard must stay up across collector restarts.

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

## Split-process deployment (worker + dashboard)

**Status: target architecture.** The single-binary mode above stays the
default. This section specifies how to separate the scrape/storage lifecycle
from the UI lifecycle on a host (systemd / Compose) or whenever the dashboard
must survive collector restarts.

### Why split

One process couples two independent lifecycles: a collector restart (config
change, storage init, compaction/purge, crash) takes the dashboard down with
it, and UI uptime is bounded by the writer's. The dashboard is already
display-only (`js/app.js` renders; queries run in the browser), so the
serving side is a thin read/proxy layer that can be restarted freely.
Splitting also keeps the worker on loopback while only the dashboard is
exposed through the reverse proxy.

### Topology

```
scrape targets ──15s──▶ collector worker   data/fjall (RW, sole opener)
  (loopback :8081)         │ scrape loop · compaction · purge
                           ├─ /metrics                (self + mirrored)
                           ├─ /api/v1/query_range     (hot ring)
                           ├─ /api/v1/labels
                           └─ /api/v1/status          (new worker contract)
                           ▼
                    metrics_cold_*.parquet (shared data/cold volume)
                           ▼
dashboard serve (0.0.0.0:8080, read-only)
  dist/ · /api/files · /telemetry/parquet · /telemetry/cold/:file  (local cold reads)
  /api/v1/{query_range,labels,status} ──reverse-proxy──▶ worker
  worker unreachable → cold charts + "collector offline" status still work
```

### Process roles

One binary, two roles (`collector worker <config>` / `collector serve
<config>`): the shared lib, tests, and CI gate stay single. A new
`COLLECTOR_UPSTREAM` env tells `serve` where the worker is;
`COLLECTOR_LISTEN` / `COLLECTOR_FJALL_DIR` are unchanged.

- **worker** — config → tonggeret/Fjall init → scrape loop → compaction/purge.
  Binds private/loopback. The only process that opens `data/fjall`. Serves
  `/metrics`, the hot APIs, and `/api/v1/status`. Does not serve `dist/`.
- **serve** — read-only. Serves `dist/` and the cold-file surface
  (`/api/files`, `/telemetry/parquet`, `/telemetry/cold/:file`) directly from
  the shared `data/cold`. Reverse-proxies `/api/v1/{query_range,labels,status}`
  to `COLLECTOR_UPSTREAM`. Never opens `data/fjall`.

### Status contract (`GET /api/v1/status`)

New worker endpoint so the dashboard can show collector status without
parsing `/metrics` text. Backed by a small `Arc<Mutex<StatusState>>` updated
inline by `scrape::scrape_once` — plain shared memory, no new background task
(the `AGENTS.md` memory-budget rule still holds).

```json
{
  "status": "ok",
  "uptime_secs": 1234,
  "interval_secs": 15,
  "buffer": { "samples": 1234, "cap": 20000 },
  "cold_files": 5,
  "retention_days": 30,
  "targets": [
    {
      "name": "beruang",
      "url": "http://localhost:8000/metrics",
      "last_scrape_ts": 1700000000,
      "last_status": "ok",
      "last_samples": 123,
      "last_duration_ms": 12,
      "consecutive_failures": 0,
      "totals": { "ok": 100, "fetch_error": 2, "parse_error": 0 }
    }
  ]
}
```

`last_*` extends today's aggregate `collector_scrape_total{target,status}`
counter with per-target recency, which the registry cannot express. The
dashboard polls this (~`interval_secs`) and renders a status badge/panel:
worker reachable, per-target last-scrape age and failure streaks, buffer
occupancy, cold-file count. `status` is `degraded` when any target has a
non-zero failure streak and `starting` before the first scrape.

### Storage ownership & invariants

Load-bearing: the tonggeret engine opens the Fjall keyspace exactly once per
process and exposes no read handles (`src/query.rs` header). Therefore:

- `data/fjall` is opened **only** by the worker; `serve` must never be pointed
  at it (the engine forbids a second open — lock/corruption risk, and there is
  no read API anyway).
- `data/cold` is the only shared path: worker writes Parquet, `serve` reads it.
  Mount it read-only for the dashboard where possible.
- The hot ring lives in worker memory; the frontend reaches it only through
  the proxy, never by re-reading Fjall.

### systemd deployment (VPS)

Two units on one host, with nginx terminating TLS in front of the dashboard
(worker stays loopback-only):

- `tonggeret-collector.service` — `collector worker`, `Restart=always`,
  `ProtectSystem=strict`, `ReadWritePaths=data/`,
  `Environment=COLLECTOR_LISTEN=127.0.0.1:8081`; health via `/api/v1/status`.
- `tonggeret-dashboard.service` — `collector serve`, `Restart=always`,
  `ReadOnlyPaths=data/cold`,
  `Environment=COLLECTOR_LISTEN=0.0.0.0:8080`,
  `Environment=COLLECTOR_UPSTREAM=http://127.0.0.1:8081`;
  `After=tonggeret-collector.service` for ordering only — it must start and
  keep serving while the worker is down.

### Degradation matrix

| Worker | Dashboard | Result |
|---|---|---|
| up | up | hot charts via proxy + status; cold as fallback |
| down | up | cold charts from `data/cold` + "collector offline" status; hot endpoints 503 |
| up | down | collection/history continue unaffected |
| restart | up | dashboard never 5xx; hot resumes when the worker returns |

### Rejected alternatives

- **Frontend opens Fjall read-only** — the engine opens once/process with no
  read handles; a second opener is unsafe and unsupported.
- **External TSDB (Prometheus/VictoriaMetrics) as the read backend** — against
  the embedded <10 MiB ethos; revisit only for multi-instance aggregation.
- **Worker pushes to the frontend** — adds coupling, backpressure, and a second
  durability path; polling/proxying the HTTP contract is simpler and reuses
  the existing hot API.

### Migration phases

1. Single process: add `StatusState` + `GET /api/v1/status` + the dashboard
   status panel — status visibility with no topology change.
2. Add the `worker`/`serve` roles + `COLLECTOR_UPSTREAM`; split the serve
   router (cold local, hot proxied, explicit 503 when upstream is down); add
   `/healthz` to both roles.
3. Ship the systemd units + docs; keep single-process mode as the default
   until the split is proven in production.

## Conventions

Load-bearing rules (shared-registry discipline, memory budget, cold-storage
and JS display-only posture) live in `AGENTS.md` and are not repeated here.
Day-to-day commands live in the `Makefile` (`make help`); CI runs the same
gate (`.github/workflows/ci.yml`).
