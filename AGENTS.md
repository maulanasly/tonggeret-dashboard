# Agent Workflow

`tonggeret-dashboard`: Prometheus scrape collector with embedded history.
One Rust binary scrapes any `/metrics` targets on an interval, stores
samples in embedded Fjall (<10 MiB RAM) via `tonggeret`, compacts 30d of
history to Parquet, and serves the original DuckDB-Wasm dashboard which
graphs the cold files. No Python toolchain.

## Environment

Rust 1.85+ (collector) · Node 18+ (dashboard `npm run` scripts only) ·
`gh` CLI.

## Workflow

1. Feature branch off `main` → implement + test
2. Gate must be green before commit — `make gate` (or, explicitly:
   `cargo clippy --all-targets -- -D warnings` ·
   `cargo fmt --check` · `cargo test` · `cargo audit` ·
   `node --check js/*.js js/components/*.js` · `npm test`)
3. Push → PR to `main`

## Verification

| Command | What |
|---|---|
| `cargo test` | unit (mapping/allowlist/config/serve/status/targets/queue incl. HTTP-level control + proxy) + e2e scrape→registry→Parquet |
| `cargo audit` | vulnerabilities fail; `paste`/`lexical-core` informationals allowed (upstream, see tonggeret repo) |
| `npm run mock-server` | isolated UI dev without the collector |

## Structure

```
src/main.rs      roles: single-binary (worker + UI) / worker / serve → init + unified executor (tick + queue) + serve
src/config.rs    collector.toml + COLLECTOR_LISTEN / COLLECTOR_FJALL_DIR / COLLECTOR_UPSTREAM / COLLECTOR_CONTROL_TOKEN + validate()
src/scrape.rs    fetch → prometheus-parse → allowlist/denylist → record_* + recent buffer + status ; outcome series
src/targets.rs   TargetRegistry: config + persisted dynamic targets (CRUD, dedupe, caps, atomic JSON)
src/queue.rs     JobQueue: bounded, manual-first pop, pause (manual only), recent history
src/serve.rs     worker_router (hot APIs + /metrics + cold + status + control API) ; dashboard_router (dist/ + cold + proxy)
src/status.rs    StatusTracker (per-target last scrape/status/streak + runtime add/remove) → GET /api/v1/status
src/query.rs     RecentBuffer (bounded, drop-oldest) + selector/step parsing + Prometheus matrix JSON
tests/collector_flow.rs  mock /metrics → registry mirror → cold Parquet (short retention)
index.html js/ css/      DuckDB-Wasm dashboard (display only) + status/targets panels
deploy/          systemd units: tonggeret-collector.service + tonggeret-dashboard.service
scripts/         mock-server (UI dev) + gen-mock-parquet (samples) + build-singlefile (dist/)
collector.toml   example config (beruang :8000; uncomment example :3000)
data/            LOCAL ONLY, gitignored: data/fjall (hot) + data/cold (history) + data/targets.json (dynamic targets)
```

## Conventions

- **Shared-registry discipline (load-bearing).** Scraped samples are
  mirrored into the same Prometheus registry as the collector's own
  `collector_*` series, so names/labels must stay disjoint:
  - every stored sample gains `scrape_target` (closed set from config);
  - label pairs are **sorted by key** (`scrape.rs`) — the registry matches
    values positionally, parsed maps iterate randomly;
  - denylist: exact `tonggeret_dropped_total`, prefix `collector_*`
    (engine-internal / self series — meaningless or colliding downstream);
  - **no `track` middleware on the serve router** — self `http_*` (3 labels)
    would collide with scraped `http_*` (+`scrape_target`) and panic the
    registry. Self-observability is the `collector_*` outcome series.
  - residual risk (documented, accepted): two targets emitting the same
    name with *different* label keys are warn-dropped by the registry
    (samples lost, no panic) — keep instrumentation consistent across
    scraped apps.
  - never add the collector as its own target.
- **Histograms/summaries** are stored as cumulative bucket/quantile
  counters (`*_bucket{le}`, `*`+`quantile`); never `sum()` gauge estimates
  like `unique_visitors_estimate` — take latest per `(target, region)`.
- **Visitor metrics are optional by construction.** `visitors_total` /
  `unique_visitors_estimate` ride the generic mapping (counter/gauge);
  apps without them store zero visitor rows, no error. Dashboard preset
  must keep its empty-state copy. Allowlist defaults must keep the
  `visitors_` / `unique_` entries.
- **Memory budget:** tonggeret defaults (8 MiB cache + 2 MiB memtable) +
  1 MiB body cap + 5k samples/scrape cap + sequential targets + one
  scraper task. No new background tasks without a budget note.
  `StatusTracker` (one entry per target) and the status/targets panel polls
  are plain shared memory + client timers. The **unified executor** is the
  one task: interval tick + queue drain in the same task (no overlap), with
  a bounded queue (`queue_capacity`, default 256) + 100-entry history.
- **Dynamic targets + queue (load-bearing).** `TargetRegistry` persists to
  `state.targets_file` atomically: dynamically added targets plus
  `overrides` for config targets (disable/tombstone), so a removed or
  disabled `collector.toml` target stays that way across restarts (a legacy
  bare-array file is still read). `JobQueue::pop_next` is
  **manual-first while running, scheduled-only while paused** — pause never
  stops the recurring sweep; **freeze** (`worker/freeze`) is a superset that
  halts everything until resume. Control endpoints mutate state and require
  `x-control-token` when `control_token`/`COLLECTOR_CONTROL_TOKEN` is set
  (browser stores it, `serve` forwards it). Errors: 422 bad input, 429
  queue/target cap, 404 unknown id.
- **Target filter.** The top-bar `targetFilter` scopes the summary cards and
  every chart to one `scrape_target`; it threads through both data paths
  (`Queries.*(range, target)` cold, `{__name__,scrape_target}` selectors
  hot). Options come from `/api/v1/targets` when hot, else
  `Queries.listTargets()`. Queue "recent" shows only the newest 5 (backend
  keeps 100).
- **Hot-mode step is buffer-relative (load-bearing).** The recent buffer is
  bounded and often far shorter than the selected range. `HotReshape.plan`
  zooms the query window to the covered span and derives the step from it
  (`max(15s, span/240)`), retrying wider on the server point-limit error —
  a step coarser than the span collapses counter diffs to zero. `buffer.
  oldest_ts/newest_ts` in `/api/v1/status` feed this; don't reintroduce fixed
  per-range steps.
- **Frozen connection.** The dashboard binds to `window.location.origin` by
  default (no source input); the top bar is a connection chip + freeze
  toggle. The Advanced toggle reveals the source field for static hosting;
  when non-empty it overrides same-origin. Keep it that way — don't
  reintroduce a default remote source.
- **JS stays display-only for data** (SQL builders + rendering). The
  Targets & queue panel (`js/targets_client.js`) is form wiring that calls
  the proxied control API; the worker owns all state. No client-side math.
- Single-binary `collector <config>` (worker, no role) stays supported.
- **Cold storage:** `metrics_cold_*.parquet` only; purge horizon
  (`cold_purge_days`) must exceed `retention_days`. `data/` never committed.
- **Split-process roles (load-bearing).** `collector worker` is the only
  process that opens Fjall (the engine opens once/process, no read handles);
  `collector serve` must never be pointed at `data/fjall`. The only shared
  path is `data/cold` (worker writes, serve reads). `serve` reverse-proxies
  the hot + control APIs and answers 503 (200 `offline` for status) when the
  worker is down, so cold history always renders.
- **No npm/CDN changes** without noting the zero-build + CSP posture.
  - **Vendored frontend deps (offline posture, 2026-09).** `vendor/`
    self-hosts DuckDB-Wasm (MVP-only: no COOP/COEP headers are sent, so
    `selectBundle` picks MVP anyway), Arrow ESM, ECharts, and Lucide
    (note: the previous `lucide-static` CDN path never existed — 404).
    `npm run build` copies `vendor/` to `dist/vendor/` (never inlined)
    and fails on any remotely-*loaded* URL. CSP posture unchanged (no CSP
    headers; same-origin scripts need none). Still zero-build: plain static
    files, no bundler, no new npm dependencies. Re-vendor procedure: pinned
    URLs + sizes are recorded in the vendor commit; `eh`/`coi` WASM builds
    were deliberately omitted (~38 MiB).
- Errors: scrape failures are counted (`collector_scrape_total{status}`),
  never fatal; storage init failure IS fatal (fail fast).
