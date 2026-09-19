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
| `cargo test` | 29 unit (mapping/allowlist/config/serve/query incl. HTTP-level) + e2e scrape→registry→Parquet |
| `cargo audit` | vulnerabilities fail; `paste`/`lexical-core` informationals allowed (upstream, see tonggeret repo) |
| `npm run mock-server` | isolated UI dev without the collector |

## Structure

```
src/main.rs      init (fatal) → scrape loop → serve → graceful shutdown
src/config.rs    collector.toml + COLLECTOR_LISTEN / COLLECTOR_FJALL_DIR + validate()
src/scrape.rs    fetch → prometheus-parse → allowlist/denylist → record_* + recent buffer ; outcome series
src/serve.rs     UI (dist/) + /metrics + /telemetry/parquet + /telemetry/cold/:file + /api/files + /api/v1/query_range (hot buffer)
src/query.rs     RecentBuffer (bounded, drop-oldest) + selector/step parsing + Prometheus matrix JSON
tests/collector_flow.rs  mock /metrics → registry mirror → cold Parquet (short retention)
index.html js/ css/      DuckDB-Wasm dashboard (display only, same-origin)
scripts/           mock-server (UI dev) + gen-mock-parquet (samples) + build-singlefile (dist/)
collector.toml     example config (beruang :8000; uncomment example :3000)
data/              LOCAL ONLY, gitignored: data/fjall (hot) + data/cold (history)
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
- **Cold storage:** `metrics_cold_*.parquet` only; purge horizon
  (`cold_purge_days`) must exceed `retention_days`. `data/` never committed.
- **JS stays display-only:** SQL builders + rendering; no math beyond
  bucket/summary reshaping. No npm/CDN changes without noting the
  zero-build + CSP posture.
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
