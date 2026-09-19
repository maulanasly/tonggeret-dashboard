# tonggeret-dashboard task runner.
#
# Thin wrappers over the exact AGENTS.md / package.json commands — every
# recipe echoes what it runs, so this file never obscures behavior.
# Prereqs: Rust 1.85+ (collector) · Node 18+ (dashboard scripts only).

SHELL := /bin/sh
COLLECTOR_CONFIG ?= collector.toml

.PHONY: help gate build test run dev mock gen-mock ui tools clean-data \
	check-rust check-node

help: ## Show this list.
	@echo "Usage: make <target> [ARGS=... COLLECTOR_CONFIG=...]"
	@echo ""
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

check-rust:
	@command -v cargo >/dev/null || \
		{ echo "error: cargo not found (need Rust 1.85+)"; exit 1; }

check-node:
	@command -v node >/dev/null || \
		{ echo "error: node not found (need Node 18+)"; exit 1; }

gate: check-rust check-node ## Full AGENTS.md gate (fail-fast, in order).
	@echo "==> cargo clippy --all-targets -- -D warnings"
	cargo clippy --all-targets -- -D warnings
	@echo "==> cargo fmt --check"
	cargo fmt --check
	@echo "==> cargo test"
	cargo test
	@echo "==> cargo audit"
	cargo audit --deny warnings --ignore RUSTSEC-2024-0436 --ignore RUSTSEC-2023-0086
	@echo "==> node --check js/*.js js/components/*.js"
	node --check js/*.js js/components/*.js
	@echo "==> npm test"
	npm test

build: check-rust check-node ## Release collector binary + rebundled dist/.
	@echo "==> cargo build --release"
	cargo build --release
	@echo "==> npm run build"
	npm run build

test: check-rust check-node ## cargo test + npm test.
	@echo "==> cargo test"
	cargo test
	@echo "==> npm test"
	npm test

run: check-rust ## Run the collector (COLLECTOR_CONFIG=..., ARGS=... passthrough).
	@echo "==> cargo run -- $(COLLECTOR_CONFIG) $(ARGS)"
	cargo run -- $(COLLECTOR_CONFIG) $(ARGS)

dev: check-node ## Alias for mock (isolated UI dev without the collector).
	@$(MAKE) mock

mock: check-node ## Static + CORS + Range + manifest mock server on :8080.
	@echo "==> npm run mock-server"
	npm run mock-server

gen-mock: check-node ## Synthesize a sample Parquet export into public/sample/.
	@echo "==> npm run gen-mock"
	npm run gen-mock

ui: check-node ## Rebundle dist/index.html from index.html + js/ + css/.
	@echo "==> npm run build"
	npm run build

tools: check-rust ## Install helper tools (cargo-audit) if missing.
	@command -v cargo-audit >/dev/null || \
		{ echo "==> cargo install cargo-audit --locked"; \
		  cargo install cargo-audit --locked; }

clean-data: ## Delete local telemetry (data/, gitignored). Asks first.
	@echo "This deletes ./data (hot Fjall store + cold Parquet history)."
	@printf "Type YES to continue: "; read ans; \
		[ "$$ans" = "YES" ] || { echo "aborted"; exit 1; }
	rm -rf data data-*
	@echo "removed."
