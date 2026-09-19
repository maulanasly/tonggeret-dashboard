#!/usr/bin/env node
// Bundle the zero-build app into a single static file: dist/index.html.
// - Inlines css/styles.css into <style>.
// - Bundles js/app.js + relative ESM imports into one inline module script.
//   Rules the bundler relies on (kept true by construction):
//   * one named export object per module, no default/circular imports;
//   * relative imports use `import { X } from './y.js'` (stripped at bundle);
//   * bare-specifier imports (local importmap) are deduped, first wins.
// - Copies vendor/ (self-hosted DuckDB-Wasm, Arrow, ECharts, Lucide) to
//   dist/vendor/ as files: the WASM binaries are MBs and must not be
//   inlined; the importmap + script tags reference them relatively.
// - dist/ is fully offline-capable: no remote URLs remain (verified below).
//
// Usage: npm run build

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(here, '..');
const ENTRY = path.join(ROOT, 'js', 'app.js');
const INDEX = path.join(ROOT, 'index.html');
const OUT = path.join(ROOT, 'dist', 'index.html');

const REL_IMPORT =
  /^\s*import\s+(?:[^'"]*?\s+from\s+)?['"]([^'"]+)['"]\s*;?\s*$/;

/// All vendored `.js`/`.mjs` files (runtime-resolved via the importmap).
function walkVendor() {
  const out = [];
  const dir = path.join(ROOT, 'vendor');
  const visit = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) visit(p);
      else if (/\.m?js$/.test(e.name)) out.push(p);
    }
  };
  visit(dir);
  return out;
}

/// Bare specifiers reachable at runtime: follow relative imports from the
/// vendored entry files and report any bare specifier not mapped in the
/// importmap (the browser would fail to resolve it). Node-only files are
/// excluded by construction — they are never loaded in the browser.
function unmappedVendorSpecifiers(importMapped) {
  const entries = [
    'vendor/arrow/Arrow.mjs',
    'vendor/flatbuffers/flatbuffers.js',
    'vendor/duckdb/duckdb-browser.mjs',
  ].map((p) => path.join(ROOT, p));
  const seen = new Set();
  const bare = new Map();
  const walk = (file) => {
    if (seen.has(file)) return;
    seen.add(file);
    for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
      const trimmed = line.trim();
      if (trimmed.startsWith('//') || trimmed.startsWith('*') || trimmed.startsWith('/*')) continue;
      const m = /\bfrom\s+['"]([^'"]+)['"]/.exec(line);
      if (!m) continue;
      const spec = m[1];
      if (spec.startsWith('.')) {
        const next = path.normalize(path.join(path.dirname(file), spec));
        if (fs.existsSync(next)) walk(next);
      } else if (!/^https?:|^\/|^[A-Za-z]:/.test(spec) && !importMapped.has(spec)) {
        bare.set(spec, file);
      }
    }
  };
  for (const entry of entries) walk(entry);
  return [...bare.entries()].map(([spec, file]) => `${spec} (${path.relative(ROOT, file)})`);
}

function collect(entry) {
  const order = [];
  const seen = new Set();
  const bare = [];
  const bareSeen = new Set();
  (function visit(file) {
    if (seen.has(file)) return;
    seen.add(file);
    const src = fs.readFileSync(file, 'utf8');
    for (const line of src.split('\n')) {
      const m = REL_IMPORT.exec(line);
      if (!m) continue;
      const spec = m[1];
      if (spec.startsWith('.')) {
        visit(path.normalize(path.join(path.dirname(file), spec)));
      } else if (!bareSeen.has(line.trim())) {
        bareSeen.add(line.trim());
        bare.push(line);
      }
    }
    order.push(file);
  })(entry);
  return { order, bare };
}

function stripModule(src) {
  return (
    src
      .split('\n')
      // Drop every static import line: relative imports are inlined below,
      // bare-specifier (local importmap) imports are re-emitted once at the top.
      .filter((line) => !REL_IMPORT.test(line))
      .join('\n')
      // `export const X` -> `const X` (same for function/class/async).
      .replace(/^(\s*)export\s+(default\s+)?/gm, '$1')
      // `export { A, B };` -> `` (names already top-level).
      .replace(/^\s*export\s*\{[^}]*\}\s*;?\s*$/gm, '')
  );
}

function bundle(entry) {
  const { order, bare } = collect(entry);
  const parts = [];
  for (const b of bare) {
    if (/from\s+['"]@duckdb\/duckdb-wasm['"]/.test(b)) parts.push(b);
  }
  for (const b of bare) {
    if (!/from\s+['"]@duckdb\/duckdb-wasm['"]/.test(b)) parts.push(b);
  }
  // Inlined modules share one scope: duplicate top-level declarations
  // would be a runtime SyntaxError (frozen UI), so fail the build instead.
  const declared = new Map();
  for (const file of order) {
    const rel = path.relative(ROOT, file);
    const stripped = stripModule(fs.readFileSync(file, 'utf8'));
    for (const line of stripped.split('\n')) {
      const m = /^(?:function|const|let|var|class)\s+([A-Za-z_$][\w$]*)/.exec(line);
      if (!m) continue;
      if (declared.has(m[1])) {
        throw new Error(
          `duplicate top-level declaration '${m[1]}' in ${declared.get(m[1])} and ${rel} — rename one`,
        );
      }
      declared.set(m[1], rel);
    }
    parts.push(`\n/* ---- ${rel} ---- */\n` + stripped);
  }
  const code = parts.join('\n');
  if (/from\s+['"]\.\.?\//.test(code)) {
    throw new Error('bundle still contains relative imports — bundler assumption violated');
  }
  // Bare specifiers must be covered by the importmap (./vendor/...); a
  // missing mapping is a browser runtime error, so fail the build instead.
  const mapped = new Set(
    [...html.matchAll(/"([^"]+)":\s*"\.\/vendor\//g)].map((m) => m[1]),
  );
  const unmapped = [...code.matchAll(/from\s+['"]([^'"]+)['"]/g)]
    .map((m) => m[1])
    .filter((s) => !s.startsWith('.') && !mapped.has(s));
  if (unmapped.length > 0) {
    throw new Error(`unmapped bare specifier(s) not covered by the importmap: ${[...new Set(unmapped)].join(', ')}`);
  }
  return code;
}

let html = fs.readFileSync(INDEX, 'utf8');

// Inline stylesheet.
const cssPath = path.join(ROOT, 'css', 'styles.css');
const css = fs.readFileSync(cssPath, 'utf8');
if (!html.includes('href="css/styles.css"')) throw new Error('stylesheet link not found in index.html');
html = html.replace(
  /<link\s+rel="stylesheet"\s+href="css\/styles\.css"\s*\/?>/,
  () => `<style>\n${css}\n</style>`,
);

// Inline module bundle.
if (!html.includes('src="js/app.js"')) throw new Error('entry script tag not found in index.html');
const code = bundle(ENTRY);
html = html.replace(
  /<script\s+type="module"\s+src="js\/app\.js"\s*><\/script>/,
  () => `<script type="module">\n${code}\n</script>`,
);

html = html.replace('</head>', '<!-- bundled: npm run build (tonggeret-dashboard) -->\n</head>');

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, html);
console.log(`wrote ${OUT} (${(fs.statSync(OUT).size / 1024).toFixed(1)} KiB)`);

// Copy self-hosted vendor assets alongside (never inlined: MBs of WASM).
// dist/ stays fully offline-capable; fail loudly if any *loaded* resource
// (script/src/href/import, not placeholder text) still points remote.
fs.rmSync(path.join(ROOT, 'dist', 'vendor'), { recursive: true, force: true });
fs.cpSync(path.join(ROOT, 'vendor'), path.join(ROOT, 'dist', 'vendor'), { recursive: true });
const remoteRefs =
  html.match(/(?:src|href)\s*=\s*"https?:\/\/[^"<>]+|(?:from|import\()\s*["']https?:\/\/[^"'<>]+/g) ?? [];
if (remoteRefs.length > 0) {
  throw new Error(`dist/index.html still loads remote URLs: ${remoteRefs.slice(0, 5).join(', ')}`);
}

// Every bare specifier reachable at runtime (vendor entries → relative
// closure) must be mapped; an unmapped one is a browser SyntaxError.
const mapped = new Set(
  [...html.matchAll(/"([^"]+)":\s*"\.\/vendor\//g)].map((m) => m[1]),
);
const unmapped = unmappedVendorSpecifiers(mapped);
if (unmapped.length > 0) {
  throw new Error(`unmapped bare specifier(s) not covered by the importmap:\n  ${unmapped.join('\n  ')}`);
}
console.log('vendor copied to dist/vendor (offline-capable, no remote loads)');
