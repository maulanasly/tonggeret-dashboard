#!/usr/bin/env node
// Zero-dependency static server for isolated dashboard development.
// - Serves the dashboard directory with CORS + HTTP Range support.
// - GET /telemetry/parquet  -> newest public/sample/metrics_cold_*.parquet (404 when absent).
// - GET /api/files          -> JSON manifest of sample files (multi-file future).
//
// Usage: npm run mock-server [-- --port 8080 --root .]

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(here, '..');
const SAMPLE_DIR = path.join(ROOT, 'public', 'sample');

const args = process.argv.slice(2);
const portIdx = args.indexOf('--port');
const PORT = portIdx >= 0 ? Number(args[portIdx + 1]) || 8080 : 8080;

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.parquet': 'application/vnd.apache.parquet',
  '.txt': 'text/plain; charset=utf-8',
  '.svg': 'image/svg+xml',
};

function newestSample() {
  try {
    const files = fs
      .readdirSync(SAMPLE_DIR)
      .filter((f) => f.startsWith('metrics_cold_') && f.endsWith('.parquet'))
      .map((f) => path.join(SAMPLE_DIR, f))
      .sort();
    return files.length > 0 ? files[files.length - 1] : null;
  } catch {
    return null;
  }
}

function sampleManifest(req) {
  let files = [];
  try {
    files = fs
      .readdirSync(SAMPLE_DIR)
      .filter((f) => f.startsWith('metrics_cold_') && f.endsWith('.parquet'))
      .sort();
  } catch {
    files = [];
  }
  const host = `http://${req.headers.host}`;
  return files.map((f) => `${host}/sample/${f}`);
}

function sendFile(req, res, absPath) {
  let stat;
  try {
    stat = fs.statSync(absPath);
    if (!stat.isFile()) throw new Error('not a file');
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('not found');
    return;
  }
  const ext = path.extname(absPath).toLowerCase();
  const headers = {
    'content-type': MIME[ext] ?? 'application/octet-stream',
    'accept-ranges': 'bytes',
    'access-control-allow-origin': '*',
    'access-control-expose-headers': 'content-range, accept-ranges, content-length',
    'cache-control': ext === '.parquet' ? 'public, max-age=60' : 'no-cache',
  };
  const range = req.headers.range;
  if (range) {
    const m = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
    if (m) {
      let start = m[1] === '' ? null : Number(m[1]);
      let end = m[2] === '' ? null : Number(m[2]);
      if (start == null && end != null) start = Math.max(0, stat.size - end), (end = stat.size - 1);
      if (start == null) start = 0;
      if (end == null || end >= stat.size) end = stat.size - 1;
      if (start < stat.size) {
        headers['content-range'] = `bytes ${start}-${end}/${stat.size}`;
        headers['content-length'] = String(end - start + 1);
        res.writeHead(206, headers);
        fs.createReadStream(absPath, { start, end }).pipe(res);
        return;
      }
    }
  }
  headers['content-length'] = String(stat.size);
  res.writeHead(200, headers);
  fs.createReadStream(absPath).pipe(res);
}

const server = http.createServer((req, res) => {
  // CORS preflight.
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, HEAD, OPTIONS',
      'access-control-allow-headers': 'range, content-type',
      'access-control-max-age': '86400',
    });
    res.end();
    return;
  }

  const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
  const pathname = decodeURIComponent(url.pathname);

  if (pathname === '/telemetry/parquet') {
    const latest = newestSample();
    if (!latest) {
      res.writeHead(404, {
        'content-type': 'text/plain; charset=utf-8',
        'access-control-allow-origin': '*',
      });
      res.end('no parquet export available yet (run npm run gen-mock first)');
      return;
    }
    sendFile(req, res, latest);
    return;
  }

  if (pathname === '/api/files') {
    const body = JSON.stringify(sampleManifest(req));
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'access-control-allow-origin': '*',
    });
    res.end(body);
    return;
  }

  // Static: /sample/<f> maps to public/sample/<f>; / maps to index.html.
  let rel = pathname.replace(/^\/+/, '');
  if (rel === '' || rel === 'index.html') rel = 'index.html';
  else if (rel.startsWith('sample/')) rel = path.join('public', rel);
  const abs = path.normalize(path.join(ROOT, rel));
  if (!abs.startsWith(ROOT)) {
    res.writeHead(403, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('forbidden');
    return;
  }
  if (req.method === 'HEAD') {
    try {
      const stat = fs.statSync(abs);
      res.writeHead(200, {
        'content-length': String(stat.size),
        'accept-ranges': 'bytes',
        'access-control-allow-origin': '*',
      });
      res.end();
    } catch {
      res.writeHead(404);
      res.end();
    }
    return;
  }
  sendFile(req, res, abs);
});

server.listen(PORT, () => {
  console.log(`tonggeret-dashboard mock server: http://localhost:${PORT}/`);
  console.log(`  /telemetry/parquet -> ${newestSample() ?? '(no sample yet — run npm run gen-mock)'}`);
});
