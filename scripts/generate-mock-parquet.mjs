#!/usr/bin/env node
// Generate a realistic sample Parquet export matching the tonggeret cold
// schema (ts Timestamp[us], name Utf8, value Float64, metric_type Utf8,
// labels Utf8-JSON, ZSTD). Output: public/sample/metrics_cold_<stamp>.parquet
//
// Engine preference: node `duckdb` package (npm i -D duckdb) → python3+duckdb
// fallback. 72h of 5-minute buckets: request counters with a 5xx spike,
// latency histograms, orders_total / queue_depth business series, plus
// visitors_total (counter) / unique_visitors_estimate (gauge) per region.
//
// Usage: npm run gen-mock [-- --rows-scale 1 --out <file>]

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const here = path.dirname(fileURLToPath(import.meta.url));
const SAMPLE_DIR = path.resolve(here, '..', 'public', 'sample');

const args = process.argv.slice(2);
const outIdx = args.indexOf('--out');
const stamp = new Date().toISOString().replace(/[-:]/g, '').slice(0, 15).replace('T', 'T');
const OUT = outIdx >= 0 ? path.resolve(args[outIdx + 1]) : path.join(SAMPLE_DIR, `metrics_cold_${stamp}.parquet`);

const GEN_SQL = (out) => `
COPY (
  WITH buckets AS (
    SELECT ((now()::TIMESTAMP) - (g.g * INTERVAL 5 MINUTE)) AS bkt FROM generate_series(0, 863) AS g(g)
  ),
  req AS (
    SELECT
      b.bkt + (CAST(floor(random() * 300) AS INTEGER) * INTERVAL 1 SECOND) AS ts,
      'http_requests_total' AS name,
      1.0 AS value,
      'counter' AS metric_type,
      '{"method":"' || CASE WHEN random() < 0.72 THEN 'GET' ELSE 'POST' END
        || '","path":"' || (['/','/orders','/users','/search'])[1 + CAST(floor(random()*4) AS INTEGER)]
        || '","status":"' || CASE
             WHEN b.bkt > (now()::TIMESTAMP) - INTERVAL 5 HOUR AND b.bkt < (now()::TIMESTAMP) - INTERVAL 3 HOUR AND random() < 0.10 THEN '500'
             WHEN random() < 0.015 THEN '500'
             WHEN random() < 0.04 THEN '404'
             ELSE '200' END || '"}' AS labels
    FROM buckets b, generate_series(1, 3 + CAST(floor(random()*22 + 8*sin(extract(epoch FROM b.bkt)/3600.0/24.0*6.283)) AS INTEGER)) g
  ),
  lat AS (
    SELECT
      b.bkt + (CAST(floor(random() * 300) AS INTEGER) * INTERVAL 1 SECOND) AS ts,
      'http_request_duration_ms' AS name,
      ROUND((6 + random()*38 + CASE WHEN random() < 0.02 THEN random()*400 ELSE 0 END)::DOUBLE, 2) AS value,
      'histogram' AS metric_type,
      '{"method":"' || CASE WHEN random() < 0.72 THEN 'GET' ELSE 'POST' END
        || '","path":"' || (['/','/orders','/users','/search'])[1 + CAST(floor(random()*4) AS INTEGER)] || '"}' AS labels
    FROM buckets b, generate_series(1, 6) g
  ),
  biz AS (
    SELECT b.bkt AS ts, 'orders_total' AS name, 1.0 AS value, 'counter' AS metric_type,
      '{"route":"/orders","status":"ok"}' AS labels
    FROM buckets b WHERE random() < 0.6
  ),
  gauge AS (
    SELECT b.bkt AS ts, 'queue_depth' AS name, (20 + 15*sin(extract(epoch FROM b.bkt)/3600.0) + random()*6)::DOUBLE AS value,
      'gauge' AS metric_type, '{"region":"eu"}' AS labels
    FROM buckets b
  ),
  vis AS (
    SELECT b.bkt AS ts, 'visitors_total' AS name,
      (SUM(5 + CAST(floor(random()*20) AS INTEGER)) OVER (PARTITION BY r.region ORDER BY b.bkt ROWS UNBOUNDED PRECEDING))::DOUBLE AS value,
      'counter' AS metric_type,
      '{"region":"' || r.region || '","scrape_target":"mock"}' AS labels
    FROM buckets b CROSS JOIN (VALUES ('DE'), ('US')) AS r(region)
  ),
  uniq AS (
    SELECT b.bkt AS ts, 'unique_visitors_estimate' AS name,
      (120 + 40*sin(extract(epoch FROM b.bkt)/86400.0*6.283) + random()*10)::DOUBLE AS value,
      'gauge' AS metric_type,
      '{"region":"' || r.region || '","scrape_target":"mock"}' AS labels
    FROM buckets b CROSS JOIN (VALUES ('DE'), ('US')) AS r(region)
  )
  SELECT * FROM req UNION ALL SELECT * FROM lat UNION ALL SELECT * FROM biz UNION ALL SELECT * FROM gauge
    UNION ALL SELECT * FROM vis UNION ALL SELECT * FROM uniq
) TO '${out.replaceAll("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD);
`;

async function tryNodeDuckdb(out) {
  let duckdb;
  try {
    duckdb = (await import('duckdb')).default;
  } catch {
    return false;
  }
  const db = new duckdb.Database(':memory:');
  try {
    await new Promise((resolve, reject) => {
      db.exec(GEN_SQL(out), (err) => (err ? reject(err) : resolve()));
    });
    return true;
  } finally {
    db.close();
  }
}

function tryPythonDuckdb(out) {
  try {
    execFileSync('python3', ['-c', 'import duckdb'], { stdio: 'ignore' });
  } catch {
    return false;
  }
  const py = `
import duckdb
sql = open(${JSON.stringify(path.join(here, '.gen.sql.tmp'))}).read()
duckdb.sql(sql)
`;
  fs.writeFileSync(path.join(here, '.gen.sql.tmp'), GEN_SQL(out));
  try {
    execFileSync('python3', ['-c', py], { stdio: 'inherit' });
    return true;
  } finally {
    fs.rmSync(path.join(here, '.gen.sql.tmp'), { force: true });
  }
}

const outEsc = OUT;
fs.mkdirSync(path.dirname(outEsc), { recursive: true });

let ok = false;
try {
  ok = await tryNodeDuckdb(outEsc);
} catch (e) {
  console.error(`node duckdb generation failed: ${e.message}`);
}
if (!ok) {
  try {
    ok = tryPythonDuckdb(outEsc);
  } catch (e) {
    console.error(`python duckdb generation failed: ${e.message}`);
  }
}
if (!ok) {
  console.error(
    'No Parquet engine found. Install one and retry:\n' +
      '  npm i -D duckdb     # preferred (pure prebuilt binary)\n' +
      '  # or: pip install duckdb',
  );
  process.exit(1);
}
const stat = fs.statSync(outEsc);
console.log(`wrote ${outEsc} (${(stat.size / 1024).toFixed(1)} KiB)`);
