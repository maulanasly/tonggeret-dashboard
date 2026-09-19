//! Smoke tests for the DOM-free SQL builders in `queries.js`.
//! Run with `npm test` (node:test, stdlib only — no new dependencies).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { Queries } from './queries.js';

describe('Ranges', () => {
  it('exposes 1h/24h/all with bucket granularity', () => {
    assert.equal(Queries.Ranges['1h'].bucketPart, 'minute');
    assert.equal(Queries.Ranges['24h'].bucketPart, 'hour');
    assert.equal(Queries.Ranges['all'].bucketPart, 'day');
  });
});

describe('timeFilter', () => {
  it('emits range predicates, TRUE for all', () => {
    assert.match(Queries.timeFilter('1h'), /INTERVAL 1 HOUR/);
    assert.match(Queries.timeFilter('24h'), /INTERVAL 24 HOUR/);
    assert.equal(Queries.timeFilter('all'), 'TRUE');
  });
});

describe('bucketExpr', () => {
  it('uses date_trunc with the range bucket part', () => {
    assert.equal(Queries.bucketExpr('1h'), "date_trunc('minute', ts)");
    assert.equal(Queries.bucketExpr('24h'), "date_trunc('hour', ts)");
  });

  it('falls back to 24h for unknown ranges', () => {
    assert.equal(Queries.bucketExpr('bogus'), "date_trunc('hour', ts)");
  });
});

describe('throughputLatency', () => {
  it('queries the request/latency series with microsecond buckets', () => {
    const sql = Queries.throughputLatency('24h');
    assert.match(sql, /FROM metrics_all/);
    assert.match(sql, /name = 'http_requests_total'/);
    assert.match(sql, /name = 'http_request_duration_ms'/);
    assert.match(sql, /bucket_us/);
    assert.match(sql, /INTERVAL 24 HOUR/);
  });
});

describe('errorDistribution', () => {
  it('matches 4xx/5xx via the $.status label', () => {
    const sql = Queries.errorDistribution('1h');
    assert.match(sql, /\$\.status/);
    assert.match(sql, /LIKE '4%'|LIKE '5%'/);
    assert.match(sql, /INTERVAL 1 HOUR/);
  });
});

describe('listMetricNames', () => {
  it('selects distinct names', () => {
    assert.match(Queries.listMetricNames(), /SELECT DISTINCT name FROM metrics_all/);
  });
});

describe('visitorsByRegion', () => {
  it('reads both visitor series grouped by target + region', () => {
    const sql = Queries.visitorsByRegion('24h');
    assert.match(sql, /name IN \('visitors_total', 'unique_visitors_estimate'\)/);
    assert.match(sql, /\$\.scrape_target/);
    assert.match(sql, /\$\.region/);
    assert.match(sql, /GROUP BY 1, 2, 3/);
    assert.match(sql, /INTERVAL 24 HOUR/);
  });

  it('takes latest per bucket, never sums the uniques gauge', () => {
    const sql = Queries.visitorsByRegion('all');
    assert.match(sql, /arg_max\(value, ts\) FILTER \(WHERE name = 'unique_visitors_estimate'\)/);
    assert.doesNotMatch(sql, /sum\(value\)/);
  });
});

describe('customSeries', () => {
  it('supports every documented aggregation', () => {
    for (const [agg, fragment] of [
      ['count', 'count(*)'],
      ['sum', 'sum(value)'],
      ['avg', 'avg(value)'],
      ['min', 'min(value)'],
      ['max', 'max(value)'],
      ['p50', 'quantile_cont(value, 0.5)'],
      ['p99', 'quantile_cont(value, 0.99)'],
    ]) {
      assert.match(Queries.customSeries({ metric: 'm', agg, range: 'all' }), new RegExp(fragment.replace(/[()*,.]/g, '\\$&')));
    }
  });

  it('defaults to avg over 24h', () => {
    const sql = Queries.customSeries({ metric: 'm' });
    assert.match(sql, /avg\(value\)/);
    assert.match(sql, /INTERVAL 24 HOUR/);
  });

  it('escapes single quotes in metric and label values', () => {
    const sql = Queries.customSeries({
      metric: "o'brien",
      agg: 'sum',
      labelKey: 'path',
      labelValue: "/a'b",
      range: 'all',
    });
    assert.match(sql, /name = 'o''brien'/);
    assert.match(sql, /'\/a''b'/);
  });

  it('filters on label presence when no value is given', () => {
    const sql = Queries.customSeries({ metric: 'm', labelKey: 'path', range: 'all' });
    assert.match(sql, /IS NOT NULL/);
  });
});
