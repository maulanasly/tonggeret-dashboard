//! Tests for the hot-mode reshaper (DOM-free, stdlib only).
//! Run with `npm test`.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { HotReshape } from './hot_reshape.js';

function matrix(entries) {
  return entries.map(([metric, values]) => ({ metric, values }));
}

describe('windowFor', () => {
  it('maps UI ranges to windows', () => {
    assert.equal(HotReshape.windowFor('1h'), 3600);
    assert.equal(HotReshape.windowFor('24h'), 86_400);
    assert.equal(HotReshape.windowFor('all'), 7 * 86_400);
  });
});

describe('plan', () => {
  const now = 1_000_000;

  it('falls back to the nominal range when coverage is unknown', () => {
    const p = HotReshape.plan('24h', null, now);
    assert.equal(p.start, now - 86_400);
    assert.equal(p.end, now);
    assert.equal(p.step, 360); // ceil(86400 / 240)
    assert.equal(p.coveredSecs, 86_400);
  });

  it('zooms to the covered span and keeps cadence buckets when the buffer is short', () => {
    // 45s buffer, user asked 24h: every series must land in >=2 buckets so
    // counter diffs survive.
    const p = HotReshape.plan('24h', { oldest_ts: now - 45, newest_ts: now }, now);
    assert.equal(p.start, now - 45);
    assert.equal(p.end, now);
    assert.equal(p.step, 15);
    assert.equal(p.coveredSecs, 45);
    assert.ok(Math.floor(p.coveredSecs / p.step) >= 2);
  });

  it('keeps the selected range when the buffer covers it', () => {
    const p = HotReshape.plan('1h', { oldest_ts: now - 7200, newest_ts: now }, now);
    assert.equal(p.start, now - 3600);
    assert.equal(p.end, now);
    assert.equal(p.step, 15);
  });
});

describe('fmtDuration', () => {
  it('renders compact units', () => {
    assert.equal(HotReshape.fmtDuration(45), '45s');
    assert.equal(HotReshape.fmtDuration(300), '5m');
    assert.equal(HotReshape.fmtDuration(7200), '2h');
    assert.equal(HotReshape.fmtDuration(172800), '2d');
    assert.equal(HotReshape.fmtDuration(0), '0s');
  });
});

describe('groupBySeries', () => {
  it('groups by name + labels, sorts points, nulls non-finite', () => {
    const groups = HotReshape.groupBySeries(
      matrix([
        [{ __name__: 'm', a: '1' }, [[3, '30'], [1, '10'], [2, 'NaN']]],
        [{ __name__: 'm', a: '2' }, [[1, '5']]],
      ]),
    );
    assert.equal(groups.length, 2);
    assert.deepEqual(
      groups[0].points.map((p) => p.t),
      [1, 2, 3],
    );
    assert.equal(groups[0].points[2].v, 30);
    assert.equal(groups[0].points[1].v, null);
    assert.deepEqual(groups[0].labels, [['a', '1']]);
  });
});

describe('throughputFromGroups', () => {
  it('diffs cumulative counters and derives avg from sum/count', () => {
    const total = HotReshape.groupBySeries(
      matrix([[{ __name__: 'http_requests_total' }, [[100, '10'], [160, '16'], [220, '14']]]]),
    );
    const sum = HotReshape.groupBySeries(
      matrix([[{ __name__: 'http_request_duration_ms_sum' }, [[100, '50'], [160, '110'], [220, '110']]]]),
    );
    const count = HotReshape.groupBySeries(
      matrix([[{ __name__: 'http_request_duration_ms_count' }, [[100, '10'], [160, '20'], [220, '20']]]]),
    );
    const rows = HotReshape.throughputFromGroups(total, sum, count);
    assert.equal(rows.length, 2);
    // Bucket 160: 6 reqs, avg (110-50)/(20-10) = 6ms; p99 unavailable hot.
    assert.equal(rows[0].reqs, 6);
    assert.equal(rows[0].avg_ms, 6);
    assert.equal(rows[0].p99_ms, null);
    // Bucket 220: counter reset (14 < 16) clamps to 0, no sum/count delta → avg null.
    assert.equal(rows[1].reqs, 0);
    assert.equal(rows[1].avg_ms, null);
  });
});

describe('errorsFromGroups', () => {
  it('groups by path with range-delta totals, drops healthy paths', () => {
    const groups = HotReshape.groupBySeries(
      matrix([
        [{ __name__: 'http_requests_total', path: '/a', status: '200' }, [[1, '100'], [2, '150']]],
        [{ __name__: 'http_requests_total', path: '/a', status: '500' }, [[1, '0'], [2, '3']]],
        [{ __name__: 'http_requests_total', path: '/b', status: '200' }, [[1, '7'], [2, '9']]],
        [{ __name__: 'http_requests_total', status: '404' }, [[1, '1'], [2, '4']]],
      ]),
    );
    const rows = HotReshape.errorsFromGroups(groups);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].path, '/a');
    assert.equal(rows[0].c5xx, 3);
    assert.equal(rows[0].total, 53);
    assert.equal(rows[1].path, '(unknown)');
    assert.equal(rows[1].c4xx, 3);
  });

  it('returns empty when nothing errored', () => {
    const groups = HotReshape.groupBySeries(
      matrix([[{ __name__: 'http_requests_total', path: '/', status: '200' }, [[1, '1'], [2, '2']]]]),
    );
    assert.deepEqual(HotReshape.errorsFromGroups(groups), []);
  });
});

describe('visitorsFromGroups', () => {
  it('maps matrix cells to preset rows', () => {
    const total = HotReshape.groupBySeries(
      matrix([
        [{ __name__: 'visitors_total', scrape_target: 'app', region: 'DE' }, [[100, '10'], [200, '15']]],
      ]),
    );
    const uniques = HotReshape.groupBySeries(
      matrix([
        [{ __name__: 'unique_visitors_estimate', scrape_target: 'app', region: 'DE' }, [[100, '8']]],
      ]),
    );
    const rows = HotReshape.visitorsFromGroups(total, uniques);
    assert.equal(rows.length, 2);
    assert.deepEqual(rows[0], {
      bucket_us: 100_000_000,
      target: 'app',
      region: 'DE',
      visitors: 10,
      uniques: 8,
    });
    assert.equal(rows[1].visitors, 15);
    assert.equal(rows[1].uniques, null);
  });
});

describe('customFromGroups', () => {
  it('averages contributing series per bucket', () => {
    const groups = HotReshape.groupBySeries(
      matrix([
        [{ __name__: 'g', r: 'a' }, [[100, '10'], [200, '20']]],
        [{ __name__: 'g', r: 'b' }, [[100, '30']]],
      ]),
    );
    const rows = HotReshape.customFromGroups(groups);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].v, 20);
    assert.equal(rows[0].n, 2);
    assert.equal(rows[1].v, 20);
  });
});

describe('summarize', () => {
  it('derives cards from throughput + error rows', () => {
    const s = HotReshape.summarize(
      [
        { bucket_us: 1, reqs: 10, avg_ms: 5, p99_ms: null },
        { bucket_us: 2, reqs: 30, avg_ms: 15, p99_ms: null },
      ],
      [{ path: '/a', c4xx: 0, c5xx: 4, total: 40 }],
      60,
    );
    assert.equal(s.total, 40);
    assert.equal(s.avgMs, 12.5);
    assert.equal(s.errorCount, 4);
    assert.equal(s.errorRate, 0.1);
  });
});
