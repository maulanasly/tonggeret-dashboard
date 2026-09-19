import test from 'node:test';
import assert from 'node:assert/strict';

import { StatusClient } from './status_client.js';

test('normalize fills defaults for an offline body', () => {
  const n = StatusClient.normalize({ status: 'offline', error: 'refused' });
  assert.equal(n.status, 'offline');
  assert.equal(n.error, 'refused');
  assert.deepEqual(n.targets, []);
  assert.equal(n.bufferCap, 0);
  assert.equal(StatusClient.normalize(null).status, 'offline');
});

test('normalize maps a worker snapshot including target totals', () => {
  const n = StatusClient.normalize({
    status: 'degraded',
    uptime_secs: 60,
    interval_secs: 15,
    buffer: { samples: 10, cap: 20 },
    cold_files: 2,
    queue: { frozen: true, paused: false, depth: 1 },
    targets: [
      {
        name: 'a',
        url: 'http://a/metrics',
        last_status: 'fetch_error',
        last_scrape_ts: 100,
        last_samples: 0,
        last_duration_ms: 5,
        consecutive_failures: 3,
        totals: { ok: 1, fetch_error: 3, parse_error: 0 },
      },
    ],
  });
  assert.equal(n.status, 'degraded');
  assert.equal(n.frozen, true);
  assert.equal(n.bufferSamples, 10);
  assert.equal(n.coldFiles, 2);
  assert.equal(n.targets[0].lastStatus, 'fetch_error');
  assert.equal(n.targets[0].consecutiveFailures, 3);
  assert.equal(n.targets[0].totals.fetchError, 3);
});

test('normalize defaults frozen to false when absent', () => {
  assert.equal(StatusClient.normalize({ status: 'ok' }).frozen, false);
  assert.equal(StatusClient.normalize(null).frozen, false);
});

test('ageText rounds to human units', () => {
  assert.equal(StatusClient.ageText(null, 100), 'never');
  assert.equal(StatusClient.ageText(95, 100), '5s ago');
  assert.equal(StatusClient.ageText(100 - 120, 100), '2m ago');
  assert.equal(StatusClient.ageText(100 - 7200, 100), '2h ago');
  assert.equal(StatusClient.ageText(100 - 172800, 100), '2d ago');
});

test('uptimeText stays compact', () => {
  assert.equal(StatusClient.uptimeText(0), '0s');
  assert.equal(StatusClient.uptimeText(120), '2m');
  assert.equal(StatusClient.uptimeText(7200), '2h');
  assert.equal(StatusClient.uptimeText(172800), '2d');
});
