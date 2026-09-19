import test from 'node:test';
import assert from 'node:assert/strict';

import { TargetsClient } from './targets_client.js';

test('parseUrls splits lines/commas and drops blanks', () => {
  assert.deepEqual(
    TargetsClient.parseUrls('https://a/metrics\nhttps://b/metrics,  https://c/metrics \n\n'),
    ['https://a/metrics', 'https://b/metrics', 'https://c/metrics'],
  );
  assert.deepEqual(TargetsClient.parseUrls(''), []);
  assert.deepEqual(TargetsClient.parseUrls(null), []);
});

test('parseAllow splits commas/whitespace', () => {
  assert.deepEqual(TargetsClient.parseAllow('http_, beruang_  visitors_'), [
    'http_',
    'beruang_',
    'visitors_',
  ]);
  assert.deepEqual(TargetsClient.parseAllow(''), []);
});

test('queueBadge reflects offline/frozen/paused/running/idle', () => {
  assert.deepEqual(TargetsClient.queueBadge(null), { text: 'offline', status: 'offline' });
  assert.deepEqual(TargetsClient.queueBadge({ frozen: true, paused: false, depth: 3 }), {
    text: 'frozen · 3 queued',
    status: 'frozen',
  });
  assert.deepEqual(TargetsClient.queueBadge({ frozen: false, paused: true, depth: 2 }), {
    text: 'paused · 2 queued',
    status: 'degraded',
  });
  assert.deepEqual(TargetsClient.queueBadge({ frozen: false, paused: false, depth: 1 }), {
    text: 'running · 1 queued',
    status: 'starting',
  });
  assert.deepEqual(TargetsClient.queueBadge({ frozen: false, paused: false, depth: 0 }), {
    text: 'queue idle',
    status: 'ok',
  });
});

test('ageText rounds to human units', () => {
  assert.equal(TargetsClient.ageText(null, 100), 'never');
  assert.equal(TargetsClient.ageText(95, 100), '5s ago');
  assert.equal(TargetsClient.ageText(100 - 120, 100), '2m ago');
  assert.equal(TargetsClient.ageText(100 - 7200, 100), '2h ago');
  assert.equal(TargetsClient.ageText(100 - 172800, 100), '2d ago');
});

test('esc neutralizes HTML in labels', () => {
  assert.equal(
    TargetsClient.esc('<script>"&'),
    '&lt;script&gt;&quot;&amp;',
  );
});
