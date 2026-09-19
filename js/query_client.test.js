import test from 'node:test';
import assert from 'node:assert/strict';

import { parsePointLimit } from './query_client.js';

test('parsePointLimit extracts the point count from the over-limit error', () => {
  assert.equal(
    parsePointLimit('query would return 12345 points (limit 10000); increase step'),
    12345,
  );
  assert.equal(parsePointLimit('some other error'), null);
  assert.equal(parsePointLimit(undefined), null);
  assert.equal(parsePointLimit(null), null);
});
