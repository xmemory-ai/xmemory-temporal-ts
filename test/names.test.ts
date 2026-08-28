import assert from 'node:assert/strict';
import { test } from 'node:test';
import * as names from '../src/names';

// Baked into recorded workflow histories: a rename must fail HERE, not silently
// pass because every other test references the constant. The `TYPE_*` literals are
// pinned the same way in errors.test.ts.
test('activity-name literals are pinned', () => {
  assert.equal(names.ACTIVITY_READ, 'xmemory_read');
  assert.equal(names.ACTIVITY_WRITE, 'xmemory_write');
  assert.equal(names.ACTIVITY_WRITE_START, 'xmemory_write_start');
  assert.equal(names.ACTIVITY_WRITE_STATUS, 'xmemory_write_status');
});
