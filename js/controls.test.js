import test from 'node:test';
import assert from 'node:assert/strict';

// Minimal DOM/localStorage stubs so the DOM-bound Controls module can be
// unit-tested. Only the pieces Controls touches are implemented.
function makeEl(value = '', hidden = false) {
  const classes = new Set(hidden ? ['hidden'] : []);
  return {
    value,
    classList: {
      contains: (c) => classes.has(c),
      toggle: (c, on) => {
        // Match DOM semantics: `toggle(c, force)` adds when force is truthy,
        // removes when false, and flips when omitted.
        const add = on === undefined ? !classes.has(c) : Boolean(on);
        if (add) classes.add(c);
        else classes.delete(c);
      },
      add: (c) => classes.add(c),
      remove: (c) => classes.delete(c),
    },
    addEventListener: () => {},
  };
}

const els = new Map();
globalThis.document = {
  getElementById: (id) => els.get(id) ?? null,
};
const store = new Map();
globalThis.localStorage = {
  getItem: (k) => (store.has(k) ? store.get(k) : null),
  setItem: (k, v) => store.set(k, String(v)),
  removeItem: (k) => store.delete(k),
};

const { Controls } = await import('./components/controls.js');

function reset({ src = '', advanced = false, savedSrc = null, savedAdvanced = null } = {}) {
  els.clear();
  store.clear();
  els.set('srcInput', makeEl(src));
  els.set('rangeSelect', makeEl('24h'));
  els.set('advancedBar', makeEl('', !advanced));
  if (savedSrc !== null) store.set('dm:baseUrl', savedSrc);
  if (savedAdvanced !== null) store.set('dm:advanced', savedAdvanced);
}

test('getBase ignores the source while Advanced is hidden', () => {
  reset({ src: 'http://localhost:3000', advanced: false });
  assert.equal(Controls.getBase(), '', 'hidden advanced must not override origin');
});

test('getBase honors the source when Advanced is visible', () => {
  reset({ src: 'https://remote.example:3000', advanced: true });
  assert.equal(Controls.getBase(), 'https://remote.example:3000');
});

test('loadPersisted drops a stale source saved before frozen connection', () => {
  reset({ savedSrc: 'http://localhost:3000', savedAdvanced: '0' });
  Controls.loadPersisted();
  assert.equal(store.has('dm:baseUrl'), false, 'stale source removed');
  assert.equal(Controls.getBase(), '', 'connection stays frozen to origin');
});

test('loadPersisted keeps the source when Advanced was explicitly used', () => {
  reset({ savedSrc: 'https://remote.example:3000', savedAdvanced: '1' });
  Controls.loadPersisted();
  assert.equal(store.get('dm:baseUrl'), 'https://remote.example:3000');
  assert.equal(Controls.getBase(), 'https://remote.example:3000');
});
