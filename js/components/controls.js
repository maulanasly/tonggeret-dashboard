//! Form controls: range dropdown, worker freeze toggle, advanced data-source
//! override, and the custom visualizer. Persists source/range/advanced to
//! localStorage.
//!
//! The connection is frozen to this page's own collector by default; the
//! source input lives behind the **Advanced** toggle for static hosting that
//! points at a remote Parquet host.

const LS_SOURCE = 'dm:baseUrl';
const LS_RANGE = 'dm:range';
const LS_ADVANCED = 'dm:advanced';

function val(id) {
  return document.getElementById(id)?.value ?? '';
}

export const Controls = {
  onRefresh: null,
  onCustom: null,
  onFreeze: null,

  init({ onRefresh, onCustom, onFreeze }) {
    this.onRefresh = onRefresh;
    this.onCustom = onCustom;
    this.onFreeze = onFreeze;

    document.getElementById('srcGo')?.addEventListener('click', () => this.handleRefresh(true));
    document.getElementById('srcInput')?.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') this.handleRefresh(true);
    });
    document.getElementById('rangeSelect')?.addEventListener('change', () => this.handleRefresh(false));
    document.getElementById('refreshBtn')?.addEventListener('click', () => this.handleRefresh(false));
    document.getElementById('customRun')?.addEventListener('click', () => onCustom?.());
    document.getElementById('freezeBtn')?.addEventListener('click', () => onFreeze?.());
    document.getElementById('advancedBtn')?.addEventListener('click', () => this.toggleAdvanced());
  },

  loadPersisted() {
    const src = document.getElementById('srcInput');
    const range = document.getElementById('rangeSelect');
    try {
      const advanced = localStorage.getItem(LS_ADVANCED) === '1';
      // Migration: a source saved before the frozen-connection change (the
      // old `http://localhost:3000` default) must not hijack the connection.
      // Drop it unless the user is explicitly in Advanced mode.
      if (!advanced) localStorage.removeItem(LS_SOURCE);
      const s = localStorage.getItem(LS_SOURCE);
      if (s && src) src.value = s;
      const r = localStorage.getItem(LS_RANGE);
      if (r && range && ['1h', '24h', 'all'].includes(r)) range.value = r;
      if (advanced) this.toggleAdvanced(true);
    } catch {
      // private mode: ignore
    }
  },

  persist() {
    try {
      localStorage.setItem(LS_SOURCE, val('srcInput').trim());
      localStorage.setItem(LS_RANGE, val('rangeSelect'));
    } catch {
      // ignore
    }
  },

  /// Show/hide the advanced source bar (persisted). `force` overrides.
  toggleAdvanced(force) {
    const bar = document.getElementById('advancedBar');
    if (!bar) return;
    const show = force === undefined ? bar.classList.contains('hidden') : force;
    bar.classList.toggle('hidden', !show);
    try {
      localStorage.setItem(LS_ADVANCED, show ? '1' : '0');
    } catch {
      // ignore
    }
  },

  advancedVisible() {
    return !document.getElementById('advancedBar')?.classList.contains('hidden');
  },

  handleRefresh(reconnect) {
    if (reconnect) this.persist();
    this.onRefresh?.({ reconnect });
  },

  /// Advanced source override. Empty string means "this page's own origin".
  ///
  /// The override only applies while the Advanced bar is visible: a stale
  /// `dm:baseUrl` from before the frozen-connection change (e.g. the old
  /// `http://localhost:3000` default) must not silently redirect the
  /// dashboard away from its own collector.
  getBase() {
    if (!this.advancedVisible()) return '';
    return val('srcInput').trim();
  },

  getRange() {
    const r = val('rangeSelect');
    return ['1h', '24h', 'all'].includes(r) ? r : '24h';
  },

  getCustom() {
    return {
      metric: val('metricSelect').trim() || 'http_request_duration_ms',
      agg: val('aggSelect').trim() || 'avg',
      labelKey: val('labelKey').trim(),
      labelValue: val('labelValue').trim(),
      range: this.getRange(),
    };
  },

  setNames(names) {
    const sel = document.getElementById('metricSelect');
    if (!sel) return;
    const prev = sel.value;
    sel.innerHTML = '';
    const list = names.length > 0 ? names : ['http_requests_total', 'http_request_duration_ms'];
    for (const n of list) {
      const opt = document.createElement('option');
      opt.value = n;
      opt.textContent = n;
      sel.appendChild(opt);
    }
    if (list.includes(prev)) sel.value = prev;
    else if (list.includes('http_request_duration_ms')) sel.value = 'http_request_duration_ms';
  },

  setSql(text) {
    const el = document.getElementById('sqlPreview');
    if (el) el.textContent = text;
  },

  /// Reflect the worker freeze state on the top toggle.
  setFreeze(frozen) {
    const btn = document.getElementById('freezeBtn');
    const label = document.getElementById('freezeLabel');
    if (btn) {
      btn.dataset.state = frozen ? 'frozen' : 'running';
      btn.title = frozen ? 'Unfreeze the worker' : 'Freeze the worker (stop all scraping)';
    }
    if (label) label.textContent = frozen ? 'Unfreeze' : 'Freeze';
  },
};
