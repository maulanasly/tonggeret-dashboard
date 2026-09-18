//! Form controls: data-source input, range dropdown, custom visualizer.
//! Persists source + range to localStorage.

const LS_SOURCE = 'dm:baseUrl';
const LS_RANGE = 'dm:range';

function val(id) {
  return document.getElementById(id)?.value ?? '';
}

export const Controls = {
  onRefresh: null,
  onCustom: null,

  init({ onRefresh, onCustom }) {
    this.onRefresh = onRefresh;
    this.onCustom = onCustom;

    document.getElementById('srcGo')?.addEventListener('click', () => this.handleRefresh(true));
    document.getElementById('srcInput')?.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') this.handleRefresh(true);
    });
    document.getElementById('rangeSelect')?.addEventListener('change', () => this.handleRefresh(false));
    document.getElementById('refreshBtn')?.addEventListener('click', () => this.handleRefresh(false));
    document.getElementById('customRun')?.addEventListener('click', () => onCustom?.());
  },

  loadPersisted() {
    const src = document.getElementById('srcInput');
    const range = document.getElementById('rangeSelect');
    try {
      const s = localStorage.getItem(LS_SOURCE);
      if (s && src) src.value = s;
      const r = localStorage.getItem(LS_RANGE);
      if (r && range && ['1h', '24h', 'all'].includes(r)) range.value = r;
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

  handleRefresh(reconnect) {
    this.persist();
    this.onRefresh?.({ reconnect });
  },

  getSource() {
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
};
