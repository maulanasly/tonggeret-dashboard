//! Targets & queue client: add/remove scrape targets and control the worker
//! job queue (pause/run) through the proxied control API. Display + form
//! wiring only; the worker owns all state.
//!
//! In split-process mode the dashboard proxies these calls to the worker.
//! When `COLLECTOR_CONTROL_TOKEN` is set on the worker, the token entered
//! here is stored in localStorage and sent as `x-control-token`.

const TARGETS_POLL_MS = 15000;
const TOKEN_KEY = 'dm:controlToken';

export const TargetsClient = {
  baseUrl: '',
  token: '',
  timer: null,
  wired: false,
  /// Latest worker-wide freeze state (from the queue snapshot).
  frozen: false,

  /// Wire the form/buttons once and restore the optional control token.
  init() {
    if (this.wired) return;
    this.wired = true;
    this.token = this.loadToken();
    const tokenEl = document.getElementById('controlToken');
    if (tokenEl) tokenEl.value = this.token;
    document.getElementById('targetAdd')?.addEventListener('click', () => this.addFromForm());
    document.getElementById('queuePause')?.addEventListener('click', () => this.act(() => this.pause()));
    document.getElementById('queueResume')?.addEventListener('click', () => this.act(() => this.resume()));
    document.getElementById('queueClear')?.addEventListener('click', () => this.act(() => this.clearPending()));
    tokenEl?.addEventListener('change', () => {
      this.token = tokenEl.value.trim();
      this.persistToken();
    });
    this.wireList('targetList');
    this.wireList('queueList');
  },

  /// Splitting helper: one URL per line or comma (trimmed, blanks dropped).
  parseUrls(text) {
    return String(text ?? '')
      .split(/[\n,]+/)
      .map((s) => s.trim())
      .filter((s) => s !== '');
  },

  /// Splitting helper: comma/space-separated allow prefixes.
  parseAllow(text) {
    return String(text ?? '')
      .split(/[\s,]+/)
      .map((s) => s.trim())
      .filter((s) => s !== '');
  },

  /// Badge text/status for the current queue snapshot.
  queueBadge(queue) {
    if (!queue) return { text: 'offline', status: 'offline' };
    if (queue.frozen) return { text: `frozen · ${queue.depth} queued`, status: 'frozen' };
    if (queue.paused) return { text: `paused · ${queue.depth} queued`, status: 'degraded' };
    if (queue.depth > 0) return { text: `running · ${queue.depth} queued`, status: 'starting' };
    return { text: 'queue idle', status: 'ok' };
  },

  loadToken() {
    try {
      return localStorage.getItem(TOKEN_KEY) ?? '';
    } catch {
      return '';
    }
  },

  persistToken() {
    try {
      if (this.token) localStorage.setItem(TOKEN_KEY, this.token);
      else localStorage.removeItem(TOKEN_KEY);
    } catch {
      // private mode: ignore
    }
  },

  wireList(id) {
    document.getElementById(id)?.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-action]');
      if (!btn) return;
      const { action, id: rowId, url } = btn.dataset;
      this.act(() => this.dispatch(action, rowId, url));
    });
  },

  async dispatch(action, id, url) {
    if (action === 'enable') return this.setEnabled(id, true);
    if (action === 'disable') return this.setEnabled(id, false);
    if (action === 'remove') return this.remove(id);
    if (action === 'enqueue') return this.enqueue([url]);
    if (action === 'cancel') return this.cancel(id);
    throw new Error(`unknown action ${action}`);
  },

  /// Run a mutation, surface errors, then refresh both lists.
  async act(fn) {
    try {
      await fn();
      this.showError('');
      await this.refresh();
    } catch (err) {
      this.showError(String(err?.message ?? err));
    }
  },

  async api(path, { method = 'GET', body } = {}) {
    if (!this.baseUrl) throw new Error('not connected');
    const headers = {};
    if (body !== undefined) headers['content-type'] = 'application/json';
    if (this.token) headers['x-control-token'] = this.token;
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    let parsed = null;
    try {
      parsed = text === '' ? null : JSON.parse(text);
    } catch {
      parsed = null;
    }
    if (!res.ok) {
      const detail = parsed?.detail ?? parsed?.error ?? `HTTP ${res.status}`;
      throw new Error(detail);
    }
    return parsed;
  },

  async addFromForm() {
    const urls = this.parseUrls(document.getElementById('targetUrls')?.value);
    const mode = document.getElementById('targetMode')?.value ?? 'recurring';
    const name = document.getElementById('targetName')?.value.trim() || undefined;
    const allow = this.parseAllow(document.getElementById('targetAllow')?.value);
    if (urls.length === 0) {
      this.showError('add at least one URL');
      return;
    }
    if (urls.length > 1 && name) {
      this.showError('name only applies to a single URL');
      return;
    }
    await this.act(async () => {
      await this.api('/api/v1/targets', {
        method: 'POST',
        body: { urls, mode, name, allow: allow.length > 0 ? allow : undefined },
      });
      const urlsEl = document.getElementById('targetUrls');
      const nameEl = document.getElementById('targetName');
      if (urlsEl) urlsEl.value = '';
      if (nameEl) nameEl.value = '';
    });
  },

  enqueue(urls) {
    return this.api('/api/v1/queue', { method: 'POST', body: { urls } });
  },

  setEnabled(id, enabled) {
    return this.api(`/api/v1/targets/${encodeURIComponent(id)}/${enabled ? 'enable' : 'disable'}`, {
      method: 'POST',
    });
  },

  remove(id) {
    return this.api(`/api/v1/targets/${encodeURIComponent(id)}`, { method: 'DELETE' });
  },

  cancel(id) {
    return this.api(`/api/v1/queue/${encodeURIComponent(id)}`, { method: 'DELETE' });
  },

  pause() {
    return this.api('/api/v1/queue/pause', { method: 'POST' });
  },

  resume() {
    return this.api('/api/v1/queue/resume', { method: 'POST' });
  },

  /// Worker-wide freeze: halt all scraping (scheduled + manual).
  freezeWorker() {
    return this.api('/api/v1/worker/freeze', { method: 'POST' });
  },

  /// Lift the worker freeze and let pending work drain.
  resumeWorker() {
    return this.api('/api/v1/worker/resume', { method: 'POST' });
  },

  clearPending() {
    return this.api('/api/v1/queue', { method: 'DELETE' });
  },

  /// Start polling `baseUrl` for targets + queue.
  watch(baseUrl) {
    this.init();
    this.stop();
    this.baseUrl = String(baseUrl ?? '').trim().replace(/\/$/, '');
    this.refresh().catch(() => {});
    this.timer = setInterval(() => {
      this.refresh().catch(() => {});
    }, TARGETS_POLL_MS);
  },

  stop() {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  },

  async refresh() {
    if (!this.baseUrl) return;
    const [targets, queue] = await Promise.all([
      this.api('/api/v1/targets').catch(() => null),
      this.api('/api/v1/queue').catch(() => null),
    ]);
    this.frozen = Boolean(queue?.frozen);
    this.renderTargets(targets?.targets ?? null);
    this.renderQueue(queue);
  },

  ageText(ts, nowSecs) {
    if (ts == null || !Number.isFinite(ts)) return 'never';
    const delta = Math.max(0, Math.floor(nowSecs - ts));
    if (delta < 60) return `${delta}s ago`;
    if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
    if (delta < 86400) return `${Math.floor(delta / 3600)}h ago`;
    return `${Math.floor(delta / 86400)}d ago`;
  },

  esc(s) {
    return String(s).replace(
      /[&<>"']/g,
      (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c],
    );
  },

  renderTargets(list) {
    const el = document.getElementById('targetList');
    if (!el) return;
    if (list === null) {
      el.innerHTML = '<span class="status-empty">collector offline — targets unavailable</span>';
      return;
    }
    if (list.length === 0) {
      el.innerHTML = '<span class="status-empty">no targets configured</span>';
      return;
    }
    const esc = (s) => this.esc(s);
    el.innerHTML = list
      .map((t) => {
        const dynamic = t.origin === 'dynamic';
        const buttons =
          `<button class="btn tiny" data-action="enqueue" data-url="${esc(t.url)}" title="Run now">run</button>` +
          (dynamic
            ? `<button class="btn tiny" data-action="${t.enabled ? 'disable' : 'enable'}" data-id="${esc(t.id)}">${t.enabled ? 'disable' : 'enable'}</button>` +
              `<button class="btn tiny danger" data-action="remove" data-id="${esc(t.id)}">remove</button>`
            : '');
        return (
          `<div class="target-row">` +
          `<span class="target-name">${esc(t.name)}</span>` +
          `<span class="target-meta">${esc(t.mode)} · ${esc(t.origin)} · ${t.enabled ? 'enabled' : 'disabled'}</span>` +
          `<span class="target-url" title="${esc(t.url)}">${esc(t.url)}</span>` +
          `<span class="row-actions">${buttons}</span>` +
          `</div>`
        );
      })
      .join('');
  },

  renderQueue(queue) {
    const listEl = document.getElementById('queueList');
    const badge = document.getElementById('queueBadge');
    if (!listEl) return;
    const badgeInfo = this.queueBadge(queue);
    if (badge) {
      badge.textContent = badgeInfo.text;
      badge.dataset.status = badgeInfo.status;
    }
    if (!queue) {
      listEl.innerHTML = '<span class="status-empty">collector offline — queue unavailable</span>';
      return;
    }
    const esc = (s) => this.esc(s);
    const now = Date.now() / 1000;
    const running = queue.running
      ? `<div class="queue-row running"><span class="queue-state">running</span><span class="queue-meta">${esc(queue.running.target)}</span></div>`
      : '';
    const pending = (queue.pending ?? [])
      .map(
        (j) =>
          `<div class="queue-row"><span class="queue-state">queued</span>` +
          `<span class="queue-meta">${esc(j.target)} · ${esc(this.ageText(j.enqueued_at, now))}</span>` +
          `<button class="btn tiny danger" data-action="cancel" data-id="${esc(j.id)}">cancel</button></div>`,
      )
      .join('');
    const recent = (queue.recent ?? [])
      .slice(0, 10)
      .map(
        (j) =>
          `<div class="queue-row done"><span class="queue-state" data-state="${esc(j.state)}">${esc(j.state)}</span>` +
          `<span class="queue-meta">${esc(j.target)}${j.error ? ` · ${esc(j.error)}` : ` · ${j.samples} samples`}</span></div>`,
      )
      .join('');
    listEl.innerHTML =
      `<div class="queue-summary">depth ${queue.depth}/${queue.cap} · done ${queue.done} · failed ${queue.failed}</div>` +
      running +
      pending +
      (recent ? `<div class="subhead">recent</div>${recent}` : '');
  },

  showError(message) {
    const el = document.getElementById('targetsError');
    if (!el) return;
    if (!message) {
      el.classList.add('hidden');
      el.textContent = '';
      return;
    }
    el.classList.remove('hidden');
    el.textContent = message;
  },
};
