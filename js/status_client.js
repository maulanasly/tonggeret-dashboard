//! Collector status client: poll `GET /api/v1/status` and render a compact
//! worker/target health panel. Display-only, like the rest of the dashboard:
//! no math beyond formatting, no server query code.
//!
//! In split-process mode the dashboard proxies this endpoint, so the same
//! URL works when the UI is served by the worker (single-binary) or by the
//! read-only `serve` role. An unreachable worker yields an `offline` status.

const STATUS_POLL_MS = 15000;

export const StatusClient = {
  baseUrl: '',
  timer: null,
  /// Optional callback invoked with each normalized snapshot (e.g. to sync
  /// the freeze toggle state).
  onSnapshot: null,

  /// Normalize either a worker snapshot or an offline body into one shape.
  normalize(raw) {
    const targets = Array.isArray(raw?.targets) ? raw.targets : [];
    return {
      status: typeof raw?.status === 'string' ? raw.status : 'offline',
      frozen: Boolean(raw?.queue?.frozen),
      uptimeSecs: Number(raw?.uptime_secs ?? 0),
      intervalSecs: Number(raw?.interval_secs ?? 0),
      bufferSamples: Number(raw?.buffer?.samples ?? 0),
      bufferCap: Number(raw?.buffer?.cap ?? 0),
      coldFiles: Number(raw?.cold_files ?? 0),
      error: typeof raw?.error === 'string' ? raw.error : null,
      targets: targets.map((t) => ({
        name: String(t?.name ?? '(unknown)'),
        url: String(t?.url ?? ''),
        lastStatus: t?.last_status == null ? null : String(t.last_status),
        lastScrapeTs: t?.last_scrape_ts == null ? null : Number(t.last_scrape_ts),
        lastSamples: Number(t?.last_samples ?? 0),
        lastDurationMs: Number(t?.last_duration_ms ?? 0),
        consecutiveFailures: Number(t?.consecutive_failures ?? 0),
        totals: {
          ok: Number(t?.totals?.ok ?? 0),
          fetchError: Number(t?.totals?.fetch_error ?? 0),
          parseError: Number(t?.totals?.parse_error ?? 0),
        },
      })),
    };
  },

  /// Human "time since" text for a unix-seconds timestamp.
  ageText(ts, nowSecs) {
    if (ts == null || !Number.isFinite(ts)) return 'never';
    const delta = Math.max(0, Math.floor(nowSecs - ts));
    if (delta < 60) return `${delta}s ago`;
    if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
    if (delta < 86400) return `${Math.floor(delta / 3600)}h ago`;
    return `${Math.floor(delta / 86400)}d ago`;
  },

  /// Compact uptime text for the summary line.
  uptimeText(secs) {
    if (!Number.isFinite(secs) || secs <= 0) return '0s';
    if (secs < 3600) return `${Math.floor(secs / 60)}m`;
    if (secs < 86400) return `${Math.floor(secs / 3600)}h`;
    return `${Math.floor(secs / 86400)}d`;
  },

  /// Fetch `/api/v1/status`; never throws — failures become `offline`.
  async fetchSnapshot(baseUrl) {
    const base = String(baseUrl ?? '').trim().replace(/\/$/, '');
    if (base === '') return { status: 'offline', error: 'no source' };
    try {
      const res = await fetch(`${base}/api/v1/status`);
      if (!res.ok) return { status: 'offline', error: `HTTP ${res.status}` };
      return await res.json();
    } catch (err) {
      return { status: 'offline', error: String(err?.message ?? err) };
    }
  },

  /// Poll `baseUrl` immediately and every 15s; render into the panel.
  watch(baseUrl) {
    this.stop();
    this.baseUrl = String(baseUrl ?? '').trim().replace(/\/$/, '');
    const tick = async () => {
      const snapshot = this.normalize(await this.fetchSnapshot(this.baseUrl));
      this.onSnapshot?.(snapshot);
      this.render(snapshot);
    };
    tick();
    this.timer = setInterval(tick, STATUS_POLL_MS);
  },

  stop() {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  },

  /// Fetch + render one snapshot immediately (e.g. after freeze/resume).
  async refreshNow() {
    const snapshot = this.normalize(await this.fetchSnapshot(this.baseUrl));
    this.onSnapshot?.(snapshot);
    this.render(snapshot);
  },

  /// Render a normalized snapshot into `#collectorBadge` + `#collectorStatus`
  /// and the top `#connChip`.
  render(snapshot) {
    const badge = document.getElementById('collectorBadge');
    const body = document.getElementById('collectorStatus');
    if (!body) return;
    if (badge) {
      badge.textContent = snapshot.status;
      badge.dataset.status = snapshot.status;
    }
    // Top connection chip: offline > frozen > overall status.
    const chip = document.getElementById('connChip');
    const chipText = document.getElementById('connChipText');
    const chipStatus =
      snapshot.status === 'offline' ? 'offline' : snapshot.frozen ? 'frozen' : snapshot.status;
    if (chip) chip.dataset.status = chipStatus;
    if (chipText) chipText.textContent = chipStatus;
    const esc = (s) =>
      String(s).replace(
        /[&<>"']/g,
        (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c],
      );
    if (snapshot.status === 'offline') {
      body.innerHTML = '';
      const msg = document.createElement('span');
      msg.className = 'status-empty';
      msg.textContent = `collector offline${snapshot.error ? `: ${snapshot.error}` : ''}`;
      body.appendChild(msg);
      return;
    }
    const now = Date.now() / 1000;
    const meta =
      `uptime ${this.uptimeText(snapshot.uptimeSecs)} · interval ${snapshot.intervalSecs}s · ` +
      `buffer ${snapshot.bufferSamples}/${snapshot.bufferCap} · cold ${snapshot.coldFiles} file(s)`;
    const rows = snapshot.targets
      .map((t) => {
        const state = t.consecutiveFailures > 0 ? 'bad' : (t.lastStatus ?? 'idle');
        const errs = t.totals.fetchError + t.totals.parseError;
        const detail =
          `${this.ageText(t.lastScrapeTs, now)} · ${t.lastSamples} samples · ` +
          `${t.lastDurationMs}ms · ok ${t.totals.ok} / err ${errs}`;
        return (
          `<div class="status-row">` +
          `<span class="status-name">${esc(t.name)}</span>` +
          `<span class="status-state" data-state="${esc(state)}">${esc(state)}</span>` +
          `<span class="status-detail">${esc(detail)}</span>` +
          `</div>`
        );
      })
      .join('');
    body.innerHTML =
      `<div class="status-meta">${esc(meta)}</div>` +
      (rows || '<span class="status-empty">no targets configured</span>');
  },
};
