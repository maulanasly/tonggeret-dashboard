//! Summary cards: Total Requests, Avg Latency, Peak Throughput, Error Rate.

const fmtInt = new Intl.NumberFormat('en-US');
const fmtPct = new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 2 });

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

export const Cards = {
  render(summary, mode) {
    setText('statTotal', fmtInt.format(summary.total));
    setText('statAvg', summary.avgMs == null ? '—' : `${summary.avgMs.toFixed(1)} ms`);
    setText(
      'statPeak',
      summary.peakPerSec >= 10
        ? `${summary.peakPerSec.toFixed(0)} rps`
        : `${summary.peakPerSec.toFixed(2)} rps`,
    );
    setText(
      'statErr',
      summary.total === 0 ? '—' : `${fmtPct.format(summary.errorRate)} (${fmtInt.format(summary.errorCount)})`,
    );
    setText('modeBadge', mode === 'unknown' ? 'not connected' : `via ${mode}`);
  },

  reset() {
    this.render({ total: 0, avgMs: null, peakPerSec: 0, errorRate: 0, errorCount: 0 }, 'unknown');
  },
};
