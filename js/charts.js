//! ECharts wrappers: dark-theme time-series + error charts with graceful
//! empty/loading states. Expects `window.echarts` from the CDN script tag.
//! uPlot can replace these renderers later without touching callers.

const AXIS = '#8b93a7';
const SPLIT = 'rgba(148, 163, 184, 0.14)';
const ACCENT = '#38bdf8';
const ACCENT2 = '#a78bfa';
const WARN = '#fbbf24';
const BAD = '#f87171';
const FONT = 'Inter, system-ui, -apple-system, sans-serif';

function baseOption() {
  return {
    backgroundColor: 'transparent',
    textStyle: { fontFamily: FONT, color: '#e2e8f0' },
    animationDuration: 300,
  };
}

function emptyOption(message) {
  return {
    ...baseOption(),
    graphic: {
      elements: [
        {
          type: 'text',
          left: 'center',
          top: 'middle',
          style: { text: message, fill: '#64748b', font: `14px ${FONT}` },
        },
      ],
    },
    xAxis: { show: false },
    yAxis: { show: false },
    series: [],
  };
}

function tooltipBase(extra) {
  return {
    trigger: 'axis',
    backgroundColor: '#0f172a',
    borderColor: '#334155',
    textStyle: { color: '#e2e8f0', fontSize: 12 },
    ...(extra ?? {}),
  };
}

const msOf = (bucketUs) => Math.floor(Number(bucketUs) / 1000);

function available() {
  return typeof window !== 'undefined' && typeof window.echarts !== 'undefined';
}

function getChart(el) {
  if (!available()) {
    el.innerHTML = '<div class="chart-fallback">chart library (CDN) failed to load</div>';
    return null;
  }
  const existing = window.echarts.getInstanceByDom(el);
  if (existing) return existing;
  return window.echarts.init(el, null, { renderer: 'canvas' });
}

export const Charts = {
  _els: {},

  init() {
    if (!available()) return false;
    for (const id of ['chartThroughput', 'chartLatency', 'chartErrors', 'chartVisitors', 'chartCustom']) {
      const el = document.getElementById(id);
      if (el) {
        this._els[id] = getChart(el);
        this._els[id]?.setOption(emptyOption('no data yet — pick a source and refresh'));
      }
    }
    window.addEventListener('resize', () => {
      for (const c of Object.values(this._els)) c?.resize();
    });
    return true;
  },

  renderThroughput(rows) {
    const chart = this._els.chartThroughput;
    if (!chart) return;
    if (rows.length === 0) {
      chart.setOption(emptyOption('no request samples in range'), true);
      return;
    }
    const data = rows.map((r) => [msOf(r.bucket_us), r.reqs]);
    chart.setOption(
      {
        ...baseOption(),
        tooltip: tooltipBase(),
        grid: { left: 56, right: 16, top: 28, bottom: 30 },
        xAxis: {
          type: 'time',
          axisLine: { lineStyle: { color: SPLIT } },
          axisLabel: { color: AXIS },
        },
        yAxis: {
          type: 'value',
          name: 'reqs/bucket',
          nameTextStyle: { color: AXIS },
          splitLine: { lineStyle: { color: SPLIT } },
          axisLabel: { color: AXIS },
        },
        series: [
          {
            name: 'requests',
            type: 'line',
            showSymbol: false,
            sampling: 'lttb',
            smooth: 0.15,
            data,
            lineStyle: { width: 2, color: ACCENT },
            areaStyle: { color: 'rgba(56, 189, 248, 0.12)' },
          },
        ],
      },
      true,
    );
  },

  renderLatency(rows) {
    const chart = this._els.chartLatency;
    if (!chart) return;
    if (rows.length === 0 || rows.every((r) => r.avg_ms == null)) {
      chart.setOption(emptyOption('no latency samples in range'), true);
      return;
    }
    const avg = rows.filter((r) => r.avg_ms != null).map((r) => [msOf(r.bucket_us), +r.avg_ms.toFixed(2)]);
    const p99 = rows.filter((r) => r.p99_ms != null).map((r) => [msOf(r.bucket_us), +r.p99_ms.toFixed(2)]);
    chart.setOption(
      {
        ...baseOption(),
        tooltip: tooltipBase({ valueFormatter: (v) => `${v} ms` }),
        legend: { textStyle: { color: AXIS }, top: 0 },
        grid: { left: 56, right: 16, top: 36, bottom: 30 },
        xAxis: { type: 'time', axisLabel: { color: AXIS } },
        yAxis: {
          type: 'value',
          name: 'ms',
          nameTextStyle: { color: AXIS },
          splitLine: { lineStyle: { color: SPLIT } },
          axisLabel: { color: AXIS },
        },
        series: [
          {
            name: 'avg latency',
            type: 'line',
            showSymbol: false,
            sampling: 'lttb',
            smooth: 0.15,
            data: avg,
            lineStyle: { width: 2, color: ACCENT2 },
          },
          {
            name: 'p99',
            type: 'line',
            showSymbol: false,
            sampling: 'lttb',
            data: p99,
            lineStyle: { width: 1.5, type: 'dashed', color: WARN },
          },
        ],
      },
      true,
    );
  },

  renderErrors(rows) {
    const chart = this._els.chartErrors;
    if (!chart) return;
    if (rows.length === 0) {
      chart.setOption(emptyOption('no 4xx/5xx in range — healthy'), true);
      return;
    }
    const paths = rows.map((r) => r.path);
    chart.setOption(
      {
        ...baseOption(),
        tooltip: tooltipBase({ trigger: 'axis', axisPointer: { type: 'shadow' } }),
        legend: { textStyle: { color: AXIS }, top: 0 },
        grid: { left: 8, right: 48, top: 36, bottom: 8, containLabel: true },
        xAxis: {
          type: 'value',
          splitLine: { lineStyle: { color: SPLIT } },
          axisLabel: { color: AXIS },
        },
        yAxis: { type: 'category', data: paths, axisLabel: { color: AXIS } },
        series: [
          {
            name: '4xx',
            type: 'bar',
            stack: 'err',
            data: rows.map((r) => r.c4xx),
            itemStyle: { color: WARN },
            label: { show: false },
          },
          {
            name: '5xx',
            type: 'bar',
            stack: 'err',
            data: rows.map((r) => r.c5xx),
            itemStyle: { color: BAD },
          },
        ],
      },
      true,
    );
  },

  renderVisitors(rows) {
    const chart = this._els.chartVisitors;
    if (!chart) return;
    const present = rows.filter((r) => r.visitors != null || r.uniques != null);
    if (present.length === 0) {
      chart.setOption(emptyOption('no visitor metrics in range — this app has no visitor instrumentation'), true);
      return;
    }
    const groups = new Map();
    for (const r of present) {
      const key = `${r.target} / ${r.region}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(r);
    }
    const palette = [ACCENT, ACCENT2, '#34d399', WARN, BAD];
    let i = 0;
    const series = [];
    for (const [key, rs] of groups) {
      const color = palette[i++ % palette.length];
      series.push({
        name: `${key} uniques`,
        type: 'line',
        showSymbol: false,
        sampling: 'lttb',
        smooth: 0.15,
        data: rs.filter((r) => r.uniques != null).map((r) => [msOf(r.bucket_us), r.uniques]),
        lineStyle: { width: 2, color },
      });
      series.push({
        name: `${key} total`,
        type: 'line',
        showSymbol: false,
        sampling: 'lttb',
        smooth: 0.15,
        data: rs.filter((r) => r.visitors != null).map((r) => [msOf(r.bucket_us), r.visitors]),
        lineStyle: { width: 1.5, type: 'dashed', color },
      });
    }
    chart.setOption(
      {
        ...baseOption(),
        tooltip: tooltipBase(),
        legend: { textStyle: { color: AXIS }, top: 0 },
        grid: { left: 56, right: 16, top: 36, bottom: 30 },
        xAxis: { type: 'time', axisLabel: { color: AXIS } },
        yAxis: {
          type: 'value',
          splitLine: { lineStyle: { color: SPLIT } },
          axisLabel: { color: AXIS },
        },
        series,
      },
      true,
    );
  },

  renderCustom(rows, metric, agg) {
    const chart = this._els.chartCustom;
    if (!chart) return;
    if (rows.length === 0) {
      chart.setOption(emptyOption(`no samples for ${metric}`), true);
      return;
    }
    const data = rows.map((r) => [msOf(r.bucket_us), r.v]);
    chart.setOption(
      {
        ...baseOption(),
        tooltip: tooltipBase(),
        grid: { left: 56, right: 16, top: 28, bottom: 30 },
        xAxis: { type: 'time', axisLabel: { color: AXIS } },
        yAxis: {
          type: 'value',
          splitLine: { lineStyle: { color: SPLIT } },
          axisLabel: { color: AXIS },
        },
        series: [
          {
            name: `${metric} (${agg})`,
            type: 'line',
            showSymbol: false,
            sampling: 'lttb',
            smooth: 0.15,
            data,
            lineStyle: { width: 2, color: '#34d399' },
            areaStyle: { color: 'rgba(52, 211, 153, 0.10)' },
          },
        ],
      },
      true,
    );
  },
};
