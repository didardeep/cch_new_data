import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import {
  ArrowDownToLine, ArrowUpFromLine, Cpu,
  Database, RefreshCw, ShieldCheck,
  AlertTriangle, TrendingUp, TrendingDown, Wifi, Signal, Zap,
} from 'lucide-react';
import {
  Area, AreaChart, CartesianGrid, ReferenceLine, ReferenceArea,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiGet } from '../../api';

/* ═══════════════════════════════════════════════════════════
   DESIGN TOKENS
   ═══════════════════════════════════════════════════════════ */
const CFG = {
  accessibility:       { colors: ['#00338D', '#4F46E5'], icon: ShieldCheck,     max: 100, unit: '%', label: 'Higher = better' },
  retainability:       { colors: ['#4F46E5', '#8B5CF6'], icon: RefreshCw,       max: 100, unit: '%', label: 'Network retain rate' },
  downlink_throughput: { colors: ['#06B6D4', '#00338D'], icon: ArrowDownToLine, max: 100, unit: '',  label: 'DL cell average' },
  prb_utilization:     { colors: ['#0d9488', '#06B6D4'], icon: Cpu,             max: 100, unit: '%', label: 'Radio resource load' },
  downlink_volume:     { colors: ['#00338D', '#8B5CF6'], icon: Database,        max: null,unit: '',  label: 'DL data volume' },
  uplink_volume:       { colors: ['#4F46E5', '#06B6D4'], icon: ArrowUpFromLine, max: null,unit: '',  label: 'UL data volume' },
};
const FALLBACK = { colors: ['#00338D', '#4F46E5'], icon: ShieldCheck, max: 100, unit: '', label: '' };

const lin = ([c1, c2], deg = 135) => `linear-gradient(${deg}deg, ${c1}, ${c2})`;

const glass = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border)',
  borderRadius: 16,
  boxShadow: 'var(--shadow-md), inset 0 1px 0 rgba(255,255,255,0.45)',
};

const FADE = (delay = 0) => ({
  initial: { opacity: 0, y: 20 },
  animate: { opacity: 1, y: 0 },
  transition: { duration: 0.4, delay, ease: [0.22, 1, 0.36, 1] },
});

/* ─── Circular SVG Gauge ──────────────────────────────────── */
function CircleGauge({ value, max = 100, color, size = 60 }) {
  const r = 24, cx = 30, cy = 30;
  const circ = 2 * Math.PI * r;   // 150.8
  const pct  = Math.min(1, Math.max(0, value / max));
  const offset = circ * (1 - pct);
  return (
    <div style={{ position: 'relative', width: size, height: size, flexShrink: 0 }}>
      <svg viewBox="0 0 60 60" style={{ transform: 'rotate(-90deg)', width: '100%', height: '100%' }}>
        <circle cx={cx} cy={cy} r={r} fill="none" stroke="var(--border)" strokeWidth={4.5} />
        <circle cx={cx} cy={cy} r={r} fill="none"
          stroke={color} strokeWidth={4.5}
          strokeDasharray={circ} strokeDashoffset={offset}
          strokeLinecap="round"
          style={{ transition: 'stroke-dashoffset 1s ease', filter: `drop-shadow(0 0 4px ${color}60)` }}
        />
      </svg>
    </div>
  );
}

/* ─── Mini sparkline bars ─────────────────────────────────── */
function Sparkline({ series = [], color }) {
  const items = series.slice(-8);
  const mx = Math.max(...items.map(d => d.value || 0), 1);
  return (
    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 3, height: 36, flex: 1 }}>
      {items.map((d, i) => {
        const v = d.value || 0;
        return (
          <div key={i} title={`${d.date || ''}: ${v}`} style={{
            flex: 1, borderRadius: '3px 3px 0 0',
            background: color,
            opacity: 0.3 + (v / mx) * 0.55,
            height: `${Math.max(15, (v / mx) * 100)}%`,
            transition: 'height 0.5s ease',
            cursor: 'pointer',
          }} />
        );
      })}
    </div>
  );
}

/* ─── Custom Tooltip ──────────────────────────────────────── */
function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  const color = payload[0]?.stroke || '#6366f1';
  return (
    <div style={{ ...glass, padding: '10px 14px', fontSize: 12, minWidth: 130 }}>
      <p style={{ color: 'var(--text-muted)', marginBottom: 8, fontWeight: 700, fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{label}</p>
      <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
        <span style={{ width: 8, height: 8, borderRadius: '50%', background: color, boxShadow: `0 0 6px ${color}70` }} />
        <span style={{ color: 'var(--text)', fontWeight: 800 }}>
          {(payload[0]?.value ?? 0).toLocaleString()}
        </span>
      </div>
    </div>
  );
}

/* ─── Progress bar ────────────────────────────────────────── */
function GradBar({ value, max = 100, colors }) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  return (
    <div title={`${value} / ${max}`} style={{ background: 'var(--border)', borderRadius: 6, height: 5, width: '100%', overflow: 'hidden', marginTop: 8, cursor: 'pointer' }}>
      <div style={{
        width: `${pct}%`, height: '100%',
        background: lin(colors, 90),
        borderRadius: 6, transition: 'width 0.8s ease',
        boxShadow: `0 0 6px ${colors[0]}50`,
      }} />
    </div>
  );
}

/* ─── Heatmap grid (for DL/UL volume visual) ──────────────── */
function HeatGrid({ series = [], color }) {
  const cells = series.slice(-16);
  const vals = cells.map(d => d?.value || 0);
  const mn = Math.min(...vals);
  const mx = Math.max(...vals);
  const range = mx - mn || 1;
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 8, color: 'var(--text-muted)', marginBottom: 3 }}>
        <span>Min: <b style={{ color }}>{mn.toFixed(2)}</b></span>
        <span>Max: <b style={{ color }}>{mx.toFixed(2)}</b></span>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(8, 1fr)', gap: 3, height: 60 }}>
        {Array.from({ length: 16 }).map((_, i) => {
          const v = cells[i]?.value || 0;
          const d = cells[i]?.date || '';
          const norm = (v - mn) / range;
          return (
            <div key={i} title={`${d}: ${v.toLocaleString()}`} style={{
              borderRadius: 3,
              background: color,
              opacity: 0.15 + norm * 0.85,
              transition: 'opacity 0.4s',
              cursor: 'pointer',
            }} />
          );
        })}
      </div>
    </div>
  );
}

/* ─── Donut gauge ─────────────────────────────────────────── */
function Donut({ value, max = 100, colors, label }) {
  const r = 46, cx = 56, cy = 56;
  const circ = 2 * Math.PI * r;
  const pct  = Math.min(1, Math.max(0, value / max));
  const offset = circ * (1 - pct);
  return (
    <div style={{ position: 'relative', width: 112, height: 112, flexShrink: 0 }}>
      <svg viewBox="0 0 112 112" style={{ transform: 'rotate(-90deg)', width: '100%', height: '100%' }}>
        <defs>
          <linearGradient id="donutGrad" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor={colors[0]} />
            <stop offset="100%" stopColor={colors[1]} />
          </linearGradient>
        </defs>
        <circle cx={cx} cy={cy} r={r} fill="none" stroke="var(--border)" strokeWidth={10} />
        <circle cx={cx} cy={cy} r={r} fill="none"
          stroke="url(#donutGrad)" strokeWidth={10}
          strokeDasharray={circ} strokeDashoffset={offset}
          strokeLinecap="round"
          style={{ filter: `drop-shadow(0 0 6px ${colors[0]}50)`, transition: 'stroke-dashoffset 1s ease' }}
        />
      </svg>
      <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
        <span style={{ fontSize: 18, fontWeight: 900, background: lin(colors), WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text' }}>
          {Math.round(value)}%
        </span>
        <span style={{ fontSize: 8, color: 'var(--text-muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{label}</span>
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════
   MAIN COMPONENT
   ═══════════════════════════════════════════════════════════ */
export default function TechnicalKPI() {
  const [tab, setTab] = useState('RAN');
  const [data, setData] = useState(null);
  const [coreData, setCoreData] = useState(null);
  const [coreLoading, setCoreLoading] = useState(false);
  const [coreFetched, setCoreFetched] = useState(false);
  const [siteSearch, setSiteSearch] = useState('');

  useEffect(() => {
    apiGet('/api/cto/technical-kpi').then(setData);
  }, []);

  useEffect(() => {
    if (tab === 'CORE' && !coreFetched && !coreLoading) {
      setCoreLoading(true);
      setCoreFetched(true);
      setCoreData(null);
      apiGet('/api/cto/core-kpi')
        .then(d => setCoreData(d))
        .catch(() => setCoreData({ available: false, error: true }))
        .finally(() => setCoreLoading(false));
    }
  }, [tab, coreFetched, coreLoading]);

  if (!data) return <div className="page-loader"><div className="spinner" /></div>;

  /* normalise data */
  const byKey = {};
  (data.cards || []).forEach(c => { byKey[c.key] = c; });
  const ser = data.series || {};

  const acc  = byKey.accessibility       || { key: 'accessibility',       value: 0, label: 'Accessibility' };
  const ret  = byKey.retainability       || { key: 'retainability',       value: 0, label: 'Retainability' };
  const thr  = byKey.downlink_throughput || { key: 'downlink_throughput', value: 0, label: 'DL Throughput' };
  const prb  = byKey.prb_utilization     || { key: 'prb_utilization',     value: 0, label: 'PRB Utilization' };
  const dlv  = byKey.downlink_volume     || { key: 'downlink_volume',     value: 0, label: 'DL Volume' };
  const ulv  = byKey.uplink_volume       || { key: 'uplink_volume',       value: 0, label: 'UL Volume' };

  const accCfg = CFG.accessibility;
  const retCfg = CFG.retainability;
  const thrCfg = CFG.downlink_throughput;
  const prbCfg = CFG.prb_utilization;
  const dlvCfg = CFG.downlink_volume;
  const ulvCfg = CFG.uplink_volume;

  /* series for main trend chart — use accessibility */
  const trendSeries = ser.accessibility || [];

  /* throughput week-over-week delta */
  const thrSlice  = ser.downlink_throughput || [];
  const thisWeek  = thrSlice.slice(-7).map(d => d.value);
  const lastWeek  = thrSlice.slice(-14, -7).map(d => d.value);
  const _avg = arr => arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : null;
  const twAvg = _avg(thisWeek);
  const lwAvg = _avg(lastWeek);
  const lwDelta = (twAvg != null && lwAvg != null && lwAvg !== 0)
    ? ((twAvg - lwAvg) / lwAvg * 100).toFixed(1)
    : null;

  /* gradient def component for recharts */
  const GradDefs = () => (
    <defs>
      <linearGradient id="trendGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"  stopColor="#10b981" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#10b981" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="cssrGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"  stopColor="#3b82f6" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="erabGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"  stopColor="#ef4444" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#ef4444" stopOpacity={0.02} />
      </linearGradient>
    </defs>
  );

  /* ─── KPI Thresholds for breach detection ────────────────── */
  /* Since backend now returns accessibility = CSSR only and
     retainability = E-RAB Drop Rate only, the trend series
     directly represent those individual KPIs.                  */
  const retainSeries = ser.retainability || [];

  const THRESHOLDS = {
    accessibility:       { value: 95,  direction: 'min', label: 'Min 95%', color: '#ef4444' },
    e_rab_drop:          { value: 1.0, direction: 'max', label: 'Max 1%',  color: '#ef4444' },
    prb_utilization:     { value: 70,  direction: 'max', label: 'Max 70%', color: '#f59e0b' },
    downlink_throughput: { value: 5,   direction: 'min', label: 'Min 5',   color: '#ef4444' },
  };

  /* Find breach regions in a series */
  function findBreachRegions(series, threshold, direction) {
    const regions = [];
    let start = null;
    for (let i = 0; i < series.length; i++) {
      const v = series[i].value;
      const breached = direction === 'min' ? v < threshold : v > threshold;
      if (breached && start === null) {
        start = i;
      } else if (!breached && start !== null) {
        regions.push({ x1: series[start].date, x2: series[i - 1].date });
        start = null;
      }
    }
    if (start !== null) {
      regions.push({ x1: series[start].date, x2: series[series.length - 1].date });
    }
    return regions;
  }

  /* Generate insight text for a KPI */
  function generateInsight(series, name, threshold, direction) {
    if (!series.length) return null;
    const latest = series[series.length - 1].value;
    const breached = direction === 'min' ? latest < threshold : latest > threshold;
    const breachCount = series.filter(p => direction === 'min' ? p.value < threshold : p.value > threshold).length;
    const pct = Math.round((breachCount / series.length) * 100);
    if (breachCount === 0) return { text: `${name}: All values within threshold. Network performance is healthy.`, severity: 'good' };
    if (breached) return { text: `${name}: Currently breaching threshold (${latest}). ${pct}% of data points exceeded limits. Immediate attention needed.`, severity: 'critical' };
    return { text: `${name}: ${breachCount} breach(es) detected (${pct}% of period). Currently recovered to ${latest}. Monitor closely.`, severity: 'warning' };
  }

  const accBreaches = findBreachRegions(trendSeries, THRESHOLDS.accessibility.value, 'min');
  const erabBreaches = findBreachRegions(retainSeries, THRESHOLDS.e_rab_drop.value, 'max');

  const accInsight = generateInsight(trendSeries, 'Call Setup Success Rate', THRESHOLDS.accessibility.value, 'min');
  const erabInsight = generateInsight(retainSeries, 'E-RAB Drop Rate', THRESHOLDS.e_rab_drop.value, 'max');

  return (
    <div style={{ paddingBottom: 40 }}>

      {/* ── Page Header ─────────────────────────────────────── */}
      <motion.div {...FADE(0)} style={{ display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', marginBottom: 28, flexWrap: 'wrap', gap: 12 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 26, fontWeight: 900, letterSpacing: '-0.02em', color: 'var(--text)' }}>Technical KPI</h1>
          <p style={{ margin: '5px 0 0', fontSize: 13, color: 'var(--text-muted)' }}>
            Live network performance — accessibility, retainability, throughput, utilization &amp; traffic
          </p>
        </div>
        <div style={{ display: 'flex', gap: 6, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 999, padding: 4 }}>
          {['RAN', 'CORE'].map(t => (
            <button key={t} onClick={() => setTab(t)} style={{
              padding: '6px 20px', borderRadius: 999, border: 'none', cursor: 'pointer',
              fontSize: 12, fontWeight: 700, transition: 'all 0.2s',
              background: tab === t ? 'var(--bg-card)' : 'transparent',
              color: tab === t ? 'var(--text)' : 'var(--text-muted)',
              boxShadow: tab === t ? 'var(--shadow-sm)' : 'none',
            }}>{t}</button>
          ))}
        </div>
      </motion.div>

      {/* ══════════════════════════════════════════════════════
          CORE TAB
          ══════════════════════════════════════════════════════ */}
      {tab === 'CORE' && (
        <CoreTab data={coreData} loading={coreLoading} onRetry={() => { setCoreFetched(false); }} />
      )}

      {/* ══════════════════════════════════════════════════════
          BENTO GRID — top KPI cards (RAN only)
          ══════════════════════════════════════════════════════ */}
      {tab === 'RAN' && <>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 20 }}>

        {/* ── Hero card: Accessibility (col-span-2) ─────────── */}
        <motion.div {...FADE(0.05)} whileHover={{ scale: 1.015, transition: { duration: 0.2 } }}
          style={{ ...glass, gridColumn: 'span 2', overflow: 'hidden', borderLeft: `4px solid ${accCfg.colors[0]}` }}>
          {/* faint watermark icon */}
          <div style={{ position: 'absolute', top: -4, right: 8, opacity: 0.04, pointerEvents: 'none', fontSize: 120, lineHeight: 1 }}>
            <ShieldCheck size={120} />
          </div>
          <div style={{ padding: '20px 24px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: `${accCfg.colors[0]}18`, borderRadius: 8, padding: 8, boxShadow: `0 0 10px ${accCfg.colors[0]}28` }}>
                  <ShieldCheck size={16} color={accCfg.colors[0]} strokeWidth={2.2} />
                </div>
                <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-muted)' }}>
                  {acc.label}
                </span>
              </div>
              <span style={{
                fontSize: 10, fontWeight: 800, padding: '3px 8px', borderRadius: 999,
                background: `${accCfg.colors[0]}18`, color: accCfg.colors[0],
                border: `1px solid ${accCfg.colors[0]}28`,
              }}>▲ Live</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: 16 }}>
              <div style={{
                fontSize: 48, fontWeight: 900, lineHeight: 1,
                background: lin(accCfg.colors),
                WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text',
              }}>{acc.value}<span style={{ fontSize: 22, fontWeight: 700, opacity: 0.5 }}>{accCfg.unit}</span></div>
              <Sparkline series={ser.accessibility || []} color={accCfg.colors[0]} />
            </div>
          </div>
        </motion.div>

        {/* ── Retainability ─────────────────────────────────── */}
        <motion.div {...FADE(0.10)} whileHover={{ scale: 1.02, transition: { duration: 0.2 } }}
          style={{ ...glass, borderLeft: `4px solid ${retCfg.colors[0]}`, padding: '18px 20px', overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
            <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-muted)' }}>
              {ret.label}
            </span>
            <RefreshCw size={16} color={`${retCfg.colors[0]}40`} />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div>
              <div style={{ fontSize: 30, fontWeight: 900, background: lin(retCfg.colors), WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text', lineHeight: 1 }}>
                {ret.value}{retCfg.unit}
              </div>
              <div style={{ marginTop: 6, display: 'flex', alignItems: 'center', gap: 5, fontSize: 11 }}>
                <span style={{ width: 7, height: 7, borderRadius: '50%', background: ret.value >= 90 ? '#10b981' : ret.value >= 60 ? '#f59e0b' : '#ef4444', boxShadow: `0 0 5px ${ret.value >= 90 ? '#10b981' : ret.value >= 60 ? '#f59e0b' : '#ef4444'}` }} />
                <span style={{ color: ret.value >= 90 ? '#10b981' : ret.value >= 60 ? '#f59e0b' : '#ef4444', fontWeight: 600 }}>{ret.value >= 90 ? 'Stable' : ret.value >= 60 ? '⚠ Warning' : '⚠ Critical'}</span>
              </div>
            </div>
            <CircleGauge value={ret.value} max={100} color={retCfg.colors[0]} />
          </div>
        </motion.div>

        {/* ── PRB Utilization ───────────────────────────────── */}
        <motion.div {...FADE(0.15)} whileHover={{ scale: 1.02, transition: { duration: 0.2 } }}
          style={{ ...glass, borderLeft: `4px solid ${prbCfg.colors[0]}`, padding: '18px 20px', overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
            <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-muted)' }}>
              {prb.label}
            </span>
            <Cpu size={16} color={`${prbCfg.colors[0]}50`} />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div>
              <div style={{ fontSize: 30, fontWeight: 900, background: lin(prbCfg.colors), WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text', lineHeight: 1 }}>
                {prb.value}{prbCfg.unit}
              </div>
              <div style={{ marginTop: 6, fontSize: 11, color: prb.value > 70 ? '#ef4444' : prb.value > 50 ? '#f59e0b' : '#10b981', fontWeight: 600, display: 'flex', alignItems: 'center', gap: 4 }}>
                {prb.value > 70 ? '⚠ High Load' : prb.value > 50 ? '⚠ Moderate' : '● Normal'}
              </div>
            </div>
            <CircleGauge value={prb.value} max={100} color={prbCfg.colors[0]} />
          </div>
        </motion.div>

        {/* ── DL Throughput ─────────────────────────────────── */}
        <motion.div {...FADE(0.20)} whileHover={{ scale: 1.02, transition: { duration: 0.2 } }}
          style={{ ...glass, padding: '18px 20px', overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
            <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-muted)' }}>
              {thr.label}
            </span>
            <ArrowDownToLine size={16} color={`${thrCfg.colors[0]}50`} />
          </div>
          <div style={{ fontSize: 30, fontWeight: 900, background: lin(thrCfg.colors), WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text', lineHeight: 1, marginBottom: 10 }}>
            {thr.value}<span style={{ fontSize: 14, fontWeight: 600, opacity: 0.5 }}> Mbps</span>
          </div>
          <GradBar value={thr.value} max={100} colors={thrCfg.colors} />
        </motion.div>

        {/* ── DL Volume ─────────────────────────────────────── */}
        <motion.div {...FADE(0.25)} whileHover={{ scale: 1.02, transition: { duration: 0.2 } }}
          style={{ ...glass, padding: '18px 20px', overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
            <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-muted)' }}>
              {dlv.label}
            </span>
            <Database size={16} color={`${dlvCfg.colors[0]}50`} />
          </div>
          <div style={{ fontSize: 30, fontWeight: 900, background: lin(dlvCfg.colors), WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text', lineHeight: 1, marginBottom: 8 }}>
            {dlv.value.toLocaleString()}
          </div>
          <Sparkline series={ser.downlink_volume || []} color={dlvCfg.colors[0]} />
        </motion.div>

        {/* ── UL Volume ─────────────────────────────────────── */}
        <motion.div {...FADE(0.28)} whileHover={{ scale: 1.02, transition: { duration: 0.2 } }}
          style={{ ...glass, padding: '18px 20px', overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
            <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-muted)' }}>
              {ulv.label}
            </span>
            <ArrowUpFromLine size={16} color={`${ulvCfg.colors[0]}50`} />
          </div>
          <div style={{ fontSize: 30, fontWeight: 900, background: lin(ulvCfg.colors), WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text', lineHeight: 1, marginBottom: 8 }}>
            {ulv.value.toLocaleString()}
          </div>
          <Sparkline series={ser.uplink_volume || []} color={ulvCfg.colors[0]} />
        </motion.div>
      </div>

      {/* ══════════════════════════════════════════════════════
          CHART SECTION — 3-column grid
          ══════════════════════════════════════════════════════ */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginBottom: 16 }}>

        {/* ── Main Trend Chart (col-span-2) ─────────────────── */}
        <motion.div {...FADE(0.35)} style={{ ...glass, gridColumn: 'span 2', overflow: 'hidden' }}>
          <div style={{ height: 3, background: lin(accCfg.colors, 90) }} />
          <div style={{ padding: '18px 20px 12px', borderBottom: '1px solid var(--border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <h3 style={{ margin: 0, fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>Accessibility — Call Setup Success Rate (%)</h3>
              <p style={{ margin: '3px 0 0', fontSize: 11, color: 'var(--text-muted)' }}>LTE Call Setup Success Rate trend — last 30 data points</p>
            </div>
            <div style={{ display: 'flex', gap: 16 }}>
              {[['#00338D', 'Current'], ['var(--text-muted)', 'Baseline']].map(([c, l]) => (
                <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: 'var(--text-muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  <span style={{ width: 8, height: 8, borderRadius: '50%', background: c }} />
                  {l}
                </div>
              ))}
            </div>
          </div>
          <div style={{ padding: '8px 8px 4px', height: 260 }}>
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trendSeries} margin={{ top: 6, right: 12, left: -10, bottom: 0 }}>
                <GradDefs />
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 10, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                  domain={[(dataMin) => Math.floor(dataMin), (dataMax) => Math.ceil(dataMax)]}
                  allowDecimals={false} />
                <Tooltip content={<ChartTooltip />} />
                {accBreaches.map((b, i) => (
                  <ReferenceArea key={`ab-${i}`} x1={b.x1} x2={b.x2} fill="#ef4444" fillOpacity={0.08} />
                ))}
                <ReferenceLine y={THRESHOLDS.accessibility.value} stroke="#ef4444" strokeDasharray="6 4" strokeWidth={1.5}
                  label={{ value: `Threshold ${THRESHOLDS.accessibility.value}%`, position: 'right', fill: '#ef4444', fontSize: 9, fontWeight: 700 }} />
                <Area type="monotoneX" dataKey="value" stroke="#10b981" strokeWidth={3}
                  fill="url(#trendGrad)" name={acc.label} dot={false}
                  activeDot={{ r: 5, fill: '#00338D', stroke: '#4F46E5', strokeWidth: 2 }} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          {accInsight && (
            <div style={{
              margin: '0 16px 12px', padding: '8px 14px', borderRadius: 8, fontSize: 11, fontWeight: 600, lineHeight: 1.5,
              background: accInsight.severity === 'critical' ? 'rgba(239,68,68,0.08)' : accInsight.severity === 'warning' ? 'rgba(245,158,11,0.08)' : 'rgba(16,185,129,0.08)',
              color: accInsight.severity === 'critical' ? '#dc2626' : accInsight.severity === 'warning' ? '#b45309' : '#059669',
              border: `1px solid ${accInsight.severity === 'critical' ? '#fecaca' : accInsight.severity === 'warning' ? '#fed7aa' : '#bbf7d0'}`,
            }}>
              {accInsight.severity === 'critical' ? '⚠ ' : accInsight.severity === 'warning' ? '⚡ ' : '✓ '}{accInsight.text}
            </div>
          )}
        </motion.div>

        {/* ── KPI Forecast Panel ─────────────────────────────── */}
        <motion.div {...FADE(0.40)} style={{ ...glass, overflow: 'hidden' }}>
          <div style={{ height: 3, background: 'linear-gradient(90deg, #00338D, #06B6D4)' }} />
          <div style={{ padding: '18px 20px 14px', borderBottom: '1px solid var(--border)' }}>
            <h3 style={{ margin: 0, fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>KPI Forecast <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)' }}>(7 Days)</span></h3>
          </div>
          <div style={{ padding: '12px 20px', display: 'flex', flexDirection: 'column', gap: 16 }}>
            {[
              { key: 'accessibility', label: 'Accessibility', unit: '%', goodUp: true },
              { key: 'retainability', label: 'Retainability', unit: '%', goodUp: true },
              { key: 'downlink_throughput', label: 'DL Throughput', unit: '', goodUp: true },
              { key: 'prb_utilization', label: 'PRB Utilization', unit: '%', goodUp: false },
            ].map(({ key, label, unit, goodUp }) => {
              const fc = (data.forecast || {})[key];
              if (!fc) return null;
              const diff = fc.predicted_7d - fc.current;
              const isUp = diff > 0;
              const isGood = goodUp ? isUp : !isUp;
              const isNeutral = Math.abs(diff) < 0.05;
              const color = isNeutral ? 'var(--text-muted)' : isGood ? '#10b981' : '#ef4444';
              const arrow = isNeutral ? '→' : isUp ? '▲' : '▼';
              return (
                <div key={key}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                    <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text)' }}>{label}</span>
                    <span style={{ fontSize: 11, fontWeight: 800, color }}>
                      {arrow} {Math.abs(diff).toFixed(2)}{unit}
                    </span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: 'var(--text-muted)', marginBottom: 3 }}>
                        <span>Now: <b style={{ color: 'var(--text)' }}>{fc.current}{unit}</b></span>
                        <span>7d: <b style={{ color }}>{fc.predicted_7d}{unit}</b></span>
                      </div>
                      <div style={{ background: 'var(--border)', borderRadius: 4, height: 4, overflow: 'hidden', position: 'relative' }}>
                        <div style={{
                          width: '100%', height: '100%',
                          background: `linear-gradient(90deg, var(--text-muted) 70%, ${color} 100%)`,
                          borderRadius: 4, opacity: 0.4,
                        }} />
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
            <div style={{ fontSize: 9, color: 'var(--text-muted)', fontStyle: 'italic', textAlign: 'center', marginTop: 2 }}>
              Based on linear trend analysis of last 30 days
            </div>
          </div>
        </motion.div>
      </div>

      {/* ══════════════════════════════════════════════════════
          BOTTOM ROW — Donut + Throughput mini + DL/UL heatmap
          ══════════════════════════════════════════════════════ */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>

        {/* ── Packet Loss Panel ─────────────────────────────── */}
        <motion.div {...FADE(0.45)} style={{ ...glass, overflow: 'hidden' }}>
          {(() => {
            const pl = data.packet_loss || {};
            const avg = pl.avg ?? 0;
            const plColor = avg < 1 ? '#10b981' : avg < 3 ? '#f59e0b' : '#ef4444';
            const plLabel = avg < 1 ? 'Normal' : avg < 3 ? 'Warning' : 'Critical';
            return (
              <>
                <div style={{ height: 3, background: `linear-gradient(90deg, ${plColor}, ${plColor}88)` }} />
                <div style={{ padding: '16px 20px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                    <h4 style={{ margin: 0, fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--text-muted)' }}>
                      Packet Loss
                    </h4>
                    <span style={{
                      fontSize: 9, fontWeight: 800, padding: '2px 8px', borderRadius: 999,
                      background: `${plColor}18`, color: plColor, border: `1px solid ${plColor}30`,
                    }}>{plLabel}</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'flex-end', gap: 16, marginBottom: 12 }}>
                    <div style={{ fontSize: 36, fontWeight: 900, lineHeight: 1, color: plColor }}>
                      {avg}<span style={{ fontSize: 16, fontWeight: 600, opacity: 0.6 }}>%</span>
                    </div>
                    <Sparkline series={pl.series || []} color={plColor} />
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                    <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                      All Sites ({(pl.worst_sites || []).length})
                    </span>
                  </div>
                  <div style={{ maxHeight: 120, overflowY: 'auto' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      {(pl.worst_sites || []).map(s => {
                        const c = s.value < 1 ? '#10b981' : s.value < 3 ? '#f59e0b' : '#ef4444';
                        return (
                          <div key={s.site_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 11, padding: '2px 0', borderBottom: '1px solid var(--border)' }}>
                            <span style={{ fontWeight: 600, color: 'var(--text)' }}>{s.site_id.replace('GUR_LTE_', '')}</span>
                            <span style={{ fontWeight: 800, color: c }}>{s.value}%</span>
                          </div>
                        );
                      })}
                      {(pl.worst_sites || []).length === 0 && (
                        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>No data</span>
                      )}
                    </div>
                  </div>
                </div>
              </>
            );
          })()}
        </motion.div>

        {/* ── Throughput mini bars ──────────────────────────── */}
        <motion.div {...FADE(0.50)} style={{ ...glass, overflow: 'hidden' }}>
          <div style={{ height: 3, background: lin(thrCfg.colors, 90) }} />
          <div style={{ padding: '16px 20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <h4 style={{ margin: 0, fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--text-muted)' }}>
                Throughput Trend
              </h4>
              {lwDelta != null && (
                <span style={{ fontSize: 11, fontWeight: 800, color: parseFloat(lwDelta) >= 0 ? thrCfg.colors[0] : '#ba1a1a' }}>
                  {parseFloat(lwDelta) >= 0 ? '▲' : '▼'} {parseFloat(lwDelta) >= 0 ? '+' : ''}{lwDelta}% vs LW
                </span>
              )}
            </div>
            {(() => {
              const items = (ser.downlink_throughput || []).slice(-14);
              const vals = items.map(x => x.value);
              const mn = Math.min(...vals);
              const mx = Math.max(...vals);
              const range = mx - mn || 1;
              return (
                <>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, color: 'var(--text-muted)', marginBottom: 4 }}>
                    <span>Min: <b style={{ color: thrCfg.colors[0] }}>{mn.toFixed(2)}</b></span>
                    <span>Max: <b style={{ color: thrCfg.colors[0] }}>{mx.toFixed(2)}</b></span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'flex-end', gap: 4, height: 80 }}>
                    {items.map((d, i) => {
                      const h = 20 + ((d.value - mn) / range) * 80;
                      return (
                        <div key={i} title={`${d.date || ''}: ${d.value} Mbps`} style={{
                          flex: 1, borderRadius: '3px 3px 0 0',
                          background: lin(thrCfg.colors, 180),
                          height: `${h}%`,
                          transition: 'all 0.3s',
                          cursor: 'pointer',
                          boxShadow: i === items.length - 1 ? `0 0 8px ${thrCfg.colors[0]}60` : 'none',
                        }} />
                      );
                    })}
                  </div>
                </>
              );
            })()}
          </div>
        </motion.div>

        {/* ── DL / UL Heatmap ───────────────────────────────── */}
        <motion.div {...FADE(0.55)} style={{ ...glass, overflow: 'hidden' }}>
          <div style={{ height: 3, background: lin(dlvCfg.colors, 90) }} />
          <div style={{ padding: '16px 20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <h4 style={{ margin: 0, fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--text-muted)' }}>
                Volume Heatmap
              </h4>
              <span style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-muted)' }}>DL / UL</span>
            </div>
            <div style={{ marginBottom: 10 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 600, marginBottom: 5 }}>Downlink</div>
              <HeatGrid series={ser.downlink_volume || []} color={dlvCfg.colors[0]} />
            </div>
            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 600, marginBottom: 5 }}>Uplink</div>
              <HeatGrid series={ser.uplink_volume || []} color={ulvCfg.colors[0]} />
            </div>
          </div>
        </motion.div>
      </div>

      {/* ══════════════════════════════════════════════════════
          RETAINABILITY TREND — E-RAB Call Drop Rate
          ══════════════════════════════════════════════════════ */}
      <motion.div {...FADE(0.60)} style={{ ...glass, overflow: 'hidden', marginTop: 16 }}>
        <div style={{ height: 3, background: 'linear-gradient(90deg, #ef4444, #f97316)' }} />
        <div style={{ padding: '18px 20px 12px', borderBottom: '1px solid var(--border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h3 style={{ margin: 0, fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>Retainability — E-RAB Call Drop Rate (%)</h3>
            <p style={{ margin: '3px 0 0', fontSize: 11, color: 'var(--text-muted)' }}>E-RAB Call Drop Rate_1 trend — last 30 data points</p>
          </div>
          {retainSeries.length > 0 && (
            <span style={{
              fontSize: 10, fontWeight: 800, padding: '3px 8px', borderRadius: 999,
              background: retainSeries[retainSeries.length - 1]?.value <= 1 ? '#dcfce7' : '#fee2e2',
              color: retainSeries[retainSeries.length - 1]?.value <= 1 ? '#16a34a' : '#dc2626',
            }}>{retainSeries[retainSeries.length - 1]?.value}%</span>
          )}
        </div>
        <div style={{ padding: '8px 8px 4px', height: 260 }}>
          {retainSeries.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={retainSeries} margin={{ top: 6, right: 12, left: -10, bottom: 0 }}>
                <GradDefs />
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
                <XAxis dataKey="date" tick={{ fontSize: 9, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 10, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                  domain={[0, (dataMax) => Math.max(2, Math.ceil(dataMax + 0.5))]}
                  allowDecimals />
                <Tooltip content={<ChartTooltip />} />
                {erabBreaches.map((b, i) => (
                  <ReferenceArea key={`eb-${i}`} x1={b.x1} x2={b.x2} fill="#ef4444" fillOpacity={0.08} />
                ))}
                <ReferenceLine y={THRESHOLDS.e_rab_drop.value} stroke="#ef4444" strokeDasharray="6 4" strokeWidth={1.5}
                  label={{ value: 'Threshold 1%', position: 'right', fill: '#ef4444', fontSize: 9, fontWeight: 700 }} />
                <Area type="monotoneX" dataKey="value" stroke="#ef4444" strokeWidth={3}
                  fill="url(#erabGrad)" name="E-RAB Drop Rate %"
                  dot={{ r: 2, fill: '#ef4444' }}
                  activeDot={{ r: 5, fill: '#ef4444', stroke: '#f97316', strokeWidth: 2 }} />
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 12 }}>
              No E-RAB Call Drop Rate data available
            </div>
          )}
        </div>
        {erabInsight && retainSeries.length > 0 && (
          <div style={{
            margin: '0 16px 12px', padding: '8px 14px', borderRadius: 8, fontSize: 11, fontWeight: 600, lineHeight: 1.5,
            background: erabInsight.severity === 'critical' ? 'rgba(239,68,68,0.08)' : erabInsight.severity === 'warning' ? 'rgba(245,158,11,0.08)' : 'rgba(16,185,129,0.08)',
            color: erabInsight.severity === 'critical' ? '#dc2626' : erabInsight.severity === 'warning' ? '#b45309' : '#059669',
            border: `1px solid ${erabInsight.severity === 'critical' ? '#fecaca' : erabInsight.severity === 'warning' ? '#fed7aa' : '#bbf7d0'}`,
          }}>
            {erabInsight.severity === 'critical' ? '⚠ ' : erabInsight.severity === 'warning' ? '⚡ ' : '✓ '}{erabInsight.text}
          </div>
        )}
      </motion.div>
      </>
      }
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════
   CORE TAB — design matches reference HTML
   ═══════════════════════════════════════════════════════════ */

/* colour tokens matching reference */
const C = {
  primary:   '#003465',
  secondary: '#006972',
  error:     '#ba1a1a',
  errCont:   '#ffdad6',
  secCont:   '#d0f8ff',
  amber:     '#b45309',
  amberCont: '#fef3c7',
  bg:        'rgba(255,255,255,0.75)',
  border:    'rgba(194,198,210,0.4)',
  muted:     '#727782',
  surface:   '#f2f4f6',
};

function coreColor(label, value) {
  if (value == null) return C.muted;
  if (label === 'CPU Usage')         return value > 80 ? C.error : value > 60 ? C.amber : C.secondary;
  if (label === 'Auth Success Rate') return value >= 99 ? C.secondary : value >= 95 ? C.amber : C.error;
  if (label === '4G Attach Success') return value >= 98 ? C.secondary : value >= 95 ? C.amber : C.error;
  if (label === '4G Bearer Success') return value >= 91 ? C.secondary : value >= 90 ? C.amber : C.error;
  return C.secondary;
}

function coreBadge(label, value) {
  const color = coreColor(label, value);
  const bg = color === C.error ? C.errCont : color === C.amber ? C.amberCont : C.secCont;
  const text = color === C.error ? C.error : color === C.amber ? C.amber : C.secondary;
  return { bg, text, color };
}

function CoreGauge({ value, max = 100, color, icon: Icon }) {
  const r = 28, cx = 32, cy = 32, circ = 175.9;
  const pct = Math.min(1, Math.max(0, (value ?? 0) / max));
  const offset = circ * (1 - pct);
  return (
    <div style={{ position: 'relative', width: 64, height: 64, flexShrink: 0 }}>
      <svg viewBox="0 0 64 64" style={{ transform: 'rotate(-90deg)', width: '100%', height: '100%' }}>
        <circle cx={cx} cy={cy} r={r} fill="transparent" stroke="#e0e3e5" strokeWidth={4} />
        <circle cx={cx} cy={cy} r={r} fill="transparent" stroke={color}
          strokeWidth={4} strokeDasharray={circ} strokeDashoffset={offset}
          strokeLinecap="round"
          style={{ transition: 'stroke-dashoffset 1s ease' }} />
      </svg>
      {Icon && (
        <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <Icon size={16} color={color} />
        </div>
      )}
    </div>
  );
}

function CoreTab({ data, loading, onRetry }) {
  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  if (!data || !data.available) {
    return (
      <motion.div {...FADE(0)} style={{
        background: C.bg, backdropFilter: 'blur(20px)', border: `1px solid ${C.border}`,
        borderRadius: 16, padding: '48px 32px', textAlign: 'center', marginBottom: 24,
      }}>
        <div style={{ fontSize: 40, marginBottom: 16 }}>🖥️</div>
        <h3 style={{ margin: '0 0 8px', color: C.primary, fontWeight: 800 }}>
          {data?.error ? 'Could not load Core KPI data' : 'No Core KPI Data'}
        </h3>
        <p style={{ margin: '0 0 20px', color: C.muted, fontSize: 14 }}>
          {data?.error
            ? 'The server returned an error. Try refreshing below.'
            : <>Ask your admin to upload a Core KPI file from <strong>Admin → Data Upload</strong>.</>}
        </p>
        <button onClick={onRetry} style={{
          background: C.primary, color: '#fff', border: 'none', borderRadius: 8,
          padding: '10px 24px', fontSize: 13, fontWeight: 700, cursor: 'pointer',
        }}>Retry</button>
      </motion.div>
    );
  }

  const isComponent = data.data_source === 'component';

  // ── Legacy (flexible/site-based) path ──
  if (!isComponent) {
    return <LegacyCoreTab data={data} />;
  }

  // ── New component-based rich data ──
  const hero = data.hero || {};
  const typeHealth = data.type_health || {};
  const typeCompare = data.type_comparison || [];
  const trend = data.trend || [];
  const components = data.components || [];
  const anomalies = data.anomalies || [];

  const TYPE_COLORS = {
    MME: '#3b82f6', SGW: '#10b981', PGW: '#f59e0b',
    HSS: '#8b5cf6', PCRF: '#ef4444',
  };
  const typeColor = (t) => TYPE_COLORS[t] || '#64748b';

  const healthColor = (v) => v == null ? C.muted : v >= 99 ? '#059669' : v >= 95 ? '#0891b2' : v >= 90 ? '#ea580c' : '#dc2626';
  const utilColor = (v) => v == null ? C.muted : v > 85 ? '#dc2626' : v > 70 ? '#ea580c' : v > 50 ? '#0891b2' : '#059669';

  const gc = {
    background: C.bg, backdropFilter: 'blur(20px)',
    WebkitBackdropFilter: 'blur(20px)',
    border: `1px solid ${C.border}`, borderRadius: 12,
    boxShadow: '0 4px 20px rgba(0,52,101,0.06)',
  };

  // Big circular gauge
  const BigGauge = ({ value, max = 100, color, size = 120, label, sublabel }) => {
    const r = size / 2 - 12;
    const cx = size / 2, cy = size / 2;
    const circ = 2 * Math.PI * r;
    const pct = Math.min(1, Math.max(0, (value ?? 0) / max));
    return (
      <div style={{ position: 'relative', width: size, height: size, flexShrink: 0 }}>
        <svg viewBox={`0 0 ${size} ${size}`} style={{ transform: 'rotate(-90deg)', width: '100%', height: '100%' }}>
          <circle cx={cx} cy={cy} r={r} fill="transparent" stroke="#e5e7eb" strokeWidth={8} />
          <circle cx={cx} cy={cy} r={r} fill="transparent" stroke={color}
            strokeWidth={8} strokeDasharray={circ} strokeDashoffset={circ * (1 - pct)}
            strokeLinecap="round" style={{ transition: 'stroke-dashoffset 1s ease' }} />
        </svg>
        <div style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
          <div style={{ fontSize: size * 0.22, fontWeight: 900, color, lineHeight: 1 }}>
            {value != null ? value.toFixed(1) : '—'}
          </div>
          {label && <div style={{ fontSize: 9, fontWeight: 700, color: C.muted, textTransform: 'uppercase', letterSpacing: '0.06em', marginTop: 4 }}>{label}</div>}
          {sublabel && <div style={{ fontSize: 9, color: C.muted, marginTop: 2 }}>{sublabel}</div>}
        </div>
      </div>
    );
  };

  // Small horizontal bar
  const Bar = ({ value, max = 100, color, height = 6 }) => (
    <div style={{ height, background: C.surface, borderRadius: height / 2, overflow: 'hidden' }}>
      <div style={{ height: '100%', width: `${Math.min(100, (value / max) * 100)}%`, background: color, borderRadius: height / 2, transition: 'width 0.8s ease' }} />
    </div>
  );

  // Hero metrics
  const heroMetrics = [
    {
      label: 'Core Network Health',
      value: hero.overall_health,
      suffix: '%',
      color: healthColor(hero.overall_health),
      subtitle: `${hero.component_types || 0} component types · ${hero.total_nodes || 0} nodes`,
      icon: ShieldCheck,
    },
    {
      label: 'Avg CPU Usage',
      value: hero.avg_cpu,
      suffix: '%',
      color: utilColor(hero.avg_cpu),
      subtitle: hero.avg_cpu == null ? 'No data' : hero.avg_cpu > 80 ? 'Critical' : hero.avg_cpu > 60 ? 'Elevated' : 'Normal',
      icon: Cpu,
    },
    {
      label: 'Avg Memory',
      value: hero.avg_memory,
      suffix: '%',
      color: utilColor(hero.avg_memory),
      subtitle: hero.avg_memory == null ? 'No data' : hero.avg_memory > 80 ? 'Critical' : hero.avg_memory > 60 ? 'Elevated' : 'Normal',
      icon: Database,
    },
    {
      label: 'Service Success',
      value: hero.avg_success_rate,
      suffix: '%',
      color: healthColor(hero.avg_success_rate),
      subtitle: `${hero.total_kpis || 0} KPIs tracked`,
      icon: Zap,
    },
  ];

  // Max values for trend charts
  const maxCpuTrend = Math.max(...trend.map(d => d.cpu || 0), 1);
  const maxMemTrend = Math.max(...trend.map(d => d.memory || 0), 1);

  return (
    <>
      {/* ── Hero KPI Cards ────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 20, marginBottom: 24 }}>
        {heroMetrics.map(({ label, value, suffix, color, subtitle, icon: Icon }, i) => (
          <motion.div key={label} {...FADE(i * 0.07)} style={{ ...gc, padding: 24, borderLeft: `4px solid ${color}`, position: 'relative', overflow: 'hidden' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
              <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: C.muted }}>{label}</span>
              <Icon size={16} color={color} />
            </div>
            <div style={{ fontSize: 36, fontWeight: 900, color: C.primary, lineHeight: 1 }}>
              {value != null ? `${value.toFixed(1)}${suffix}` : '—'}
            </div>
            <div style={{ fontSize: 11, color, marginTop: 6, fontWeight: 700 }}>{subtitle}</div>
            <div style={{ marginTop: 12 }}>
              <Bar value={value ?? 0} color={color} />
            </div>
          </motion.div>
        ))}
      </div>

      {/* ── Component Type Health Cards (big gauges) ─────────── */}
      <motion.div {...FADE(0.2)} style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 14 }}>
          <h3 style={{ margin: 0, fontSize: 16, fontWeight: 800, color: C.primary }}>Component Health by Type</h3>
          <span style={{ fontSize: 11, color: C.muted, fontWeight: 600 }}>
            {data.date_range?.from && `${data.date_range.from} → ${data.date_range.to}`}
          </span>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: `repeat(${Math.min(typeCompare.length, 5)}, 1fr)`, gap: 16 }}>
          {typeCompare.map((t, i) => {
            const ct = t.type;
            const th = typeHealth[ct] || {};
            const color = typeColor(ct);
            const hColor = healthColor(t.health);
            return (
              <motion.div key={ct} {...FADE(0.2 + i * 0.05)} style={{
                ...gc, padding: 20, borderTop: `3px solid ${color}`,
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
                  <div>
                    <div style={{ fontSize: 20, fontWeight: 900, color: C.primary, letterSpacing: '-0.02em' }}>{ct}</div>
                    <div style={{ fontSize: 10, fontWeight: 600, color: C.muted, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                      {t.instances} nodes · {t.total_kpis} KPIs
                    </div>
                  </div>
                  <BigGauge value={t.health} color={hColor} size={72} />
                </div>

                {/* CPU & Memory bars */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, fontWeight: 700, color: C.muted, marginBottom: 3 }}>
                      <span>CPU</span>
                      <span style={{ color: utilColor(t.cpu) }}>{t.cpu != null ? `${t.cpu.toFixed(1)}%` : '—'}</span>
                    </div>
                    <Bar value={t.cpu ?? 0} color={utilColor(t.cpu)} height={5} />
                  </div>
                  <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, fontWeight: 700, color: C.muted, marginBottom: 3 }}>
                      <span>MEMORY</span>
                      <span style={{ color: utilColor(t.memory) }}>{t.memory != null ? `${t.memory.toFixed(1)}%` : '—'}</span>
                    </div>
                    <Bar value={t.memory ?? 0} color={utilColor(t.memory)} height={5} />
                  </div>
                </div>

                {/* Top 3 success-rate KPIs */}
                {Object.keys(th.success_rates || {}).length > 0 && (
                  <div style={{ marginTop: 12, paddingTop: 10, borderTop: `1px solid ${C.border}` }}>
                    <div style={{ fontSize: 9, fontWeight: 700, color: C.muted, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
                      Key Success Rates
                    </div>
                    {Object.entries(th.success_rates || {}).slice(0, 3).map(([k, v]) => (
                      <div key={k} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, marginBottom: 3 }}>
                        <span style={{ color: C.muted, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '70%' }} title={k}>
                          {k.length > 22 ? k.slice(0, 20) + '…' : k}
                        </span>
                        <span style={{ fontWeight: 700, color: healthColor(v) }}>{v.toFixed(1)}%</span>
                      </div>
                    ))}
                  </div>
                )}
              </motion.div>
            );
          })}
        </div>
      </motion.div>

      {/* ── Trend Charts: CPU/Memory/Health over time ──────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 20, marginBottom: 24 }}>

        {/* Multi-line trend chart using recharts */}
        <motion.div {...FADE(0.3)} style={{ ...gc, padding: 24 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
            <div>
              <h3 style={{ margin: 0, fontSize: 16, fontWeight: 800, color: C.primary }}>Resource Utilisation & Health Trend</h3>
              <p style={{ margin: '4px 0 0', fontSize: 11, color: C.muted }}>CPU · Memory · Service Success across core components</p>
            </div>
            <div style={{ display: 'flex', gap: 14 }}>
              {[['#ea580c', 'CPU'], ['#8b5cf6', 'Memory'], ['#059669', 'Health']].map(([c, l]) => (
                <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: C.muted, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  <span style={{ width: 8, height: 8, borderRadius: '50%', background: c }} />{l}
                </div>
              ))}
            </div>
          </div>
          <ResponsiveContainer width="100%" height={260}>
            <AreaChart data={trend} margin={{ top: 10, right: 8, left: -8, bottom: 0 }}>
              <defs>
                <linearGradient id="cpuGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#ea580c" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="#ea580c" stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="memGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#8b5cf6" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="#8b5cf6" stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="healthGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#059669" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="#059669" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
              <XAxis dataKey="date" tick={{ fontSize: 10, fill: C.muted }} tickFormatter={(d) => d ? new Date(d).toLocaleDateString('en', { month: 'short', day: 'numeric' }) : ''} />
              <YAxis tick={{ fontSize: 10, fill: C.muted }} domain={[0, 100]} />
              <Tooltip contentStyle={{ background: '#fff', border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }} />
              <Area type="monotone" dataKey="cpu" stroke="#ea580c" strokeWidth={2} fill="url(#cpuGrad)" name="CPU %" />
              <Area type="monotone" dataKey="memory" stroke="#8b5cf6" strokeWidth={2} fill="url(#memGrad)" name="Memory %" />
              <Area type="monotone" dataKey="health" stroke="#059669" strokeWidth={2} fill="url(#healthGrad)" name="Health %" />
            </AreaChart>
          </ResponsiveContainer>
        </motion.div>

        {/* Component type comparison bars */}
        <motion.div {...FADE(0.35)} style={{ ...gc, padding: 24 }}>
          <h3 style={{ margin: '0 0 4px', fontSize: 16, fontWeight: 800, color: C.primary }}>Resource Load by Type</h3>
          <p style={{ margin: '0 0 16px', fontSize: 11, color: C.muted }}>CPU vs Memory per component type</p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            {typeCompare.map(t => {
              const color = typeColor(t.type);
              return (
                <div key={t.type}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, fontWeight: 700, marginBottom: 5 }}>
                    <span style={{ color, background: color + '15', padding: '2px 8px', borderRadius: 999 }}>{t.type}</span>
                    <span style={{ color: C.muted }}>{t.instances} nodes</span>
                  </div>
                  <div style={{ display: 'flex', gap: 4, alignItems: 'center', marginBottom: 3 }}>
                    <span style={{ fontSize: 9, color: C.muted, width: 32, fontWeight: 700 }}>CPU</span>
                    <div style={{ flex: 1 }}><Bar value={t.cpu ?? 0} color="#ea580c" height={6} /></div>
                    <span style={{ fontSize: 10, color: utilColor(t.cpu), width: 36, textAlign: 'right', fontWeight: 700 }}>{t.cpu != null ? `${t.cpu.toFixed(0)}%` : '—'}</span>
                  </div>
                  <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
                    <span style={{ fontSize: 9, color: C.muted, width: 32, fontWeight: 700 }}>MEM</span>
                    <div style={{ flex: 1 }}><Bar value={t.memory ?? 0} color="#8b5cf6" height={6} /></div>
                    <span style={{ fontSize: 10, color: utilColor(t.memory), width: 36, textAlign: 'right', fontWeight: 700 }}>{t.memory != null ? `${t.memory.toFixed(0)}%` : '—'}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </motion.div>
      </div>

      {/* ── Bottom: Anomalies + Component Instance Table ───── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: 20 }}>
        {/* Anomalies */}
        <motion.div {...FADE(0.45)}>
          <h3 style={{ margin: '0 0 6px', fontSize: 18, fontWeight: 800, color: C.primary }}>
            Active Anomalies {anomalies.length > 0 && <span style={{ fontSize: 12, fontWeight: 700, color: C.error, background: C.errCont, padding: '2px 8px', borderRadius: 999, marginLeft: 6 }}>{anomalies.length}</span>}
          </h3>
          <p style={{ margin: '0 0 16px', fontSize: 12, color: C.muted, lineHeight: 1.5 }}>
            Components with degraded health, elevated CPU, or high memory usage.
          </p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10, maxHeight: 520, overflowY: 'auto' }}>
            {anomalies.length === 0 && (
              <div style={{ ...gc, padding: '16px 18px', fontSize: 13, color: C.secondary, fontWeight: 700 }}>
                ✓ All core components healthy
              </div>
            )}
            {anomalies.map(a => {
              const ctc = typeColor(a.component_type);
              const hc = healthColor(a.health);
              return (
                <div key={`${a.component_type}:${a.component_id}`} style={{ ...gc, padding: 14, borderLeft: `3px solid ${hc}` }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ fontSize: 10, fontWeight: 700, color: ctc, background: ctc + '15', padding: '2px 7px', borderRadius: 999 }}>{a.component_type}</span>
                      <span style={{ fontSize: 13, fontWeight: 800, color: C.primary }}>{a.component_id}</span>
                    </div>
                    <span style={{ fontSize: 14, fontWeight: 900, color: hc }}>{a.health.toFixed(1)}%</span>
                  </div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                    {a.reasons.map((r, i) => (
                      <span key={i} style={{ fontSize: 10, fontWeight: 700, color: C.error, background: C.errCont, padding: '2px 7px', borderRadius: 4 }}>{r}</span>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </motion.div>

        {/* Component instance table */}
        <motion.div {...FADE(0.5)} style={{ ...gc, padding: 24, overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
            <h3 style={{ margin: 0, fontSize: 16, fontWeight: 800, color: C.primary }}>Component Instances</h3>
            <span style={{ fontSize: 11, color: C.muted, fontWeight: 600 }}>{components.length} nodes · sorted by health</span>
          </div>
          <div style={{ overflowX: 'auto', overflowY: 'auto', maxHeight: 500 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${C.border}` }}>
                  {['Type', 'Instance', 'Health', 'CPU', 'Memory', 'KPIs'].map(h => (
                    <th key={h} style={{ padding: '8px 12px', textAlign: h === 'Type' || h === 'Instance' ? 'left' : 'right', fontWeight: 700, fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.07em', color: C.muted, position: 'sticky', top: 0, background: '#fff', zIndex: 1 }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[...components].sort((a, b) => a.health - b.health).map(row => {
                  const ctc = typeColor(row.component_type);
                  return (
                    <tr key={`${row.component_type}:${row.component_id}`}
                      style={{ borderBottom: `1px solid ${C.border}`, transition: 'background 0.15s' }}
                      onMouseEnter={e => e.currentTarget.style.background = `${C.primary}06`}
                      onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                      <td style={{ padding: '10px 12px' }}>
                        <span style={{ fontSize: 10, fontWeight: 700, color: ctc, background: ctc + '15', padding: '2px 8px', borderRadius: 999 }}>
                          {row.component_type}
                        </span>
                      </td>
                      <td style={{ padding: '10px 12px', fontWeight: 700, color: C.primary }}>{row.component_id}</td>
                      <td style={{ padding: '10px 12px', textAlign: 'right', fontWeight: 800, color: healthColor(row.health) }}>
                        {row.health != null ? `${row.health.toFixed(1)}%` : '—'}
                      </td>
                      <td style={{ padding: '10px 12px', textAlign: 'right', fontWeight: 600, color: utilColor(row.cpu) }}>
                        {row.cpu != null ? `${row.cpu.toFixed(1)}%` : '—'}
                      </td>
                      <td style={{ padding: '10px 12px', textAlign: 'right', fontWeight: 600, color: utilColor(row.memory) }}>
                        {row.memory != null ? `${row.memory.toFixed(1)}%` : '—'}
                      </td>
                      <td style={{ padding: '10px 12px', textAlign: 'right', color: C.muted, fontWeight: 600 }}>
                        {row.kpi_count}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </motion.div>
      </div>
    </>
  );
}

// ── Legacy site-based core tab (kept for FlexibleKpiUpload fallback) ──
function LegacyCoreTab({ data }) {
  const cols    = data.columns || [];
  const summary = data.summary || {};
  const items   = data.sites || [];
  const trend   = data.trend || [];

  /* find a column by its display label */
  const byLabel = (lbl) => {
    const c = cols.find(c => c.label === lbl);
    return c ? { key: c.key, avg: summary[c.key]?.latest_avg ?? summary[c.key]?.avg ?? null } : null;
  };

  const cpu    = byLabel('CPU Usage');
  const auth   = byLabel('Auth Success Rate');
  const attach = byLabel('4G Attach Success');
  const bearer = byLabel('4G Bearer Success');

  /* trend delta: last date minus first date */
  const trendDelta = (colKey) => {
    if (!colKey || trend.length < 2) return null;
    const first = trend[0][colKey];
    const last  = trend[trend.length - 1][colKey];
    if (first == null || last == null || first === 0) return null;
    return parseFloat((last - first).toFixed(2));
  };

  /* KPI card definitions with icons */
  const kpiCards = [
    { label: 'CPU Usage',         col: cpu,    subtitle: 'Critical Threshold: 90%', icon: AlertTriangle },
    { label: 'Auth Success Rate', col: auth,   subtitle: 'Optimal if ≥ 99%',        icon: ShieldCheck   },
    { label: '4G Attach Success', col: attach, subtitle: 'Target: > 98.0%',         icon: Wifi          },
    { label: '4G Bearer Success', col: bearer, subtitle: 'SLA floor: 91%',          icon: Signal        },
  ];

  /* CPU trend bars: date-based from trend data */
  const cpuKey    = cpu?.key;
  const cpuTrend  = trend.map(d => ({ date: d.date, v: d[cpuKey] ?? 0 }));
  const maxCpuT   = Math.max(...cpuTrend.map(d => d.v), 1);

  /* auth sparkline from trend (last 8 points) */
  const authKey      = auth?.key;
  const authSparkline = trend.slice(-8).map(d => d[authKey] ?? 0);
  const maxAuthSpark  = Math.max(...authSparkline, 1);

  /* attach: top 4 components or sites */
  const attachKey   = attach?.key;
  const attachItems = attachKey
    ? [...items].sort((a, b) => (b[attachKey] ?? 0) - (a[attachKey] ?? 0)).slice(0, 4)
    : [];

  /* anomalies */
  const bearerKey = bearer?.key;
  const anomalies = bearerKey
    ? [...items]
        .filter(s => (s[bearerKey] ?? 100) < 91)
        .sort((a, b) => (b[cpuKey] ?? 0) - (a[cpuKey] ?? 0))
        .slice(0, 5)
    : [];

  const itemLabel = (item) => item.site_id;
  const isComponent = false;
  const compTypes = [];
  const typeSummary = {};

  /* glass card base style */
  const gc = {
    background: C.bg, backdropFilter: 'blur(20px)',
    WebkitBackdropFilter: 'blur(20px)',
    border: `1px solid ${C.border}`, borderRadius: 12,
    boxShadow: '0 4px 20px rgba(0,52,101,0.06)',
  };

  /* Component type color map */
  const TYPE_COLORS = { MME: '#3b82f6', SGW: '#10b981', PGW: '#f59e0b', HSS: '#8b5cf6', PCRF: '#ef4444' };

  return (
    <>
      {/* ── KPI Cards ─────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 20, marginBottom: 24 }}>
        {kpiCards.map(({ label, col, subtitle, icon: Icon }, i) => {
          const val   = col?.avg;
          const color = coreColor(label, val);
          const delta = col?.key ? trendDelta(col.key) : null;
          const isUp  = delta != null && delta > 0;
          const isGoodDir = label === 'CPU Usage' ? !isUp : isUp;
          const deltaColor = delta == null ? C.muted : (isGoodDir ? C.secondary : C.error);
          const deltaBg    = delta == null ? C.surface : (isGoodDir ? C.secCont  : C.errCont);
          return (
            <motion.div key={label} {...FADE(i * 0.07)} style={{ ...gc, padding: 24, borderLeft: `4px solid ${color}` }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
                <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: C.muted }}>{label}</span>
                <span style={{ background: deltaBg, color: deltaColor, fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 999, display: 'flex', alignItems: 'center', gap: 3 }}>
                  {delta != null
                    ? <>{isUp ? <TrendingUp size={10} /> : <TrendingDown size={10} />} {Math.abs(delta).toFixed(1)}%</>
                    : (val != null ? (label === 'CPU Usage' && val > 80 ? '▲ High' : val >= 98 ? '✓ Good' : val >= 95 ? '~ Fair' : '▼ Low') : '—')}
                </span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <div>
                  <div style={{ fontSize: 30, fontWeight: 900, color: C.primary, lineHeight: 1 }}>
                    {val != null ? `${val.toFixed(1)}%` : '—'}
                  </div>
                  <div style={{ fontSize: 11, color: color === C.error ? C.error : C.muted, marginTop: 6, fontWeight: 600 }}>{subtitle}</div>
                </div>
                <CoreGauge value={val ?? 0} max={100} color={color} icon={Icon} />
              </div>
            </motion.div>
          );
        })}
      </div>

      {/* ── Component Type Summary (only for component-based data) ── */}
      {isComponent && compTypes.length > 0 && (
        <motion.div {...FADE(0.15)} style={{ display: 'grid', gridTemplateColumns: `repeat(${Math.min(compTypes.length, 5)}, 1fr)`, gap: 16, marginBottom: 24 }}>
          {compTypes.map((ct, i) => {
            const ts = typeSummary[ct] || {};
            const kpis = ts.kpis || {};
            const instances = ts.instances || 0;
            const ctColor = TYPE_COLORS[ct] || C.secondary;
            return (
              <motion.div key={ct} {...FADE(0.15 + i * 0.05)} style={{ ...gc, padding: 20, borderTop: `3px solid ${ctColor}` }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                  <span style={{ fontSize: 16, fontWeight: 900, color: C.primary }}>{ct}</span>
                  <span style={{ fontSize: 10, fontWeight: 700, color: ctColor, background: ctColor + '15', padding: '2px 8px', borderRadius: 999 }}>
                    {instances} nodes
                  </span>
                </div>
                {Object.entries(kpis).map(([key, val]) => {
                  const col = cols.find(c => c.key === key);
                  const label = col?.label || key;
                  const color = coreColor(label, val);
                  return (
                    <div key={key} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 6 }}>
                      <span style={{ color: C.muted, fontWeight: 600 }}>{label}</span>
                      <span style={{ fontWeight: 700, color }}>{val != null ? `${val.toFixed(1)}%` : '—'}</span>
                    </div>
                  );
                })}
              </motion.div>
            );
          })}
        </motion.div>
      )}

      {/* ── Chart Section (2/3 CPU trend + 1/3 stacked cards) ── */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 20, marginBottom: 24 }}>

        {/* CPU Usage Trend */}
        <motion.div {...FADE(0.3)} style={{ ...gc, padding: 28, position: 'relative', overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 28 }}>
            <div>
              <h3 style={{ margin: 0, fontSize: 16, fontWeight: 800, color: C.primary }}>CPU Usage Trend</h3>
              <p style={{ margin: '4px 0 0', fontSize: 11, color: C.muted }}>
                {isComponent ? 'Processing load across core components' : 'Real-time processing load across core elements'}
                {data.date_range?.from && ` · ${data.date_range.from} → ${data.date_range.to}`}
              </p>
            </div>
            <div style={{ display: 'flex', gap: 14 }}>
              {[['#003465', 'Average Load'], ['#ba1a1a', 'Critical']].map(([c, l]) => (
                <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: C.muted, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  <span style={{ width: 8, height: 8, borderRadius: '50%', background: c }} />{l}
                </div>
              ))}
            </div>
          </div>
          <div style={{ height: 240, display: 'flex', alignItems: 'flex-end', gap: 3, padding: '0 2px 4px' }}>
            {cpuTrend.length > 0 ? cpuTrend.map((d, i) => {
              const v   = d.v;
              const pct = (v / maxCpuT) * 100;
              const col = v > 80 ? C.error : v > 60 ? C.amber : C.secondary;
              return (
                <div key={i} title={`${d.date}: ${v.toFixed(1)}%`}
                  style={{ flex: 1, height: '100%', display: 'flex', flexDirection: 'column', justifyContent: 'flex-end' }}>
                  <div style={{
                    width: '100%', borderRadius: '3px 3px 0 0',
                    height: `${Math.max(4, pct)}%`,
                    background: `linear-gradient(to top, ${col}22, ${col})`,
                    borderTop: v > 80 ? `2px solid ${C.error}` : 'none',
                    transition: 'height 0.6s ease',
                  }} />
                </div>
              );
            }) : (
              <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: C.muted, fontSize: 13 }}>No CPU data</div>
            )}
          </div>
          {cpuTrend.length > 0 && (() => {
            const step = Math.max(1, Math.floor(cpuTrend.length / 6));
            const labels = cpuTrend.filter((_, i) => i % step === 0 || i === cpuTrend.length - 1);
            return (
              <div style={{ display: 'flex', justifyContent: 'space-between', padding: '6px 2px 0', fontSize: 10, fontWeight: 700, color: C.muted, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                {labels.map(d => (
                  <span key={d.date}>{new Date(d.date).toLocaleDateString('en', { month: 'short', day: 'numeric' })}</span>
                ))}
              </div>
            );
          })()}
        </motion.div>

        {/* Right stacked: Auth dark card + Attach light card */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

          {/* Auth dark card */}
          <motion.div {...FADE(0.35)} style={{
            background: C.primary, borderRadius: 12, padding: 28, color: '#fff',
            position: 'relative', overflow: 'hidden', flex: 1,
            boxShadow: '0 8px 30px rgba(0,52,101,0.25)',
          }}>
            <div style={{ position: 'relative', zIndex: 1 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', opacity: 0.6 }}>Success Trend</span>
                <Zap size={16} color="#75d5e2" />
              </div>
              <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 4 }}>Auth Success</div>
              <div style={{ fontSize: 32, fontWeight: 900, marginBottom: 16, lineHeight: 1 }}>
                {auth?.avg != null ? `${auth.avg.toFixed(1)}%` : '—'}
              </div>
              <div style={{ height: 48, display: 'flex', alignItems: 'flex-end', gap: 3, opacity: 0.5 }}>
                {authSparkline.map((v, i) => (
                  <div key={i} title={`${trend.slice(-8)[i]?.date || ''}: ${v}`} style={{ flex: 1, background: 'rgba(255,255,255,0.7)', borderRadius: '2px 2px 0 0', height: `${Math.max(15, (v / maxAuthSpark) * 100)}%`, cursor: 'pointer' }} />
                ))}
              </div>
            </div>
            <div style={{ position: 'absolute', right: -10, bottom: -10, opacity: 0.07 }}>
              <ShieldCheck size={100} color="#fff" />
            </div>
          </motion.div>

          {/* Attach light card */}
          <motion.div {...FADE(0.4)} style={{ ...gc, padding: 24, flex: 1 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
              <h4 style={{ margin: 0, fontSize: 14, fontWeight: 800, color: C.primary }}>Attach Success</h4>
              <span style={{ fontSize: 10, fontWeight: 700, color: C.secondary, textTransform: 'uppercase', letterSpacing: '0.06em' }}>Active</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: 8, marginBottom: 16 }}>
              <span style={{ fontSize: 32, fontWeight: 900, color: C.primary, lineHeight: 1 }}>
                {attach?.avg != null ? attach.avg.toFixed(1) : '—'}
              </span>
              {attach?.avg != null && <span style={{ fontSize: 12, fontWeight: 700, color: C.secondary, marginBottom: 4 }}>%</span>}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {attachItems.map(s => {
                const v = s[attachKey] ?? 0;
                return (
                  <div key={itemLabel(s)}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, fontWeight: 700, color: C.muted, marginBottom: 4 }}>
                      <span>{itemLabel(s)}</span><span style={{ color: C.primary }}>{v.toFixed(1)}%</span>
                    </div>
                    <div style={{ height: 4, background: C.surface, borderRadius: 4, overflow: 'hidden' }}>
                      <div style={{ height: '100%', background: C.secondary, width: `${v}%`, borderRadius: 4, transition: 'width 0.8s ease' }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </motion.div>
        </div>
      </div>

      {/* ── Bottom Section ────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 3fr', gap: 20 }}>

        {/* Anomalies sidebar */}
        <motion.div {...FADE(0.45)}>
          <h3 style={{ margin: '0 0 8px', fontSize: 18, fontWeight: 800, color: C.primary }}>Active Anomalies</h3>
          <p style={{ margin: '0 0 20px', fontSize: 13, color: C.muted, lineHeight: 1.6 }}>
            {isComponent ? 'Components with elevated CPU or degraded bearer success.' : 'Sites with elevated CPU or degraded bearer success.'}
          </p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {anomalies.length === 0 && (
              <div style={{ ...gc, padding: '14px 16px', fontSize: 13, color: C.secondary, fontWeight: 600 }}>
                ✓ No anomalies detected
              </div>
            )}
            {anomalies.map(s => {
              const isHighCpu = cpuKey && (s[cpuKey] ?? 0) > 80;
              const color = isHighCpu ? C.error : C.amber;
              const bg    = isHighCpu ? C.errCont : C.amberCont;
              return (
                <div key={itemLabel(s)} style={{ display: 'flex', gap: 12, padding: 14, borderRadius: 12, background: bg + '50', border: `1px solid ${color}20` }}>
                  <div style={{ width: 40, height: 40, borderRadius: '50%', background: bg, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                    {isHighCpu ? <AlertTriangle size={18} color={color} /> : <TrendingDown size={18} color={color} />}
                  </div>
                  <div>
                    <div style={{ fontSize: 12, fontWeight: 700, color: C.primary }}>{itemLabel(s)}</div>
                    <div style={{ fontSize: 11, color: C.muted, marginTop: 3 }}>
                      {cpuKey ? `CPU: ${(s[cpuKey] ?? 0).toFixed(1)}%` : ''}
                      {bearerKey && (s[bearerKey] ?? 100) < 91 ? `  |  Bearer: ${(s[bearerKey] ?? 0).toFixed(1)}%` : ''}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </motion.div>

        {/* Component / Site distribution table */}
        <motion.div {...FADE(0.5)} style={{ ...gc, padding: 28, overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
            <h3 style={{ margin: 0, fontSize: 16, fontWeight: 800, color: C.primary }}>
              {isComponent ? 'Core KPI — Component Distribution' : 'Core KPI — Site Distribution'}
            </h3>
            <span style={{ fontSize: 11, color: C.muted, fontWeight: 600 }}>
              {isComponent ? `${data.total_components} components` : `${data.total_sites} sites`}
            </span>
          </div>
          <div style={{ overflowX: 'auto', overflowY: 'auto', maxHeight: 480 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${C.border}` }}>
                  {isComponent && (
                    <th style={{ padding: '6px 12px', textAlign: 'left', fontWeight: 700, fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.07em', color: C.muted, position: 'sticky', top: 0, background: 'white', zIndex: 1 }}>Type</th>
                  )}
                  <th style={{ padding: '6px 12px', textAlign: 'left', fontWeight: 700, fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.07em', color: C.muted, position: 'sticky', top: 0, background: 'white', zIndex: 1 }}>
                    {isComponent ? 'Component' : 'Site'}
                  </th>
                  {cols.map(col => (
                    <th key={col.key} style={{ padding: '6px 12px', textAlign: 'right', fontWeight: 700, fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.07em', color: C.primary, whiteSpace: 'nowrap', position: 'sticky', top: 0, background: 'white', zIndex: 1 }}>
                      {col.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {items.map(row => {
                  const rowKey = isComponent ? `${row.component_type}:${row.component_id}` : row.site_id;
                  const ctColor = isComponent ? (TYPE_COLORS[row.component_type] || C.secondary) : C.primary;
                  return (
                    <tr key={rowKey}
                      style={{ borderBottom: `1px solid ${C.border}`, transition: 'background 0.15s', cursor: 'default' }}
                      onMouseEnter={e => e.currentTarget.style.background = `${C.primary}06`}
                      onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                      {isComponent && (
                        <td style={{ padding: '10px 12px' }}>
                          <span style={{ fontSize: 10, fontWeight: 700, color: ctColor, background: ctColor + '15', padding: '2px 8px', borderRadius: 999 }}>
                            {row.component_type}
                          </span>
                        </td>
                      )}
                      <td style={{ padding: '10px 12px', fontWeight: 700, color: C.primary }}>
                        {isComponent ? row.component_id : row.site_id}
                      </td>
                      {cols.map(col => {
                        const val   = row[col.key];
                        const color = coreColor(col.label, val);
                        return (
                          <td key={col.key} style={{ padding: '10px 12px', textAlign: 'right', fontWeight: 600, color: val != null ? color : C.muted }}>
                            {val != null ? `${val.toFixed(1)}%` : '—'}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </motion.div>
      </div>
    </>
  );
}
