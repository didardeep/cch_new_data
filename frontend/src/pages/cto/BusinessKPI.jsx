import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import {
  Activity, AlertTriangle, IndianRupee,
  MapPin, RefreshCw, TrendingDown, TrendingUp,
  UserMinus, Users, Zap,
} from 'lucide-react';
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid,
  Cell, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiGet } from '../../api';

/* ═══════════════════════════════════════════════════════════
   DESIGN TOKENS — Premium gradient palette
   ═══════════════════════════════════════════════════════════ */
const G = {
  indigo:   ['#00338D', '#4F46E5'],   // users
  violet:   ['#4F46E5', '#8B5CF6'],   // avg users
  emerald:  ['#0d9488', '#06B6D4'],   // growth+ / roi
  amber:    ['#00338D', '#06B6D4'],   // arpu / revenue at risk
  rose:     ['#ef4444', '#f97316'],   // churn critical / growth-
  teal:     ['#06B6D4', '#4F46E5'],   // info
  health: (v) => v < 40
    ? ['#ef4444', '#f97316']
    : v < 70 ? ['#f59e0b', '#f97316'] : ['#0d9488', '#06B6D4'],
  glow: (from, opacity = 0.22) =>
    `0 8px 32px ${from}${Math.round(opacity * 255).toString(16).padStart(2, '0')}`,
};

const linear = (colors) => `linear-gradient(135deg, ${colors[0]}, ${colors[1]})`;

/* ─── Base card style ─────────────────────────────────────── */
const glassCard = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border)',
  borderRadius: 16,
  boxShadow: 'var(--shadow-md), inset 0 1px 0 rgba(255,255,255,0.5)',
};

const FADE_UP = (delay = 0) => ({
  initial: { opacity: 0, y: 24 },
  animate: { opacity: 1, y: 0 },
  transition: { duration: 0.45, delay, ease: [0.22, 1, 0.36, 1] },
});

/* ─── Custom chart tooltip ────────────────────────────────── */
function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: 'var(--bg-card)',
      border: '1px solid var(--border)',
      borderRadius: 12,
      boxShadow: '0 16px 40px rgba(0,0,0,0.12), inset 0 1px 0 rgba(255,255,255,0.5)',
      padding: '10px 14px',
      fontSize: 12,
      minWidth: 140,
    }}>
      <p style={{ color: 'var(--text-muted)', marginBottom: 8, fontWeight: 700, fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label}</p>
      {payload.map((p) => (
        <div key={p.name} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 4 }}>
          <span style={{ width: 8, height: 8, borderRadius: '50%', background: p.color, flexShrink: 0, boxShadow: `0 0 6px ${p.color}60` }} />
          <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{p.name}:</span>
          <span style={{ color: 'var(--text)', fontWeight: 800, marginLeft: 'auto', paddingLeft: 8 }}>
            {typeof p.value === 'number' ? p.value.toLocaleString() : p.value}
          </span>
        </div>
      ))}
    </div>
  );
}

/* ─── Gradient Pill Badge ─────────────────────────────────── */
function Pill({ value, suffix = '', positive, negative, neutral }) {
  let bg, color;
  if (positive) { bg = 'rgba(0,51,141,0.1)'; color = '#00338D'; }
  else if (negative) { bg = 'rgba(239,68,68,0.13)'; color = '#ef4444'; }
  else if (neutral) { bg = 'rgba(13,148,136,0.13)'; color = '#0d9488'; }
  else { bg = 'rgba(148,163,184,0.12)'; color = 'var(--text-muted)'; }
  return (
    <span style={{
      background: bg,
      color,
      borderRadius: 999,
      padding: '3px 9px',
      fontSize: 11,
      fontWeight: 800,
      whiteSpace: 'nowrap',
      border: `1px solid ${color}28`,
    }}>
      {value}{suffix}
    </span>
  );
}

/* ─── Gradient Progress Bar ───────────────────────────────── */
function ProgressBar({ value, max = 100, colors }) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  const grad = colors ? linear(colors) : linear(G.emerald);
  return (
    <div style={{ background: 'var(--border)', borderRadius: 6, height: 6, width: 84, overflow: 'hidden' }}>
      <div style={{
        width: `${pct}%`, height: '100%',
        background: grad, borderRadius: 6,
        transition: 'width 0.7s cubic-bezier(0.22,1,0.36,1)',
        boxShadow: `0 0 8px ${(colors || G.emerald)[0]}60`,
      }} />
    </div>
  );
}

/* ─── KPI Card ────────────────────────────────────────────── */
function KpiCard({ label, value, sub, colors, icon: Icon, delay = 0 }) {
  const [from] = colors;
  const glowShadow = G.glow(from);

  return (
    <motion.div
      {...FADE_UP(delay)}
      whileHover={{ scale: 1.035, boxShadow: `${glowShadow}, inset 0 1px 0 rgba(255,255,255,0.6)`, transition: { duration: 0.2 } }}
      style={{ ...glassCard, overflow: 'hidden', cursor: 'default', transition: 'box-shadow 0.25s ease' }}
    >
      {/* Gradient top bar */}
      <div style={{ height: 3, background: linear(colors), borderRadius: '16px 16px 0 0' }} />

      <div style={{ padding: '18px 20px 20px' }}>
        {/* Ambient glow behind icon */}
        <div style={{ position: 'relative', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 14 }}>
          <span style={{
            fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
            letterSpacing: '0.07em', color: 'var(--text-muted)',
          }}>{label}</span>
          <div style={{
            background: `${from}18`,
            borderRadius: 10, padding: 9, flexShrink: 0,
            boxShadow: `0 0 12px ${from}30`,
          }}>
            <Icon size={18} color={from} strokeWidth={2.2} />
          </div>
        </div>

        {/* Value */}
        <div style={{
          fontSize: 32, fontWeight: 900, lineHeight: 1,
          background: linear(colors),
          WebkitBackgroundClip: 'text',
          WebkitTextFillColor: 'transparent',
          backgroundClip: 'text',
          marginBottom: 6,
        }}>{value}</div>

        <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>{sub}</div>
      </div>
    </motion.div>
  );
}

/* ─── Section wrapper ─────────────────────────────────────── */
function Section({ title, children, delay = 0, style, accentColor }) {
  return (
    <motion.div {...FADE_UP(delay)} style={{ ...glassCard, overflow: 'hidden', ...style }}>
      <div style={{
        padding: '15px 20px 13px',
        borderBottom: '1px solid var(--border)',
        display: 'flex', alignItems: 'center', gap: 10,
        background: 'var(--bg-card)',
      }}>
        {accentColor && (
          <div style={{ width: 3, height: 16, borderRadius: 2, background: linear(accentColor), flexShrink: 0 }} />
        )}
        <h3 style={{ margin: 0, fontSize: 13, fontWeight: 700, color: 'var(--text)', letterSpacing: '-0.01em' }}>{title}</h3>
      </div>
      <div style={{ padding: '16px 20px' }}>{children}</div>
    </motion.div>
  );
}

/* ─── Premium Table ───────────────────────────────────────── */
function PremiumTable({ cols, rows, emptyText = 'No data' }) {
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
      <thead>
        <tr style={{ background: 'var(--bg)' }}>
          {cols.map((c) => (
            <th key={c.key} style={{
              padding: '8px 8px 8px 0',
              textAlign: 'left', fontSize: 9.5, fontWeight: 700,
              textTransform: 'uppercase', letterSpacing: '0.07em',
              color: 'var(--text-muted)', whiteSpace: 'nowrap',
            }}>{c.label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 && (
          <tr>
            <td colSpan={cols.length} style={{ padding: '28px 0', textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>{emptyText}</td>
          </tr>
        )}
        {rows.map((row, i) => (
          <tr
            key={i}
            style={{ borderTop: '1px solid var(--border)', transition: 'background 0.15s' }}
            onMouseEnter={e => { e.currentTarget.style.background = 'rgba(0,51,141,0.05)'; }}
            onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}
          >
            {cols.map((c) => (
              <td key={c.key} style={{ padding: '10px 8px 10px 0', color: 'var(--text)', ...c.cellStyle }}>
                {c.render ? c.render(row[c.key], row) : row[c.key]}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/* ═══════════════════════════════════════════════════════════
   MAIN COMPONENT
   ═══════════════════════════════════════════════════════════ */
export default function BusinessKPI() {
  const [data, setData]       = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = () => {
    setRefreshing(true);
    apiGet('/api/cto/business-kpi').then((resp) => { setData(resp); setRefreshing(false); });
  };

  useEffect(() => { load(); }, []);

  if (!data) return <div className="page-loader"><div className="spinner" /></div>;

  const summary = data.summary || {};
  const churn   = summary.churn_rate       || 0;
  const roi     = summary.network_roi      || 0;
  const health  = summary.avg_health_score || 0;
  const growth  = summary.growth           || 0;

  /* ── Gradient defs for recharts ─────────────────────────── */
  const GradientDefs = () => (
    <defs>
      <linearGradient id="usersGrad"   x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"  stopColor="#00338D" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#00338D" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="revenueGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"  stopColor="#0d9488" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#0d9488" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="arpuGrad"    x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"  stopColor="#00338D" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#00338D" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="barGrad"     x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"  stopColor="#00338D" />
        <stop offset="100%" stopColor="#4F46E5" />
      </linearGradient>
    </defs>
  );

  return (
    <div style={{ padding: '0 0 40px' }}>

      {/* ── Page Header ─────────────────────────────────────── */}
      <motion.div {...FADE_UP(0)} style={{
        display: 'flex', alignItems: 'flex-start',
        justifyContent: 'space-between', marginBottom: 28,
        flexWrap: 'wrap', gap: 12,
      }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 26, fontWeight: 900, letterSpacing: '-0.02em', color: 'var(--text)' }}>
            Business KPI
          </h1>
          <p style={{ margin: '5px 0 0', fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.5 }}>
            Commercial impact — site users, revenue, growth, ARPU, utilization risk &amp; declining demand
          </p>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={load} disabled={refreshing} style={{
            display: 'flex', alignItems: 'center', gap: 6,
            padding: '8px 16px', background: 'var(--bg-card)',
            border: '1px solid var(--border)', borderRadius: 10,
            cursor: 'pointer', fontSize: 13, fontWeight: 600,
            color: 'var(--text)', boxShadow: 'var(--shadow-sm)',
            transition: 'all 0.2s',
          }}>
            <RefreshCw size={14} style={{ animation: refreshing ? 'spin 1s linear infinite' : 'none' }} />
            Refresh
          </button>
        </div>
      </motion.div>

      {/* ── KPI Cards (4-column) ─────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 24 }}>
        <KpiCard label="Total Users"
          value={Math.round(summary.total_users || 0).toLocaleString()}
          sub="Across all sites" colors={G.indigo} icon={Users} delay={0.05} />
        <KpiCard label="Avg Users / Site"
          value={Math.round(summary.avg_users || 0).toLocaleString()}
          sub="Per site average" colors={G.violet} icon={MapPin} delay={0.10} />
        <KpiCard label="Growth %"
          value={`${Number(growth).toFixed(2)}%`}
          sub="Week-over-week"
          colors={growth >= 0 ? G.emerald : G.rose}
          icon={growth >= 0 ? TrendingUp : TrendingDown}
          delay={0.15} />
        <KpiCard label="ARPU"
          value={Number(summary.arpu || 0).toFixed(2)}
          sub="Avg revenue per user" colors={G.amber} icon={IndianRupee} delay={0.20} />
        <KpiCard label="Revenue At Risk"
          value={Math.round(summary.revenue_at_risk || 0).toLocaleString()}
          sub="Declining or overloaded" colors={G.amber} icon={AlertTriangle} delay={0.25} />
        <KpiCard label="Churn Rate"
          value={`${Number(churn).toFixed(1)}%`}
          sub="Declining sites ratio"
          colors={churn > 20 ? G.rose : G.amber}
          icon={UserMinus} delay={0.30} />
        <KpiCard label="Network ROI"
          value={`${Math.round(roi).toLocaleString()}%`}
          sub="Revenue vs cost baseline" colors={G.emerald} icon={Zap} delay={0.35} />
        <KpiCard label="Avg Site Health"
          value={Number(health).toFixed(1)}
          sub="Composite score /100"
          colors={G.health(health)} icon={Activity} delay={0.40} />
      </div>

      {/* ── Users & Revenue Trend (side by side) ────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 20 }}>
        <Section title="Users Trend" delay={0.45} accentColor={G.indigo}>
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={data.trend || []} margin={{ top: 6, right: 16, left: 0, bottom: 0 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                domain={[
                  (dataMin) => Math.floor(dataMin * 0.98),
                  (dataMax) => Math.ceil(dataMax * 1.02),
                ]}
                allowDecimals={false} />
              <Tooltip content={<ChartTooltip />} />
              <Area type="monotoneX" dataKey="users" stroke="#00338D" strokeWidth={3} fill="url(#usersGrad)" name="Users" dot={false} activeDot={{ r: 5, fill: '#00338D', strokeWidth: 0 }} />
            </AreaChart>
          </ResponsiveContainer>
        </Section>

        <Section title="Revenue Trend" delay={0.47} accentColor={G.emerald}>
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={data.trend || []} margin={{ top: 6, right: 16, left: 0, bottom: 0 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                domain={[
                  (dataMin) => Math.floor(dataMin * 0.98),
                  (dataMax) => Math.ceil(dataMax * 1.02),
                ]}
                allowDecimals={false} />
              <Tooltip content={<ChartTooltip />} />
              <Area type="monotoneX" dataKey="revenue" stroke="#0d9488" strokeWidth={3} fill="url(#revenueGrad)" name="Revenue" dot={false} activeDot={{ r: 5, fill: '#0d9488', strokeWidth: 0 }} />
            </AreaChart>
          </ResponsiveContainer>
        </Section>
      </div>

      {/* ── ARPU Trend + Top 10 Sites ────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1.1fr 1fr', gap: 20, marginBottom: 20 }}>
        <Section title="ARPU Trend (30 Days)" delay={0.50} accentColor={G.amber}>
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={data.arpu_trend || []} margin={{ top: 6, right: 16, left: 0, bottom: 0 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                domain={[
                  (dataMin) => Math.floor(dataMin * 0.98),
                  (dataMax) => Math.ceil(dataMax * 1.02),
                ]}
                allowDecimals={false} />
              <Tooltip content={<ChartTooltip />} />
              <Area type="monotoneX" dataKey="arpu" stroke="#00338D" strokeWidth={3} fill="url(#arpuGrad)" name="ARPU" dot={false} activeDot={{ r: 5, fill: '#00338D', strokeWidth: 0 }} />
            </AreaChart>
          </ResponsiveContainer>
        </Section>

        <Section title="Top 10 Sites by Revenue" delay={0.55} accentColor={G.indigo}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={data.top_sites || []} layout="vertical" margin={{ left: 10, right: 16 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis type="category" dataKey="site_id" width={82} tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="revenue" radius={[0, 6, 6, 0]} name="Revenue">
                {(data.top_sites || []).map((_, idx) => (
                  <Cell key={idx} fill="url(#barGrad)" />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Section>
      </div>

      {/* ── Site Health Worst 10 ─────────────────────────────── */}
      <Section title="Site Health Score — Worst 10 Sites" delay={0.60} accentColor={G.rose} style={{ marginBottom: 20 }}>
        <PremiumTable
          emptyText="No site health data available"
          cols={[
            { key: 'site_id',      label: 'Site',         cellStyle: { fontWeight: 700 } },
            { key: 'health_score', label: 'Health Score', render: (v) => (
              <Pill value={v} positive={v >= 70} neutral={v >= 40 && v < 70} negative={v < 40} />
            )},
            { key: 'utilization',  label: 'Utilization',  render: (v) => (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <ProgressBar value={v}
                  colors={v > 80 ? G.rose : v > 60 ? G.amber : G.emerald} />
                <span style={{ fontSize: 12, color: 'var(--text-muted)', minWidth: 30 }}>{v}%</span>
              </div>
            )},
            { key: 'users',   label: 'Users',   render: (v) => (v || 0).toLocaleString() },
            { key: 'revenue', label: 'Revenue', render: (v) => (v || 0).toLocaleString() },
          ]}
          rows={data.site_health || []}
        />
      </Section>

      {/* ── Declining + Overloaded ───────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
        <Section title="Declining Sites" delay={0.65} accentColor={G.rose}>
          <PremiumTable
            emptyText="No declining sites"
            cols={[
              { key: 'site_id', label: 'Site',   cellStyle: { fontWeight: 600 } },
              { key: 'users',   label: 'Users',  render: (v) => (v || 0).toLocaleString() },
              { key: 'growth',  label: 'Growth', render: (v) => (
                <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                  <TrendingDown size={13} color="#ef4444" />
                  <Pill value={v} suffix="%" negative />
                </div>
              )},
            ]}
            rows={data.declining_sites || []}
          />
        </Section>

        <Section title="Overloaded Sites" delay={0.70} accentColor={G.amber}>
          <PremiumTable
            emptyText="No overloaded sites"
            cols={[
              { key: 'site_id',     label: 'Site',        cellStyle: { fontWeight: 600 } },
              { key: 'utilization', label: 'Utilization', render: (v) => (
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <ProgressBar value={v} colors={G.amber} />
                  <Pill value={v} suffix="%" neutral />
                </div>
              )},
              { key: 'revenue', label: 'Revenue', render: (v) => (v || 0).toLocaleString() },
            ]}
            rows={data.overloaded_sites || []}
          />
        </Section>
      </div>

      <style>{`
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
    </div>
  );
}
