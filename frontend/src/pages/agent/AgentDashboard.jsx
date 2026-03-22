import { useState, useEffect, useCallback } from 'react';
import {
  AreaChart, Area, BarChart, Bar, PieChart, Pie, Cell, Legend,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine,
  RadialBarChart, RadialBar,
} from 'recharts';
import { apiGet } from '../../api';
import { useAuth } from '../../AuthContext';

/* ── Themes ──────────────────────────────────────────────────────────────── */
const T_LIGHT = {
  bg:'#F0F2F5', surface:'#FFFFFF', surface2:'#F8FAFC',
  border:'#E2E8F0', text:'#0F172A', textSub:'#475569', muted:'#94A3B8',
  blue:'#00338D', blue2:'#0050c8', green:'#16A34A', amber:'#D97706',
  red:'#DC2626', purple:'#7C3AED', teal:'#0891B2',
  cardShadow:'0 1px 8px rgba(0,51,141,0.06)',
  kpiCardBg:'#fff', ribbonBg:'#fff', iconBg:'#f8fafc',
  progressTrack:'#e2e8f0', csatBg:'#f8fafc',
  gridStroke:'#f1f5f9',
};
const T_DARK = {
  bg:'#06101E', surface:'#0E1B30', surface2:'#152238',
  border:'#1C2E48', text:'#E2E8F0', textSub:'#94A3B8', muted:'#4A5568',
  blue:'#4DA3FF', blue2:'#60A5FA', green:'#34D399', amber:'#FBBF24',
  red:'#F87171', purple:'#A78BFA', teal:'#22D3EE',
  cardShadow:'0 4px 24px rgba(0,0,0,0.45)',
  kpiCardBg:'#0E1B30', ribbonBg:'#0E1B30', iconBg:'#152238',
  progressTrack:'#1C2E48', csatBg:'#152238',
  gridStroke:'#1C2E48',
};

const PRIORITY_COLORS = {
  critical:'#ef4444', high:'#f97316', medium:'#f59e0b', low:'#10b981',
};
const PRIORITY_GRADIENT = {
  critical:['#ef4444','#dc2626'], high:['#fb923c','#f97316'],
  medium:['#fbbf24','#f59e0b'], low:['#34d399','#10b981'],
};

/* ── Tiny SVG icons ──────────────────────────────────────────────────────── */
const IC = {
  clock:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>,
  check:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>,
  target: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>,
  star:   <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>,
  repeat: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="17 1 21 5 17 9"/><path d="M3 11V9a4 4 0 0 1 4-4h14"/><polyline points="7 23 3 19 7 15"/><path d="M21 13v2a4 4 0 0 1-4 4H3"/></svg>,
  alert:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>,
  zap:    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>,
  clip:   <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>,
  search: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>,
  aging:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>,
};

const IC_REFRESH = (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="23 4 23 10 17 10" /><polyline points="1 20 1 14 7 14" />
    <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
  </svg>
);

const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;600&display=swap');
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
@keyframes countUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.dash-kpi{transition:all .25s ease;cursor:default;}
.dash-kpi:hover{transform:translateY(-3px);box-shadow:0 12px 32px rgba(0,51,141,.12)!important;}
.dash-ribbon:hover{transform:translateY(-2px);box-shadow:0 8px 28px rgba(0,51,141,.14)!important;}
`;

/* ── Gauge (Power BI style ring) ─────────────────────────────────────────── */
function Gauge({ score, label, T, color }) {
  const c = color || (score >= 80 ? T.green : score >= 60 ? T.amber : T.red);
  const r = 40, cx = 50, cy = 50, circ = 2 * Math.PI * r, arc = circ * 0.75;
  const fill = (Math.min(score, 100) / 100) * arc;
  return (
    <div style={{ display:'flex', flexDirection:'column', alignItems:'center', gap: 2 }}>
      <svg width={100} height={72} style={{ overflow:'visible' }}>
        <defs>
          <linearGradient id={`g_${label}`} x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor={c} stopOpacity={1} />
            <stop offset="100%" stopColor={c} stopOpacity={0.5} />
          </linearGradient>
        </defs>
        <circle cx={cx} cy={cy} r={r} fill="none" stroke={T.progressTrack} strokeWidth={8}
          strokeDasharray={`${arc} ${circ - arc}`} strokeLinecap="round" transform={`rotate(-225 ${cx} ${cy})`} />
        <circle cx={cx} cy={cy} r={r} fill="none" stroke={`url(#g_${label})`} strokeWidth={8}
          strokeDasharray={`${fill} ${circ - fill}`} strokeLinecap="round" transform={`rotate(-225 ${cx} ${cy})`}
          style={{ transition:'stroke-dasharray 1.2s cubic-bezier(.4,0,.2,1)' }} />
        <text x={cx} y={cy + 4} textAnchor="middle" fontSize={18} fontWeight={800}
          fill={T.text} fontFamily="'IBM Plex Mono',monospace">{score}</text>
      </svg>
      <span style={{ fontSize: 9.5, fontWeight: 700, color: c, textTransform:'uppercase', letterSpacing:'0.05em' }}>{label}</span>
    </div>
  );
}

/* ── KPI Card ────────────────────────────────────────────────────────────── */
function KpiCard({ label, value, unit, icon, sub, alert: isAlert, T }) {
  const accent = isAlert ? T.red : T.blue;
  return (
    <div className="dash-kpi" style={{
      background: T.kpiCardBg, borderRadius: 12,
      border: `1px solid ${T.border}`,
      padding: '14px 14px 12px',
      display: 'flex', flexDirection: 'column', gap: 5,
      boxShadow: T.cardShadow,
      borderTop: `3px solid ${accent}`,
    }}>
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between' }}>
        <span style={{ fontSize: 9.5, fontWeight: 700, color: T.muted, textTransform:'uppercase', letterSpacing:'0.06em', lineHeight: 1.3 }}>{label}</span>
        <span style={{ color: accent, display:'flex', opacity: 0.7 }}>{icon}</span>
      </div>
      <div style={{ fontSize: 22, fontWeight: 800, color: T.text, lineHeight: 1, fontFamily:"'IBM Plex Mono',monospace", animation:'countUp .5s ease' }}>
        {value}
        <span style={{ fontSize: 10, fontWeight: 500, color: T.muted, marginLeft: 3 }}>{unit}</span>
      </div>
      {sub && <div style={{ fontSize: 9.5, color: T.muted, lineHeight: 1.4 }}>{sub}</div>}
    </div>
  );
}

/* ── Progress Bar ────────────────────────────────────────────────────────── */
function ProgressBar({ label, value, target, color, T }) {
  const pct = Math.min(value, 100);
  const met = value >= target;
  const barColor = met ? (color || T.green) : T.red;
  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'baseline', marginBottom: 5 }}>
        <span style={{ fontSize: 12, fontWeight: 600, color: T.textSub }}>{label}</span>
        <span style={{ fontSize: 11, fontWeight: 700, color: met ? T.green : T.red, fontFamily:"'IBM Plex Mono',monospace" }}>
          {value}%
          <span style={{ fontWeight: 400, color: T.muted, marginLeft: 4, fontFamily:'inherit' }}>/ {target}%</span>
        </span>
      </div>
      <div style={{ background: T.progressTrack, borderRadius: 6, height: 10, overflow:'hidden', position:'relative' }}>
        <div style={{
          height:'100%', width: `${pct}%`,
          background: `linear-gradient(90deg, ${barColor}, ${barColor}cc)`,
          borderRadius: 6, transition:'width 1.2s cubic-bezier(.4,0,.2,1)',
          boxShadow: `0 0 8px ${barColor}44`,
        }} />
        {/* Target marker */}
        <div style={{
          position:'absolute', top: -2, left: `${Math.min(target, 100)}%`,
          width: 2, height: 14, background: T.textSub, borderRadius: 1, opacity: 0.4,
        }} />
      </div>
    </div>
  );
}

/* ── Tooltip ─────────────────────────────────────────────────────────────── */
function DashTooltip({ active, payload, label, T }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: T.surface, border: `1px solid ${T.border}`,
      borderRadius: 10, padding: '10px 14px', boxShadow:'0 8px 32px rgba(0,0,0,.15)',
      fontSize: 11, minWidth: 130, backdropFilter:'blur(8px)',
    }}>
      <div style={{ fontWeight: 700, color: T.blue, marginBottom: 6, fontSize: 12, borderBottom:`1px solid ${T.border}`, paddingBottom: 5 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ display:'flex', alignItems:'center', gap: 6, marginBottom: 3 }}>
          <span style={{ width: 8, height: 8, borderRadius: 2, background: p.color, flexShrink: 0 }} />
          <span style={{ color: T.textSub, flex: 1, fontSize: 10.5 }}>{p.name}</span>
          <strong style={{ color: T.text, fontFamily:"'IBM Plex Mono',monospace", fontSize: 11 }}>{typeof p.value === 'number' ? p.value.toFixed(1) : p.value}</strong>
        </div>
      ))}
    </div>
  );
}

/* ── Donut center label (rendered as customized layer) ────────────────────── */
function DonutCenter({ width, height, total, T }) {
  const cx = width / 2;
  const cy = height * 0.46;
  return (
    <g>
      <text x={cx} y={cy - 4} textAnchor="middle" fontSize={22} fontWeight={800}
        fill={T.text} fontFamily="'IBM Plex Mono',monospace">{total}</text>
      <text x={cx} y={cy + 12} textAnchor="middle" fontSize={9} fontWeight={600}
        fill={T.muted}>TOTAL</text>
    </g>
  );
}

/* ═══════════════════════════════════════════════════════════════════════════
   MAIN
   ═══════════════════════════════════════════════════════════════════════════ */
export default function AgentDashboard() {
  const { user } = useAuth();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [dark, setDark] = useState(false);
  const T = dark ? T_DARK : T_LIGHT;

  const fetchDashboard = useCallback(async (silent = false) => {
    if (!silent) setRefreshing(true);
    try {
      const d = await apiGet('/api/agent/dashboard');
      setData(d);
      setLastUpdated(new Date());
    } catch {
      // keep previous data on error
    } finally {
      setLoading(false);
      if (!silent) setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchDashboard(false);
    const iv = setInterval(() => fetchDashboard(true), 30_000);
    return () => clearInterval(iv);
  }, [fetchDashboard]);

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const kpis    = data?.kpis    || {};
  const summary = data?.summary || {};
  const monthly = data?.monthly_trend    || [];
  const pChart  = data?.priority_chart   || [];
  const sChart  = data?.sla_priority_chart || [];

  const nn = (v) => Math.max(0, v ?? 0); // clamp: never display negative
  const nh = (v) => +Math.max(0, v ?? 0).toFixed(1); // clamp + 1 decimal for hour values

  const kpiRows = [
    { label:'MTTR',                      value: kpis.mttr ?? 0,                          unit:'hrs', icon: IC.clock,  sub:'Mean Time To Resolve' },
    { label:'SLA Compliance',            value: `${kpis.sla_compliance_rate ?? 0}`,      unit:'%',   icon: IC.check,  sub:'Resolved within SLA' },
    { label:'First Contact Resolution',  value: `${kpis.first_contact_resolution ?? 0}`, unit:'%',   icon: IC.target, sub:'No re-open needed' },
    { label:'CSAT Score',                value: kpis.csat ?? 0,                          unit:'/ 5', icon: IC.star,   sub: `${kpis.csat_pct ?? 0}% rated 4+` },
    { label:'Reopen Rate',               value: `${kpis.reopen_rate ?? 0}`,              unit:'%',   icon: IC.repeat, sub:'Tickets re-opened', alert: (kpis.reopen_rate ?? 0) > 10 },
    { label:'H/S Incident Resolution',   value: kpis.hs_incident_resolution_time ?? 0,   unit:'hrs', icon: IC.alert,  sub:'Critical & High tickets', alert: true },
    { label:'H/S Incident Response',     value: kpis.hs_incident_response_time ?? 0,     unit:'hrs', icon: IC.zap,    sub:'Avg first response' },
    { label:'Complaint Resolution',      value: kpis.complaint_resolution_time ?? 0,     unit:'hrs', icon: IC.clip,   sub:'All priorities' },
    { label:'RCA Timely Completion',     value: `${kpis.rca_timely_completion ?? 0}`,    unit:'%',   icon: IC.search, sub:'On-time root cause analyses' },
    { label:'Avg Open Ticket Age',       value: kpis.avg_aging_hours ?? 0,               unit:'hrs', icon: IC.aging,  sub:'Unresolved tickets', alert: (kpis.avg_aging_hours ?? 0) > 48 },
  ];

  const totalTickets = pChart.reduce((a, p) => a + (p.value || 0), 0);
  const TipC = (p) => <DashTooltip T={T} {...p} />;

  // Radial bar data for SLA/FCR/RCA
  const radialData = [
    { name:'RCA', value: Math.round(kpis.rca_timely_completion ?? 0), fill: T.teal },
    { name:'FCR', value: Math.round(kpis.first_contact_resolution ?? 0), fill: T.purple },
    { name:'SLA', value: Math.round(kpis.sla_compliance_rate ?? 0), fill: T.blue },
  ];

  return (
    <div style={{ background: T.bg, minHeight:'100vh', padding:'0 0 40px', fontFamily:"'Plus Jakarta Sans', system-ui, sans-serif" }}>
      <style>{CSS}</style>

      {/* ── Header bar ────────────────────────────────────────────── */}
      <div style={{
        background: dark ? 'linear-gradient(135deg,#0A1628,#0F2040)' : 'linear-gradient(135deg,#00338D,#005EB8,#0091DA)',
        padding:'16px 28px 18px', borderBottom:`1px solid ${T.border}`,
      }}>
        <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', flexWrap:'wrap', gap:12 }}>
          <div>
            <h1 style={{ margin:0, fontSize:20, fontWeight:800, color:'#fff', letterSpacing:'-0.01em' }}>
              Performance Dashboard
            </h1>
            <p style={{ margin:'3px 0 0', fontSize:12, color:'rgba(255,255,255,0.65)' }}>
              Welcome back, {user?.name}. Real-time KPI and SLA overview.
            </p>
          </div>
          <div style={{ display:'flex', alignItems:'center', gap:8 }}>
            {lastUpdated && (
              <span style={{ fontSize:10, color:'rgba(255,255,255,0.45)' }}>
                {lastUpdated.toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' })}
              </span>
            )}
            <button onClick={() => setDark(d => !d)}
              style={{ padding:'5px 12px', borderRadius:18, fontSize:10.5, fontWeight:600,
                background: dark ? '#F59E0B22' : 'rgba(255,255,255,0.15)',
                border:`1px solid ${dark ? '#F59E0B44' : 'rgba(255,255,255,0.25)'}`,
                color: dark ? '#F59E0B' : '#fff', cursor:'pointer' }}>
              {dark ? '☀️ Light' : '🌙 Dark'}
            </button>
            <button onClick={() => fetchDashboard(false)} disabled={refreshing}
              style={{ display:'flex', alignItems:'center', gap:5, padding:'5px 14px',
                borderRadius:18, fontSize:10.5, fontWeight:600,
                background:'rgba(255,255,255,0.15)', border:'1px solid rgba(255,255,255,0.25)',
                color:'#fff', cursor:'pointer', opacity: refreshing ? 0.5 : 1 }}>
              <span style={{ display:'inline-block', animation: refreshing ? 'spin 1s linear infinite' : 'none' }}>
                {IC_REFRESH}
              </span>
              {refreshing ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>
        </div>
      </div>

      <div style={{ padding:'20px 28px 0' }}>

        {/* ── Summary ribbon ──────────────────────────────────────── */}
        <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:14, marginBottom:24 }}>
          {[
            { label:'Total Tickets',      value: summary.total_tickets ?? 0, icon: IC.clip,  desc:'All assigned',      gradient:'linear-gradient(135deg,#00338D,#005EB8)' },
            { label:'Resolved',           value: summary.resolved ?? 0,      icon: IC.check, desc:'Closed successfully', gradient:'linear-gradient(135deg,#059669,#10b981)' },
            { label:'Open / In Progress', value: summary.open ?? 0,          icon: IC.clock, desc:'Awaiting resolution', gradient:'linear-gradient(135deg,#d97706,#f59e0b)' },
            { label:'Customer Feedbacks', value: summary.total_feedback ?? 0, icon: IC.star,  desc:'Ratings received',    gradient:'linear-gradient(135deg,#7c3aed,#a78bfa)' },
          ].map(({ label, value, icon, desc, gradient }) => (
            <div key={label} className="dash-ribbon" style={{
              background: T.ribbonBg, borderRadius:12,
              border:`1px solid ${T.border}`, padding:'16px 18px',
              boxShadow: T.cardShadow,
              display:'flex', alignItems:'center', gap:14,
              animation:'fadeIn .4s ease', transition:'all .25s ease',
            }}>
              <div style={{
                width:44, height:44, borderRadius:12, background:gradient,
                display:'flex', alignItems:'center', justifyContent:'center',
                color:'#fff', flexShrink:0, boxShadow:'0 4px 14px rgba(0,0,0,0.18)',
              }}>
                {icon}
              </div>
              <div>
                <div style={{ fontSize:10.5, color:T.muted, fontWeight:500, marginBottom:2 }}>{label}</div>
                <div style={{ fontSize:28, fontWeight:800, color:T.text, lineHeight:1, fontFamily:"'IBM Plex Mono',monospace" }}>{value}</div>
                <div style={{ fontSize:10, color:T.muted, marginTop:2 }}>{desc}</div>
              </div>
            </div>
          ))}
        </div>

        {/* ── KPI Cards ───────────────────────────────────────────── */}
        <SectionLabel T={T}>Core Performance Metrics</SectionLabel>
        <div style={{ display:'grid', gridTemplateColumns:'repeat(5,1fr)', gap:12, marginBottom:20 }}>
          {kpiRows.slice(0, 5).map(k => <KpiCard key={k.label} {...k} T={T} />)}
        </div>

        <SectionLabel T={T}>Response Time &amp; Quality</SectionLabel>
        <div style={{ display:'grid', gridTemplateColumns:'repeat(5,1fr)', gap:12, marginBottom:24 }}>
          {kpiRows.slice(5).map(k => <KpiCard key={k.label} {...k} T={T} />)}
        </div>

        {/* ── Charts Row 1: Trend + Donut ─────────────────────────── */}
        <div style={{ display:'grid', gridTemplateColumns:'1.2fr 0.8fr', gap:16, marginBottom:18 }}>

          {/* Monthly Resolution Trend — Power BI style area */}
          <ChartCard T={T} title="Monthly Resolution Trend">
            {monthly.length > 0 ? (
              <ResponsiveContainer width="100%" height={260}>
                <AreaChart data={monthly} margin={{ top:10, right:15, bottom:5, left:-5 }}>
                  <defs>
                    <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={T.blue} stopOpacity={0.4} />
                      <stop offset="50%" stopColor={T.blue} stopOpacity={0.15} />
                      <stop offset="100%" stopColor={T.blue} stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={T.gridStroke} vertical={false} />
                  <XAxis dataKey="month" tick={{ fontSize:11, fill:T.muted, fontWeight:500 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fontSize:11, fill:T.muted }} axisLine={false} tickLine={false} width={35} allowDecimals={false} />
                  <Tooltip content={TipC} />
                  {monthly.length > 1 && (() => {
                    const avg = monthly.reduce((a, r) => a + (r.resolved || 0), 0) / monthly.length;
                    return <ReferenceLine y={avg} stroke={T.muted} strokeDasharray="6 4" strokeWidth={1}
                      label={{ value: `Avg ${avg.toFixed(1)}`, position:'insideTopRight', fontSize:9, fill:T.muted, fontWeight:600 }} />;
                  })()}
                  <Area type="monotone" dataKey="resolved" stroke={T.blue} strokeWidth={3}
                    fill="url(#areaGrad)" name="Resolved"
                    dot={{ r:5, fill:T.surface, stroke:T.blue, strokeWidth:2.5 }}
                    activeDot={{ r:7, fill:T.blue, stroke:T.surface, strokeWidth:3 }} />
                </AreaChart>
              </ResponsiveContainer>
            ) : <EmptyState T={T} />}
          </ChartCard>

          {/* Priority Distribution — Power BI donut with center label */}
          <ChartCard T={T} title="Ticket Priority Distribution">
            {pChart.length > 0 ? (
              <ResponsiveContainer width="100%" height={260}>
                <PieChart>
                  <defs>
                    {pChart.map((entry, i) => {
                      const grad = PRIORITY_GRADIENT[entry.name] || ['#8b5cf6','#6d28d9'];
                      return (
                        <linearGradient key={i} id={`pie_${i}`} x1="0" y1="0" x2="1" y2="1">
                          <stop offset="0%" stopColor={grad[0]} />
                          <stop offset="100%" stopColor={grad[1]} />
                        </linearGradient>
                      );
                    })}
                  </defs>
                  <Pie data={pChart} dataKey="value" nameKey="name"
                    cx="50%" cy="46%" outerRadius={90} innerRadius={55}
                    paddingAngle={4} strokeWidth={0} cornerRadius={4}
                    label={({ name, percent }) => percent >= 0.05 ? `${name} ${(percent * 100).toFixed(0)}%` : ''}
                    labelLine={false}>
                    {pChart.map((_, i) => <Cell key={i} fill={`url(#pie_${i})`} />)}
                  </Pie>
                  <text x="50%" y="42%" textAnchor="middle" fontSize={22} fontWeight={800}
                    fill={T.text} fontFamily="'IBM Plex Mono',monospace" dominantBaseline="central">{totalTickets}</text>
                  <text x="50%" y="52%" textAnchor="middle" fontSize={9} fontWeight={600}
                    fill={T.muted} dominantBaseline="central">TOTAL</text>
                  <Tooltip content={TipC} />
                  <Legend iconType="circle" iconSize={8}
                    wrapperStyle={{ fontSize:10.5, color:T.textSub, paddingTop:4 }}
                    formatter={(val) => <span style={{ color: T.textSub, fontWeight: 600, fontSize: 10.5 }}>{val}</span>} />
                </PieChart>
              </ResponsiveContainer>
            ) : <EmptyState T={T} />}
          </ChartCard>
        </div>

        {/* ── Charts Row 2: SLA Bars + Key Targets ────────────────── */}
        <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:16, marginBottom:18 }}>

          {/* SLA Compliance by Priority — gradient bars + target line */}
          <ChartCard T={T} title="SLA Compliance by Priority">
            {sChart.length > 0 ? (
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={sChart} barSize={36} margin={{ top:5, right:15, bottom:5, left:-5 }}>
                  <defs>
                    {sChart.map((entry, i) => {
                      const c = entry.compliance >= 90 ? T.green : entry.compliance >= 70 ? T.amber : T.red;
                      return (
                        <linearGradient key={i} id={`sla_${i}`} x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor={c} stopOpacity={1} />
                          <stop offset="100%" stopColor={c} stopOpacity={0.55} />
                        </linearGradient>
                      );
                    })}
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={T.gridStroke} vertical={false} />
                  <XAxis dataKey="priority" tick={{ fontSize:11, fill:T.muted, fontWeight:600 }} axisLine={false} tickLine={false} />
                  <YAxis domain={[0, 100]} unit="%" tick={{ fontSize:10, fill:T.muted }} axisLine={false} tickLine={false} width={40} />
                  <Tooltip content={TipC} formatter={v => `${v}%`} />
                  <ReferenceLine y={90} stroke={T.green} strokeDasharray="6 4" strokeWidth={1.5} strokeOpacity={0.5}
                    label={{ value:'Target 90%', position:'insideTopRight', fontSize:9, fill:T.green, fontWeight:700 }} />
                  <Bar dataKey="compliance" name="SLA %" radius={[8, 8, 0, 0]}
                    label={{ position:'top', fontSize:10, fontWeight:700, fill:T.textSub, fontFamily:"'IBM Plex Mono',monospace", formatter: v => `${v}%` }}>
                    {sChart.map((_, i) => <Cell key={i} fill={`url(#sla_${i})`} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            ) : <EmptyState T={T} />}
          </ChartCard>

          {/* Key Targets — progress bars + radial chart + CSAT */}
          <ChartCard T={T} title="Key Targets">
            <ProgressBar label="SLA Compliance Rate"      value={kpis.sla_compliance_rate ?? 0}      target={95} color={T.blue}  T={T} />
            <ProgressBar label="First Contact Resolution" value={kpis.first_contact_resolution ?? 0} target={80} color={T.purple} T={T} />
            <ProgressBar label="RCA Timely Completion"    value={kpis.rca_timely_completion ?? 0}    target={90} color={T.teal} T={T} />

            {/* Radial bar chart — Power BI style */}
            <div style={{ display:'flex', alignItems:'center', justifyContent:'center', margin:'8px 0' }}>
              <ResponsiveContainer width={200} height={120}>
                <RadialBarChart cx="50%" cy="50%" innerRadius="30%" outerRadius="95%" barSize={10}
                  data={radialData} startAngle={180} endAngle={0}>
                  <RadialBar background={{ fill: T.progressTrack }} dataKey="value" cornerRadius={5}
                    label={{ position:'insideStart', fontSize:9, fontWeight:700, fill:'#fff' }} />
                </RadialBarChart>
              </ResponsiveContainer>
              <div style={{ display:'flex', flexDirection:'column', gap:6, marginLeft:8 }}>
                {radialData.map(d => (
                  <div key={d.name} style={{ display:'flex', alignItems:'center', gap:6 }}>
                    <span style={{ width:8, height:8, borderRadius:2, background:d.fill, flexShrink:0 }} />
                    <span style={{ fontSize:10, color:T.textSub, fontWeight:600 }}>{d.name}</span>
                    <span style={{ fontSize:11, fontWeight:800, color:T.text, fontFamily:"'IBM Plex Mono',monospace", marginLeft:4 }}>{d.value}%</span>
                  </div>
                ))}
              </div>
            </div>

            {/* CSAT Stars */}
            <div style={{ padding:'12px 14px', background:T.csatBg, borderRadius:10, border:`1px solid ${T.border}`, marginTop:8 }}>
              <div style={{ fontSize:9.5, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:6 }}>
                Customer Satisfaction (CSAT)
              </div>
              <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                <div style={{ fontSize:24, letterSpacing:3, lineHeight:1 }}>
                  {[1,2,3,4,5].map(i => (
                    <span key={i} style={{ color: i <= Math.round(kpis.csat ?? 0) ? '#f59e0b' : T.progressTrack }}>★</span>
                  ))}
                </div>
                <span style={{ fontSize:22, fontWeight:800, color:T.text, fontFamily:"'IBM Plex Mono',monospace" }}>{kpis.csat ?? 0}</span>
                <span style={{ fontSize:11, color:T.muted }}>/ 5.0</span>
              </div>
              <div style={{ fontSize:10.5, color:T.textSub, marginTop:4 }}>
                {kpis.csat_pct ?? 0}% of customers rated 4 or higher
              </div>
            </div>
          </ChartCard>
        </div>

        {/* ── Aging alert ─────────────────────────────────────────── */}
        {(kpis.avg_aging_hours ?? 0) > 48 && (
          <div style={{
            display:'flex', alignItems:'center', gap:14,
            padding:'14px 20px', borderRadius:10,
            background: dark ? '#7f1d1d22' : '#fef2f2',
            border:`1px solid ${dark ? '#991b1b44' : '#fecaca'}`,
            animation:'fadeIn .4s ease',
          }}>
            <span style={{ color:T.red, flexShrink:0 }}>{IC.alert}</span>
            <div>
              <div style={{ fontSize:13, fontWeight:700, color:T.red, marginBottom:2 }}>High Aging Alert</div>
              <div style={{ fontSize:12, color: dark ? '#fca5a5' : '#b91c1c' }}>
                Average open ticket age is <strong>{kpis.avg_aging_hours} hrs</strong>. Some tickets may be approaching SLA breach.
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Helpers ──────────────────────────────────────────────────────────────── */
function ChartCard({ T, title, children }) {
  return (
    <div style={{
      background:T.surface, borderRadius:12,
      border:`1px solid ${T.border}`, boxShadow:T.cardShadow,
      padding:'16px 20px', animation:'fadeIn .4s ease',
    }}>
      <div style={{ fontSize:11.5, fontWeight:700, color:T.blue, textTransform:'uppercase', letterSpacing:'0.04em', marginBottom:10 }}>
        {title}
      </div>
      {children}
    </div>
  );
}

function SectionLabel({ T, children }) {
  return (
    <div style={{ fontSize:10, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:1.2, marginBottom:10, paddingLeft:2 }}>
      {children}
    </div>
  );
}

function EmptyState({ T }) {
  return (
    <div style={{ height:200, display:'flex', alignItems:'center', justifyContent:'center', fontSize:12, color:T.muted, flexDirection:'column', gap:6 }}>
      <span style={{ fontSize:28, opacity:0.4 }}>—</span>
      No data available yet
    </div>
  );
}
