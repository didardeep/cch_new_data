import { useState, useEffect, useCallback } from 'react';
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts';
import { apiGet } from '../../api';
import { useAuth } from '../../AuthContext';

const CHART_COLORS = ['#00338D', '#0050c8', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

const PRIORITY_COLORS = {
  critical: '#ef4444',
  high: '#f97316',
  medium: '#f59e0b',
  low: '#10b981',
};

/* ── Tiny SVG icons ─────────────────────────────────────────────────────────── */
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

function KpiCard({ label, value, unit, icon, sub, alert }) {
  return (
    <div style={{
      background: '#fff',
      borderRadius: 10,
      border: '1px solid #e2e8f0',
      padding: '16px 16px 14px',
      display: 'flex',
      flexDirection: 'column',
      gap: 6,
      boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
      borderLeft: alert ? '3px solid #ef4444' : '3px solid #00338d',
    }}>
      {/* Icon + label row */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
        <span style={{ color: alert ? '#ef4444' : '#94a3b8', display: 'flex' }}>{icon}</span>
        <span style={{ fontSize: 10.5, fontWeight: 600, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.04em', lineHeight: 1.3 }}>{label}</span>
      </div>

      {/* Value */}
      <div style={{ fontSize: 24, fontWeight: 800, color: '#0f172a', lineHeight: 1, marginTop: 2 }}>
        {value}
        <span style={{ fontSize: 11, fontWeight: 500, color: '#94a3b8', marginLeft: 4 }}>{unit}</span>
      </div>

      {/* Sub */}
      {sub && (
        <div style={{ fontSize: 10, color: '#94a3b8', lineHeight: 1.4 }}>{sub}</div>
      )}
    </div>
  );
}

function ProgressBar({ label, value, target, color }) {
  const pct = Math.min(value, 100);
  const met = value >= target;
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 6 }}>
        <span style={{ fontSize: 13, color: 'var(--text-secondary)' }}>{label}</span>
        <span style={{ fontSize: 12, fontWeight: 700, color: met ? 'var(--success)' : 'var(--danger)' }}>
          {value}%
          <span style={{ fontWeight: 400, color: 'var(--text-muted)', marginLeft: 4 }}>/ {target}% target</span>
        </span>
      </div>
      <div style={{ background: 'var(--bg)', borderRadius: 6, height: 8, overflow: 'hidden' }}>
        <div style={{
          height: '100%', width: `${pct}%`,
          background: met ? (color || 'var(--success)') : 'var(--danger)',
          borderRadius: 6, transition: 'width 1s ease',
        }} />
      </div>
    </div>
  );
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="rpt-tooltip">
      <div className="rpt-tooltip-label">{label}</div>
      {payload.map((p, i) => (
        <div key={i} className="rpt-tooltip-row">
          <span className="rpt-tooltip-dot" style={{ background: p.color }} />
          {p.name}: <strong>{p.value}</strong>
        </div>
      ))}
    </div>
  );
};

const IC_REFRESH = (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="23 4 23 10 17 10" /><polyline points="1 20 1 14 7 14" />
    <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
  </svg>
);

export default function AgentDashboard() {
  const { user } = useAuth();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

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
    // Auto-refresh every 30 s so KPIs stay live after ticket resolutions / assignments
    const iv = setInterval(() => fetchDashboard(true), 30_000);
    return () => clearInterval(iv);
  }, [fetchDashboard]);

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const kpis    = data?.kpis    || {};
  const summary = data?.summary || {};
  const monthly = data?.monthly_trend    || [];
  const pChart  = data?.priority_chart   || [];
  const sChart  = data?.sla_priority_chart || [];

  const kpiRows = [
    { label: 'MTTR',                       value: kpis.mttr ?? 0,                         unit: 'hrs', icon: IC.clock,  sub: 'Mean Time To Resolve' },
    { label: 'SLA Compliance Rate',        value: `${kpis.sla_compliance_rate ?? 0}`,     unit: '%',   icon: IC.check,  sub: 'Resolved within SLA' },
    { label: 'First Contact Resolution',   value: `${kpis.first_contact_resolution ?? 0}`,unit: '%',   icon: IC.target, sub: 'No re-open needed' },
    { label: 'CSAT Score',                 value: kpis.csat ?? 0,                         unit: '/ 5', icon: IC.star,   sub: `${kpis.csat_pct ?? 0}% rated 4+` },
    { label: 'Reopen Rate',                value: `${kpis.reopen_rate ?? 0}`,             unit: '%',   icon: IC.repeat, sub: 'Tickets re-opened', alert: (kpis.reopen_rate ?? 0) > 10 },
    { label: 'H/S Incident Resolution',    value: kpis.hs_incident_resolution_time ?? 0,  unit: 'hrs', icon: IC.alert,  sub: 'Critical & High tickets', alert: true },
    { label: 'H/S Incident Response',      value: kpis.hs_incident_response_time ?? 0,    unit: 'hrs', icon: IC.zap,    sub: 'Avg first response' },
    { label: 'Complaint Resolution Time',  value: kpis.complaint_resolution_time ?? 0,    unit: 'hrs', icon: IC.clip,   sub: 'All priorities' },
    { label: 'RCA Timely Completion',      value: `${kpis.rca_timely_completion ?? 0}`,   unit: '%',   icon: IC.search, sub: 'On-time root cause analyses' },
    { label: 'Avg Open Ticket Age',        value: kpis.avg_aging_hours ?? 0,              unit: 'hrs', icon: IC.aging,  sub: 'Unresolved tickets', alert: (kpis.avg_aging_hours ?? 0) > 48 },
  ];

  return (
    <div>
      {/* Page header */}
      <div className="page-header" style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12 }}>
        <div>
          <h1>Performance Dashboard</h1>
          <p>Welcome back, {user?.name}. Your real-time KPI and SLA overview.</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexShrink: 0 }}>
          {lastUpdated && (
            <span style={{ fontSize: 11, color: 'var(--text-muted, #94a3b8)' }}>
              Updated {lastUpdated.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </span>
          )}
          <button
            onClick={() => fetchDashboard(false)}
            disabled={refreshing}
            className="btn btn-ghost btn-sm"
            style={{ display: 'flex', alignItems: 'center', gap: 6, opacity: refreshing ? 0.6 : 1 }}
          >
            <span style={{ display: 'inline-block', animation: refreshing ? 'spin 1s linear infinite' : 'none' }}>
              {IC_REFRESH}
            </span>
            {refreshing ? 'Refreshing…' : 'Refresh'}
          </button>
        </div>
      </div>

      {/* ── Summary ribbon ───────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 14, marginBottom: 28 }}>
        {[
          { label: 'Total Tickets',      value: summary.total_tickets ?? 0, icon: IC.clip,  desc: 'All assigned tickets' },
          { label: 'Resolved',           value: summary.resolved ?? 0,      icon: IC.check, desc: 'Successfully closed' },
          { label: 'Open / In Progress', value: summary.open ?? 0,          icon: IC.clock, desc: 'Awaiting resolution' },
          { label: 'Customer Feedbacks', value: summary.total_feedback ?? 0,icon: IC.star,  desc: 'Ratings received' },
        ].map(({ label, value, icon, desc }) => (
          <div key={label} style={{
            background: '#fff',
            borderRadius: 10,
            border: '1px solid #e2e8f0',
            borderLeft: '3px solid #00338d',
            padding: '18px 20px',
            boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
            display: 'flex',
            alignItems: 'center',
            gap: 16,
          }}>
            <div style={{
              width: 42, height: 42, borderRadius: 10,
              background: '#f8fafc', border: '1px solid #e2e8f0',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: '#64748b', flexShrink: 0,
            }}>
              {icon}
            </div>
            <div>
              <div style={{ fontSize: 11, color: '#64748b', fontWeight: 500, marginBottom: 3 }}>{label}</div>
              <div style={{ fontSize: 30, fontWeight: 800, color: '#0f172a', lineHeight: 1 }}>{value}</div>
              <div style={{ fontSize: 10.5, color: '#94a3b8', marginTop: 3 }}>{desc}</div>
            </div>
          </div>
        ))}
      </div>

      {/* ── KPI Cards ────────────────────────────────────────────────── */}
      <div style={{ marginBottom: 4 }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: 1.2, marginBottom: 10, paddingLeft: 2 }}>
          Core Performance Metrics
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 12, marginBottom: 20 }}>
          {kpiRows.slice(0, 5).map(k => <KpiCard key={k.label} {...k} />)}
        </div>
      </div>

      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: 1.2, marginBottom: 10, paddingLeft: 2 }}>
          Response Time &amp; Quality
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 12 }}>
          {kpiRows.slice(5).map(k => <KpiCard key={k.label} {...k} />)}
        </div>
      </div>

      {/* Charts row 1 */}
      <div className="rpt-chart-grid cols-2" style={{ marginBottom: 20 }}>
        {/* Monthly resolution trend */}
        <div className="rpt-chart-card">
          <div className="rpt-chart-title">Monthly Resolution Trend</div>
          {monthly.length > 0 ? (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={monthly}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="month" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} />
                <YAxis tick={{ fontSize: 11, fill: 'var(--text-muted)' }} />
                <Tooltip content={<CustomTooltip />} />
                <Line type="monotone" dataKey="resolved" stroke="#00338D" strokeWidth={2.5} dot={{ r: 4, fill: '#00338D' }} name="Resolved" />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="empty-state"><p>No resolution data yet.</p></div>
          )}
        </div>

        {/* Priority distribution */}
        <div className="rpt-chart-card">
          <div className="rpt-chart-title">Ticket Priority Distribution</div>
          {pChart.length > 0 ? (
            <ResponsiveContainer width="100%" height={220}>
              <PieChart>
                <Pie
                  data={pChart} dataKey="value" nameKey="name"
                  cx="50%" cy="50%" outerRadius={80} innerRadius={45}
                  label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                  labelLine={false}
                >
                  {pChart.map((entry, i) => (
                    <Cell key={i} fill={PRIORITY_COLORS[entry.name] || CHART_COLORS[i % CHART_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip content={<CustomTooltip />} />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="empty-state"><p>No ticket data yet.</p></div>
          )}
        </div>
      </div>

      {/* Charts row 2 */}
      <div className="rpt-chart-grid cols-2" style={{ marginBottom: 20 }}>
        {/* SLA compliance by priority */}
        <div className="rpt-chart-card">
          <div className="rpt-chart-title">SLA Compliance by Priority</div>
          {sChart.length > 0 ? (
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={sChart} barSize={28}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="priority" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} />
                <YAxis domain={[0, 100]} unit="%" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} />
                <Tooltip content={<CustomTooltip />} formatter={v => `${v}%`} />
                <Bar dataKey="compliance" name="SLA Compliance %" radius={[4, 4, 0, 0]}>
                  {sChart.map((entry, i) => (
                    <Cell key={i} fill={entry.compliance >= 90 ? '#10b981' : entry.compliance >= 70 ? '#f59e0b' : '#ef4444'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="empty-state"><p>No resolved tickets yet.</p></div>
          )}
        </div>

        {/* KPI progress bars + CSAT */}
        <div className="rpt-chart-card">
          <div className="rpt-chart-title">Key Targets</div>
          <ProgressBar label="SLA Compliance Rate"      value={kpis.sla_compliance_rate ?? 0}      target={95} color="#00338D" />
          <ProgressBar label="First Contact Resolution" value={kpis.first_contact_resolution ?? 0} target={80} color="#0050c8" />
          <ProgressBar label="RCA Timely Completion"    value={kpis.rca_timely_completion ?? 0}    target={90} color="#10b981" />
          {/* CSAT stars */}
          <div style={{ marginTop: 18, padding: '14px 16px', background: '#f8fafc', borderRadius: 8, border: '1px solid #e2e8f0' }}>
            <div style={{ fontSize: 10.5, fontWeight: 700, color: '#475569', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Customer Satisfaction (CSAT)
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <div style={{ fontSize: 22, letterSpacing: 2 }}>
                {[1,2,3,4,5].map(i => (
                  <span key={i} style={{ color: i <= Math.round(kpis.csat ?? 0) ? '#f59e0b' : '#e2e8f0' }}>★</span>
                ))}
              </div>
              <span style={{ fontSize: 20, fontWeight: 700, color: '#0f172a' }}>{kpis.csat ?? 0}</span>
              <span style={{ fontSize: 12, color: '#94a3b8' }}>/ 5.0</span>
            </div>
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>
              {kpis.csat_pct ?? 0}% of customers rated 4 or higher
            </div>
          </div>
        </div>
      </div>

      {/* Aging alert */}
      {(kpis.avg_aging_hours ?? 0) > 48 && (
        <div style={{
          display: 'flex', alignItems: 'center', gap: 14,
          padding: '14px 20px', borderRadius: 'var(--radius-sm)',
          background: 'var(--danger-bg)', border: '1px solid #fecaca',
        }}>
          <span style={{ color: 'var(--danger)', flexShrink: 0 }}>{IC.alert}</span>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--danger)', marginBottom: 2 }}>High Aging Alert</div>
            <div style={{ fontSize: 13, color: '#b91c1c' }}>
              Average open ticket age is {kpis.avg_aging_hours} hours. Some tickets may be approaching SLA breach.
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
