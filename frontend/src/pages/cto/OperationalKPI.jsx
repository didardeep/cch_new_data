import React, { useEffect, useState } from 'react';
import { Cell, Line, LineChart, CartesianGrid, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useNavigate } from 'react-router-dom';
import { apiGet, apiPut } from '../../api';

// ── Priority config ───────────────────────────────────────────────────────────
const PRIORITY = {
  critical: { label: 'P0-CRITICAL', color: '#ef4444', bg: 'rgba(239,68,68,0.1)'  },
  high:     { label: 'P1-HIGH',     color: '#f97316', bg: 'rgba(249,115,22,0.1)' },
  medium:   { label: 'P2-MED',      color: '#0d9488', bg: 'rgba(13,148,136,0.1)' },
  low:      { label: 'P3-LOW',      color: '#64748b', bg: 'rgba(100,116,139,0.1)'},
};

const STATUS_LABEL = {
  pending:           'Open',
  in_progress:       'In Progress',
  resolved:          'Resolved',
  escalated:         'Escalated',
  manager_escalated: 'Escalated',
  open:              'Open',
};

const DONUT_COLORS = ['#00338D', '#0d9488', '#94a3b8', '#ef4444', '#f97316'];

// ── Sub-components ────────────────────────────────────────────────────────────

function SLAGauge({ value = 0 }) {
  const pct = Math.min(Math.max(Math.round(value), 0), 100);
  const fillColor = pct >= 90 ? '#10b981' : pct >= 70 ? '#f59e0b' : '#ef4444';
  const gaugeData = [
    { v: pct,       fill: fillColor   },
    { v: 100 - pct, fill: 'var(--border)' },
  ];
  return (
    <div style={{ position: 'relative', width: 120, height: 70, margin: '0 auto' }}>
      <PieChart width={120} height={120} style={{ position: 'absolute', top: -28 }}>
        <Pie
          data={gaugeData}
          cx={60} cy={76}
          startAngle={180} endAngle={0}
          innerRadius={40} outerRadius={56}
          dataKey="v" stroke="none"
        >
          {gaugeData.map((d, i) => <Cell key={i} fill={d.fill} />)}
        </Pie>
      </PieChart>
      <div style={{
        position: 'absolute', bottom: 0, left: 0, right: 0,
        textAlign: 'center', fontSize: 22, fontWeight: 800,
        color: fillColor, lineHeight: 1,
      }}>
        {pct}%
      </div>
    </div>
  );
}

function Stars({ value }) {
  const rounded = Math.round(value * 2) / 2;
  return (
    <div style={{ display: 'flex', gap: 3, marginTop: 6 }}>
      {[1, 2, 3, 4, 5].map(i => (
        <svg key={i} width={14} height={14} viewBox="0 0 24 24"
          fill={i <= rounded ? '#f59e0b' : 'var(--border)'}>
          <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
        </svg>
      ))}
    </div>
  );
}

function PriorityBadge({ priority }) {
  const cfg = PRIORITY[priority] || PRIORITY.low;
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      background: cfg.bg, color: cfg.color,
      border: `1px solid ${cfg.color}44`,
      borderRadius: 5, padding: '2px 9px',
      fontSize: 10, fontWeight: 800, letterSpacing: 0.4,
    }}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: cfg.color, flexShrink: 0 }} />
      {cfg.label}
    </span>
  );
}

function SlaTimer({ clock, remaining }) {
  const color = remaining < 0 ? '#ef4444' : remaining < 600 ? '#f59e0b' : '#10b981';
  return (
    <span style={{ fontFamily: 'monospace', fontWeight: 700, fontSize: 13, color, letterSpacing: 0.5 }}>
      {clock}
    </span>
  );
}


// ── Main component ────────────────────────────────────────────────────────────
export default function OperationalKPI() {
  const [data, setData] = useState(null);
  const [reviewTicket, setReviewTicket] = useState(null);
  const [closing, setClosing] = useState(null);
  const navigate = useNavigate();

  const [cdo, setCdo] = useState(null);
  const reload = () => {
    apiGet('/api/cto/operational-kpi').then(setData);
    apiGet('/api/cto/cdo-engagement-kpi').then(setCdo);
  };
  useEffect(() => { reload(); }, []);

  const handleResolve = async (inc) => {
    if (!window.confirm(`Resolve ticket #${inc.id}?`)) return;
    setClosing(inc.id);
    try {
      await apiPut(`/api/manager/tickets/${inc.db_id}`, { status: 'resolved' });
      reload();
      setReviewTicket(null);
    } catch (e) { alert('Failed to resolve: ' + e.message); }
    setClosing(null);
  };

  if (!data) return <div className="page-loader"><div className="spinner" /></div>;

  const s          = data.summary || {};
  const incidents  = data.critical_incidents || [];

  // Donut data
  const statusBreakdown = (data.status_breakdown || []).map((item, i) => ({
    name:  STATUS_LABEL[item.name] || item.name,
    value: item.value,
    fill:  DONUT_COLORS[i % DONUT_COLORS.length],
  }));
  const totalTickets = statusBreakdown.reduce((acc, r) => acc + r.value, 0) || s.total_tickets || 0;

  // Agent workload sorted descending
  const agentWorkload = [...(data.agent_workload || [])].sort((a, b) => b.tickets - a.tickets);
  const maxTickets    = agentWorkload[0]?.tickets || 1;

  return (
    <div>
      {/* ── Page Header — uses global .page-header which has the left colour strip */}
      <div className="page-header">
        <h1>Operational KPI</h1>
        <p>Service operations performance — SLA compliance, breaches, resolution speed, CSAT &amp; agent workload.</p>
      </div>

      {/* ── Top KPI Cards */}
      <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>

        {/* Total Tickets — accent hero card */}
        <div className="stat-card" style={{
          background: 'linear-gradient(135deg, var(--primary) 0%, #0f2444 100%)',
          borderTop: 'none', borderColor: 'transparent',
          position: 'relative', overflow: 'hidden',
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <div className="stat-card-label" style={{ color: 'rgba(255,255,255,0.65)' }}>Total Tickets</div>
            <span style={{
              background: '#10b981', color: '#fff',
              fontSize: 9, fontWeight: 800, padding: '2px 8px',
              borderRadius: 20, letterSpacing: 0.8,
            }}>LIVE</span>
          </div>
          <div className="stat-card-value" style={{ color: '#fff', fontSize: 34, marginTop: 8 }}>
            {(s.total_tickets || 0).toLocaleString()}
          </div>
          <div className="stat-card-sub" style={{ color: (s.ticket_growth_pct || 0) >= 0 ? '#ef4444' : '#10b981' }}>
            {(s.ticket_growth_pct || 0) >= 0 ? '↑' : '↓'} {s.ticket_growth_pct > 0 ? '+' : ''}{s.ticket_growth_pct || 0}% from last week
          </div>
          <div style={{ position: 'absolute', right: -16, top: -16, width: 80, height: 80, borderRadius: '50%', background: 'rgba(255,255,255,0.06)' }} />
          <div style={{ position: 'absolute', right: 12, bottom: -24, width: 60, height: 60, borderRadius: '50%', background: 'rgba(255,255,255,0.05)' }} />
        </div>

        {/* SLA Compliance */}
        <div className="stat-card" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <div className="stat-card-label" style={{ alignSelf: 'flex-start', marginBottom: 10 }}>SLA Compliance</div>
          <SLAGauge value={s.sla_compliance || 0} />
        </div>

        {/* SLA Breaches */}
        <div className="stat-card" style={{ borderTopColor: '#ef4444' }}>
          <div className="stat-card-label">SLA Breaches</div>
          <div className="stat-card-value" style={{ color: '#ef4444', fontSize: 34, marginTop: 8 }}>
            {s.sla_breaches || 0}
          </div>
          <span style={{
            display: 'inline-flex', alignItems: 'center', gap: 5, marginTop: 4,
            background: 'rgba(239,68,68,0.1)', color: '#ef4444',
            borderRadius: 20, padding: '2px 10px', fontSize: 11, fontWeight: 600,
          }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#ef4444' }} />
            {s.top_breach_category || 'Breached'}
          </span>
        </div>

        {/* Avg Resolution */}
        <div className="stat-card">
          <div className="stat-card-label">Avg Resolution</div>
          <div style={{ marginTop: 8 }}>
            <span className="stat-card-value" style={{ fontSize: 34 }}>{s.avg_resolution_time || 0}</span>
            <span className="stat-card-sub" style={{ marginLeft: 5 }}>hrs</span>
          </div>
          <div className="stat-card-sub" style={{ color: (s.resolution_change || 0) <= 0 ? '#10b981' : '#ef4444', marginTop: 4 }}>
            {(s.resolution_change || 0) <= 0 ? '' : '+'}{s.resolution_change || 0}h {(s.resolution_change || 0) <= 0 ? 'improvement' : 'slower'}
          </div>
        </div>

        {/* CSAT Score */}
        <div className="stat-card" style={{ borderTopColor: '#f59e0b' }}>
          <div className="stat-card-label">CSAT Score</div>
          <div className="stat-card-value" style={{ fontSize: 34, marginTop: 8 }}>{s.csat || 0}</div>
          <Stars value={s.csat || 0} />
        </div>
      </div>

      {/* ── CDO Row 1: Workload Forecast + Resolution Funnel ── */}
      {cdo && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 24 }}>

          {/* Workload Forecast — Premium */}
          <div style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 20,
            boxShadow: '0 8px 32px rgba(0,0,0,0.06), inset 0 1px 0 rgba(255,255,255,0.5)',
            overflow: 'hidden', transition: 'transform 0.2s, box-shadow 0.2s',
          }}>
            <div style={{ height: 3, background: 'linear-gradient(90deg, #4F46E5, #06B6D4)' }} />
            <div style={{ padding: '22px 26px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4 }}>
                <div style={{ width: 32, height: 32, borderRadius: 10, background: 'linear-gradient(135deg, #4F46E5, #06B6D4)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" strokeLinecap="round"><path d="M3 12h4l3 8 4-16 3 8h4" /></svg>
                </div>
                <div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>Predictive Workload Forecast</div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>AI-predicted ticket volume for next 7 days</div>
                </div>
              </div>
              <ResponsiveContainer width="100%" height={190}>
                <LineChart data={cdo.workload.series} margin={{ top: 16, right: 10, left: -10, bottom: 0 }}>
                  <defs>
                    <linearGradient id="wlGrad" x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stopColor="#4F46E5" /><stop offset="100%" stopColor="#06B6D4" /></linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.4} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                    tickFormatter={d => { const dt = new Date(d); return `${['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dt.getDay()]} ${String(dt.getDate()).padStart(2,'0')}/${String(dt.getMonth()+1).padStart(2,'0')}`; }} />
                  <YAxis tick={{ fontSize: 9, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} allowDecimals={false} />
                  <Tooltip content={({ active, payload }) => {
                    if (!active || !payload?.length) return null;
                    const d = payload[0]?.payload;
                    return (
                      <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, padding: '10px 14px', fontSize: 11, boxShadow: '0 12px 36px rgba(0,0,0,0.12)' }}>
                        <div style={{ fontWeight: 700, color: 'var(--text)', marginBottom: 4 }}>{d.date}</div>
                        <div style={{ color: d.type === 'forecast' ? '#4F46E5' : 'var(--text-muted)', fontWeight: 600 }}>
                          {d.count} tickets {d.type === 'forecast' ? '(predicted)' : ''}
                        </div>
                      </div>
                    );
                  }} />
                  <Line type="monotone" dataKey="count" stroke="#4F46E5" strokeWidth={3}
                    activeDot={{ r: 6, fill: '#4F46E5', stroke: '#fff', strokeWidth: 2 }}
                    dot={(props) => {
                      const { cx, cy, payload } = props;
                      return (
                        <circle cx={cx} cy={cy} r={payload.type === 'forecast' ? 5 : 4}
                          fill={payload.type === 'forecast' ? '#4F46E5' : '#fff'}
                          stroke="#4F46E5" strokeWidth={2.5} />
                      );
                    }} />
                </LineChart>
              </ResponsiveContainer>
              <div style={{ display: 'flex', justifyContent: 'space-around', marginTop: 16, textAlign: 'center' }}>
                {[
                  { label: 'Current Rate', value: `${cdo.workload.current_rate}%`, color: cdo.workload.current_rate >= 90 ? '#10B981' : '#F59E0B' },
                  { label: 'Target', value: `${cdo.workload.target_rate}%`, color: '#4F46E5' },
                  { label: 'To Target', value: cdo.workload.gap_to_target <= 0 ? 'Achieved' : `+${cdo.workload.gap_to_target}%`, color: cdo.workload.gap_to_target <= 0 ? '#10B981' : '#EF4444' },
                ].map(item => (
                  <div key={item.label} style={{ padding: '10px 16px', background: `${item.color}0a`, borderRadius: 12, border: `1px solid ${item.color}20` }}>
                    <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>{item.label}</div>
                    <div style={{ fontSize: 22, fontWeight: 900, color: item.color, lineHeight: 1 }}>{item.value}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Resolution Funnel — Premium with SVG trapezoid */}
          <div style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 20,
            boxShadow: '0 8px 32px rgba(0,0,0,0.06), inset 0 1px 0 rgba(255,255,255,0.5)',
            overflow: 'hidden',
          }}>
            <div style={{ height: 3, background: 'linear-gradient(90deg, #00338D, #8B5CF6)' }} />
            <div style={{ padding: '22px 26px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
                <div style={{ width: 32, height: 32, borderRadius: 10, background: 'linear-gradient(135deg, #00338D, #8B5CF6)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" strokeLinecap="round"><path d="M22 2L2 22M22 22V2H2" /></svg>
                </div>
                <div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>Resolution Funnel</div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Conversation to resolution flow</div>
                </div>
              </div>

              {/* SVG Funnel */}
              <svg viewBox="0 0 400 280" style={{ width: '100%', height: 'auto' }}>
                <defs>
                  <linearGradient id="fg1" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#00338D" /><stop offset="100%" stopColor="#1e52a0" /></linearGradient>
                  <linearGradient id="fg2" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#1e52a0" /><stop offset="100%" stopColor="#3b7dd8" /></linearGradient>
                  <linearGradient id="fg3" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#3b7dd8" /><stop offset="100%" stopColor="#6366f1" /></linearGradient>
                  <linearGradient id="fg4" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#6366f1" /><stop offset="100%" stopColor="#8B5CF6" /></linearGradient>
                  <filter id="fShadow"><feDropShadow dx="0" dy="2" stdDeviation="3" floodOpacity="0.15" /></filter>
                </defs>
                {[
                  { y: 0, topW: 380, botW: 310, fill: 'url(#fg1)', label: 'Conversations', value: cdo.funnel.conversations },
                  { y: 65, topW: 310, botW: 240, fill: 'url(#fg2)', label: 'AI Resolved', value: cdo.funnel.ai_resolved },
                  { y: 130, topW: 240, botW: 180, fill: 'url(#fg3)', label: 'Escalated', value: cdo.funnel.escalated },
                  { y: 195, topW: 180, botW: 140, fill: 'url(#fg4)', label: 'Resolved', value: cdo.funnel.human_resolved },
                ].map((s, i) => {
                  const cx = 200;
                  const tl = cx - s.topW / 2, tr = cx + s.topW / 2;
                  const bl = cx - s.botW / 2, br = cx + s.botW / 2;
                  const dropoff = i > 0 ? (() => {
                    const prev = [cdo.funnel.conversations, cdo.funnel.ai_resolved, cdo.funnel.escalated][i - 1];
                    return prev ? Math.round(((prev - s.value) / prev) * 100) : 0;
                  })() : null;
                  return (
                    <g key={s.label}>
                      <path d={`M${tl},${s.y} L${tr},${s.y} L${br},${s.y + 58} L${bl},${s.y + 58} Z`}
                        fill={s.fill} filter="url(#fShadow)" rx="8" />
                      <text x={cx} y={s.y + 24} textAnchor="middle" fill="#fff" fontSize="12" fontWeight="600">{s.label}</text>
                      <text x={cx} y={s.y + 46} textAnchor="middle" fill="#fff" fontSize="22" fontWeight="900">{s.value}</text>
                      {dropoff != null && dropoff > 0 && (
                        <text x={tr + 8} y={s.y + 10} fill="#94A3B8" fontSize="9" fontWeight="700">-{dropoff}%</text>
                      )}
                    </g>
                  );
                })}
              </svg>

              <div style={{ display: 'flex', gap: 14, justifyContent: 'center', marginTop: 10, flexWrap: 'wrap' }}>
                {[['#00338D','Conversations'],['#1e52a0','AI Resolved'],['#3b7dd8','Escalated'],['#8B5CF6','Resolved']].map(([c, l]) => (
                  <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: 'var(--text-muted)' }}>
                    <div style={{ width: 8, height: 8, borderRadius: 3, background: c }} />{l}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── CDO Row 2: Activity Heatmap + Sentiment ── */}
      {cdo && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 24 }}>

          {/* Activity Heatmap — Premium */}
          <div style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 20,
            boxShadow: '0 8px 32px rgba(0,0,0,0.06), inset 0 1px 0 rgba(255,255,255,0.5)',
            overflow: 'hidden',
          }}>
            <div style={{ height: 3, background: 'linear-gradient(90deg, #06B6D4, #4F46E5)' }} />
            <div style={{ padding: '22px 26px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
                <div style={{ width: 32, height: 32, borderRadius: 10, background: 'linear-gradient(135deg, #06B6D4, #4F46E5)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" strokeLinecap="round"><rect x="3" y="3" width="7" height="7" rx="1" /><rect x="14" y="3" width="7" height="7" rx="1" /><rect x="3" y="14" width="7" height="7" rx="1" /><rect x="14" y="14" width="7" height="7" rx="1" /></svg>
                </div>
                <div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>Weekly Activity Heatmap</div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Productivity pattern by day and hour</div>
                </div>
              </div>
              {['Sun','Mon','Tue','Wed','Thu','Fri','Sat'].map((day, di) => (
                <div key={day} style={{ display: 'grid', gridTemplateColumns: '36px repeat(24, 1fr)', gap: 3, marginBottom: 3 }}>
                  <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-muted)', display: 'flex', alignItems: 'center' }}>{day}</div>
                  {Array.from({ length: 24 }, (_, hr) => {
                    const cell = (cdo.heatmap.cells || []).find(c => c.day === di && c.hour === hr);
                    const count = cell?.count || 0;
                    const intensity = cdo.heatmap.peak_activity ? count / cdo.heatmap.peak_activity : 0;
                    return (
                      <div key={hr} title={`${day} ${hr}:00 — ${count} activities`} style={{
                        height: 16, borderRadius: 4, cursor: 'pointer',
                        background: count > 0
                          ? `linear-gradient(135deg, rgba(6,182,212,${0.15 + intensity * 0.85}), rgba(79,70,229,${0.15 + intensity * 0.85}))`
                          : 'var(--border)',
                        transition: 'all 0.2s ease',
                        boxShadow: count > 0 ? `0 2px 8px rgba(79,70,229,${intensity * 0.2})` : 'none',
                      }} />
                    );
                  })}
                </div>
              ))}
              <div style={{ display: 'flex', gap: 5, alignItems: 'center', marginTop: 10, fontSize: 9, color: 'var(--text-muted)' }}>
                <span>Less</span>
                {[0.1, 0.3, 0.5, 0.7, 1].map((o, i) => (
                  <div key={i} style={{ width: 12, height: 12, borderRadius: 4, background: `linear-gradient(135deg, rgba(6,182,212,${o}), rgba(79,70,229,${o}))` }} />
                ))}
                <span>More</span>
              </div>
            </div>
          </div>

          {/* Customer Sentiment — Premium */}
          <div style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 20,
            boxShadow: '0 8px 32px rgba(0,0,0,0.06), inset 0 1px 0 rgba(255,255,255,0.5)',
            overflow: 'hidden',
          }}>
            <div style={{ height: 3, background: 'linear-gradient(90deg, #4F46E5, #8B5CF6)' }} />
            <div style={{ padding: '22px 26px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
                <div style={{ width: 32, height: 32, borderRadius: 10, background: 'linear-gradient(135deg, #4F46E5, #8B5CF6)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" strokeLinecap="round"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" /></svg>
                </div>
                <div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>Customer Sentiment Analysis</div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Feedback rating distribution</div>
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
                <div style={{ position: 'relative', width: 150, height: 150, flexShrink: 0 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie
                        data={(cdo.sentiment.distribution || []).filter(d => d.count > 0)}
                        cx="50%" cy="50%"
                        innerRadius={46} outerRadius={70}
                        dataKey="count" stroke="none" paddingAngle={3}
                        animationDuration={1000} animationEasing="ease-in-out"
                      >
                        {(cdo.sentiment.distribution || []).filter(d => d.count > 0).map((d, i) => (
                          <Cell key={i} fill={{'Excellent':'#4F46E5','Good':'#06B6D4','Neutral':'#94A3B8','Poor':'#F59E0B','Bad':'#EF4444'}[d.label]} />
                        ))}
                      </Pie>
                      <Tooltip formatter={(val, name) => [`${val} (${cdo.sentiment.total ? Math.round(val / cdo.sentiment.total * 100) : 0}%)`, name]} />
                    </PieChart>
                  </ResponsiveContainer>
                  <div style={{
                    position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)',
                    textAlign: 'center', pointerEvents: 'none',
                  }}>
                    <div style={{ fontSize: 24, fontWeight: 900, color: 'var(--text)', lineHeight: 1 }}>{cdo.sentiment.total}</div>
                    <div style={{ fontSize: 8, color: 'var(--text-muted)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Total</div>
                  </div>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8, flex: 1 }}>
                  {(cdo.sentiment.distribution || []).map(d => {
                    const clr = {'Excellent':'#4F46E5','Good':'#06B6D4','Neutral':'#94A3B8','Poor':'#F59E0B','Bad':'#EF4444'}[d.label];
                    return (
                      <div key={d.label} style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 12 }}>
                        <div style={{ width: 10, height: 10, borderRadius: 4, background: clr, flexShrink: 0, boxShadow: `0 0 6px ${clr}40` }} />
                        <span style={{ color: 'var(--text)', fontWeight: 500, flex: 1 }}>{d.label}</span>
                        <span style={{ fontWeight: 800, color: clr, minWidth: 22, textAlign: 'right' }}>{d.count}</span>
                        <span style={{ fontSize: 11, color: 'var(--text-muted)', minWidth: 36, textAlign: 'right', fontWeight: 600 }}>{d.pct}%</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── Middle Row: Donut + Agent Workload */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 24 }}>

        {/* Ticket Status Breakdown — donut */}
        <div className="section-card">
          <div className="section-card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <h3>Ticket Status Breakdown</h3>
            <span style={{ color: 'var(--text-muted)', fontSize: 18, letterSpacing: 2, cursor: 'pointer' }}>···</span>
          </div>
          <div className="section-card-body">
            <div style={{ position: 'relative' }}>
              <ResponsiveContainer width="100%" height={230}>
                <PieChart>
                  <Pie
                    data={statusBreakdown}
                    cx="50%" cy="50%"
                    innerRadius={70} outerRadius={105}
                    dataKey="value" stroke="none" paddingAngle={2}
                  >
                    {statusBreakdown.map((entry, i) => <Cell key={i} fill={entry.fill} />)}
                  </Pie>
                  <Tooltip
                    formatter={(val, name) => [`${val} (${totalTickets ? Math.round(val / totalTickets * 100) : 0}%)`, name]}
                  />
                </PieChart>
              </ResponsiveContainer>
              {/* Center label */}
              <div style={{
                position: 'absolute', top: '50%', left: '50%',
                transform: 'translate(-50%, -50%)',
                textAlign: 'center', pointerEvents: 'none',
              }}>
                <div style={{ fontSize: 26, fontWeight: 800, color: 'var(--text)', lineHeight: 1 }}>
                  {totalTickets.toLocaleString()}
                </div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 3, fontWeight: 600, letterSpacing: 0.5 }}>
                  TOTAL
                </div>
              </div>
            </div>

            {/* Legend */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px 20px', marginTop: 8 }}>
              {statusBreakdown.map(item => (
                <div key={item.name} style={{ display: 'flex', alignItems: 'center', gap: 7, fontSize: 12, color: 'var(--text-muted)' }}>
                  <div style={{ width: 10, height: 10, borderRadius: 3, background: item.fill, flexShrink: 0 }} />
                  {item.name} ({totalTickets ? Math.round(item.value / totalTickets * 100) : 0}%)
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Agent Workload Analysis */}
        <div className="section-card">
          <div className="section-card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <h3>Agent Workload Analysis</h3>
            <button
              onClick={() => navigate('/cto/roster')}
              style={{
                background: 'none', border: 'none', cursor: 'pointer',
                fontSize: 11, fontWeight: 800, color: 'var(--primary)',
                letterSpacing: 0.8, padding: 0,
              }}
            >
              VIEW ROSTER
            </button>
          </div>
          <div className="section-card-body">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              {agentWorkload.slice(0, 5).map((item, idx) => (
                <div key={item.agent}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                    <span style={{ fontSize: 13, fontWeight: 500, color: 'var(--text)' }}>{item.agent}</span>
                    <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--text-muted)' }}>{item.tickets} Tickets</span>
                  </div>
                  <div style={{ background: 'var(--border)', borderRadius: 6, height: 7, overflow: 'hidden' }}>
                    <div style={{
                      width: `${(item.tickets / maxTickets) * 100}%`,
                      height: '100%', borderRadius: 6,
                      background: idx === 0 ? 'var(--primary)' : idx === 1 ? '#2563eb' : 'var(--primary)',
                      opacity: 1 - idx * 0.12,
                      transition: 'width 0.7s ease',
                    }} />
                  </div>
                </div>
              ))}
              {agentWorkload.length === 0 && (
                <div style={{ color: 'var(--text-muted)', fontSize: 13, textAlign: 'center', padding: '20px 0' }}>
                  No agent data available
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* ── Bottom Row: Critical Incidents + Right side cards */}
      <div style={{ display: 'grid', gridTemplateColumns: '1.65fr 1fr', gap: 20 }}>

        {/* Critical Incidents & Breaches */}
        <div className="section-card">
          <div className="section-card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <h3>Critical Incidents &amp; Breaches</h3>
            {s.sla_breaches > 0 && (
              <span style={{
                display: 'inline-flex', alignItems: 'center', gap: 6,
                background: 'rgba(239,68,68,0.1)', color: '#ef4444',
                border: '1px solid rgba(239,68,68,0.25)',
                borderRadius: 20, padding: '4px 12px',
                fontSize: 10, fontWeight: 800, letterSpacing: 0.5,
              }}>
                ⚠ {s.sla_breaches} BREACHES DETECTED
              </span>
            )}
          </div>
          <div className="section-card-body">
            {incidents.length === 0 ? (
              <div style={{
                textAlign: 'center', padding: '36px 0',
                color: 'var(--text-muted)', fontSize: 13,
                background: 'var(--bg)', borderRadius: 8,
              }}>
                ✓ No active critical incidents
              </div>
            ) : (
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                  <thead>
                    <tr>
                      {['Incident ID', 'System / Service', 'Priority', 'SLA Clock', 'Status', 'Actions'].map((h, i) => (
                        <th key={h} style={{
                          padding: '0 0 12px',
                          textAlign: i === 5 ? 'right' : 'left',
                          fontWeight: 700, fontSize: 11,
                          color: 'var(--text-muted)',
                          textTransform: 'uppercase', letterSpacing: 0.6,
                          whiteSpace: 'nowrap',
                        }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {incidents.map(inc => (
                      <React.Fragment key={inc.id}>
                        <tr style={{ borderTop: '1px solid var(--border)' }}>
                          <td style={{ padding: '13px 0', fontWeight: 700, color: 'var(--primary)', fontSize: 12, whiteSpace: 'nowrap' }}>
                            #{inc.id}
                          </td>
                          <td style={{ padding: '13px 8px 13px 0', color: 'var(--text)', fontWeight: 500 }}>
                            {inc.service}
                          </td>
                          <td style={{ padding: '13px 8px 13px 0' }}>
                            <PriorityBadge priority={inc.priority} />
                          </td>
                          <td style={{ padding: '13px 8px 13px 0' }}>
                            <SlaTimer clock={inc.sla_clock} remaining={inc.sla_remaining} />
                          </td>
                          <td style={{ padding: '13px 8px 13px 0', color: 'var(--text-muted)', fontStyle: 'italic', fontSize: 12 }}>
                            {STATUS_LABEL[inc.status] || inc.status}
                          </td>
                          <td style={{ padding: '13px 0', textAlign: 'right' }}>
                            <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
                              <button
                                className="btn btn-outline"
                                style={{ padding: '4px 14px', fontSize: 11, fontWeight: 700 }}
                                onClick={() => setReviewTicket(reviewTicket?.id === inc.id ? null : inc)}
                              >
                                {reviewTicket?.id === inc.id ? 'Hide' : 'Review'}
                              </button>
                              <button
                                className="btn btn-primary"
                                style={{ padding: '4px 14px', fontSize: 11, fontWeight: 700, background: '#10b981', borderColor: '#10b981' }}
                                onClick={() => handleResolve(inc)}
                                disabled={closing === inc.id}
                              >
                                {closing === inc.id ? '...' : 'Close'}
                              </button>
                            </div>
                          </td>
                        </tr>
                        {reviewTicket?.id === inc.id && (
                          <tr>
                            <td colSpan={6} style={{ padding: '12px 0 16px', background: 'var(--bg)' }}>
                              <div style={{
                                background: 'var(--bg-card)', border: '1px solid var(--border)',
                                borderRadius: 10, padding: '20px 24px',
                              }}>
                                {/* Header */}
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                                    <span style={{ fontSize: 15, fontWeight: 800, color: 'var(--primary)' }}>#{inc.id}</span>
                                    <PriorityBadge priority={inc.priority} />
                                    {inc.sla_breached && (
                                      <span style={{ fontSize: 10, fontWeight: 800, padding: '2px 8px', borderRadius: 20, background: 'rgba(239,68,68,0.1)', color: '#ef4444', border: '1px solid rgba(239,68,68,0.25)' }}>
                                        SLA BREACHED
                                      </span>
                                    )}
                                  </div>
                                  <SlaTimer clock={inc.sla_clock} remaining={inc.sla_remaining} />
                                </div>

                                {/* Description */}
                                <div style={{ background: 'var(--bg)', borderRadius: 8, padding: '12px 16px', marginBottom: 16, border: '1px solid var(--border)' }}>
                                  <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>Description</div>
                                  <div style={{ fontSize: 13, color: 'var(--text)', lineHeight: 1.6 }}>{inc.description}</div>
                                </div>

                                {/* Details grid */}
                                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 14, marginBottom: 16, fontSize: 12 }}>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>Category</div>
                                    <div style={{ fontWeight: 700, color: 'var(--text)' }}>{inc.service}</div>
                                  </div>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>Subcategory</div>
                                    <div style={{ fontWeight: 700, color: 'var(--text)' }}>{inc.subcategory || '—'}</div>
                                  </div>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>Assigned To</div>
                                    <div style={{ fontWeight: 700, color: 'var(--text)' }}>{inc.assigned_to}</div>
                                  </div>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>SLA Window</div>
                                    <div style={{ fontWeight: 700, color: 'var(--text)' }}>{inc.sla_hours ? `${inc.sla_hours}h` : '—'}</div>
                                  </div>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>Created</div>
                                    <div style={{ fontWeight: 700, color: 'var(--text)' }}>{inc.created_at ? new Date(inc.created_at).toLocaleString() : '—'}</div>
                                  </div>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>Status</div>
                                    <div style={{ fontWeight: 700 }}>{STATUS_LABEL[inc.status] || inc.status}</div>
                                  </div>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>Time Overdue</div>
                                    <div style={{ fontWeight: 700, color: inc.sla_remaining < 0 ? '#ef4444' : '#10b981' }}>
                                      {inc.sla_remaining < 0 ? inc.sla_clock : 'On time'}
                                    </div>
                                  </div>
                                  <div>
                                    <div style={{ color: 'var(--text-muted)', fontWeight: 600, marginBottom: 3 }}>Resolution Notes</div>
                                    <div style={{ fontWeight: 700, color: 'var(--text)' }}>{inc.resolution_notes || '—'}</div>
                                  </div>
                                </div>

                                {/* Actions */}
                                <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', borderTop: '1px solid var(--border)', paddingTop: 14 }}>
                                  <button
                                    className="btn btn-primary"
                                    style={{ padding: '7px 22px', fontSize: 12, fontWeight: 700, background: '#10b981', borderColor: '#10b981' }}
                                    onClick={() => handleResolve(inc)}
                                    disabled={closing === inc.id}
                                  >
                                    {closing === inc.id ? 'Closing...' : 'Resolve & Close'}
                                  </button>
                                  <button
                                    className="btn btn-outline"
                                    style={{ padding: '7px 22px', fontSize: 12, fontWeight: 700 }}
                                    onClick={() => setReviewTicket(null)}
                                  >
                                    Dismiss
                                  </button>
                                </div>
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        {/* Right column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

          {/* Ticket Deflection Rate */}
          <div className="section-card" style={{ flex: 1 }}>
            <div className="section-card-header">
              <h3>Ticket Deflection Rate</h3>
            </div>
            <div className="section-card-body">
              {(() => {
                const f = cdo?.funnel || {};
                const total = f.conversations || 0;
                const aiResolved = f.ai_resolved || 0;
                const escalated = f.escalated || 0;
                const deflectionRate = total ? round((aiResolved / total) * 100, 1) : 0;
                const escalationRate = total ? round((escalated / total) * 100, 1) : 0;
                const deflectionColor = deflectionRate >= 70 ? '#10b981' : deflectionRate >= 40 ? '#f59e0b' : '#ef4444';
                function round(v, d) { return Math.round(v * 10 ** d) / 10 ** d; }
                return (
                  <>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, marginBottom: 12 }}>
                      <span style={{ fontSize: 36, fontWeight: 800, color: deflectionColor, lineHeight: 1 }}>
                        {deflectionRate}%
                      </span>
                      <span style={{
                        fontSize: 10, fontWeight: 800, padding: '3px 10px', borderRadius: 20,
                        background: `${deflectionColor}15`, color: deflectionColor,
                        border: `1px solid ${deflectionColor}30`,
                      }}>
                        {deflectionRate >= 70 ? 'On Target' : deflectionRate >= 40 ? 'Below Target' : 'Critical'}
                      </span>
                    </div>
                    <p style={{ fontSize: 12, color: 'var(--text-muted)', margin: '0 0 14px', lineHeight: 1.6 }}>
                      {aiResolved} of {total} conversations resolved by AI without human intervention.
                    </p>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                      {[
                        { label: 'AI Resolved', value: aiResolved, pct: total ? round((aiResolved / total) * 100, 1) : 0, color: '#10b981' },
                        { label: 'Escalated to Human', value: escalated, pct: escalationRate, color: '#ef4444' },
                        { label: 'Still Active', value: total - aiResolved - escalated, pct: total ? round(((total - aiResolved - escalated) / total) * 100, 1) : 0, color: '#f59e0b' },
                      ].map(item => (
                        <div key={item.label}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4, fontSize: 12 }}>
                            <span style={{ color: 'var(--text)', fontWeight: 500 }}>{item.label}</span>
                            <span style={{ fontWeight: 700, color: item.color }}>{item.value} ({item.pct}%)</span>
                          </div>
                          <div style={{ background: 'var(--border)', borderRadius: 4, height: 5, overflow: 'hidden' }}>
                            <div style={{
                              width: `${item.pct}%`, height: '100%', borderRadius: 4,
                              background: item.color, transition: 'width 0.7s ease',
                            }} />
                          </div>
                        </div>
                      ))}
                    </div>
                  </>
                );
              })()}
            </div>
          </div>


        </div>
      </div>

    </div>
  );
}
