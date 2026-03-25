import { useEffect, useState } from 'react';
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';
import { useNavigate } from 'react-router-dom';
import { apiGet } from '../../api';

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

function Sparkline({ data = [] }) {
  const max = Math.max(...data, 1);
  return (
    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 4, height: 36 }}>
      {data.map((v, i) => (
        <div key={i} style={{
          flex: 1,
          height: `${Math.max((v / max) * 100, 8)}%`,
          background: i === data.length - 1 ? 'var(--primary)' : 'var(--border)',
          borderRadius: '3px 3px 0 0',
        }} />
      ))}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function OperationalKPI() {
  const [data, setData] = useState(null);
  const navigate = useNavigate();

  useEffect(() => {
    apiGet('/api/cto/operational-kpi').then(setData);
  }, []);

  if (!data) return <div className="page-loader"><div className="spinner" /></div>;

  const s          = data.summary || {};
  const incidents  = data.critical_incidents || [];
  const escalTrend = data.escalation_trend || [2, 4, 3, 6, 2, 3, 4];

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
          <div className="stat-card-sub" style={{ color: '#10b981' }}>↑ +12% from last cycle</div>
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
            High Risk Sector
          </span>
        </div>

        {/* Avg Resolution */}
        <div className="stat-card">
          <div className="stat-card-label">Avg Resolution</div>
          <div style={{ marginTop: 8 }}>
            <span className="stat-card-value" style={{ fontSize: 34 }}>{s.avg_resolution_time || 0}</span>
            <span className="stat-card-sub" style={{ marginLeft: 5 }}>hrs</span>
          </div>
          <div className="stat-card-sub" style={{ color: '#10b981', marginTop: 4 }}>-0.5h improvement</div>
        </div>

        {/* CSAT Score */}
        <div className="stat-card" style={{ borderTopColor: '#f59e0b' }}>
          <div className="stat-card-label">CSAT Score</div>
          <div className="stat-card-value" style={{ fontSize: 34, marginTop: 8 }}>{s.csat || 0}</div>
          <Stars value={s.csat || 0} />
        </div>
      </div>

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
                      <tr key={inc.id} style={{ borderTop: '1px solid var(--border)' }}>
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
                          <button
                            className="btn btn-outline"
                            style={{ padding: '4px 14px', fontSize: 11, fontWeight: 700 }}
                          >
                            Review
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        {/* Right column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

          {/* Escalation Velocity */}
          <div className="section-card" style={{ flex: 1 }}>
            <div className="section-card-header">
              <h3>Escalation Velocity</h3>
            </div>
            <div className="section-card-body">
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, marginBottom: 8 }}>
                <span style={{ fontSize: 36, fontWeight: 800, color: 'var(--text)', lineHeight: 1 }}>
                  {s.escalation_rate || 0}%
                </span>
                <span style={{ fontSize: 12, fontWeight: 700, color: '#10b981' }}>↓ 0.4%</span>
              </div>
              <p style={{ fontSize: 12, color: 'var(--text-muted)', margin: '0 0 16px', lineHeight: 1.6 }}>
                Internal escalations have stabilized following the Tier-1 automation patch.
              </p>
              <Sparkline data={escalTrend} />
            </div>
          </div>

          {/* Weekly Ops Review */}
          <div className="section-card">
            <div className="section-card-body" style={{ textAlign: 'center' }}>
              <div style={{
                width: 50, height: 50, borderRadius: '50%',
                background: 'var(--primary-glow)',
                margin: '0 auto 14px',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <svg width="22" height="22" viewBox="0 0 24 24" fill="none"
                  stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
                  <circle cx="9" cy="7" r="4" />
                  <path d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75" />
                </svg>
              </div>
              <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)', marginBottom: 8 }}>
                Weekly Ops Review
              </div>
              <p style={{ fontSize: 12, color: 'var(--text-muted)', margin: '0 0 18px', lineHeight: 1.6 }}>
                Review system performance and agent capacity for next sprint.
              </p>
              <button className="btn btn-primary" style={{ width: '100%' }}>
                Schedule Sync
              </button>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
}
