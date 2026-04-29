import { useState, useEffect, useCallback } from 'react';
import { API_BASE, apiGet } from '../../api';
import {
  AreaChart, Area, BarChart, Bar, PieChart, Pie, Cell, LineChart, Line,
  ComposedChart, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

const COLORS = {
  brand: '#00338D',
  green: '#10b981',
  red: '#ef4444',
  amber: '#f59e0b',
  blue: '#3b82f6',
  purple: '#8b5cf6',
};

const PIE_COLORS = [COLORS.brand, COLORS.green, COLORS.red, COLORS.amber, COLORS.blue, COLORS.purple];
const STAR_COLORS = { 1: '#ef4444', 2: '#f59e0b', 3: '#f59e0b', 4: '#3b82f6', 5: '#10b981' };
const PRIORITY_COLORS = { critical: '#ef4444', high: '#f59e0b', medium: '#3b82f6', low: '#10b981' };

const TABS = [
  { key: 'overview', label: 'Overview' },
  { key: 'agents', label: 'Agent Performance' },
  { key: 'csat', label: 'CSAT & Feedback' },
  { key: 'sla', label: 'SLA Compliance' },
];

const RANGES = [
  { key: '7d', label: '7 Days' },
  { key: '30d', label: '30 Days' },
  { key: '90d', label: '90 Days' },
  { key: '12m', label: '12 Months' },
];

function TrendBadge({ value }) {
  if (value === 0 || value === undefined || value === null) return null;
  const isUp = value > 0;
  return (
    <span className={`rpt-trend ${isUp ? 'up' : 'down'}`}>
      {isUp ? '+' : ''}{value}%
    </span>
  );
}

function StatCard({ label, value, trend, sub, color }) {
  return (
    <div className="rpt-stat-card">
      <div className="rpt-stat-dot" style={{ background: color || COLORS.brand }} />
      <div className="rpt-stat-label">{label}</div>
      <div className="rpt-stat-value">{value}</div>
      <div className="rpt-stat-footer">
        {trend !== undefined && <TrendBadge value={trend} />}
        {sub && <span className="rpt-stat-sub">{sub}</span>}
      </div>
    </div>
  );
}

function ChartCard({ title, children, span }) {
  return (
    <div className={`rpt-chart-card${span ? ' span-' + span : ''}`}>
      <div className="rpt-chart-title">{title}</div>
      <div className="rpt-chart-body">{children}</div>
    </div>
  );
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rpt-tooltip">
      <div className="rpt-tooltip-label">{label}</div>
      {payload.map((p, i) => (
        <div key={i} className="rpt-tooltip-row">
          <span className="rpt-tooltip-dot" style={{ background: p.color }} />
          <span>{p.name}: <strong>{typeof p.value === 'number' ? (Number.isInteger(p.value) ? p.value : p.value.toFixed(1)) : p.value}</strong></span>
        </div>
      ))}
    </div>
  );
}

// ─── Overview Tab ───
function OverviewTab({ data }) {
  if (!data) return null;
  return (
    <>
      <div className="rpt-stat-grid">
        <StatCard label="Total Resolved" value={data.total_resolved} trend={data.resolved_trend} color={COLORS.green} />
        <StatCard label="Avg Resolution Time" value={`${data.avg_resolution_hours}h`} trend={data.resolution_trend} color={COLORS.blue} />
        <StatCard label="CSAT Score" value={`${data.csat_score}%`} trend={data.csat_trend} color={COLORS.purple} />
        <StatCard label="SLA Compliance" value={`${data.sla_compliance}%`} trend={data.sla_trend} color={COLORS.brand} />
      </div>
      <div className="rpt-chart-grid cols-2">
        <ChartCard title="Resolution Time Trend">
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={data.resolution_trends}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis yAxisId="left" tick={{ fontSize: 12 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12 }} />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Area yAxisId="left" type="monotone" dataKey="avg_hours" name="Avg Hours" fill={COLORS.blue + '30'} stroke={COLORS.blue} />
              <Bar yAxisId="right" dataKey="volume" name="Volume" fill={COLORS.brand} radius={[4, 4, 0, 0]} barSize={30} />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Weekly Ticket Volume">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={data.weekly_volume}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="day" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Bar dataKey="opened" name="Opened" fill={COLORS.blue} radius={[4, 4, 0, 0]} barSize={20} />
              <Bar dataKey="resolved" name="Resolved" fill={COLORS.green} radius={[4, 4, 0, 0]} barSize={20} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Issues by Category">
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={data.category_breakdown} dataKey="count" nameKey="name" cx="50%" cy="50%" innerRadius={60} outerRadius={100} paddingAngle={3} label={({ name, percent }) => `${name.split(' ')[0]} ${(percent * 100).toFixed(0)}%`}>
                {data.category_breakdown.map((_, i) => <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />)}
              </Pie>
              <Tooltip content={<CustomTooltip />} />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Priority Distribution">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={data.priority_distribution} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis type="number" tick={{ fontSize: 12 }} />
              <YAxis dataKey="priority" type="category" tick={{ fontSize: 12 }} width={70} />
              <Tooltip content={<CustomTooltip />} />
              <Bar dataKey="count" name="Tickets" radius={[0, 4, 4, 0]} barSize={24}>
                {data.priority_distribution.map((entry, i) => (
                  <Cell key={i} fill={PRIORITY_COLORS[entry.priority] || COLORS.blue} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}

// ─── Agents Tab ───
function AgentsTab({ data }) {
  if (!data) return null;
  const { agents, fastest_agent, highest_rated, total_agents } = data;
  return (
    <>
      <div className="rpt-stat-grid">
        <StatCard label="Total Agents" value={total_agents} color={COLORS.brand} />
        <StatCard label="Fastest Resolution" value={fastest_agent?.name || 'N/A'} sub={fastest_agent ? `${fastest_agent.hours}h avg` : ''} color={COLORS.blue} />
        <StatCard label="Highest Rated" value={highest_rated?.name || 'N/A'} sub={highest_rated ? `${highest_rated.rating}/5` : ''} color={COLORS.purple} />
      </div>
      <div className="rpt-chart-grid cols-1">
        <ChartCard title="Agent Performance Comparison">
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={agents}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="name" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Bar dataKey="resolved" name="Resolved" fill={COLORS.green} radius={[4, 4, 0, 0]} barSize={20} />
              <Bar dataKey="pending" name="Pending" fill={COLORS.amber} radius={[4, 4, 0, 0]} barSize={20} />
              <Bar dataKey="escalated" name="Escalated" fill={COLORS.red} radius={[4, 4, 0, 0]} barSize={20} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
      <div className="rpt-chart-grid cols-1">
        <ChartCard title="Agent Details">
          <div className="rpt-table-wrap">
            <table className="rpt-table">
              <thead>
                <tr>
                  <th>Agent</th>
                  <th>Resolved</th>
                  <th>Pending</th>
                  <th>Escalated</th>
                  <th>Avg Time (hrs)</th>
                  <th>Rating</th>
                </tr>
              </thead>
              <tbody>
                {agents.map((a) => (
                  <tr key={a.id}>
                    <td>
                      <div className="rpt-agent-cell">
                        <div className="rpt-agent-avatar">{a.name.charAt(0).toUpperCase()}</div>
                        <span>{a.name}</span>
                      </div>
                    </td>
                    <td>{a.resolved}</td>
                    <td>{a.pending}</td>
                    <td>{a.escalated}</td>
                    <td>{a.avg_resolution_hours}</td>
                    <td>
                      <span className={`rpt-rating-badge ${a.avg_rating >= 4 ? 'good' : a.avg_rating >= 3 ? 'ok' : 'poor'}`}>
                        {a.avg_rating}/5
                      </span>
                    </td>
                  </tr>
                ))}
                {agents.length === 0 && <tr><td colSpan="6" style={{ textAlign: 'center', padding: 24, color: '#94a3b8' }}>No agents found</td></tr>}
              </tbody>
            </table>
          </div>
        </ChartCard>
      </div>
    </>
  );
}

// ─── CSAT Tab ───
function CSATTab({ data }) {
  if (!data) return null;
  return (
    <>
      <div className="rpt-stat-grid">
        <StatCard label="Current CSAT" value={`${data.current_csat}%`} trend={data.csat_trend} color={COLORS.green} />
        <StatCard label="Total Responses" value={data.total_responses} trend={data.responses_trend} color={COLORS.brand} />
        <StatCard label="Avg Rating" value={`${data.avg_rating}/5`} color={COLORS.purple} />
        <StatCard label="Response Rate" value={`${data.response_rate}%`} color={COLORS.blue} />
      </div>
      <div className="rpt-chart-grid cols-2">
        <ChartCard title="CSAT Score Trend" span={2}>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={data.csat_monthly}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis domain={[0, 100]} tick={{ fontSize: 12 }} />
              <Tooltip content={<CustomTooltip />} />
              <Area type="monotone" dataKey="csat" name="CSAT %" fill={COLORS.green + '30'} stroke={COLORS.green} strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Feedback Distribution">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={data.feedback_distribution}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="stars" tick={{ fontSize: 12 }} tickFormatter={(v) => `${v} Star`} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip content={<CustomTooltip />} />
              <Bar dataKey="count" name="Responses" radius={[4, 4, 0, 0]} barSize={40}>
                {data.feedback_distribution.map((entry, i) => (
                  <Cell key={i} fill={STAR_COLORS[entry.stars] || COLORS.blue} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Response Volume Trend">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={data.response_volume}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip content={<CustomTooltip />} />
              <Line type="monotone" dataKey="count" name="Responses" stroke={COLORS.purple} strokeWidth={2} dot={{ fill: COLORS.purple, r: 4 }} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}

// ─── SLA Tab ───
function SLATab({ data }) {
  if (!data) return null;
  const donutData = [
    { name: 'Within SLA', value: data.within_count, color: COLORS.green },
    { name: 'Near Breach', value: data.near_breach_count, color: COLORS.amber },
    { name: 'Breached', value: data.breached_count, color: COLORS.red },
  ];
  return (
    <>
      <div className="rpt-stat-grid">
        <StatCard label="SLA Compliance" value={`${data.compliance_percentage}%`} trend={data.compliance_trend} color={COLORS.green} />
        <StatCard label="Near Breach" value={`${data.near_breach_percentage}%`} color={COLORS.amber} />
        <StatCard label="Breached" value={`${data.breached_percentage}%`} color={COLORS.red} />
        <StatCard label="Avg First Response" value={`${data.avg_first_response}h`} color={COLORS.blue} />
      </div>
      <div className="rpt-chart-grid cols-2">
        <ChartCard title="SLA Compliance Breakdown">
          <div style={{ display: 'flex', alignItems: 'center', gap: 32 }}>
            <ResponsiveContainer width="60%" height={260}>
              <PieChart>
                <Pie data={donutData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={55} outerRadius={95} paddingAngle={3}>
                  {donutData.map((entry, i) => <Cell key={i} fill={entry.color} />)}
                </Pie>
                <Tooltip content={<CustomTooltip />} />
              </PieChart>
            </ResponsiveContainer>
            <div className="rpt-sla-legend">
              {donutData.map((d, i) => (
                <div key={i} className="rpt-sla-legend-item">
                  <span className="rpt-sla-legend-dot" style={{ background: d.color }} />
                  <span>{d.name}</span>
                  <strong>{d.value}</strong>
                </div>
              ))}
            </div>
          </div>
        </ChartCard>
        <ChartCard title="SLA Targets">
          <div className="rpt-sla-targets">
            {data.sla_targets.map((t) => (
              <div key={t.priority} className="rpt-sla-target-row">
                <div className="rpt-sla-priority">
                  <span className="rpt-sla-priority-dot" style={{ background: PRIORITY_COLORS[t.priority] }} />
                  <span style={{ textTransform: 'capitalize' }}>{t.priority}</span>
                </div>
                <div className="rpt-sla-times">
                  <span>Target: <strong>{t.target_hours}h</strong></span>
                  <span>Actual: <strong>{t.actual_hours}h</strong></span>
                </div>
                <span className={`rpt-sla-status ${t.status}`}>
                  {t.status === 'within' ? 'Within SLA' : 'Breached'}
                </span>
              </div>
            ))}
          </div>
        </ChartCard>
        <ChartCard title="SLA Breach Trend" span={2}>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={data.breach_trend}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis domain={[0, 100]} tick={{ fontSize: 12 }} />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Area type="monotone" dataKey="compliant" name="Compliant %" stackId="1" fill={COLORS.green + '60'} stroke={COLORS.green} />
              <Area type="monotone" dataKey="near_breach" name="Near Breach %" stackId="1" fill={COLORS.amber + '60'} stroke={COLORS.amber} />
              <Area type="monotone" dataKey="breached" name="Breached %" stackId="1" fill={COLORS.red + '60'} stroke={COLORS.red} />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}

// ─── Main Reports Page ───
export default function ReportsPage() {
  const [tab, setTab] = useState('overview');
  const [range, setRange] = useState('30d');
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res = await apiGet(`/api/reports/${tab}?range=${range}`);
      setData(prev => ({ ...prev, [`${tab}_${range}`]: res }));
    } catch (e) {
      console.error('Failed to fetch report data:', e);
    }
    setLoading(false);
  }, [tab, range]);

  useEffect(() => {
    const key = `${tab}_${range}`;
    if (!data[key]) fetchData();
  }, [tab, range, data, fetchData]);

  const currentData = data[`${tab}_${range}`];

  const handleExportCSV = async () => {
    const token = sessionStorage.getItem('token');
    const resp = await fetch(`${API_BASE}/api/reports/export?format=csv&section=${tab}&range=${range}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (resp.ok) {
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `report_${tab}_${range}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    }
  };

  const handleExportPDF = async () => {
    if (!currentData) {
      alert('Report data is still loading. Please try again in a moment.');
      return;
    }
    const { default: jsPDF } = await import('jspdf');
    const { default: autoTable } = await import('jspdf-autotable');
    const doc = new jsPDF();
    doc.setFontSize(18);
    doc.setTextColor(0, 51, 141);
    doc.text(`Reports - ${TABS.find(t => t.key === tab)?.label}`, 14, 22);
    doc.setFontSize(10);
    doc.setTextColor(100, 116, 139);
    doc.text(`Range: ${RANGES.find(r => r.key === range)?.label} | Generated: ${new Date().toLocaleDateString()}`, 14, 30);

    if (tab === 'overview' && currentData) {
      autoTable(doc, {
        startY: 38,
        head: [['Metric', 'Value']],
        body: [
          ['Total Resolved', currentData.total_resolved],
          ['Avg Resolution Time', `${currentData.avg_resolution_hours}h`],
          ['CSAT Score', `${currentData.csat_score}%`],
          ['SLA Compliance', `${currentData.sla_compliance}%`],
        ],
        headStyles: { fillColor: [0, 51, 141] },
      });
      if (currentData.category_breakdown?.length) {
        autoTable(doc, {
          startY: doc.lastAutoTable ? doc.lastAutoTable.finalY + 10 : 48,
          head: [['Category', 'Count']],
          body: currentData.category_breakdown.map(c => [c.name, c.count]),
          headStyles: { fillColor: [0, 51, 141] },
        });
      }
    } else if (tab === 'agents' && currentData) {
      autoTable(doc, {
        startY: 38,
        head: [['Agent', 'Resolved', 'Pending', 'Escalated', 'Avg Hours', 'Rating']],
        body: (currentData.agents || []).map(a => [a.name, a.resolved, a.pending, a.escalated, a.avg_resolution_hours, a.avg_rating]),
        headStyles: { fillColor: [0, 51, 141] },
      });
    } else if (tab === 'csat' && currentData) {
      autoTable(doc, {
        startY: 38,
        head: [['Metric', 'Value']],
        body: [
          ['CSAT Score', `${currentData.current_csat}%`],
          ['Total Responses', currentData.total_responses],
          ['Avg Rating', `${currentData.avg_rating}/5`],
          ['Response Rate', `${currentData.response_rate}%`],
        ],
        headStyles: { fillColor: [0, 51, 141] },
      });
      if (currentData.feedback_distribution?.length) {
        autoTable(doc, {
          startY: doc.lastAutoTable ? doc.lastAutoTable.finalY + 10 : 48,
          head: [['Stars', 'Count']],
          body: currentData.feedback_distribution.map(d => [`${d.stars} Star`, d.count]),
          headStyles: { fillColor: [0, 51, 141] },
        });
      }
    } else if (tab === 'sla' && currentData) {
      autoTable(doc, {
        startY: 38,
        head: [['Priority', 'Target (h)', 'Actual (h)', 'Status']],
        body: (currentData.sla_targets || []).map(t => [t.priority, t.target_hours, t.actual_hours, t.status]),
        headStyles: { fillColor: [0, 51, 141] },
      });
    }

    doc.save(`report_${tab}_${range}.pdf`);
  };

  return (
    <div className="rpt-page">
      <div className="rpt-header">
        <div>
          <h1 className="rpt-title">Reports & Analytics</h1>
          <p className="rpt-subtitle">Comprehensive insights and performance metrics</p>
        </div>
        <div className="rpt-header-actions">
          <div className="rpt-range-group">
            {RANGES.map(r => (
              <button key={r.key} className={`rpt-range-btn${range === r.key ? ' active' : ''}`} onClick={() => setRange(r.key)}>{r.label}</button>
            ))}
          </div>
          <button className="rpt-export-btn" onClick={handleExportCSV}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            CSV
          </button>
          <button className="rpt-export-btn" onClick={handleExportPDF}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
            PDF
          </button>
        </div>
      </div>

      <div className="rpt-tabs">
        {TABS.map(t => (
          <button key={t.key} className={`rpt-tab${tab === t.key ? ' active' : ''}`} onClick={() => setTab(t.key)}>{t.label}</button>
        ))}
      </div>

      <div className="rpt-content">
        {loading ? (
          <div className="rpt-loading">
            <div className="spinner" />
          </div>
        ) : (
          <>
            {tab === 'overview' && <OverviewTab data={currentData} />}
            {tab === 'agents' && <AgentsTab data={currentData} />}
            {tab === 'csat' && <CSATTab data={currentData} />}
            {tab === 'sla' && <SLATab data={currentData} />}
          </>
        )}
      </div>
    </div>
  );
}
