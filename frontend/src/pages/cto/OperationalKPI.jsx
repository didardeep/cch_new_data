import { useEffect, useState } from 'react';
import { Bar, BarChart, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiGet } from '../../api';

const pieColors = ['#00338D', '#10b981', '#f59e0b', '#ef4444', '#6366f1'];

export default function OperationalKPI() {
  const [data, setData] = useState(null);

  useEffect(() => {
    apiGet('/api/cto/operational-kpi').then((resp) => setData(resp));
  }, []);

  if (!data) return <div className="page-loader"><div className="spinner" /></div>;

  const summary = data.summary || {};

  return (
    <div>
      <div className="page-header">
        <h1>Operational KPI</h1>
        <p>Service operations performance across SLA compliance, breaches, resolution speed, CSAT, and agent workload.</p>
      </div>

      <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
        {[
          { label: 'Total Tickets', value: summary.total_tickets || 0 },
          { label: 'SLA Compliance %', value: summary.sla_compliance || 0 },
          { label: 'SLA Breaches', value: summary.sla_breaches || 0 },
          { label: 'Avg Resolution (hrs)', value: summary.avg_resolution_time || 0 },
          { label: 'CSAT', value: summary.csat || 0 },
          { label: 'Escalation Rate %', value: summary.escalation_rate || 0 },
        ].map((card) => (
          <div key={card.label} className="stat-card">
            <div className="stat-card-label">{card.label}</div>
            <div className="stat-card-value">{card.value}</div>
            <div className="stat-card-sub">Operational control tower</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginTop: 24 }}>
        <div className="section-card">
          <div className="section-card-header"><h3>Ticket Status Breakdown</h3></div>
          <div className="section-card-body" style={{ height: 320 }}>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={data.status_breakdown || []} dataKey="value" nameKey="name" outerRadius={110} label>
                  {(data.status_breakdown || []).map((entry, index) => (
                    <Cell key={entry.name} fill={pieColors[index % pieColors.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="section-card">
          <div className="section-card-header"><h3>Agent Workload</h3></div>
          <div className="section-card-body" style={{ height: 320 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.agent_workload || []}>
                <XAxis dataKey="agent" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Bar dataKey="tickets" fill="#00338D" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
