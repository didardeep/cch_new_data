import { useEffect, useState } from 'react';
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiGet } from '../../api';
import { useTheme } from '../../ThemeContext';

export default function BusinessKPI() {
  const { isDark } = useTheme();
  const [data, setData] = useState(null);

  useEffect(() => {
    apiGet('/api/cto/business-kpi').then((resp) => setData(resp));
  }, []);

  if (!data) return <div className="page-loader"><div className="spinner" /></div>;

  const summary = data.summary || {};

  return (
    <div>
      <div className="page-header">
        <h1>Business KPI</h1>
        <p>Commercial impact from site users, revenue, growth, ARPU, utilization risk, and declining network demand.</p>
      </div>

      <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))' }}>
        {[
          { label: 'Total Users', value: summary.total_users || 0 },
          { label: 'Avg Users', value: summary.avg_users || 0 },
          { label: 'Growth %', value: summary.growth || 0 },
          { label: 'ARPU', value: summary.arpu || 0 },
          { label: 'Revenue At Risk', value: summary.revenue_at_risk || 0 },
        ].map((card) => (
          <div key={card.label} className="stat-card">
            <div className="stat-card-label">{card.label}</div>
            <div className="stat-card-value">{card.value}</div>
            <div className="stat-card-sub">Business performance view</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: 20, marginTop: 24 }}>
        <div className="section-card">
          <div className="section-card-header"><h3>Users and Revenue Trend</h3></div>
          <div className="section-card-body" style={{ height: 300 }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.trend || []}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Line type="monotone" dataKey="users" stroke="#00338D" strokeWidth={3} dot={false} />
                <Line type="monotone" dataKey="revenue" stroke="#10b981" strokeWidth={3} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="section-card">
          <div className="section-card-header"><h3>Top 10 Sites</h3></div>
          <div className="section-card-body" style={{ height: 300 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.top_sites || []} layout="vertical" margin={{ left: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis type="category" dataKey="site_id" width={90} tick={{ fontSize: 11 }} />
                <Tooltip />
                <Bar dataKey="revenue" fill="#00338D" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginTop: 24 }}>
        <div className="section-card">
          <div className="section-card-header"><h3>Declining Sites</h3></div>
          <div className="section-card-body">
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ textAlign: 'left', color: isDark ? '#94a3b8' : '#64748b' }}>
                  <th style={{ paddingBottom: 10 }}>Site</th>
                  <th style={{ paddingBottom: 10 }}>Users</th>
                  <th style={{ paddingBottom: 10 }}>Growth</th>
                </tr>
              </thead>
              <tbody>
                {(data.declining_sites || []).map((row) => (
                  <tr key={row.site_id} style={{ borderTop: `1px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>
                    <td style={{ padding: '10px 0' }}>{row.site_id}</td>
                    <td>{row.users}</td>
                    <td style={{ color: '#dc2626', fontWeight: 700 }}>{row.growth}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="section-card">
          <div className="section-card-header"><h3>Overloaded Sites</h3></div>
          <div className="section-card-body">
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ textAlign: 'left', color: isDark ? '#94a3b8' : '#64748b' }}>
                  <th style={{ paddingBottom: 10 }}>Site</th>
                  <th style={{ paddingBottom: 10 }}>Utilization</th>
                  <th style={{ paddingBottom: 10 }}>Revenue</th>
                </tr>
              </thead>
              <tbody>
                {(data.overloaded_sites || []).map((row) => (
                  <tr key={row.site_id} style={{ borderTop: `1px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>
                    <td style={{ padding: '10px 0' }}>{row.site_id}</td>
                    <td style={{ color: '#f59e0b', fontWeight: 700 }}>{row.utilization}%</td>
                    <td>{row.revenue}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
