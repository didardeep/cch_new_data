import { useEffect, useState } from 'react';
import { Line, LineChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiGet } from '../../api';

export default function TechnicalKPI() {
  const [data, setData] = useState(null);

  useEffect(() => {
    apiGet('/api/cto/technical-kpi').then((resp) => setData(resp));
  }, []);

  if (!data) return <div className="page-loader"><div className="spinner" /></div>;

  return (
    <div>
      <div className="page-header">
        <h1>Technical KPI</h1>
        <p>Live technical performance across accessibility, retainability, throughput, utilization, and traffic volume.</p>
      </div>

      <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))' }}>
        {(data.cards || []).map((card) => (
          <div key={card.key} className="stat-card">
            <div className="stat-card-label">{card.label}</div>
            <div className="stat-card-value">{card.value}</div>
            <div className="stat-card-sub">Latest network average</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: 20, marginTop: 24 }}>
        {(data.cards || []).map((card) => (
          <div key={card.key} className="section-card">
            <div className="section-card-header"><h3>{card.label}</h3></div>
            <div className="section-card-body" style={{ height: 280 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data.series?.[card.key] || []}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Line type="monotone" dataKey="value" stroke="#00338D" strokeWidth={3} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
