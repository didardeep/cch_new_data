import { useNavigate } from 'react-router-dom';
import CTOMap from './CTOMap';

const navItems = [
  { label: 'Technical KPI', path: '/cto/technical' },
  { label: 'Business KPI', path: '/cto/business' },
  { label: 'Operational KPI', path: '/cto/operational' },
];

export default function CTODashboard() {
  const navigate = useNavigate();

  return (
    <div>
      <div className="page-header" style={{ marginBottom: 18 }}>
        <h1>CTO Network Map</h1>
        <p>Explore all telecom sites on the live map and jump into technical, business, or operational KPIs.</p>
      </div>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 18 }}>
        {navItems.map((item) => (
          <button
            key={item.path}
            className="btn btn-primary"
            onClick={() => navigate(item.path)}
            style={{ minWidth: 170 }}
          >
            {item.label}
          </button>
        ))}
      </div>

      <div className="section-card" style={{ overflow: 'hidden' }}>
        <div className="section-card-body" style={{ padding: 0 }}>
          <CTOMap />
        </div>
      </div>
    </div>
  );
}
