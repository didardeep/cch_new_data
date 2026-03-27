import { useNavigate } from 'react-router-dom';
import { Activity, IndianRupee, BarChart2, ArrowRight } from 'lucide-react';
import CTOMap from './CTOMap';

const navItems = [
  {
    label: 'Technical KPI',
    subtitle: 'Network performance & health',
    path: '/cto/technical',
    icon: Activity,
    gradient: 'linear-gradient(135deg, #002266, #00338D)',
    glow: '#00338D',
  },
  {
    label: 'Business KPI',
    subtitle: 'Revenue, users & growth',
    path: '/cto/business',
    icon: IndianRupee,
    gradient: 'linear-gradient(135deg, #002266, #00338D)',
    glow: '#00338D',
  },
  {
    label: 'Operational KPI',
    subtitle: 'Operations & site management',
    path: '/cto/operational',
    icon: BarChart2,
    gradient: 'linear-gradient(135deg, #002266, #00338D)',
    glow: '#00338D',
  },
];

export default function CTODashboard() {
  const navigate = useNavigate();

  return (
    <div>
      <div className="page-header" style={{ marginBottom: 18 }}>
        <h1>CTO Network Map</h1>
        <p>Explore all telecom sites on the live map and jump into technical, business, or operational KPIs.</p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 16, marginBottom: 20 }}>
        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.path}
              onClick={() => navigate(item.path)}
              style={{
                background: item.gradient,
                border: 'none',
                borderRadius: 16,
                padding: '20px 24px',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: 16,
                boxShadow: `0 8px 24px ${item.glow}40`,
                transition: 'all 0.25s ease',
                textAlign: 'left',
                width: '100%',
              }}
              onMouseEnter={e => {
                e.currentTarget.style.transform = 'translateY(-3px)';
                e.currentTarget.style.boxShadow = `0 14px 32px ${item.glow}60`;
              }}
              onMouseLeave={e => {
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = `0 8px 24px ${item.glow}40`;
              }}
            >
              <div style={{
                background: 'rgba(255,255,255,0.2)',
                borderRadius: 12,
                padding: 12,
                flexShrink: 0,
              }}>
                <Icon size={24} color="#fff" strokeWidth={2.2} />
              </div>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 16, fontWeight: 800, color: '#fff', letterSpacing: '-0.01em' }}>
                  {item.label}
                </div>
                <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.8)', marginTop: 2 }}>
                  {item.subtitle}
                </div>
              </div>
              <ArrowRight size={20} color="rgba(255,255,255,0.7)" />
            </button>
          );
        })}
      </div>

      <div className="section-card" style={{ overflow: 'hidden' }}>
        <div className="section-card-body" style={{ padding: 0 }}>
          <CTOMap />
        </div>
      </div>
    </div>
  );
}
