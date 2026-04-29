import { useEffect, useState, Component } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from '../../components/Sidebar';
import { useTheme } from '../../ThemeContext';
import { apiGet } from '../../api';
import { IndianRupee } from 'lucide-react';

// Error boundary so a crash in any one CTO page (e.g. Leaflet getMinZoom
// race during mount/unmount) doesn't take down the whole portal.
class CTOErrorBoundary extends Component {
  constructor(p) { super(p); this.state = { hasError: false, err: null }; }
  static getDerivedStateFromError(err) { return { hasError: true, err }; }
  componentDidCatch(err, info) { console.error('[CTO] render error:', err, info); }
  render() {
    if (this.state.hasError) {
      return (
        <div style={{padding:32,maxWidth:760}}>
          <h2 style={{color:'#dc2626',margin:'0 0 12px'}}>Page render error</h2>
          <p style={{color:'#475569',lineHeight:1.6}}>
            This page hit a runtime error (likely the map layer racing to mount).
            Click <b>Retry</b> or navigate to another section from the sidebar.
          </p>
          <pre style={{background:'#f1f5f9',padding:12,borderRadius:8,fontSize:12,overflow:'auto',maxHeight:200,whiteSpace:'pre-wrap'}}>
            {String(this.state.err?.message || this.state.err || 'Unknown error')}
          </pre>
          <button onClick={() => this.setState({ hasError: false, err: null })}
            style={{marginTop:12,padding:'8px 20px',background:'#00338D',color:'#fff',border:'none',borderRadius:6,cursor:'pointer',fontWeight:600}}>
            Retry
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

const ICON = (d) => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    {Array.isArray(d) ? d.map((p, i) => <path key={i} d={p} />) : <path d={d} />}
  </svg>
);

export default function CTOLayout() {
  const { theme } = useTheme();
  const [unreadAlerts, setUnreadAlerts] = useState(0);

  useEffect(() => {
    const fetchCount = () => {
      apiGet('/api/cto/sla-alerts')
        .then(d => {
          const alerts = d.alerts || [];
          setUnreadAlerts(alerts.filter(a => !a.is_read).length);
        })
        .catch(() => {});
    };
    fetchCount();
    const iv = setInterval(fetchCount, 30000);
    return () => clearInterval(iv);
  }, []);

  const links = [
    { path: '/cto/dashboard', label: 'Map Overview', icon: ICON("M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z") },
    { path: '/cto/technical', label: 'Technical KPI', icon: ICON("M3 12h4l3 8 4-16 3 8h4") },
    { path: '/cto/business', label: 'Business KPI', icon: <IndianRupee size={18} /> },
    { path: '/cto/operational', label: 'Operational KPI', icon: ICON("M4 19h16M4 15h10M4 11h16M4 7h7") },
    { path: '/cto/tickets', label: 'All Tickets', icon: ICON("M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z") },
    { path: '/cto/alerts', label: 'Alert Box', icon: ICON("M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0zM12 9v4M12 17h.01"), badge: unreadAlerts },
    { path: '/cto/roster', label: 'Duty Roster', icon: ICON("M8 2v4M16 2v4M3 8h18M5 12h4M10 12h4M15 12h4M5 16h4M10 16h4M15 16h4") },
    { path: '/cto/change-workflow', label: 'Change Workflow', icon: ICON("M16 3l5 0 0 5M4 20l17-17M21 16l0 5-5 0M15 15l6 6M4 4l5 5") },
  ];

  return (
    <div className="dashboard-layout" data-theme={theme}>
      <Sidebar links={links} />
      <main className="main-content">
        <CTOErrorBoundary>
          <Outlet />
        </CTOErrorBoundary>
      </main>
    </div>
  );
}
