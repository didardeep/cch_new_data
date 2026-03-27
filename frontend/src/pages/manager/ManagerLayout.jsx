import { useEffect, useState } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from '../../components/Sidebar';
import { useTheme } from '../../ThemeContext';
import { apiGet } from '../../api';

const ICON = (d) => <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>;

export default function ManagerLayout() {
  const { theme } = useTheme();
  const [unreadAlerts, setUnreadAlerts] = useState(0);

  useEffect(() => {
    const fetchCount = () => {
      apiGet('/api/manager/sla-alerts')
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
    { path: '/manager/dashboard', label: 'Dashboard', icon: ICON("M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z") },
    { path: '/manager/chat', label: 'Chat Support', icon: ICON("M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z") },
    { path: '/manager/tickets', label: 'Active Tickets', icon: ICON("M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z") },
    { path: '/manager/tracking', label: 'Issue Tracking', icon: ICON("M12 20V10M18 20V4M6 20v-4") },
    { path: '/manager/reports', label: 'Reports & Analytics', icon: ICON("M3 3v18h18M9 17V9m4 8V5m4 12v-4") },
    { path: '/manager/alerts', label: 'Alert Box', icon: ICON("M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0zM12 9v4M12 17h.01"), badge: unreadAlerts },
    { path: '/manager/change-workflow', label: 'Change Workflow', icon: ICON("M17 3a2.828 2.828 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3zM15 6l3 3") },
  ];

  return (
    <div className="dashboard-layout" data-theme={theme}>
      <Sidebar links={links} statusToggle={{ endpoint: '/api/manager/status' }} />
      <main className="main-content">
        <Outlet />
      </main>
    </div>
  );
}
