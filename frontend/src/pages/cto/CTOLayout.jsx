import { Outlet } from 'react-router-dom';
import Sidebar from '../../components/Sidebar';

const ICON = (d) => <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>;

const links = [
  { path: '/cto/dashboard', label: 'Overview', icon: ICON("M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z") },
  { path: '/cto/tickets', label: 'All Tickets', icon: ICON("M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z") },
  { path: '/cto/tracking', label: 'Issue Tracking', icon: ICON("M12 20V10M18 20V4M6 20v-4") },
  { path: '/cto/alerts', label: 'Alert Box', icon: ICON("M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0zM12 9v4M12 17h.01") },
  { path: '/cto/roster', label: 'Duty Roster', icon: ICON("M8 2v4M16 2v4M3 8h18M5 12h4M10 12h4M15 12h4M5 16h4M10 16h4M15 16h4") },
];

export default function CTOLayout() {
  return (
    <div className="dashboard-layout">
      <Sidebar links={links} />
      <main className="main-content">
        <Outlet />
      </main>
    </div>
  );
}
