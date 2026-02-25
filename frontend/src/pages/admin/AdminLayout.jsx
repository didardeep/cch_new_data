import { Outlet } from 'react-router-dom';
import Sidebar from '../../components/Sidebar';

const ICON = (d) => <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d={d} /></svg>;

const links = [
  { path: '/admin/dashboard', label: 'Dashboard', icon: ICON("M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z") },
  { path: '/admin/users', label: 'User Management', icon: ICON("M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75") },
  { path: '/admin/tickets', label: 'Active Tickets', icon: ICON("M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z") },
  { path: '/admin/tracking', label: 'Issue Tracking', icon: ICON("M12 20V10M18 20V4M6 20v-4") },
  { path: '/admin/agent-issues', label: 'Agent Issues', icon: ICON("M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2M9 7a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM22 21v-2a4 4 0 0 0-3-3.87M19 7a4 4 0 0 1 0 7.75") },
  { path: '/admin/feedback', label: 'Feedback', icon: ICON("M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z") },
  { path: '/admin/reports', label: 'Reports & Analytics', icon: ICON("M3 3v18h18M9 17V9m4 8V5m4 12v-4") },
  { path: '/admin/data-upload', label: 'Data Upload', icon: ICON("M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M17 8l-5-5-5 5M12 3v12") },
];

export default function AdminLayout() {
  return (
    <div className="dashboard-layout">
      <Sidebar links={links} />
      <main className="main-content">
        <Outlet />
      </main>
    </div>
  );
}
