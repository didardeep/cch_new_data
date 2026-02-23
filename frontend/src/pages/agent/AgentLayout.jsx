import { useState } from 'react';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../../AuthContext';
import { apiPut } from '../../api';

const ICON_GRID = (
  <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" />
    <rect x="14" y="14" width="7" height="7" /><rect x="3" y="14" width="7" height="7" />
  </svg>
);
const ICON_TICKET = (
  <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M15 5v2M15 11v2M15 17v2M5 5h14a2 2 0 0 1 2 2v3a2 2 0 0 0 0 4v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-3a2 2 0 0 0 0-4V7a2 2 0 0 1 2-2z" />
  </svg>
);
const ICON_LOGOUT = (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
    <polyline points="16 17 21 12 16 7" /><line x1="21" y1="12" x2="9" y2="12" />
  </svg>
);

const navLinks = [
  { path: '/agent/dashboard', label: 'Dashboard', icon: ICON_GRID },
  { path: '/agent/tickets',   label: 'Assigned Ticket Bucket', icon: ICON_TICKET },
];

export default function AgentLayout() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [isOnline, setIsOnline] = useState(user?.is_online || false);
  const [toggling, setToggling] = useState(false);

  const handleToggle = async () => {
    setToggling(true);
    try {
      const res = await apiPut('/api/agent/status', { is_online: !isOnline });
      setIsOnline(res.is_online);
    } catch (_) {}
    finally { setToggling(false); }
  };

  const handleLogout = async () => {
    try { await apiPut('/api/agent/status', { is_online: false }); } catch (_) {}
    logout();
    navigate('/');
  };

  return (
    <div className="dashboard-layout">
      {/* ── Sidebar ─────────────────────────────────────────────────────── */}
      <aside className="sidebar">

        {/* Brand / Logo */}
        <div className="sidebar-header">
          <div className="sidebar-brand" onClick={() => navigate('/')} style={{ cursor: 'pointer' }}>
            <img
              src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg"
              alt="KPMG"
              style={{ height: 26, filter: 'brightness(0) invert(1)' }}
            />
            <div className="sidebar-brand-text">
              <h3>Customer Handling</h3>
              <span>Agent Portal</span>
            </div>
          </div>
        </div>

        {/* Online / Offline Status Toggle */}
        <div style={{
          padding: '14px 20px',
          borderBottom: '1px solid rgba(255,255,255,0.08)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{
              width: 8, height: 8, borderRadius: '50%',
              background: isOnline ? '#22c55e' : '#64748b',
              display: 'inline-block',
              boxShadow: isOnline ? '0 0 0 3px rgba(34,197,94,0.25)' : 'none',
              transition: 'all 0.3s',
            }} />
            <span style={{ fontSize: 12, color: 'rgba(255,255,255,0.7)', fontWeight: 500 }}>
              {isOnline ? 'Online' : 'Offline'}
            </span>
          </div>
          <button
            onClick={handleToggle}
            disabled={toggling}
            title={isOnline ? 'Go Offline' : 'Go Online'}
            style={{
              position: 'relative', width: 40, height: 22,
              borderRadius: 11, border: 'none', cursor: toggling ? 'not-allowed' : 'pointer',
              background: isOnline ? '#22c55e' : 'rgba(255,255,255,0.2)',
              transition: 'background 0.3s', padding: 0, flexShrink: 0,
            }}
          >
            <span style={{
              position: 'absolute', top: 3,
              left: isOnline ? 21 : 3,
              width: 16, height: 16, borderRadius: '50%',
              background: '#fff',
              transition: 'left 0.25s',
              boxShadow: '0 1px 3px rgba(0,0,0,0.3)',
            }} />
          </button>
        </div>

        {/* Navigation */}
        <nav className="sidebar-nav">
          <div className="sidebar-section-label">Navigation</div>
          {navLinks.map(link => (
            <button
              key={link.path}
              className={`sidebar-link${location.pathname.startsWith(link.path) ? ' active' : ''}`}
              onClick={() => navigate(link.path)}
            >
              {link.icon}
              {link.label}
            </button>
          ))}
        </nav>

        {/* Footer – user info + logout */}
        <div className="sidebar-footer">
          <div className="sidebar-user">
            <div className="sidebar-avatar">
              {user?.name?.charAt(0)?.toUpperCase() || 'A'}
            </div>
            <div className="sidebar-user-info">
              <div className="sidebar-user-name">{user?.name}</div>
              <div className="sidebar-user-role">{user?.employee_id || 'Human Agent'}</div>
            </div>
            <button className="sidebar-logout" onClick={handleLogout} title="Logout">
              {ICON_LOGOUT}
            </button>
          </div>
        </div>
      </aside>

      {/* ── Main content ─────────────────────────────────────────────────── */}
      <main className="main-content">
        <Outlet />
      </main>
    </div>
  );
}
