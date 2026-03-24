import { useNavigate, useLocation, Outlet } from 'react-router-dom';
import { useAuth } from '../../AuthContext';
import { useTheme } from '../../ThemeContext';
import { useState } from 'react';
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

const ICON_NETWORK = (
  <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="2"/>
    <path d="M16.24 7.76a6 6 0 010 8.49M7.76 16.24a6 6 0 010-8.49M20.49 3.51a12 12 0 010 16.99M3.51 20.49a12 12 0 010-16.99"/>
  </svg>
);

const ICON_ALERT = (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
    <line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
  </svg>
);
const ICON_AI = (
  <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 2L15.09 8.26L22 9.27L17 14.14L18.18 21.02L12 17.77L5.82 21.02L7 14.14L2 9.27L8.91 8.26L12 2z"/>
  </svg>
);

const navLinks = [
  { path: '/agent/dashboard', label: 'Dashboard',              icon: ICON_GRID    },
  { path: '/agent/tickets',   label: 'Assigned Ticket Bucket', icon: ICON_TICKET  },
  { path: '/agent/network',   label: 'Network Analysis',       icon: ICON_NETWORK },
  { path: '/agent/network-ai', label: 'Network AI',            icon: ICON_AI      },
  { path: '/agent/network-issues', label: 'Network Issues',    icon: ICON_ALERT   },
];

export default function AgentLayout() {
  const { user, logout } = useAuth();
  const { theme, isDark, toggleTheme } = useTheme();
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
    <div className="dashboard-layout" data-theme={theme}>
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
              className={`sidebar-link${location.pathname===link.path||location.pathname.startsWith(link.path+'/') ? ' active' : ''}`}
              onClick={() => navigate(link.path)}
            >
              {link.icon}
              {link.label}
            </button>
          ))}

          <div className="sidebar-section-label" style={{ marginTop: 16 }}>Account</div>
          <button
            className={`sidebar-link${location.pathname === '/agent/settings' ? ' active' : ''}`}
            onClick={() => navigate('/agent/settings')}
          >
            <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="3" />
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
            </svg>
            Settings
          </button>
        </nav>

        {/* Appearance toggle */}
        <div style={{ padding: '8px 12px' }}>
          <div className="sidebar-section-label">Appearance</div>
          <button className="sidebar-link" onClick={toggleTheme} style={{ gap: 10 }}>
            {isDark ? (
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="5" />
                <line x1="12" y1="1" x2="12" y2="3" /><line x1="12" y1="21" x2="12" y2="23" />
                <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
                <line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" />
                <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" /><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
              </svg>
            ) : (
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
              </svg>
            )}
            {isDark ? 'Light Mode' : 'Dark Mode'}
          </button>
        </div>

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