import { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import { useTheme } from '../ThemeContext';
import { apiPut } from '../api';

const SunIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="5" />
    <line x1="12" y1="1" x2="12" y2="3" /><line x1="12" y1="21" x2="12" y2="23" />
    <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
    <line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" />
    <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" /><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
  </svg>
);

const MoonIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
  </svg>
);

export default function Sidebar({ links, statusToggle }) {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useAuth();
  const { isDark, toggleTheme } = useTheme();
  const [isOnline, setIsOnline] = useState(user?.is_online || false);
  const [toggling, setToggling] = useState(false);
  const showToggle = !!statusToggle?.endpoint;

  const settingsPath = (() => {
    const r = user?.role;
    if (r === 'customer')    return '/customer/settings';
    if (r === 'manager')     return '/manager/settings';
    if (r === 'cto')         return '/cto/settings';
    if (r === 'admin')       return '/admin/settings';
    if (r === 'human_agent') return '/agent/settings';
    return null;
  })();

  const handleToggle = async () => {
    if (!showToggle) return;
    setToggling(true);
    try {
      const res = await apiPut(statusToggle.endpoint, { is_online: !isOnline });
      setIsOnline(res.is_online);
    } catch (_) {}
    finally { setToggling(false); }
  };

  const handleLogout = async () => {
    if (showToggle) {
      try { await apiPut(statusToggle.endpoint, { is_online: false }); } catch (_) {}
    }
    logout();
    navigate('/');
  };

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <div className="sidebar-brand" onClick={() => navigate('/')} style={{ cursor: 'pointer' }}>
          <img src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg" alt="KPMG" style={{ height: 26, filter: 'brightness(0) invert(1)' }} />
          <div className="sidebar-brand-text">
            <h3>Customer Handling</h3>
            <span>{user?.role === 'human_agent' ? 'Human Agent' : user?.role} Portal</span>
          </div>
        </div>
      </div>

      {showToggle && (
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
      )}

      <nav className="sidebar-nav">
        <div className="sidebar-section-label">Navigation</div>
        {links.map(link => (
          <button
            key={link.path}
            className={`sidebar-link${location.pathname === link.path ? ' active' : ''}`}
            onClick={() => navigate(link.path)}
          >
            {link.icon}
            {link.label}
          </button>
        ))}

        {settingsPath && (
          <>
            <div className="sidebar-section-label" style={{ marginTop: 16 }}>Account</div>
            <button
              className={`sidebar-link${location.pathname === settingsPath ? ' active' : ''}`}
              onClick={() => navigate(settingsPath)}
            >
              <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="3" />
                <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
              </svg>
              Settings
            </button>
          </>
        )}
      </nav>

      <div style={{ padding: '8px 12px' }}>
        <div className="sidebar-section-label">Appearance</div>
        <button
          className="sidebar-link"
          onClick={toggleTheme}
          style={{ gap: 10 }}
        >
          {isDark ? <SunIcon /> : <MoonIcon />}
          {isDark ? 'Light Mode' : 'Dark Mode'}
        </button>
      </div>

      <div className="sidebar-footer">
        <div className="sidebar-user">
          <div className="sidebar-avatar">
            {user?.name?.charAt(0)?.toUpperCase() || 'U'}
          </div>
          <div className="sidebar-user-info">
            <div className="sidebar-user-name">{user?.name}</div>
            <div className="sidebar-user-role">{user?.employee_id || (user?.role === 'human_agent' ? 'Human Agent' : user?.role)}</div>
          </div>
          <button className="sidebar-logout" onClick={handleLogout} title="Logout">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
              <polyline points="16 17 21 12 16 7" />
              <line x1="21" y1="12" x2="9" y2="12" />
            </svg>
          </button>
        </div>
      </div>
    </aside>
  );
}
