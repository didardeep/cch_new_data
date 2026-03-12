import { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import { apiPut } from '../api';

export default function Sidebar({ links, statusToggle }) {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useAuth();
  const [isOnline, setIsOnline] = useState(user?.is_online || false);
  const [toggling, setToggling] = useState(false);
  const showToggle = !!statusToggle?.endpoint;

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
      </nav>

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
