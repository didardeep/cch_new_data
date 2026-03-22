import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';
import { useTheme } from '../../ThemeContext';

const getLevelStyle = (isDark) => ({
  '625': { label: '62.5%', bg: isDark ? '#422006' : '#fffbeb', border: isDark ? '#854d0e' : '#fde68a', text: isDark ? '#fbbf24' : '#92400e', badge: '#f59e0b' },
  '750': { label: '75%',   bg: isDark ? '#431407' : '#fff7ed', border: isDark ? '#9a3412' : '#fed7aa', text: isDark ? '#fb923c' : '#9a3412', badge: '#f97316' },
  '875': { label: '87.5%', bg: isDark ? '#450a0a' : '#fef2f2', border: isDark ? '#991b1b' : '#fecaca', text: isDark ? '#fca5a5' : '#991b1b', badge: '#ef4444' },
  breach: { label: 'BREACH', bg: isDark ? '#450a0a' : '#fef2f2', border: isDark ? '#991b1b' : '#fca5a5', text: isDark ? '#fca5a5' : '#7f1d1d', badge: '#dc2626' },
});

const getPriorityBadge = (isDark) => ({
  critical: { bg: isDark ? '#450a0a' : '#fef2f2', color: isDark ? '#fca5a5' : '#dc2626', border: isDark ? '#991b1b' : '#fecaca' },
  high:     { bg: isDark ? '#431407' : '#fff7ed', color: isDark ? '#fb923c' : '#ea580c', border: isDark ? '#9a3412' : '#fed7aa' },
  medium:   { bg: isDark ? '#422006' : '#fffbeb', color: isDark ? '#fbbf24' : '#d97706', border: isDark ? '#854d0e' : '#fde68a' },
  low:      { bg: isDark ? '#052e16' : '#f0fdf4', color: isDark ? '#4ade80' : '#16a34a', border: isDark ? '#166534' : '#bbf7d0' },
});

export default function ManagerAlertBox() {
  const { isDark } = useTheme();
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all'); // 'all' | 'unread' | '625' | '750' | '875'

  const LEVEL_STYLE = getLevelStyle(isDark);
  const PRIORITY_BADGE = getPriorityBadge(isDark);

  const fetchAlerts = useCallback(async () => {
    try {
      const d = await apiGet('/api/manager/sla-alerts');
      if (d?.alerts) setAlerts(d.alerts);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    fetchAlerts().then(() => setLoading(false));
    const iv = setInterval(fetchAlerts, 30000);
    return () => clearInterval(iv);
  }, [fetchAlerts]);

  const markRead = async (id) => {
    await apiPut(`/api/manager/sla-alerts/${id}/read`);
    setAlerts(prev => prev.map(a => a.id === id ? { ...a, is_read: true } : a));
  };

  const markAllRead = async () => {
    await apiPut('/api/manager/sla-alerts/read-all');
    setAlerts(prev => prev.map(a => ({ ...a, is_read: true })));
  };

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const unreadCount = alerts.filter(a => !a.is_read).length;

  const filtered = alerts.filter(a => {
    if (filter === 'unread') return !a.is_read;
    if (filter === '625' || filter === '750' || filter === '875') return a.alert_level === filter;
    return true;
  });

  return (
    <div>
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <h1>Alert Box</h1>
          {unreadCount > 0 && (
            <span style={{
              background: '#dc2626', color: '#fff', fontSize: 12, fontWeight: 700,
              padding: '3px 10px', borderRadius: 12, lineHeight: '18px',
            }}>
              {unreadCount} unread
            </span>
          )}
        </div>
        <p>SLA escalation alerts for all tickets assigned to human agents.</p>
      </div>

      {/* Filter bar + Mark all read */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        marginBottom: 20, flexWrap: 'wrap', gap: 10,
      }}>
        <div style={{ display: 'flex', gap: 6 }}>
          {[
            { key: 'all', label: 'All' },
            { key: 'unread', label: 'Unread' },
            { key: '625', label: '62.5%' },
            { key: '750', label: '75%' },
            { key: '875', label: '87.5%' },
          ].map(f => (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              style={{
                padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                border: '1px solid', cursor: 'pointer',
                background: filter === f.key ? '#00338D' : (isDark ? '#1e293b' : '#fff'),
                color: filter === f.key ? '#fff' : (isDark ? '#94a3b8' : '#475569'),
                borderColor: filter === f.key ? '#00338D' : (isDark ? '#334155' : '#e2e8f0'),
              }}
            >
              {f.label}
            </button>
          ))}
        </div>
        {unreadCount > 0 && (
          <button
            onClick={markAllRead}
            style={{
              padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600,
              background: '#00338D', color: '#fff', border: 'none', cursor: 'pointer',
            }}
          >
            Mark All Read
          </button>
        )}
      </div>

      {/* Alert list */}
      {filtered.length === 0 ? (
        <div className="section-card">
          <div className="section-card-body" style={{ textAlign: 'center', padding: 60 }}>
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" strokeWidth="1.5" style={{ marginBottom: 12 }}>
              <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
            </svg>
            <h4 style={{ color: isDark ? '#94a3b8' : '#64748b', margin: 0 }}>No alerts found</h4>
            <p style={{ color: isDark ? '#64748b' : '#94a3b8', fontSize: 13, margin: '6px 0 0' }}>
              {filter === 'unread' ? 'All alerts have been read.' : 'No SLA alerts to display.'}
            </p>
          </div>
        </div>
      ) : (
        <div className="section-card" style={{ overflow: 'hidden' }}>
          <div className="section-card-body" style={{ padding: 0 }}>
            {filtered.map(a => {
              const ls = LEVEL_STYLE[a.alert_level] || LEVEL_STYLE['625'];
              const pb = PRIORITY_BADGE[a.priority] || PRIORITY_BADGE.medium;
              const deadline = a.sla_deadline ? new Date(a.sla_deadline) : null;
              const now = new Date();
              const hoursLeft = deadline ? ((deadline - now) / 3600000) : null;
              const timeStr = hoursLeft !== null
                ? (hoursLeft > 0
                  ? (hoursLeft >= 1 ? `${Math.round(hoursLeft * 10) / 10}h left` : `${Math.round(hoursLeft * 60)}m left`)
                  : `Overdue by ${Math.round(Math.abs(hoursLeft) * 10) / 10}h`)
                : '';

              return (
                <div key={a.id} style={{
                  display: 'flex', alignItems: 'flex-start', gap: 14,
                  padding: '16px 20px',
                  borderBottom: isDark ? '1px solid #334155' : '1px solid #f1f5f9',
                  background: a.is_read ? (isDark ? '#1e293b' : '#fff') : ls.bg,
                  opacity: a.is_read ? 0.65 : 1,
                  transition: 'opacity 0.2s',
                }}>
                  {/* Severity badge */}
                  <span style={{
                    background: ls.badge, color: '#fff', fontSize: 10, fontWeight: 800,
                    padding: '4px 8px', borderRadius: 4, whiteSpace: 'nowrap',
                    marginTop: 2, letterSpacing: '0.03em', flexShrink: 0,
                  }}>
                    {ls.label}
                  </span>

                  {/* Details */}
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 5 }}>
                      <span style={{ fontSize: 14, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a' }}>{a.reference_number}</span>
                      <span style={{
                        fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4,
                        background: pb.bg, color: pb.color, border: `1px solid ${pb.border}`,
                        textTransform: 'uppercase',
                      }}>
                        {a.priority}
                      </span>
                      {timeStr && (
                        <span style={{
                          fontSize: 11, fontWeight: 600,
                          color: hoursLeft > 0 ? '#d97706' : '#dc2626',
                        }}>
                          {timeStr}
                        </span>
                      )}
                    </div>
                    <p style={{ fontSize: 13, color: isDark ? '#cbd5e1' : '#334155', margin: '0 0 6px', lineHeight: 1.45 }}>
                      {a.message}
                    </p>
                    <div style={{ display: 'flex', gap: 16, fontSize: 11, color: isDark ? '#64748b' : '#94a3b8', flexWrap: 'wrap' }}>
                      <span>{a.category}{a.subcategory ? ` / ${a.subcategory}` : ''}</span>
                      <span>Agent: <strong style={{ color: isDark ? '#94a3b8' : '#475569' }}>{a.assignee_name}</strong></span>
                      <span>SLA: <strong style={{ color: isDark ? '#94a3b8' : '#475569' }}>{a.sla_hours}h</strong></span>
                      <span>{a.created_at ? new Date(a.created_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : ''}</span>
                    </div>
                  </div>

                  {/* Mark read */}
                  {!a.is_read && (
                    <button
                      onClick={() => markRead(a.id)}
                      title="Mark as read"
                      style={{
                        background: '#00338D', color: '#fff', border: 'none', borderRadius: 6,
                        padding: '6px 14px', cursor: 'pointer', flexShrink: 0, marginTop: 2,
                        fontSize: 11, fontWeight: 600,
                      }}
                    >
                      Mark Read
                    </button>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Summary */}
      <div style={{ marginTop: 16, fontSize: 12, color: isDark ? '#64748b' : '#94a3b8', textAlign: 'right' }}>
        Showing {filtered.length} of {alerts.length} alerts &middot; Auto-refreshes every 30s
      </div>
    </div>
  );
}
