import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';

export default function CTOAlertBox() {
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all'); // 'all' | 'unread'

  const fetchAlerts = useCallback(async () => {
    try {
      const d = await apiGet('/api/cto/sla-alerts');
      if (d?.alerts) setAlerts(d.alerts);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    fetchAlerts().then(() => setLoading(false));
    const iv = setInterval(fetchAlerts, 30000);
    return () => clearInterval(iv);
  }, [fetchAlerts]);

  const markRead = async (id) => {
    await apiPut(`/api/cto/sla-alerts/${id}/read`);
    setAlerts(prev => prev.map(a => a.id === id ? { ...a, is_read: true } : a));
  };

  const markAllRead = async () => {
    await apiPut('/api/cto/sla-alerts/read-all');
    setAlerts(prev => prev.map(a => ({ ...a, is_read: true })));
  };

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const unreadCount = alerts.filter(a => !a.is_read).length;
  const filtered = filter === 'unread' ? alerts.filter(a => !a.is_read) : alerts;

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
        <p>SLA breach alerts — tickets that have exceeded their allocated SLA time.</p>
      </div>

      {/* Filter bar + Acknowledge all */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        marginBottom: 20, flexWrap: 'wrap', gap: 10,
      }}>
        <div style={{ display: 'flex', gap: 6 }}>
          {[
            { key: 'all', label: 'All Breaches' },
            { key: 'unread', label: 'Unacknowledged' },
          ].map(f => (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              style={{
                padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                border: '1px solid', cursor: 'pointer',
                background: filter === f.key ? '#dc2626' : '#fff',
                color: filter === f.key ? '#fff' : '#475569',
                borderColor: filter === f.key ? '#dc2626' : '#e2e8f0',
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
              background: '#dc2626', color: '#fff', border: 'none', cursor: 'pointer',
            }}
          >
            Acknowledge All
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
            <h4 style={{ color: '#64748b', margin: 0 }}>No breach alerts</h4>
            <p style={{ color: '#94a3b8', fontSize: 13, margin: '6px 0 0' }}>
              {filter === 'unread' ? 'All breaches have been acknowledged.' : 'No SLA breaches to display.'}
            </p>
          </div>
        </div>
      ) : (
        <div className="section-card" style={{ overflow: 'hidden', border: '1px solid #fca5a5' }}>
          <div className="section-card-body" style={{ padding: 0 }}>
            {filtered.map(a => (
              <div key={a.id} style={{
                display: 'flex', alignItems: 'flex-start', gap: 14,
                padding: '16px 20px',
                borderBottom: '1px solid #fee2e2',
                background: a.is_read ? '#fff' : '#fef2f2',
                opacity: a.is_read ? 0.65 : 1,
                transition: 'opacity 0.2s',
              }}>
                {/* BREACH badge */}
                <span style={{
                  background: '#7f1d1d', color: '#fff', fontSize: 10, fontWeight: 900,
                  padding: '4px 8px', borderRadius: 4, whiteSpace: 'nowrap',
                  marginTop: 2, letterSpacing: '0.04em', flexShrink: 0,
                }}>
                  BREACH
                </span>

                {/* Details */}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 5 }}>
                    <span style={{ fontSize: 14, fontWeight: 800, color: '#7f1d1d' }}>{a.reference_number}</span>
                    <span style={{ fontSize: 12, color: '#dc2626', fontWeight: 700 }}>
                      {a.priority?.toUpperCase()} PRIORITY
                    </span>
                  </div>
                  <p style={{ fontSize: 13, color: '#451a03', margin: '0 0 6px', lineHeight: 1.45 }}>
                    {a.message}
                  </p>
                  <div style={{ display: 'flex', gap: 16, fontSize: 11, color: '#991b1b', fontWeight: 500, flexWrap: 'wrap' }}>
                    <span>{a.category}{a.subcategory ? ` / ${a.subcategory}` : ''}</span>
                    <span>Agent: <strong>{a.assignee_name}</strong></span>
                    <span>SLA: <strong>{a.sla_hours}h</strong></span>
                    <span>Deadline: {a.sla_deadline ? new Date(a.sla_deadline).toLocaleString() : 'N/A'}</span>
                    <span>{a.created_at ? new Date(a.created_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : ''}</span>
                  </div>
                </div>

                {/* Acknowledge */}
                {!a.is_read && (
                  <button
                    onClick={() => markRead(a.id)}
                    style={{
                      background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6,
                      padding: '6px 14px', cursor: 'pointer', flexShrink: 0, marginTop: 2,
                      fontSize: 11, fontWeight: 700,
                    }}
                  >
                    Acknowledge
                  </button>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Summary */}
      <div style={{ marginTop: 16, fontSize: 12, color: '#94a3b8', textAlign: 'right' }}>
        Showing {filtered.length} of {alerts.length} breaches &middot; Auto-refreshes every 30s
      </div>
    </div>
  );
}
