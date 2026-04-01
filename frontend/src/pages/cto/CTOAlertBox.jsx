import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';
import { useTheme } from '../../ThemeContext';

export default function CTOAlertBox() {
  const [alerts, setAlerts] = useState([]);
  const [crAlerts, setCrAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all'); // 'all' | 'unread'
  const [crFilter, setCrFilter] = useState('all');
  const [section, setSection] = useState('tickets'); // 'tickets' | 'crs'
  const { isDark } = useTheme();

  const fetchAlerts = useCallback(async () => {
    try {
      const [d, cr] = await Promise.all([
        apiGet('/api/cto/sla-alerts'),
        apiGet('/api/cto/cr-alerts').catch(() => ({ alerts: [] })),
      ]);
      if (d?.alerts) setAlerts(d.alerts);
      if (cr?.alerts) setCrAlerts(cr.alerts);
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

  const markCrRead = async (id) => {
    await apiPut(`/api/cto/cr-alerts/${id}/read`);
    setCrAlerts(prev => prev.map(a => a.id === id ? { ...a, is_read: true } : a));
  };

  const markAllCrRead = async () => {
    await apiPut('/api/cto/cr-alerts/read-all');
    setCrAlerts(prev => prev.map(a => ({ ...a, is_read: true })));
  };

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const unreadCount = alerts.filter(a => !a.is_read).length;
  const crUnreadCount = crAlerts.filter(a => !a.is_read).length;
  const filtered = filter === 'unread' ? alerts.filter(a => !a.is_read) : alerts;
  const crFiltered = crFilter === 'unread' ? crAlerts.filter(a => !a.is_read) : crAlerts;

  const LEVEL_STYLE = {
    '90':    { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa', label: 'CRITICAL' },
    'breach': { bg: '#fef2f2', color: '#7f1d1d', border: '#fecaca', label: 'BREACH' },
  };

  return (
    <div>
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <h1>Alert Box</h1>
          {(unreadCount + crUnreadCount) > 0 && (
            <span style={{
              background: '#dc2626', color: '#fff', fontSize: 12, fontWeight: 700,
              padding: '3px 10px', borderRadius: 12, lineHeight: '18px',
            }}>
              {unreadCount + crUnreadCount} unread
            </span>
          )}
        </div>
        <p>SLA breach alerts for tickets and change requests.</p>
      </div>

      {/* Section Toggle */}
      <div style={{ display: 'flex', gap: 0, marginBottom: 20, borderRadius: 10, overflow: 'hidden', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, width: 'fit-content' }}>
        <button onClick={() => setSection('tickets')} style={{
          padding: '10px 24px', fontSize: 13, fontWeight: 700, cursor: 'pointer', border: 'none',
          background: section === 'tickets' ? '#dc2626' : (isDark ? '#1e293b' : '#f8fafc'),
          color: section === 'tickets' ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          Ticket SLA Alerts ({alerts.length})
          {unreadCount > 0 && <span style={{ background: section === 'tickets' ? 'rgba(255,255,255,0.3)' : '#dc2626', color: '#fff', fontSize: 10, fontWeight: 800, padding: '1px 6px', borderRadius: 8 }}>{unreadCount}</span>}
        </button>
        <button onClick={() => setSection('crs')} style={{
          padding: '10px 24px', fontSize: 13, fontWeight: 700, cursor: 'pointer', border: 'none',
          borderLeft: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
          background: section === 'crs' ? '#ea580c' : (isDark ? '#1e293b' : '#f8fafc'),
          color: section === 'crs' ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          CR SLA Alerts ({crAlerts.length})
          {crUnreadCount > 0 && <span style={{ background: section === 'crs' ? 'rgba(255,255,255,0.3)' : '#ea580c', color: '#fff', fontSize: 10, fontWeight: 800, padding: '1px 6px', borderRadius: 8 }}>{crUnreadCount}</span>}
        </button>
      </div>

      {/* ── TICKET SLA ALERTS SECTION ── */}
      {section === 'tickets' && (
        <>
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
                    background: filter === f.key ? '#dc2626' : isDark ? '#1e293b' : '#fff',
                    color: filter === f.key ? '#fff' : isDark ? '#94a3b8' : '#475569',
                    borderColor: filter === f.key ? '#dc2626' : isDark ? '#334155' : '#e2e8f0',
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

          {filtered.length === 0 ? (
            <div className="section-card">
              <div className="section-card-body" style={{ textAlign: 'center', padding: 60 }}>
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" strokeWidth="1.5" style={{ marginBottom: 12 }}>
                  <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
                </svg>
                <h4 style={{ color: isDark ? '#94a3b8' : '#64748b', margin: 0 }}>No breach alerts</h4>
                <p style={{ color: isDark ? '#64748b' : '#94a3b8', fontSize: 13, margin: '6px 0 0' }}>
                  {filter === 'unread' ? 'All breaches have been acknowledged.' : 'No SLA breaches to display.'}
                </p>
              </div>
            </div>
          ) : (
            <div className="section-card" style={{ overflow: 'hidden', border: `1px solid ${isDark ? '#7f1d1d' : '#fca5a5'}` }}>
              <div className="section-card-body" style={{ padding: 0 }}>
                {filtered.map(a => (
                  <div key={a.id} style={{
                    display: 'flex', alignItems: 'flex-start', gap: 14,
                    padding: '16px 20px',
                    borderBottom: `1px solid ${isDark ? '#451a1a' : '#fee2e2'}`,
                    background: a.is_read ? (isDark ? '#1e293b' : '#fff') : (isDark ? '#3b1111' : '#fef2f2'),
                    opacity: a.is_read ? 0.65 : 1,
                    transition: 'opacity 0.2s',
                  }}>
                    <span style={{
                      background: '#7f1d1d', color: '#fff', fontSize: 10, fontWeight: 900,
                      padding: '4px 8px', borderRadius: 4, whiteSpace: 'nowrap',
                      marginTop: 2, letterSpacing: '0.04em', flexShrink: 0,
                    }}>
                      BREACH
                    </span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 5 }}>
                        <span style={{ fontSize: 14, fontWeight: 800, color: '#7f1d1d' }}>{a.reference_number}</span>
                        <span style={{ fontSize: 12, color: '#dc2626', fontWeight: 700 }}>
                          {a.priority?.toUpperCase()} PRIORITY
                        </span>
                      </div>
                      <p style={{ fontSize: 13, color: isDark ? '#fca5a5' : '#451a03', margin: '0 0 6px', lineHeight: 1.45 }}>
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

          <div style={{ marginTop: 16, fontSize: 12, color: isDark ? '#64748b' : '#94a3b8', textAlign: 'right' }}>
            Showing {filtered.length} of {alerts.length} breaches &middot; Auto-refreshes every 30s
          </div>
        </>
      )}

      {/* ── CR SLA ALERTS SECTION ── */}
      {section === 'crs' && (
        <>
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            marginBottom: 20, flexWrap: 'wrap', gap: 10,
          }}>
            <div style={{ display: 'flex', gap: 6 }}>
              {[
                { key: 'all', label: 'All CR Alerts' },
                { key: 'unread', label: 'Unacknowledged' },
              ].map(f => (
                <button
                  key={f.key}
                  onClick={() => setCrFilter(f.key)}
                  style={{
                    padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                    border: '1px solid', cursor: 'pointer',
                    background: crFilter === f.key ? '#ea580c' : isDark ? '#1e293b' : '#fff',
                    color: crFilter === f.key ? '#fff' : isDark ? '#94a3b8' : '#475569',
                    borderColor: crFilter === f.key ? '#ea580c' : isDark ? '#334155' : '#e2e8f0',
                  }}
                >
                  {f.label}
                </button>
              ))}
            </div>
            {crUnreadCount > 0 && (
              <button
                onClick={markAllCrRead}
                style={{
                  padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                  background: '#ea580c', color: '#fff', border: 'none', cursor: 'pointer',
                }}
              >
                Acknowledge All
              </button>
            )}
          </div>

          {crFiltered.length === 0 ? (
            <div className="section-card">
              <div className="section-card-body" style={{ textAlign: 'center', padding: 60 }}>
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" strokeWidth="1.5" style={{ marginBottom: 12 }}>
                  <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
                </svg>
                <h4 style={{ color: isDark ? '#94a3b8' : '#64748b', margin: 0 }}>No CR alerts</h4>
                <p style={{ color: isDark ? '#64748b' : '#94a3b8', fontSize: 13, margin: '6px 0 0' }}>
                  {crFilter === 'unread' ? 'All CR alerts have been acknowledged.' : 'No CR SLA alerts to display.'}
                </p>
              </div>
            </div>
          ) : (
            <div className="section-card" style={{ overflow: 'hidden', border: `1px solid ${isDark ? '#7c2d12' : '#fed7aa'}` }}>
              <div className="section-card-body" style={{ padding: 0 }}>
                {crFiltered.map(a => {
                  const ls = LEVEL_STYLE[a.alert_level] || LEVEL_STYLE.breach;
                  const isBreach = a.alert_level === 'breach';
                  const srcColor = a.source === 'customer' ? '#00338D' : '#7c3aed';
                  const srcLabel = a.source === 'customer' ? 'Customer' : 'AI';

                  return (
                    <div key={a.id} style={{
                      display: 'flex', alignItems: 'flex-start', gap: 14,
                      padding: '16px 20px',
                      borderBottom: `1px solid ${isDark ? '#451a1a' : '#fee2e2'}`,
                      background: a.is_read ? (isDark ? '#1e293b' : '#fff') : (isDark ? (isBreach ? '#3b1111' : '#2d1800') : (isBreach ? '#fef2f2' : '#fff7ed')),
                      opacity: a.is_read ? 0.65 : 1,
                      transition: 'opacity 0.2s',
                    }}>
                      {/* Level badge */}
                      <span style={{
                        background: ls.color, color: '#fff', fontSize: 10, fontWeight: 900,
                        padding: '4px 8px', borderRadius: 4, whiteSpace: 'nowrap',
                        marginTop: 2, letterSpacing: '0.04em', flexShrink: 0,
                      }}>
                        {ls.label}
                      </span>

                      {/* Details */}
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 5 }}>
                          <span style={{ fontSize: 14, fontWeight: 800, fontFamily: "'IBM Plex Mono',monospace", color: isDark ? '#4da3ff' : '#00338D' }}>{a.cr_number}</span>
                          <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: `${srcColor}15`, color: srcColor, border: `1px solid ${srcColor}30` }}>{srcLabel}</span>
                          {a.change_type && (
                            <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: a.change_type === 'emergency' ? '#fef2f2' : '#fff7ed', color: a.change_type === 'emergency' ? '#dc2626' : '#ea580c', border: `1px solid ${a.change_type === 'emergency' ? '#fecaca' : '#fed7aa'}` }}>
                              {a.change_type.charAt(0).toUpperCase() + a.change_type.slice(1)}
                            </span>
                          )}
                        </div>
                        <p style={{ fontSize: 13, color: isDark ? '#fca5a5' : '#451a03', margin: '0 0 6px', lineHeight: 1.45 }}>
                          {a.message}
                        </p>
                        <div style={{ display: 'flex', gap: 16, fontSize: 11, color: isDark ? '#94a3b8' : '#991b1b', fontWeight: 500, flexWrap: 'wrap' }}>
                          {a.category && <span>{a.category}{a.subcategory ? ` / ${a.subcategory}` : ''}</span>}
                          {a.raised_by_name && <span>Agent: <strong>{a.raised_by_name}</strong></span>}
                          {a.zone && <span>Zone: {a.zone}</span>}
                          {a.cr_sla_hours && <span>SLA: <strong>{a.cr_sla_hours}h</strong></span>}
                          <span>Deadline: {a.cr_sla_deadline ? new Date(a.cr_sla_deadline).toLocaleString() : 'N/A'}</span>
                          <span>{a.created_at ? new Date(a.created_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : ''}</span>
                        </div>
                      </div>

                      {/* Acknowledge */}
                      {!a.is_read && (
                        <button
                          onClick={() => markCrRead(a.id)}
                          style={{
                            background: '#ea580c', color: '#fff', border: 'none', borderRadius: 6,
                            padding: '6px 14px', cursor: 'pointer', flexShrink: 0, marginTop: 2,
                            fontSize: 11, fontWeight: 700,
                          }}
                        >
                          Acknowledge
                        </button>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          <div style={{ marginTop: 16, fontSize: 12, color: isDark ? '#64748b' : '#94a3b8', textAlign: 'right' }}>
            Showing {crFiltered.length} of {crAlerts.length} CR alerts &middot; Auto-refreshes every 30s
          </div>
        </>
      )}
    </div>
  );
}
