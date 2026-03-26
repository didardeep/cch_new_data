import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiGet, apiPut } from '../../api';
import { useAuth } from '../../AuthContext';

const ESCALATION_BANNER_STYLE = {
  background: '#fdf4ff',
  border: '1px solid #e9d5ff',
  borderRadius: 12,
  padding: '16px 20px',
  marginBottom: 24,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 16,
};

const LEVEL_STYLE = {
  '625': { label: '62.5%', bg: '#fffbeb', border: '#fde68a', text: '#92400e', badge: '#f59e0b' },
  '750': { label: '75%',   bg: '#fff7ed', border: '#fed7aa', text: '#9a3412', badge: '#f97316' },
  '875': { label: '87.5%', bg: '#fef2f2', border: '#fecaca', text: '#991b1b', badge: '#ef4444' },
  breach: { label: 'BREACH', bg: '#fef2f2', border: '#fca5a5', text: '#7f1d1d', badge: '#dc2626' },
};

const PRIORITY_BADGE = {
  critical: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca' },
  high:     { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa' },
  medium:   { bg: '#fffbeb', color: '#d97706', border: '#fde68a' },
  low:      { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0' },
};

export default function ManagerDashboard() {
  const [data, setData] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [pendingEscalations, setPendingEscalations] = useState(0);
  const [loading, setLoading] = useState(true);
  const [alertsExpanded, setAlertsExpanded] = useState(true);
  const { user } = useAuth();
  const navigate = useNavigate();

  const fetchAlerts = useCallback(async () => {
    try {
      const d = await apiGet('/api/manager/sla-alerts');
      if (d?.alerts) setAlerts(d.alerts);
    } catch { /* ignore */ }
  }, []);

  const fetchEscalations = useCallback(async () => {
    try {
      const d = await apiGet('/api/manager/parameter-changes?status=pending');
      setPendingEscalations(d?.changes?.length || 0);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    Promise.allSettled([
      apiGet('/api/manager/dashboard'),
      apiGet('/api/manager/sla-alerts'),
      apiGet('/api/manager/parameter-changes?status=pending'),
    ]).then(([dashRes, alertRes, changesRes]) => {
      if (dashRes.status === 'fulfilled')  setData(dashRes.value);
      if (alertRes.status === 'fulfilled' && alertRes.value?.alerts) setAlerts(alertRes.value.alerts);
      if (changesRes.status === 'fulfilled') setPendingEscalations(changesRes.value?.changes?.length || 0);
      setLoading(false);
    });
    // Poll alerts and escalations every 30s
    const iv = setInterval(() => { fetchAlerts(); fetchEscalations(); }, 30000);
    return () => clearInterval(iv);
  }, [fetchAlerts, fetchEscalations]);

  const markRead = async (id) => {
    await apiPut(`/api/manager/sla-alerts/${id}/read`);
    setAlerts(prev => prev.map(a => a.id === id ? { ...a, is_read: true } : a));
  };

  const markAllRead = async () => {
    await apiPut('/api/manager/sla-alerts/read-all');
    setAlerts(prev => prev.map(a => ({ ...a, is_read: true })));
  };

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const s = data?.stats || {};
  const cats = data?.category_breakdown || [];
  const unreadAlerts = alerts.filter(a => !a.is_read);
  const unreadCount = unreadAlerts.length;

  return (
    <div>
      <div className="page-header">
        <h1>Manager Dashboard</h1>
        <p>Welcome back, {user?.name}. Here's your operational overview.</p>
      </div>

      {/* Escalation banner removed — escalations are now in Change Workflow */}

      {/* SLA Alerts removed from dashboard — available in Alert Box */}
      {false && unreadAlerts.length > 0 && (
        <div style={{
          background: '#fff', border: '1px solid #e2e8f0', borderRadius: 12,
          marginBottom: 24, overflow: 'hidden', boxShadow: '0 1px 4px rgba(0,0,0,0.05)',
        }}>
          {/* Header */}
          <div
            onClick={() => setAlertsExpanded(!alertsExpanded)}
            style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '14px 20px', cursor: 'pointer', userSelect: 'none',
              borderBottom: alertsExpanded ? '1px solid #e2e8f0' : 'none',
              background: unreadCount > 0 ? '#fef2f2' : '#f8fafc',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={unreadCount > 0 ? '#dc2626' : '#64748b'} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
              </svg>
              <span style={{ fontSize: 14, fontWeight: 700, color: '#0f172a' }}>SLA Alerts</span>
              {unreadCount > 0 && (
                <span style={{
                  background: '#dc2626', color: '#fff', fontSize: 11, fontWeight: 700,
                  padding: '2px 8px', borderRadius: 10, lineHeight: '16px',
                }}>
                  {unreadCount} unread
                </span>
              )}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              {unreadCount > 0 && (
                <button
                  onClick={(e) => { e.stopPropagation(); markAllRead(); }}
                  style={{
                    background: 'none', border: '1px solid #e2e8f0', borderRadius: 6,
                    padding: '4px 10px', fontSize: 11, fontWeight: 600, color: '#64748b',
                    cursor: 'pointer',
                  }}
                >
                  Mark all read
                </button>
              )}
              <span style={{ fontSize: 18, color: '#94a3b8', transform: alertsExpanded ? 'rotate(180deg)' : 'rotate(0)', transition: 'transform 0.2s' }}>
                ▾
              </span>
            </div>
          </div>

          {/* Alert list */}
          {alertsExpanded && (
            <div style={{ maxHeight: 400, overflowY: 'auto' }}>
              {unreadAlerts.map(a => {
                const ls = LEVEL_STYLE[a.alert_level] || LEVEL_STYLE['625'];
                const pb = PRIORITY_BADGE[a.priority] || PRIORITY_BADGE.medium;
                const deadline = a.sla_deadline ? new Date(a.sla_deadline) : null;
                const now = new Date();
                const hoursLeft = deadline ? ((deadline - now) / 3600000) : null;
                const timeStr = hoursLeft !== null
                  ? (hoursLeft > 0 ? `${Math.round(hoursLeft * 10) / 10}h left` : `Overdue by ${Math.round(Math.abs(hoursLeft) * 10) / 10}h`)
                  : '';

                return (
                  <div key={a.id} style={{
                    display: 'flex', alignItems: 'flex-start', gap: 14,
                    padding: '14px 20px',
                    borderBottom: '1px solid #f1f5f9',
                    background: '#fffbeb',
                    opacity: 1,
                  }}>
                    {/* Severity badge */}
                    <span style={{
                      background: ls.badge, color: '#fff', fontSize: 9, fontWeight: 800,
                      padding: '3px 7px', borderRadius: 4, whiteSpace: 'nowrap',
                      marginTop: 2, letterSpacing: '0.03em', flexShrink: 0,
                    }}>
                      {ls.label}
                    </span>

                    {/* Details */}
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 4 }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: '#0f172a' }}>{a.reference_number}</span>
                        <span style={{
                          fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 4,
                          background: pb.bg, color: pb.color, border: `1px solid ${pb.border}`,
                          textTransform: 'uppercase',
                        }}>
                          {a.priority}
                        </span>
                        {timeStr && (
                          <span style={{ fontSize: 11, color: hoursLeft > 0 ? '#d97706' : '#dc2626', fontWeight: 600 }}>
                            {timeStr}
                          </span>
                        )}
                      </div>
                      <div style={{ fontSize: 12, color: '#475569', marginBottom: 3 }}>
                        {a.category}{a.subcategory ? ` / ${a.subcategory}` : ''}
                      </div>
                      <div style={{ display: 'flex', gap: 16, fontSize: 11, color: '#94a3b8', flexWrap: 'wrap' }}>
                        <span>Agent: <strong style={{ color: '#475569' }}>{a.assignee_name}</strong></span>
                        <span>SLA: <strong style={{ color: '#475569' }}>{a.sla_hours}h</strong></span>
                        <span>{a.created_at ? new Date(a.created_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : ''}</span>
                      </div>
                    </div>

                    {/* Mark read */}
                    <button
                        onClick={() => markRead(a.id)}
                        title="Mark as read"
                        style={{
                          background: 'none', border: '1px solid #e2e8f0', borderRadius: 6,
                          padding: '4px 8px', cursor: 'pointer', flexShrink: 0, marginTop: 2,
                          fontSize: 11, color: '#64748b',
                        }}
                      >
                        ✓
                      </button>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* ── Stats Cards ────────────────────────────────────────────── */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon primary"></div></div>
          <div className="stat-card-label">Total Chats</div>
          <div className="stat-card-value">{s.total_chats || 0}</div>
          <div className="stat-card-sub">{s.active_chats || 0} active now</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon success"></div></div>
          <div className="stat-card-label">Resolved</div>
          <div className="stat-card-value">{s.resolved_chats || 0}</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon warning"></div></div>
          <div className="stat-card-label">Total Tickets</div>
          <div className="stat-card-value">{s.total_tickets || 0}</div>
          <div className="stat-card-sub">{s.pending_tickets || 0} pending</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon danger"></div></div>
          <div className="stat-card-label">Critical</div>
          <div className="stat-card-value">{s.critical_tickets || 0}</div>
          <div className="stat-card-sub">{s.high_tickets || 0} high priority</div>
        </div>
        <div className="stat-card" style={s.manager_escalated_tickets > 0 ? { borderColor: '#e9d5ff', background: '#fdf4ff' } : {}}>
          <div className="stat-card-header"><div className="stat-card-icon" style={{ background: '#f3e8ff' }}></div></div>
          <div className="stat-card-label" style={{ color: '#6d28d9' }}>Needs Review</div>
          <div className="stat-card-value" style={{ color: '#6d28d9' }}>{s.manager_escalated_tickets || 0}</div>
          <div className="stat-card-sub">escalated by experts</div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 20 }}>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon primary"></div></div>
          <div className="stat-card-label">Total Customers</div>
          <div className="stat-card-value">{s.total_customers || 0}</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon success"></div></div>
          <div className="stat-card-label">CSAT Score</div>
          <div className="stat-card-value">{s.csat_score || 0}%</div>
          <div className="stat-card-sub">{s.total_feedback || 0} responses</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon warning"></div></div>
          <div className="stat-card-label">Avg Rating</div>
          <div className="stat-card-value">{s.avg_rating || 0}/5</div>
          <div className="stat-card-sub">{s.total_feedback || 0} feedbacks</div>
        </div>
      </div>

      {cats.length > 0 && (
        <div className="section-card" style={{ marginTop: 24 }}>
          <div className="section-card-header">
            <h3>Issues by Category</h3>
          </div>
          <div className="section-card-body">
            {cats.map((c, i) => (
              <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: i < cats.length - 1 ? '1px solid #f0f2f5' : 'none' }}>
                <span style={{ fontSize: 14, fontWeight: 500 }}>{c.name || 'Uncategorized'}</span>
                <span style={{ fontSize: 14, fontWeight: 700, color: '#00338D' }}>{c.count}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div style={{ display: 'flex', gap: 12, marginTop: 20 }}>
        <button className="btn btn-primary btn-sm" onClick={() => navigate('/manager/tickets')}>View All Tickets</button>
        <button className="btn btn-outline btn-sm" onClick={() => navigate('/manager/tracking')}>Issue Tracking</button>
      </div>
    </div>
  );
}

