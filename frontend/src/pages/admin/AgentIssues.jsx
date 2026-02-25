import { useState, useEffect } from 'react';
import { apiGet } from '../../api';

export default function AgentIssues() {
  const [tickets, setTickets] = useState([]);
  const [agents, setAgents] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [alertsOpen, setAlertsOpen] = useState(true);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState('');
  const [agentFilter, setAgentFilter] = useState('');
  const [search, setSearch] = useState('');
  const [dismissedAlerts, setDismissedAlerts] = useState([]);

  const loadTickets = () => {
    const params = new URLSearchParams();
    if (statusFilter) params.append('status', statusFilter);
    if (agentFilter) params.append('agent_id', agentFilter);
    if (search) params.append('search', search);
    apiGet(`/api/admin/agent-tickets?${params.toString()}`).then(d => {
      setTickets(d?.tickets || []);
      if (d?.agents) setAgents(d.agents);
      setLoading(false);
    });
  };

  const loadAlerts = () => {
    apiGet('/api/admin/agent-alerts').then(d => {
      if (d?.alerts) setAlerts(d.alerts);
    });
  };

  useEffect(() => { loadTickets(); loadAlerts(); }, [statusFilter, agentFilter]);

  const handleSearch = (e) => {
    e.preventDefault();
    loadTickets();
  };

  const dismiss = (idx) => {
    setDismissedAlerts(prev => [...prev, idx]);
  };

  const visibleAlerts = alerts.filter((_, i) => !dismissedAlerts.includes(i));

  const severityConfig = {
    critical: { bg: '#fef2f2', border: '#fca5a5', color: '#991b1b', dot: '#dc2626', label: 'CRITICAL' },
    high:     { bg: '#fff7ed', border: '#fdba74', color: '#9a3412', dot: '#ea580c', label: 'HIGH' },
    warning:  { bg: '#fffbeb', border: '#fcd34d', color: '#92400e', dot: '#f59e0b', label: 'WARNING' },
  };

  const typeLabels = {
    escalation: 'Escalation',
    critical_ticket: 'Priority',
    low_rating: 'Low Rating',
    overdue: 'Overdue',
  };

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  return (
    <div>
      <div className="page-header">
        <h1>Agent Issues</h1>
        <p>Records of issues handled or being handled by human agents</p>
      </div>

      {/* Notification Center */}
      <div style={{
        background: '#fff', border: '1px solid #e2e8f0', borderRadius: 12,
        marginBottom: 20, overflow: 'hidden',
      }}>
        <div
          onClick={() => setAlertsOpen(!alertsOpen)}
          style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '14px 20px', cursor: 'pointer', userSelect: 'none',
            background: visibleAlerts.length > 0 ? '#fefce8' : '#f8fafc',
            borderBottom: alertsOpen ? '1px solid #e2e8f0' : 'none',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div style={{
              width: 32, height: 32, borderRadius: 8,
              background: visibleAlerts.length > 0 ? '#fef3c7' : '#e2e8f0',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: 16,
            }}>
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={visibleAlerts.length > 0 ? '#d97706' : '#64748b'} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
                <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
              </svg>
            </div>
            <span style={{ fontWeight: 700, fontSize: 14, color: '#1e293b' }}>
              Notification Center
            </span>
            {visibleAlerts.length > 0 && (
              <span style={{
                background: '#dc2626', color: '#fff', fontSize: 11, fontWeight: 700,
                padding: '2px 8px', borderRadius: 10, minWidth: 20, textAlign: 'center',
              }}>
                {visibleAlerts.length}
              </span>
            )}
          </div>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#64748b" strokeWidth="2"
            style={{ transform: alertsOpen ? 'rotate(180deg)' : 'rotate(0deg)', transition: 'transform 0.2s' }}>
            <polyline points="6 9 12 15 18 9"/>
          </svg>
        </div>

        {alertsOpen && (
          <div style={{ maxHeight: 320, overflowY: 'auto' }}>
            {visibleAlerts.length === 0 ? (
              <div style={{ padding: '24px 20px', textAlign: 'center', color: '#94a3b8', fontSize: 13 }}>
                No active alerts. All clear.
              </div>
            ) : (
              visibleAlerts.map((alert) => {
                const cfg = severityConfig[alert.severity] || severityConfig.warning;
                const originalIdx = alerts.indexOf(alert);
                return (
                  <div key={originalIdx} style={{
                    display: 'flex', alignItems: 'flex-start', gap: 12,
                    padding: '12px 20px', borderBottom: '1px solid #f1f5f9',
                    background: cfg.bg, transition: 'background 0.2s',
                  }}>
                    {/* Severity dot */}
                    <div style={{
                      width: 8, height: 8, borderRadius: '50%', background: cfg.dot,
                      marginTop: 6, flexShrink: 0,
                    }} />

                    {/* Content */}
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                        <span style={{
                          fontSize: 10, fontWeight: 700, color: cfg.color,
                          background: cfg.border + '40', padding: '1px 6px', borderRadius: 4,
                          textTransform: 'uppercase', letterSpacing: 0.5,
                        }}>
                          {typeLabels[alert.type] || alert.type}
                        </span>
                        <span style={{ fontSize: 11, color: '#94a3b8' }}>
                          {alert.time ? new Date(alert.time).toLocaleString() : ''}
                        </span>
                      </div>
                      <div style={{ fontSize: 13, fontWeight: 600, color: '#1e293b', marginBottom: 2 }}>
                        {alert.title}
                      </div>
                      <div style={{ fontSize: 12, color: '#64748b', lineHeight: 1.4 }}>
                        {alert.message}
                      </div>
                    </div>

                    {/* Dismiss */}
                    <button
                      onClick={() => dismiss(originalIdx)}
                      style={{
                        background: 'none', border: 'none', cursor: 'pointer',
                        color: '#94a3b8', fontSize: 16, padding: '2px 4px',
                        lineHeight: 1, flexShrink: 0, fontFamily: 'inherit',
                      }}
                      title="Dismiss"
                    >
                      x
                    </button>
                  </div>
                );
              })
            )}
          </div>
        )}
      </div>

      {/* Tickets Table */}
      <div className="table-card">
        <div className="table-header">
          <h3>Agent-Handled Tickets ({tickets.length})</h3>
          <div className="table-filters">
            <select className="filter-select" value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
              <option value="">All Status</option>
              <option value="pending">Pending</option>
              <option value="in_progress">In Progress</option>
              <option value="resolved">Resolved</option>
              <option value="escalated">Escalated</option>
            </select>
            <select className="filter-select" value={agentFilter} onChange={e => setAgentFilter(e.target.value)}>
              <option value="">All Agents</option>
              {agents.map(a => (
                <option key={a.id} value={a.id}>{a.name}</option>
              ))}
            </select>
            <form onSubmit={handleSearch} style={{ display: 'flex', gap: 6 }}>
              <input
                type="text"
                className="filter-input"
                placeholder="Search by customer name, email, ref..."
                value={search}
                onChange={e => setSearch(e.target.value)}
              />
              <button type="submit" className="btn btn-primary btn-sm">Search</button>
            </form>
          </div>
        </div>

        {tickets.length === 0 ? (
          <div className="empty-state">
            <h4>No agent-handled tickets found</h4>
            <p>No tickets are currently assigned to human agents</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ref #</th>
                  <th>Customer</th>
                  <th>Category</th>
                  <th>Subcategory</th>
                  <th>Assigned Agent</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th>Created</th>
                  <th>Resolved At</th>
                </tr>
              </thead>
              <tbody>
                {tickets.map(t => (
                  <tr key={t.id}>
                    <td>
                      <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#00338D', fontWeight: 600 }}>
                        {t.reference_number}
                      </span>
                    </td>
                    <td>
                      <div style={{ fontWeight: 500, fontSize: 13 }}>{t.user_name}</div>
                      <div style={{ fontSize: 11, color: '#94a3b8' }}>{t.user_email}</div>
                    </td>
                    <td style={{ fontSize: 13 }}>{t.category || '—'}</td>
                    <td style={{ fontSize: 13 }}>{t.subcategory || '—'}</td>
                    <td>
                      <span style={{
                        display: 'inline-flex', alignItems: 'center', gap: 6,
                        fontSize: 13, fontWeight: 500, color: '#483698',
                      }}>
                        <span style={{
                          width: 28, height: 28, borderRadius: '50%',
                          background: 'rgba(72,54,152,0.10)', display: 'inline-flex',
                          alignItems: 'center', justifyContent: 'center',
                          fontSize: 12, fontWeight: 700, color: '#483698', flexShrink: 0,
                        }}>
                          {(t.assignee_name || 'U').charAt(0).toUpperCase()}
                        </span>
                        {t.assignee_name || 'Unassigned'}
                      </span>
                    </td>
                    <td>
                      <span className={`badge badge-${t.status}`}>{t.status.replace('_', ' ')}</span>
                    </td>
                    <td>
                      <span className={`badge badge-${t.priority}`}>{t.priority}</span>
                    </td>
                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {t.created_at ? new Date(t.created_at).toLocaleString() : '—'}
                    </td>
                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {t.resolved_at ? new Date(t.resolved_at).toLocaleString() : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
