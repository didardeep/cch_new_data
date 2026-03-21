import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiGet } from '../../api';

export default function IssueTracking() {
  const [sessions, setSessions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState('');
  const navigate = useNavigate();

  const basePath = window.location.pathname.startsWith('/cto') ? '/cto' : window.location.pathname.startsWith('/admin') ? '/admin' : '/manager';

  useEffect(() => {
    const params = statusFilter ? `?status=${statusFilter}` : '';
    apiGet(`/api/manager/chats${params}`).then(d => {
      setSessions(d?.sessions || []);
      setLoading(false);
    });
  }, [statusFilter]);

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  return (
    <div>
      <div className="page-header">
        <h1>Issue Tracking</h1>
        <p>Complete view of all customer chat sessions and their status</p>
      </div>

      <div className="table-card">
        <div className="table-header">
          <h3>All Chat Sessions ({sessions.length})</h3>
          <div className="table-filters">
            <select className="filter-select" value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
              <option value="">All Status</option>
              <option value="active">Active</option>
              <option value="resolved">Resolved</option>
              <option value="escalated">Escalated</option>
            </select>
          </div>
        </div>

        {sessions.length === 0 ? (
          <div className="empty-state">
            <h4>No sessions found</h4>
            <p>No chat sessions match the current filters</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="data-table" style={{ tableLayout: 'fixed', width: '100%' }}>
              <colgroup>
                <col style={{ width: 80  }} />  {/* Chat ID */}
                <col style={{ width: 160 }} />  {/* User Name / Email */}
                <col style={{ width: 150 }} />  {/* Category */}
                <col style={{ width: 150 }} />  {/* Subcategory */}
                <col style={{ width: 150 }} />  {/* Handled By */}
                <col style={{ width: 100 }} />  {/* Status */}
                <col style={{ width: 130 }} />  {/* Created At */}
                <col style={{ width: 130 }} />  {/* Resolved At */}
                <col style={{ width: 200 }} />  {/* Resolution Summary */}
              </colgroup>
              <thead>
                <tr>
                  <th>Chat ID</th>
                  <th>User Name / Email</th>
                  <th>Category</th>
                  <th>Subcategory</th>
                  <th>Handled By</th>
                  <th>Status</th>
                  <th style={{ whiteSpace: 'nowrap' }}>Created At</th>
                  <th style={{ whiteSpace: 'nowrap' }}>Resolved At</th>
                  <th>Resolution Summary</th>
                </tr>
              </thead>
              <tbody>
                {sessions.map(s => (
                  <tr key={s.id}>
                    <td>
                      <span className="table-link" onClick={() => navigate(`${basePath}/chat-detail/${s.id}`)}>
                        #{s.id}
                      </span>
                    </td>
                    <td>
                      <div style={{ fontWeight: 500, fontSize: 13 }}>{s.user_name}</div>
                      <div style={{ fontSize: 11, color: '#94a3b8' }}>{s.user_email}</div>
                    </td>
                    <td style={{ overflow: 'hidden' }}>
                      <div style={{
                        fontSize: 13, fontWeight: 500, color: '#0f172a',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      }}>
                        {s.sector_name || '—'}
                      </div>
                    </td>
                    <td style={{ overflow: 'hidden' }}>
                      <div style={{
                        fontSize: 13, color: '#475569',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      }}>
                        {s.subprocess_name || '—'}
                      </div>
                    </td>
                    <td style={{ overflow: 'hidden' }}>
                      <div style={{
                        fontSize: 13, fontWeight: 500, color: '#0f172a',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      }}>
                        {s.assignee_name || '—'}
                      </div>
                      {s.assignee_domain && (
                        <div style={{
                          fontSize: 11, color: '#94a3b8', textTransform: 'capitalize',
                          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        }}>
                          {s.assignee_domain}
                        </div>
                      )}
                    </td>
                    <td><span className={`badge badge-${s.status}`}>{s.status}</span></td>
                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {s.created_at ? new Date(s.created_at).toLocaleString() : '—'}
                    </td>
                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {s.resolved_at ? new Date(s.resolved_at).toLocaleString() : '—'}
                    </td>
                    <td style={{ overflow: 'hidden' }}>
                      <div style={{
                        fontSize: 13, color: '#475569',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      }}>
                        {s.summary || '—'}
                      </div>
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
