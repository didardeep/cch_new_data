import { useState, useEffect } from 'react';
import { apiGet, apiPut } from '../../api';

export default function ActiveTickets() {
  const [tickets, setTickets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [changesLoading, setChangesLoading] = useState(true);
  const [changeRequests, setChangeRequests] = useState([]);
  const [statusFilter, setStatusFilter] = useState('');
  const [priorityFilter, setPriorityFilter] = useState('');
  const [search, setSearch] = useState('');
  const [editingId, setEditingId] = useState(null);
  const [editData, setEditData] = useState({});

  const loadChangeRequests = async () => {
    setChangesLoading(true);
    try {
      const d = await apiGet('/api/manager/parameter-changes?status=pending');
      setChangeRequests(d?.changes || []);
    } catch (_) {
      setChangeRequests([]);
    }
    setChangesLoading(false);
  };

  const loadTickets = () => {
    const params = new URLSearchParams();
    if (statusFilter) params.append('status', statusFilter);
    if (priorityFilter) params.append('priority', priorityFilter);
    if (search) params.append('search', search);
    apiGet(`/api/manager/tickets?${params.toString()}`).then(d => {
      setTickets(d?.tickets || []);
      setLoading(false);
    });
  };

  useEffect(() => {
    loadTickets();
    loadChangeRequests();
  }, [statusFilter, priorityFilter]);

  const handleSearch = (e) => {
    e.preventDefault();
    loadTickets();
  };

  const handleUpdate = async (id) => {
    await apiPut(`/api/manager/tickets/${id}`, editData);
    setEditingId(null);
    setEditData({});
    loadTickets();
  };

  const handleReviewChange = async (changeId, decision) => {
    const note = window.prompt(
      decision === 'approved'
        ? 'Optional note for agent (approval):'
        : 'Reason for disapproval (recommended):',
      ''
    );
    await apiPut(`/api/manager/parameter-changes/${changeId}/review`, { decision, note: note || '' });
    loadChangeRequests();
  };

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  return (
    <div>
      <div className="page-header">
        <h1>Active Tickets</h1>
        <p>Manage and resolve customer support tickets</p>
      </div>

      <div className="table-card" style={{ marginBottom: 16 }}>
        <div className="table-header">
          <h3>Pending Parameter Change Requests ({changeRequests.length})</h3>
        </div>
        {changesLoading ? (
          <div className="page-loader" style={{ minHeight: 80 }}><div className="spinner" /></div>
        ) : changeRequests.length === 0 ? (
          <div className="empty-state" style={{ padding: 16 }}>
            <p>No pending requests.</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ticket</th>
                  <th>Agent</th>
                  <th>Proposed Change</th>
                  <th>Requested At</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {changeRequests.map(c => (
                  <tr key={c.id}>
                    <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12 }}>
                      {c.ticket?.reference_number || `#${c.ticket_id}`}
                    </td>
                    <td>{c.agent_name || `Agent #${c.agent_id}`}</td>
                    <td style={{ maxWidth: 360, whiteSpace: 'normal', lineHeight: 1.5 }}>{c.proposed_change}</td>
                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {c.created_at ? new Date(c.created_at).toLocaleString() : '-'}
                    </td>
                    <td>
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                        <button className="btn btn-success btn-sm" onClick={() => handleReviewChange(c.id, 'approved')}>Approve</button>
                        <button className="btn btn-outline btn-sm" onClick={() => handleReviewChange(c.id, 'disapproved')}>Disapprove</button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="table-card">
        <div className="table-header">
          <h3>All Tickets ({tickets.length})</h3>
          <div className="table-filters">
            <select className="filter-select" value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
              <option value="">All Status</option>
              <option value="pending">Pending</option>
              <option value="in_progress">In Progress</option>
              <option value="resolved">Resolved</option>
              <option value="escalated">Escalated</option>
            </select>
            <select className="filter-select" value={priorityFilter} onChange={e => setPriorityFilter(e.target.value)}>
              <option value="">All Priority</option>
              <option value="low">Low</option>
              <option value="medium">Medium</option>
              <option value="high">High</option>
              <option value="critical">Critical</option>
            </select>
            <form onSubmit={handleSearch} style={{ display: 'flex', gap: 6 }}>
              <input type="text" className="filter-input" placeholder="Search by name, email, ref..."
                value={search} onChange={e => setSearch(e.target.value)} />
              <button type="submit" className="btn btn-primary btn-sm">Search</button>
            </form>
          </div>
        </div>

        {tickets.length === 0 ? (
          <div className="empty-state">
            <h4>No tickets found</h4>
            <p>Try adjusting your filters</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Ref #</th>
                  <th>Customer</th>
                  <th>Category</th>
                  <th>Description</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {tickets.map(t => (
                  <tr key={t.id}>
                    <td><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#00338D', fontWeight: 600 }}>{t.reference_number}</span></td>
                    <td>
                      <div style={{ fontWeight: 500, fontSize: 13 }}>{t.user_name}</div>
                      <div style={{ fontSize: 11, color: '#94a3b8' }}>{t.user_email}</div>
                    </td>
                    <td style={{ fontSize: 13 }}>{t.category}</td>
                    <td style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 13 }}>{t.description}</td>
                    <td>
                      {editingId === t.id ? (
                        <select className="filter-select" value={editData.status || t.status}
                          onChange={e => setEditData(d => ({ ...d, status: e.target.value }))}>
                          <option value="pending">Pending</option>
                          <option value="in_progress">In Progress</option>
                          <option value="resolved">Resolved</option>
                          <option value="escalated">Escalated</option>
                        </select>
                      ) : (
                        <span className={`badge badge-${t.status}`}>{t.status.replace('_', ' ')}</span>
                      )}
                    </td>
                    <td>
                      {editingId === t.id ? (
                        <select className="filter-select" value={editData.priority || t.priority}
                          onChange={e => setEditData(d => ({ ...d, priority: e.target.value }))}>
                          <option value="low">Low</option>
                          <option value="medium">Medium</option>
                          <option value="high">High</option>
                          <option value="critical">Critical</option>
                        </select>
                      ) : (
                        <span className={`badge badge-${t.priority}`}>{t.priority}</span>
                      )}
                    </td>
                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {t.created_at ? new Date(t.created_at).toLocaleString() : '—'}
                    </td>
                    <td>
                      {editingId === t.id ? (
                        <div style={{ display: 'flex', gap: 4 }}>
                          <button className="btn btn-success btn-sm" onClick={() => handleUpdate(t.id)}>Save</button>
                          <button className="btn btn-ghost btn-sm" onClick={() => { setEditingId(null); setEditData({}); }}>Cancel</button>
                        </div>
                      ) : (
                        <button className="btn btn-outline btn-sm" onClick={() => { setEditingId(t.id); setEditData({ status: t.status, priority: t.priority }); }}>Edit</button>
                      )}
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
