import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';

/* ── Customer tier config ───────────────────────────────────────────────────── */
const TIER_CFG = {
  platinum: { bg: '#f5f3ff', color: '#6d28d9', border: '#c4b5fd', label: 'Platinum' },
  gold:     { bg: '#fffbeb', color: '#b45309', border: '#fde68a', label: 'Gold'     },
  silver:   { bg: '#f1f5f9', color: '#475569', border: '#cbd5e1', label: 'Silver'   },
  bronze:   { bg: '#fff7ed', color: '#c2410c', border: '#fdba74', label: 'Bronze'   },
};

/* ── Style helpers ──────────────────────────────────────────────────────────── */
const STATUS_STYLE = {
  pending:           { bg: '#fffbeb', color: '#92400e', border: '#fde68a', label: 'Pending' },
  active:            { bg: '#eff6ff', color: '#1d4ed8', border: '#bfdbfe', label: 'Active' },
  in_progress:       { bg: '#eff6ff', color: '#1d4ed8', border: '#bfdbfe', label: 'In Progress' },
  escalated:         { bg: '#fef2f2', color: '#991b1b', border: '#fecaca', label: 'Escalated' },
  manager_escalated: { bg: '#fdf4ff', color: '#6d28d9', border: '#e9d5ff', label: 'Needs Review' },
  resolved:          { bg: '#f0fdf4', color: '#166534', border: '#bbf7d0', label: 'Resolved' },
};

const PRIORITY_STYLE = {
  critical: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca' },
  high:     { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa' },
  medium:   { bg: '#fffbeb', color: '#d97706', border: '#fde68a' },
  low:      { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0' },
};

function StatusBadge({ status }) {
  const s = STATUS_STYLE[status] || { bg: '#f8fafc', color: '#475569', border: '#e2e8f0', label: status };
  return (
    <span style={{
      display: 'inline-block', fontSize: 11, fontWeight: 700, padding: '3px 8px',
      borderRadius: 6, background: s.bg, color: s.color, border: `1px solid ${s.border}`,
      textTransform: 'capitalize', whiteSpace: 'nowrap',
    }}>
      {s.label}
    </span>
  );
}

function PriorityBadge({ priority }) {
  const p = PRIORITY_STYLE[priority] || PRIORITY_STYLE.medium;
  return (
    <span style={{
      display: 'inline-block', fontSize: 11, fontWeight: 700, padding: '3px 8px',
      borderRadius: 6, background: p.bg, color: p.color, border: `1px solid ${p.border}`,
      textTransform: 'capitalize', whiteSpace: 'nowrap',
    }}>
      {priority || 'medium'}
    </span>
  );
}

/* ── Review Dialog ──────────────────────────────────────────────────────────── */
function ReviewDialog({ dialog, note, setNote, msg, loading, onClose, onSubmit }) {
  if (!dialog.open) return null;
  const isApprove = dialog.decision === 'approved';

  return (
    <div
      style={{
        position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.5)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        zIndex: 50, padding: 16,
      }}
      onClick={onClose}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          width: '100%', maxWidth: 520, background: '#fff', borderRadius: 12,
          boxShadow: '0 20px 40px rgba(15,23,42,0.2)', border: '1px solid #e2e8f0', padding: 24,
        }}
      >
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
          <span style={{
            width: 32, height: 32, borderRadius: 8, display: 'flex', alignItems: 'center',
            justifyContent: 'center', fontSize: 16,
            background: isApprove ? '#ecfdf5' : '#fef2f2',
          }}>
            {isApprove ? '✓' : '✕'}
          </span>
          <div>
            <h3 style={{ margin: 0, fontSize: 15, color: '#0f172a' }}>
              {isApprove ? 'Approve Escalation' : 'Reject Escalation'}
            </h3>
            {dialog.ticketRef && (
              <span style={{ fontSize: 11, color: '#64748b', fontFamily: 'monospace' }}>
                {dialog.ticketRef}
              </span>
            )}
          </div>
        </div>

        <p style={{ fontSize: 13, color: '#475569', margin: '0 0 14px', lineHeight: 1.5 }}>
          {isApprove
            ? 'You will take ownership of this ticket and implement the parameter change.'
            : 'The ticket will be returned to the escalating expert with your rejection note.'}
        </p>

        {/* Escalation details if available */}
        {dialog.escalationNote && (
          <div style={{
            background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8,
            padding: '10px 14px', marginBottom: 14, fontSize: 12, color: '#0f172a', lineHeight: 1.5,
          }}>
            <strong style={{ color: '#64748b', display: 'block', marginBottom: 4 }}>Expert's proposed change:</strong>
            {dialog.escalationNote}
          </div>
        )}

        <textarea
          value={note}
          onChange={e => setNote(e.target.value)}
          placeholder={isApprove ? 'Optional note for the expert...' : 'Reason for rejection (recommended)...'}
          rows={3}
          style={{
            width: '100%', borderRadius: 8, border: '1px solid #e2e8f0',
            padding: '10px 12px', fontSize: 13, color: '#0f172a',
            resize: 'vertical', outline: 'none', boxSizing: 'border-box',
          }}
        />

        {msg && (
          <p style={{ margin: '8px 0 0', fontSize: 12, color: '#dc2626' }}>{msg}</p>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 16 }}>
          <button className="btn btn-ghost btn-sm" onClick={onClose} disabled={loading}>
            Cancel
          </button>
          <button
            className={`btn btn-sm ${isApprove ? 'btn-success' : 'btn-outline'}`}
            style={!isApprove ? { borderColor: '#fca5a5', color: '#dc2626' } : {}}
            onClick={onSubmit}
            disabled={loading}
          >
            {loading ? 'Processing...' : (isApprove ? 'Approve' : 'Reject')}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ── Main Component ─────────────────────────────────────────────────────────── */
export default function ActiveTickets() {
  const [tickets, setTickets]               = useState([]);
  const [pendingChanges, setPendingChanges] = useState([]);
  const [loading, setLoading]               = useState(true);
  const [changesLoading, setChangesLoading] = useState(true);
  const [statusFilter, setStatusFilter]     = useState('');
  const [priorityFilter, setPriorityFilter] = useState('');
  const [search, setSearch]                 = useState('');
  const [editingId, setEditingId]           = useState(null);
  const [editData, setEditData]             = useState({});
  const [editNotes, setEditNotes]           = useState('');

  /* Review dialog state */
  const [reviewDialog, setReviewDialog] = useState({
    open: false, ticketId: null, decision: null, ticketRef: '', escalationNote: '',
  });
  const [reviewNote, setReviewNote]     = useState('');
  const [reviewLoading, setReviewLoading] = useState(false);
  const [reviewMsg, setReviewMsg]       = useState('');

  /* ── Data loading ─────────────────────────────────────────────────────────── */
  const loadChanges = useCallback(async () => {
    setChangesLoading(true);
    try {
      const d = await apiGet('/api/manager/parameter-changes?status=pending');
      setPendingChanges(d?.changes || []);
    } catch { setPendingChanges([]); }
    setChangesLoading(false);
  }, []);

  const loadTickets = useCallback(() => {
    const params = new URLSearchParams();
    if (statusFilter)  params.append('status',   statusFilter);
    if (priorityFilter) params.append('priority', priorityFilter);
    if (search)        params.append('search',   search);
    setLoading(true);
    apiGet(`/api/manager/tickets?${params.toString()}`)
      .then(d => { setTickets(d?.tickets || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [statusFilter, priorityFilter, search]);

  useEffect(() => {
    loadTickets();
    loadChanges();
  }, [statusFilter, priorityFilter]); // eslint-disable-line

  /* ── Ticket inline edit ───────────────────────────────────────────────────── */
  const startEdit = (t) => {
    setEditingId(t.id);
    setEditData({ status: t.status, priority: t.priority });
    setEditNotes(t.resolution_notes || '');
  };

  const handleUpdate = async (id) => {
    try {
      await apiPut(`/api/manager/tickets/${id}`, { ...editData, resolution_notes: editNotes });
      setEditingId(null);
      setEditData({});
      setEditNotes('');
      loadTickets();
    } catch (err) {
      alert(err.message || 'Update failed');
    }
  };

  /* ── Escalation review ────────────────────────────────────────────────────── */
  const openReview = (ticketId, decision, ticketRef = '', escalationNote = '', changeId = null) => {
    setReviewDialog({ open: true, ticketId, decision, ticketRef, escalationNote, changeId });
    setReviewNote('');
    setReviewMsg('');
  };

  const closeReview = () => {
    setReviewDialog({ open: false, ticketId: null, decision: null, ticketRef: '', escalationNote: '', changeId: null });
    setReviewNote('');
    setReviewMsg('');
  };

  const submitReview = async () => {
    if (!reviewDialog.decision) return;
    setReviewLoading(true);
    setReviewMsg('');
    try {
      if (reviewDialog.changeId) {
        // Network issue parameter change — use parameter-changes review endpoint
        await apiPut(`/api/manager/parameter-changes/${reviewDialog.changeId}/review`, {
          decision: reviewDialog.decision === 'approved' ? 'approved' : 'disapproved',
          note: reviewNote.trim(),
        });
      } else if (reviewDialog.ticketId) {
        // Regular ticket escalation review
        await apiPut(`/api/manager/tickets/${reviewDialog.ticketId}/escalation-review`, {
          decision: reviewDialog.decision,
          note: reviewNote.trim(),
        });
      }
      closeReview();
      loadChanges();
      loadTickets();
    } catch (err) {
      setReviewMsg(err?.message || 'Review failed. Please try again.');
    }
    setReviewLoading(false);
  };

  /* ── Render ───────────────────────────────────────────────────────────────── */
  return (
    <div>
      <div className="page-header">
        <h1>Active Tickets</h1>
        <p>Review escalation requests and manage assigned tickets</p>
      </div>

      <ReviewDialog
        dialog={reviewDialog}
        note={reviewNote}
        setNote={setReviewNote}
        msg={reviewMsg}
        loading={reviewLoading}
        onClose={closeReview}
        onSubmit={submitReview}
      />

      {/* ── Section 1: Pending Escalation Requests ─────────────────────────── */}
      <div className="table-card" style={{ marginBottom: 20 }}>
        <div className="table-header">
          <h3 style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            Pending Escalation Requests
            {pendingChanges.length > 0 && (
              <span style={{
                background: '#dc2626', color: '#fff', fontSize: 11, fontWeight: 700,
                padding: '2px 9px', borderRadius: 10,
              }}>
                {pendingChanges.length}
              </span>
            )}
          </h3>
        </div>

        {changesLoading ? (
          <div className="page-loader" style={{ minHeight: 80 }}><div className="spinner" /></div>
        ) : pendingChanges.length === 0 ? (
          <div className="empty-state" style={{ padding: '24px 0' }}>
            <p>No pending escalations — you're all caught up.</p>
          </div>
        ) : (
          <div>
            {pendingChanges.map((c, idx) => {
              const t = c.ticket || {};
              const priority = t.priority || 'medium';
              const p = PRIORITY_STYLE[priority] || PRIORITY_STYLE.medium;
              return (
                <div
                  key={c.id}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr auto',
                    gap: 20,
                    padding: '18px 20px',
                    borderBottom: idx < pendingChanges.length - 1 ? '1px solid #f1f5f9' : 'none',
                    alignItems: 'start',
                  }}
                >
                  {/* ── Left: ticket & escalation details ── */}
                  <div>
                    {/* Top row: ref + priority + domain */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 6 }}>
                      <span style={{
                        fontFamily: "'JetBrains Mono', monospace", fontSize: 12,
                        fontWeight: 700, color: '#00338D',
                      }}>
                        {t.reference_number || `#${c.ticket_id}`}
                      </span>
                      <span style={{
                        fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 4,
                        background: p.bg, color: p.color, border: `1px solid ${p.border}`,
                        textTransform: 'uppercase',
                      }}>
                        {priority}
                      </span>
                      {t.domain && (
                        <span style={{
                          fontSize: 10, padding: '2px 6px', borderRadius: 4,
                          background: '#eff6ff', color: '#1d4ed8', border: '1px solid #bfdbfe',
                        }}>
                          {t.domain}
                        </span>
                      )}
                      <StatusBadge status={t.status || 'manager_escalated'} />
                    </div>

                    {/* Customer + category */}
                    <div style={{ fontSize: 13, fontWeight: 600, color: '#0f172a', marginBottom: 2 }}>
                      {t.user_name || 'Customer'}
                    </div>
                    <div style={{ fontSize: 12, color: '#64748b', marginBottom: 8 }}>
                      {t.category || 'General'}{t.subcategory ? ` / ${t.subcategory}` : ''}
                    </div>

                    {/* Escalated by + when */}
                    <div style={{ fontSize: 12, color: '#64748b', marginBottom: 10 }}>
                      Escalated by{' '}
                      <strong style={{ color: '#0f172a' }}>
                        {t.escalated_by_name || c.agent_name || 'Expert'}
                      </strong>
                      {t.escalated_at && (
                        <> &middot; {new Date(t.escalated_at).toLocaleString()}</>
                      )}
                    </div>

                    {/* Proposed change box */}
                    <div style={{
                      background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8,
                      padding: '10px 14px', fontSize: 12, color: '#0f172a', lineHeight: 1.6,
                    }}>
                      <span style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                        Proposed change
                      </span>
                      <p style={{ margin: '4px 0 0' }}>{c.proposed_change}</p>
                    </div>
                  </div>

                  {/* ── Right: action buttons ── */}
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minWidth: 100, paddingTop: 4 }}>
                    <button
                      className="btn btn-success btn-sm"
                      style={{ width: '100%' }}
                      onClick={() => openReview(c.ticket_id || c.network_issue_id, 'approved', t.reference_number, c.proposed_change, c.network_issue_id ? c.id : null)}
                    >
                      Approve
                    </button>
                    <button
                      className="btn btn-outline btn-sm"
                      style={{ width: '100%', borderColor: '#fca5a5', color: '#dc2626' }}
                      onClick={() => openReview(c.ticket_id || c.network_issue_id, 'rejected', t.reference_number, c.proposed_change, c.network_issue_id ? c.id : null)}
                    >
                      Reject
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ── Section 2: All Assigned Tickets ────────────────────────────────── */}
      <div className="table-card">
        <div className="table-header">
          <h3>All Assigned Tickets ({tickets.length})</h3>
          <div className="table-filters">
            <select
              className="filter-select"
              value={statusFilter}
              onChange={e => setStatusFilter(e.target.value)}
            >
              <option value="">All Status</option>
              <option value="pending">Pending</option>
              <option value="in_progress">In Progress</option>
              <option value="manager_escalated">Needs Review</option>
              <option value="escalated">Escalated</option>
              <option value="resolved">Resolved</option>
            </select>
            <select
              className="filter-select"
              value={priorityFilter}
              onChange={e => setPriorityFilter(e.target.value)}
            >
              <option value="">All Priority</option>
              <option value="critical">Critical</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
            <form onSubmit={e => { e.preventDefault(); loadTickets(); }} style={{ display: 'flex', gap: 6 }}>
              <input
                type="text"
                className="filter-input"
                placeholder="Search by name, email, ref..."
                value={search}
                onChange={e => setSearch(e.target.value)}
              />
              <button type="submit" className="btn btn-primary btn-sm">Search</button>
            </form>
          </div>
        </div>

        {loading ? (
          <div className="page-loader" style={{ minHeight: 120 }}><div className="spinner" /></div>
        ) : tickets.length === 0 ? (
          <div className="empty-state">
            <h4>No tickets found</h4>
            <p>Try adjusting your filters</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="data-table" style={{ tableLayout: 'fixed', width: '100%' }}>
              <colgroup>
                <col style={{ width: 130 }} />  {/* Ref # */}
                <col style={{ width: 160 }} />  {/* Customer */}
                <col style={{ width: 160 }} />  {/* Category */}
                <col style={{ width: 160 }} />  {/* Assigned To */}
                <col style={{ width: 180 }} />  {/* Description */}
                <col style={{ width: 110 }} />  {/* Status */}
                <col style={{ width: 90  }} />  {/* Priority */}
                <col style={{ width: 130 }} />  {/* Created */}
                <col style={{ width: 140 }} />  {/* Actions */}
              </colgroup>
              <thead>
                <tr>
                  <th style={{ whiteSpace: 'nowrap' }}>Ref #</th>
                  <th>Customer</th>
                  <th>Category</th>
                  <th>Assigned To</th>
                  <th>Description</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th style={{ whiteSpace: 'nowrap' }}>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {tickets.map(t => (
                  <tr
                    key={t.id}
                    style={t.status === 'manager_escalated' ? { background: '#fdf4ff' } : {}}
                  >
                    <td>
                      <span style={{
                        fontFamily: "'JetBrains Mono', monospace",
                        fontSize: 12, color: '#00338D', fontWeight: 600,
                      }}>
                        {t.reference_number}
                      </span>
                    </td>
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                        <span style={{ fontWeight: 500, fontSize: 13 }}>{t.user_name}</span>
                        {(() => {
                          const tc = TIER_CFG[t.user_type] || TIER_CFG.bronze;
                          return (
                            <span style={{
                              fontSize: 10, fontWeight: 700, padding: '2px 5px', borderRadius: 4,
                              background: tc.bg, color: tc.color, border: `1px solid ${tc.border}`,
                              textTransform: 'uppercase', letterSpacing: '0.04em',
                            }}>
                              {tc.label}
                            </span>
                          );
                        })()}
                      </div>
                      <div style={{ fontSize: 11, color: '#94a3b8' }}>{t.user_email}</div>
                    </td>
                    <td style={{ overflow: 'hidden' }}>
                      <div style={{
                        fontSize: 13, fontWeight: 500, color: '#0f172a',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      }}>
                        {t.category}
                      </div>
                      {t.subcategory && (
                        <div style={{
                          fontSize: 11, color: '#94a3b8',
                          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        }}>
                          {t.subcategory}
                        </div>
                      )}
                    </td>
                    <td style={{ overflow: 'hidden' }}>
                      <div style={{
                        fontSize: 13, fontWeight: 500, color: '#0f172a',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      }}>
                        {t.assignee_name || '—'}
                      </div>
                      {t.assignee_domain && (
                        <div style={{
                          fontSize: 11, color: '#94a3b8', textTransform: 'capitalize',
                          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        }}>
                          {t.assignee_domain}
                        </div>
                      )}
                    </td>
                    <td style={{ overflow: 'hidden' }}>
                      <div style={{
                        fontSize: 13, color: '#475569',
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      }}>
                        {t.description}
                      </div>
                    </td>

                    {/* Status — editable or badge */}
                    <td>
                      {editingId === t.id ? (
                        <select
                          className="filter-select"
                          value={editData.status || t.status}
                          onChange={e => setEditData(d => ({ ...d, status: e.target.value }))}
                        >
                          <option value="pending">Pending</option>
                          <option value="in_progress">In Progress</option>
                          <option value="manager_escalated">Needs Review</option>
                          <option value="escalated">Escalated</option>
                          <option value="resolved">Resolved</option>
                        </select>
                      ) : (
                        <StatusBadge status={t.status} />
                      )}
                    </td>

                    {/* Priority — editable or badge */}
                    <td>
                      {editingId === t.id ? (
                        <select
                          className="filter-select"
                          value={editData.priority || t.priority}
                          onChange={e => setEditData(d => ({ ...d, priority: e.target.value }))}
                        >
                          <option value="low">Low</option>
                          <option value="medium">Medium</option>
                          <option value="high">High</option>
                          <option value="critical">Critical</option>
                        </select>
                      ) : (
                        <PriorityBadge priority={t.priority} />
                      )}
                    </td>

                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {t.created_at ? new Date(t.created_at).toLocaleString() : '—'}
                    </td>

                    {/* Actions */}
                    <td>
                      {editingId === t.id ? (
                        <div>
                          <textarea
                            value={editNotes}
                            onChange={e => setEditNotes(e.target.value)}
                            placeholder="Resolution notes..."
                            rows={2}
                            style={{
                              width: '100%', minWidth: 160, borderRadius: 6,
                              border: '1px solid #e2e8f0', padding: '6px 8px',
                              fontSize: 12, resize: 'vertical', marginBottom: 6,
                            }}
                          />
                          <div style={{ display: 'flex', gap: 4 }}>
                            <button className="btn btn-success btn-sm" onClick={() => handleUpdate(t.id)}>Save</button>
                            <button className="btn btn-ghost btn-sm" onClick={() => { setEditingId(null); setEditData({}); setEditNotes(''); }}>Cancel</button>
                          </div>
                        </div>
                      ) : (
                        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                          {t.status === 'manager_escalated' ? (
                            <>
                              <button
                                className="btn btn-success btn-sm"
                                onClick={() => openReview(t.id, 'approved', t.reference_number, t.escalation_note)}
                              >
                                Approve
                              </button>
                              <button
                                className="btn btn-outline btn-sm"
                                style={{ borderColor: '#fca5a5', color: '#dc2626' }}
                                onClick={() => openReview(t.id, 'rejected', t.reference_number, t.escalation_note)}
                              >
                                Reject
                              </button>
                            </>
                          ) : (
                            <button
                              className="btn btn-outline btn-sm"
                              onClick={() => startEdit(t)}
                            >
                              Edit
                            </button>
                          )}
                        </div>
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
