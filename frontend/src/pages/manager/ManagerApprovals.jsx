import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';
import { useTheme } from '../../ThemeContext';

/* ── Style helpers ─────────────────────────────────────────────────────────── */
const TIER_CFG = {
  platinum: { bg: '#f5f3ff', color: '#6d28d9', border: '#c4b5fd', label: 'Platinum' },
  gold:     { bg: '#fffbeb', color: '#b45309', border: '#fde68a', label: 'Gold'     },
  silver:   { bg: '#f1f5f9', color: '#475569', border: '#cbd5e1', label: 'Silver'   },
  bronze:   { bg: '#fff7ed', color: '#c2410c', border: '#fdba74', label: 'Bronze'   },
};

const PRIORITY_STYLE = {
  critical: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca' },
  high:     { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa' },
  medium:   { bg: '#fffbeb', color: '#d97706', border: '#fde68a' },
  low:      { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0' },
};

const CHANGE_STATUS_STYLE = {
  pending:     { bg: '#fffbeb', color: '#92400e', border: '#fde68a', label: 'Pending'    },
  approved:    { bg: '#f0fdf4', color: '#166534', border: '#bbf7d0', label: 'Approved'   },
  disapproved: { bg: '#fef2f2', color: '#991b1b', border: '#fecaca', label: 'Rejected'   },
};

function PriorityBadge({ priority }) {
  const p = PRIORITY_STYLE[priority] || PRIORITY_STYLE.medium;
  return (
    <span style={{
      display: 'inline-block', fontSize: 11, fontWeight: 700, padding: '2px 7px',
      borderRadius: 5, background: p.bg, color: p.color, border: `1px solid ${p.border}`,
      textTransform: 'uppercase', whiteSpace: 'nowrap',
    }}>
      {priority || 'medium'}
    </span>
  );
}

function TierBadge({ userType }) {
  const t = TIER_CFG[userType] || TIER_CFG.bronze;
  return (
    <span style={{
      display: 'inline-block', fontSize: 10, fontWeight: 700, padding: '2px 6px',
      borderRadius: 4, background: t.bg, color: t.color, border: `1px solid ${t.border}`,
      textTransform: 'uppercase', letterSpacing: '0.04em', whiteSpace: 'nowrap',
    }}>
      {t.label}
    </span>
  );
}

function StatusBadge({ status }) {
  const s = CHANGE_STATUS_STYLE[status] || CHANGE_STATUS_STYLE.pending;
  return (
    <span style={{
      display: 'inline-block', fontSize: 11, fontWeight: 700, padding: '2px 8px',
      borderRadius: 5, background: s.bg, color: s.color, border: `1px solid ${s.border}`,
      whiteSpace: 'nowrap',
    }}>
      {s.label}
    </span>
  );
}

/* ── Review Dialog ─────────────────────────────────────────────────────────── */
function ReviewDialog({ dialog, note, setNote, msg, loading, onClose, onSubmit, isDark }) {
  if (!dialog.open) return null;
  const isApprove = dialog.decision === 'approved';

  return (
    <div
      style={{
        position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        zIndex: 1000, padding: 16,
      }}
      onClick={onClose}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          width: '100%', maxWidth: 520, background: isDark ? '#1e293b' : '#fff', borderRadius: 14,
          boxShadow: isDark ? '0 24px 48px rgba(0,0,0,0.4)' : '0 24px 48px rgba(15,23,42,0.2)', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, padding: 28,
        }}
      >
        {/* Icon + title */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 6 }}>
          <div style={{
            width: 40, height: 40, borderRadius: 10, display: 'flex',
            alignItems: 'center', justifyContent: 'center', fontSize: 18,
            background: isApprove ? '#ecfdf5' : '#fef2f2',
            color: isApprove ? '#16a34a' : '#dc2626',
            flexShrink: 0,
          }}>
            {isApprove ? '✓' : '✕'}
          </div>
          <div>
            <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a' }}>
              {isApprove ? 'Approve Request' : 'Reject Request'}
            </h3>
            {dialog.changeRef && (
              <span style={{ fontSize: 11, color: isDark ? '#94a3b8' : '#64748b', fontFamily: 'monospace' }}>
                {dialog.changeRef}
              </span>
            )}
          </div>
        </div>

        <p style={{ fontSize: 13, color: isDark ? '#94a3b8' : '#64748b', margin: '0 0 16px', lineHeight: 1.55 }}>
          {isApprove
            ? 'Approving this request will notify the expert and log your decision.'
            : 'Rejecting will notify the expert with your reason.'}
        </p>

        {/* Proposed change preview */}
        {dialog.proposedChange && (
          <div style={{
            background: isDark ? '#152238' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 8,
            padding: '10px 14px', marginBottom: 16, lineHeight: 1.5,
          }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: isDark ? '#64748b' : '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>
              Proposed Change
            </div>
            <div style={{ fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a' }}>{dialog.proposedChange}</div>
          </div>
        )}

        <textarea
          value={note}
          onChange={e => setNote(e.target.value)}
          placeholder={isApprove ? 'Optional approval note for the expert...' : 'Reason for rejection (recommended)...'}
          rows={3}
          style={{
            width: '100%', borderRadius: 8, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
            padding: '10px 12px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a',
            background: isDark ? '#0f172a' : '#fff',
            resize: 'vertical', outline: 'none', boxSizing: 'border-box',
            fontFamily: 'inherit',
          }}
          autoFocus
        />

        {msg && (
          <p style={{ margin: '8px 0 0', fontSize: 12, color: '#dc2626' }}>{msg}</p>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 18 }}>
          <button
            onClick={onClose}
            disabled={loading}
            style={{
              padding: '8px 18px', borderRadius: 7, fontSize: 13, fontWeight: 600,
              background: isDark ? '#152238' : '#f8fafc', color: isDark ? '#94a3b8' : '#475569', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, cursor: 'pointer',
            }}
          >
            Cancel
          </button>
          <button
            onClick={onSubmit}
            disabled={loading}
            style={{
              padding: '8px 22px', borderRadius: 7, fontSize: 13, fontWeight: 700,
              background: isApprove ? '#16a34a' : (isDark ? '#1e293b' : '#fff'),
              color: isApprove ? '#fff' : '#dc2626',
              border: isApprove ? 'none' : '1px solid #fca5a5',
              cursor: loading ? 'not-allowed' : 'pointer',
              opacity: loading ? 0.7 : 1,
            }}
          >
            {loading ? 'Processing...' : isApprove ? 'Approve' : 'Reject'}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ── Approval Card ─────────────────────────────────────────────────────────── */
function ApprovalCard({ change, onAction, isDark }) {
  const t = change.ticket || {};
  const priority = t.priority || 'medium';
  const p = PRIORITY_STYLE[priority] || PRIORITY_STYLE.medium;

  return (
    <div style={{
      background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
      boxShadow: isDark ? '0 1px 4px rgba(0,0,0,0.2)' : '0 1px 4px rgba(15,23,42,0.06)',
      overflow: 'hidden', marginBottom: 12,
    }}>
      {/* Top accent bar based on priority */}
      <div style={{ height: 3, background: p.color }} />

      <div style={{ padding: '16px 20px' }}>
        {/* Row 1: ticket ref + badges + time */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10, flexWrap: 'wrap', gap: 8 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <span style={{
              fontFamily: "'JetBrains Mono', monospace", fontSize: 13,
              fontWeight: 700, color: isDark ? '#4da3ff' : '#00338D',
            }}>
              {t.reference_number || `#${change.ticket_id}`}
            </span>
            <PriorityBadge priority={priority} />
            {t.domain && (
              <span style={{
                fontSize: 10, padding: '2px 7px', borderRadius: 4,
                background: '#eff6ff', color: '#1d4ed8', border: '1px solid #bfdbfe',
                fontWeight: 600,
              }}>
                {t.domain}
              </span>
            )}
            <TierBadge userType={t.user_type} />
          </div>
          <span style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8', whiteSpace: 'nowrap' }}>
            {change.created_at ? new Date(change.created_at).toLocaleString([], {
              month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
            }) : ''}
          </span>
        </div>

        {/* Row 2: customer + category */}
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, marginBottom: 4, flexWrap: 'wrap' }}>
          <span style={{ fontSize: 14, fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a' }}>{t.user_name || 'Customer'}</span>
          <span style={{ fontSize: 12, color: isDark ? '#64748b' : '#94a3b8' }}>
            {t.category || 'General'}{t.subcategory ? ` › ${t.subcategory}` : ''}
          </span>
        </div>

        {/* Row 3: submitted by */}
        <div style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#64748b', marginBottom: 12 }}>
          Submitted by{' '}
          <strong style={{ color: isDark ? '#cbd5e1' : '#374151' }}>{change.agent_name || 'Expert'}</strong>
        </div>

        {/* Proposed change box */}
        <div style={{
          background: 'linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%)',
          border: '1px solid #bae6fd', borderRadius: 8,
          padding: '12px 14px', marginBottom: 16,
        }}>
          <div style={{
            fontSize: 10, fontWeight: 800, color: '#0369a1',
            textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6,
            display: 'flex', alignItems: 'center', gap: 5,
          }}>
            <span>📋</span> Approval Request
          </div>
          <p style={{ margin: 0, fontSize: 13, color: '#0f172a', lineHeight: 1.6 }}>
            {change.proposed_change}
          </p>
        </div>

        {/* Action buttons */}
        <div style={{ display: 'flex', gap: 10 }}>
          <button
            onClick={() => onAction(change, 'approved')}
            style={{
              flex: 1, padding: '9px 0', borderRadius: 8, fontSize: 13, fontWeight: 700,
              background: 'linear-gradient(135deg, #16a34a, #15803d)',
              color: '#fff', border: 'none', cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            }}
          >
            <span>✓</span> Approve
          </button>
          <button
            onClick={() => onAction(change, 'disapproved')}
            style={{
              flex: 1, padding: '9px 0', borderRadius: 8, fontSize: 13, fontWeight: 700,
              background: isDark ? '#1e293b' : '#fff', color: '#dc2626',
              border: '1px solid #fca5a5', cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            }}
          >
            <span>✕</span> Reject
          </button>
        </div>
      </div>
    </div>
  );
}

/* ── History Row ───────────────────────────────────────────────────────────── */
function HistoryRow({ change, isDark }) {
  const t = change.ticket || {};
  return (
    <tr style={{ borderBottom: `1px solid ${isDark ? '#1e293b' : '#f1f5f9'}` }}>
      <td style={{ padding: '10px 16px' }}>
        <span style={{
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 12, color: isDark ? '#4da3ff' : '#00338D', fontWeight: 600,
        }}>
          {t.reference_number || `#${change.ticket_id}`}
        </span>
      </td>
      <td style={{ padding: '10px 16px' }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: isDark ? '#e2e8f0' : '#0f172a' }}>{t.user_name || '—'}</div>
        <TierBadge userType={t.user_type} />
      </td>
      <td style={{ padding: '10px 16px', fontSize: 12, color: isDark ? '#94a3b8' : '#475569' }}>
        <PriorityBadge priority={t.priority} />
      </td>
      <td style={{
        padding: '10px 16px', fontSize: 13, color: isDark ? '#cbd5e1' : '#374151',
        maxWidth: 240, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
      }}>
        {change.proposed_change}
      </td>
      <td style={{ padding: '10px 16px', fontSize: 12, color: isDark ? '#94a3b8' : '#475569' }}>
        {change.agent_name || '—'}
      </td>
      <td style={{ padding: '10px 16px' }}>
        <StatusBadge status={change.status} />
      </td>
      <td style={{ padding: '10px 16px', fontSize: 12, color: isDark ? '#64748b' : '#94a3b8', whiteSpace: 'nowrap' }}>
        {change.reviewed_at
          ? new Date(change.reviewed_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
          : '—'}
      </td>
      <td style={{ padding: '10px 16px', fontSize: 12, color: isDark ? '#94a3b8' : '#64748b' }}>
        {change.reviewer_name || '—'}
      </td>
      <td style={{
        padding: '10px 16px', fontSize: 12, color: isDark ? '#94a3b8' : '#475569',
        maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
      }}>
        {change.manager_note || '—'}
      </td>
    </tr>
  );
}

/* ── Main Component ─────────────────────────────────────────────────────────── */
export default function ManagerApprovals() {
  const { isDark } = useTheme();
  const [pending, setPending]   = useState([]);
  const [history, setHistory]   = useState([]);
  const [loading, setLoading]   = useState(true);
  const [tab, setTab]           = useState('pending'); // 'pending' | 'history'

  /* Review dialog */
  const [dialog, setDialog]           = useState({ open: false, change: null, decision: null });
  const [reviewNote, setReviewNote]   = useState('');
  const [reviewLoading, setReviewLoading] = useState(false);
  const [reviewMsg, setReviewMsg]     = useState('');

  /* ── Fetch data ──────────────────────────────────────────────────────────── */
  const fetchAll = useCallback(async () => {
    try {
      const [pendingRes, allRes] = await Promise.allSettled([
        apiGet('/api/manager/parameter-changes?status=pending'),
        apiGet('/api/manager/parameter-changes'),
      ]);
      if (pendingRes.status === 'fulfilled') setPending(pendingRes.value?.changes || []);
      if (allRes.status === 'fulfilled') {
        const all = allRes.value?.changes || [];
        setHistory(all.filter(c => c.status !== 'pending'));
      }
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => {
    fetchAll();
    const iv = setInterval(fetchAll, 30000);
    return () => clearInterval(iv);
  }, [fetchAll]);

  /* ── Review handlers ─────────────────────────────────────────────────────── */
  const openDialog = (change, decision) => {
    setDialog({ open: true, change, decision });
    setReviewNote('');
    setReviewMsg('');
  };

  const closeDialog = () => {
    setDialog({ open: false, change: null, decision: null });
    setReviewNote('');
    setReviewMsg('');
  };

  const submitReview = async () => {
    if (!dialog.change || !dialog.decision) return;
    setReviewLoading(true);
    setReviewMsg('');
    try {
      await apiPut(`/api/manager/parameter-changes/${dialog.change.id}/review`, {
        decision: dialog.decision,
        note: reviewNote.trim(),
      });
      closeDialog();
      fetchAll();
    } catch (err) {
      setReviewMsg(err?.message || 'Failed to process review. Please try again.');
    }
    setReviewLoading(false);
  };

  /* ── Render ──────────────────────────────────────────────────────────────── */
  const pendingCount = pending.length;

  return (
    <div>
      {/* Page header */}
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <h1>Approvals</h1>
          {pendingCount > 0 && (
            <span style={{
              background: '#dc2626', color: '#fff', fontSize: 12, fontWeight: 700,
              padding: '3px 10px', borderRadius: 12,
            }}>
              {pendingCount} pending
            </span>
          )}
        </div>
        <p>Review and action parameter change requests submitted by experts.</p>
      </div>

      {/* Review dialog */}
      <ReviewDialog
        dialog={{
          open: dialog.open,
          decision: dialog.decision,
          changeRef: dialog.change?.ticket?.reference_number,
          proposedChange: dialog.change?.proposed_change,
        }}
        note={reviewNote}
        setNote={setReviewNote}
        msg={reviewMsg}
        loading={reviewLoading}
        onClose={closeDialog}
        onSubmit={submitReview}
        isDark={isDark}
      />

      {/* Summary stat cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 14, marginBottom: 24 }}>
        {/* Pending */}
        <div style={{
          background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
          padding: '16px 20px', boxShadow: isDark ? '0 1px 3px rgba(0,0,0,0.2)' : '0 1px 3px rgba(0,0,0,0.05)',
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: isDark ? '#64748b' : '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
            Pending
          </div>
          <div style={{ fontSize: 28, fontWeight: 800, color: pendingCount > 0 ? '#dc2626' : (isDark ? '#e2e8f0' : '#0f172a') }}>
            {pendingCount}
          </div>
          <div style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#64748b', marginTop: 2 }}>Awaiting your review</div>
        </div>
        {/* Approved */}
        <div style={{
          background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
          padding: '16px 20px', boxShadow: isDark ? '0 1px 3px rgba(0,0,0,0.2)' : '0 1px 3px rgba(0,0,0,0.05)',
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: isDark ? '#64748b' : '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
            Approved
          </div>
          <div style={{ fontSize: 28, fontWeight: 800, color: '#16a34a' }}>
            {history.filter(c => c.status === 'approved').length}
          </div>
          <div style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#64748b', marginTop: 2 }}>Total approved</div>
        </div>
        {/* Rejected */}
        <div style={{
          background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
          padding: '16px 20px', boxShadow: isDark ? '0 1px 3px rgba(0,0,0,0.2)' : '0 1px 3px rgba(0,0,0,0.05)',
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: isDark ? '#64748b' : '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
            Rejected
          </div>
          <div style={{ fontSize: 28, fontWeight: 800, color: '#ea580c' }}>
            {history.filter(c => c.status === 'disapproved').length}
          </div>
          <div style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#64748b', marginTop: 2 }}>Total rejected</div>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20 }}>
        {[
          { key: 'pending', label: `Pending Approvals${pendingCount > 0 ? ` (${pendingCount})` : ''}` },
          { key: 'history', label: 'Decision History' },
        ].map(t => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            style={{
              padding: '8px 20px', borderRadius: 8, fontSize: 13, fontWeight: 600,
              border: '1px solid',
              background: tab === t.key ? (isDark ? '#4da3ff' : '#00338D') : (isDark ? '#1e293b' : '#fff'),
              color: tab === t.key ? '#fff' : (isDark ? '#94a3b8' : '#475569'),
              borderColor: tab === t.key ? (isDark ? '#4da3ff' : '#00338D') : (isDark ? '#334155' : '#e2e8f0'),
              cursor: 'pointer',
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Content */}
      {loading ? (
        <div className="page-loader" style={{ minHeight: 200 }}><div className="spinner" /></div>
      ) : tab === 'pending' ? (
        /* ── Pending tab ── */
        pendingCount === 0 ? (
          <div style={{
            background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
            padding: '60px 20px', textAlign: 'center',
          }}>
            <div style={{ fontSize: 48, marginBottom: 12 }}>✅</div>
            <h4 style={{ color: '#16a34a', margin: '0 0 6px', fontSize: 16 }}>All caught up!</h4>
            <p style={{ color: isDark ? '#94a3b8' : '#64748b', fontSize: 13, margin: 0 }}>
              There are no pending approval requests from experts.
            </p>
          </div>
        ) : (
          <div>
            {pending.map(change => (
              <ApprovalCard key={change.id} change={change} onAction={openDialog} isDark={isDark} />
            ))}
          </div>
        )
      ) : (
        /* ── History tab ── */
        history.length === 0 ? (
          <div style={{
            background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
            padding: '60px 20px', textAlign: 'center',
          }}>
            <div style={{ fontSize: 48, marginBottom: 12 }}>📋</div>
            <h4 style={{ color: isDark ? '#94a3b8' : '#64748b', margin: '0 0 6px', fontSize: 16 }}>No history yet</h4>
            <p style={{ color: isDark ? '#64748b' : '#94a3b8', fontSize: 13, margin: 0 }}>
              Reviewed approvals will appear here.
            </p>
          </div>
        ) : (
          <div style={{
            background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
            overflow: 'hidden', boxShadow: isDark ? '0 1px 3px rgba(0,0,0,0.2)' : '0 1px 3px rgba(0,0,0,0.05)',
          }}>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ background: isDark ? '#152238' : '#f8fafc' }}>
                    {['Ticket Ref', 'Customer', 'Priority', 'Proposed Change', 'Submitted By', 'Decision', 'Reviewed At', 'Reviewed By', 'Manager Note'].map(h => (
                      <th key={h} style={{
                        padding: '10px 16px', textAlign: 'left', fontSize: 11,
                        fontWeight: 700, color: isDark ? '#64748b' : '#94a3b8', textTransform: 'uppercase',
                        letterSpacing: '0.06em', borderBottom: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
                        whiteSpace: 'nowrap',
                      }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {history.map(change => (
                    <HistoryRow key={change.id} change={change} isDark={isDark} />
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )
      )}

      <div style={{ marginTop: 14, fontSize: 12, color: isDark ? '#64748b' : '#94a3b8', textAlign: 'right' }}>
        Auto-refreshes every 30s
      </div>
    </div>
  );
}
