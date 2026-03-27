import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';
import { useTheme } from '../../ThemeContext';

/* ── Constants ─────────────────────────────────────────────────────────────── */
const TYPE_STYLE = {
  urgent:    { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa', label: 'Urgent' },
  emergency: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca', label: 'Emergency' },
};

const STATUS_META = {
  pending_cto:   { label: 'Pending Review',  color: '#ea580c' },
  cto_approved:  { label: 'Approved',        color: '#16a34a' },
  cto_rejected:  { label: 'Rejected',        color: '#dc2626' },
  implementing:  { label: 'Implementing',    color: '#d97706' },
  implemented:   { label: 'Implemented',     color: '#15803d' },
  closed:        { label: 'Closed',          color: '#475569' },
};

const TABS = [
  { key: '', label: 'All' },
  { key: 'pending_cto', label: 'Pending Approval' },
  { key: 'cto_approved', label: 'Approved' },
  { key: 'cto_rejected', label: 'Rejected' },
];

/* ── SLA Timer ─────────────────────────────────────────────────────────────── */
function SLATimer({ deadline, slaHours, status }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const iv = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(iv); }, []);

  if (!deadline || ['closed', 'cto_rejected'].includes(status)) {
    const terminalLabels = { closed: 'Closed', cto_rejected: 'Rejected' };
    return <span style={{ fontSize: 11, fontWeight: 600, color: status === 'closed' ? '#16a34a' : '#dc2626' }}>{terminalLabels[status] || ''}</span>;
  }
  const dl = new Date(deadline).getTime();
  const rem = Math.max(0, dl - now);
  const h = Math.floor(rem / 3600000);
  const m = Math.floor((rem % 3600000) / 60000);
  const s = Math.floor((rem % 60000) / 1000);
  const breached = rem === 0;
  const totalMs = (slaHours || 8) * 3600000;
  const elapsed = totalMs - rem;
  const pct = Math.min(100, Math.max(0, (elapsed / totalMs) * 100));
  const color = breached ? '#dc2626' : pct > 87.5 ? '#dc2626' : pct > 75 ? '#f97316' : pct > 50 ? '#f59e0b' : '#16a34a';

  return (
    <div style={{ textAlign: 'right', minWidth: 90 }}>
      <div style={{ fontSize: 15, fontWeight: 800, fontFamily: "'IBM Plex Mono',monospace", color }}>
        {breached ? 'SLA Breached' : `+${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`}
      </div>
      <div style={{ width: 80, height: 3, borderRadius: 2, background: '#e2e8f0', marginTop: 3, marginLeft: 'auto' }}>
        <div style={{ width: `${Math.min(pct, 100)}%`, height: '100%', borderRadius: 2, background: color, transition: 'width 1s' }} />
      </div>
      {slaHours && <div style={{ fontSize: 8, color: '#94a3b8', marginTop: 2 }}>{slaHours}h SLA</div>}
    </div>
  );
}

/* ── Review Modal ──────────────────────────────────────────────────────────── */
function ReviewModal({ cr, onClose, onDone, isDark }) {
  const [decision, setDecision] = useState('');
  const [remark, setRemark] = useState('');
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');
  if (!cr) return null;

  const submit = async () => {
    if (!decision) { setMsg('Please select Approve or Reject.'); return; }
    setLoading(true); setMsg('');
    try {
      await apiPut(`/api/cr/${cr.id}/cto-approve`, { decision, remark: remark.trim() });
      onDone(); onClose();
    } catch (err) { setMsg(err?.message || 'Action failed.'); }
    setLoading(false);
  };

  const ts = TYPE_STYLE[cr.change_type] || TYPE_STYLE.urgent;

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000, padding: 16 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{ width: '100%', maxWidth: 580, background: isDark ? '#1e293b' : '#fff', borderRadius: 14, boxShadow: '0 24px 48px rgba(0,0,0,0.3)', padding: 28, maxHeight: '90vh', overflowY: 'auto' }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
          <div>
            <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a' }}>CTO Review</h3>
            <div style={{ display: 'flex', gap: 8, marginTop: 4, alignItems: 'center' }}>
              <span style={{ fontSize: 13, fontFamily: 'monospace', fontWeight: 800, color: isDark ? '#4da3ff' : '#00338D' }}>{cr.cr_number}</span>
              <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: ts.bg, color: ts.color, border: `1px solid ${ts.border}` }}>{ts.label}</span>
            </div>
          </div>
          <button onClick={onClose} style={{ border: 'none', background: isDark ? '#334155' : '#f1f5f9', borderRadius: 6, width: 28, height: 28, cursor: 'pointer', fontSize: 14, color: '#94a3b8' }}>X</button>
        </div>

        {/* CR Details */}
        <div style={{ background: isDark ? '#152238' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 8, padding: '12px 16px', marginBottom: 16 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a', marginBottom: 4 }}>{cr.title}</div>
          <div style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#64748b', lineHeight: 1.5, marginBottom: 8 }}>{cr.justification || cr.description}</div>
          {cr.category && <div style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>Category: {cr.category}{cr.subcategory ? ` > ${cr.subcategory}` : ''}</div>}
          {cr.zone && <div style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>Zone: {cr.zone} | {cr.location}</div>}
        </div>

        {/* Manager Approval Info */}
        {cr.approved_by_name && (
          <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '10px 14px', marginBottom: 12, fontSize: 12 }}>
            <div style={{ fontWeight: 700, color: '#16a34a', fontSize: 11, marginBottom: 2 }}>MANAGER APPROVED</div>
            <div style={{ color: '#0f172a' }}>By: {cr.approved_by_name} {cr.approved_at ? `on ${new Date(cr.approved_at).toLocaleString()}` : ''}</div>
            {cr.approval_remark && <div style={{ color: '#475569', marginTop: 4 }}>{cr.approval_remark}</div>}
          </div>
        )}

        {/* RF Parameters */}
        {cr.rf_params && Object.values(cr.rf_params).some(v => v.proposed != null) && (
          <div style={{ marginBottom: 14 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: isDark ? '#94a3b8' : '#64748b', textTransform: 'uppercase', marginBottom: 6 }}>RF Parameters</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 6 }}>
              {Object.entries(cr.rf_params).map(([key, val]) => (val.current != null || val.proposed != null) ? (
                <div key={key} style={{ background: isDark ? '#0f172a' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 6, padding: '5px 8px' }}>
                  <div style={{ fontSize: 8, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase' }}>{key.replace(/_/g, ' ')}</div>
                  <div style={{ fontSize: 12, color: isDark ? '#e2e8f0' : '#0f172a' }}>{val.current ?? 'N/A'} &rarr; <strong style={{ color: '#16a34a' }}>{val.proposed ?? 'N/A'}</strong></div>
                </div>
              ) : null)}
            </div>
          </div>
        )}

        {/* PDF download — authenticated */}
        {cr.pdf_filename && (
          <div style={{ marginBottom: 14 }}>
            <button onClick={async () => {
              try {
                const token = localStorage.getItem('token');
                const resp = await fetch(`/api/cr/${cr.id}/pdf`, { headers: { 'Authorization': `Bearer ${token}` } });
                if (!resp.ok) throw new Error('Download failed');
                const blob = await resp.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a'); a.href = url; a.download = cr.pdf_filename; a.click();
                URL.revokeObjectURL(url);
              } catch (e) { alert('PDF download failed: ' + e.message); }
            }} style={{
              width: '100%', padding: '10px 16px', borderRadius: 8, cursor: 'pointer', fontSize: 13, fontWeight: 600,
              background: isDark ? '#152238' : '#eff6ff', border: `1px solid ${isDark ? '#334155' : '#bfdbfe'}`,
              color: isDark ? '#4da3ff' : '#00338D', textAlign: 'left',
            }}>
              Download Report: {cr.pdf_filename}
            </button>
          </div>
        )}

        {/* Decision buttons */}
        <div style={{ display: 'flex', gap: 10, marginBottom: 14 }}>
          <button onClick={() => setDecision('approved')} style={{
            flex: 1, padding: '12px', borderRadius: 8, cursor: 'pointer', textAlign: 'center', fontSize: 14, fontWeight: 700,
            background: decision === 'approved' ? '#16a34a' : (isDark ? '#152238' : '#f8fafc'),
            color: decision === 'approved' ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
            border: `2px solid ${decision === 'approved' ? '#16a34a' : (isDark ? '#334155' : '#e2e8f0')}`,
          }}>Approve</button>
          <button onClick={() => setDecision('rejected')} style={{
            flex: 1, padding: '12px', borderRadius: 8, cursor: 'pointer', textAlign: 'center', fontSize: 14, fontWeight: 700,
            background: decision === 'rejected' ? '#dc2626' : (isDark ? '#152238' : '#f8fafc'),
            color: decision === 'rejected' ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
            border: `2px solid ${decision === 'rejected' ? '#dc2626' : (isDark ? '#334155' : '#e2e8f0')}`,
          }}>Reject</button>
        </div>

        <textarea value={remark} onChange={e => setRemark(e.target.value)} rows={3}
          placeholder={decision === 'rejected' ? 'Reason for rejection (required)...' : 'Remarks (optional)...'}
          style={{ width: '100%', borderRadius: 8, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, padding: '8px 12px', fontSize: 13, resize: 'vertical', boxSizing: 'border-box', fontFamily: 'inherit', background: isDark ? '#0f172a' : '#fff', color: isDark ? '#e2e8f0' : '#0f172a' }}
        />

        {msg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{msg}</p>}

        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
          <button onClick={onClose} disabled={loading} style={{ padding: '8px 18px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer', background: isDark ? '#152238' : '#f8fafc', color: isDark ? '#94a3b8' : '#475569', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>Cancel</button>
          <button onClick={submit} disabled={loading || !decision || (decision === 'rejected' && !remark.trim())} style={{
            padding: '8px 18px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer',
            background: decision === 'rejected' ? '#dc2626' : '#16a34a', color: '#fff', border: 'none',
            opacity: (!decision || loading) ? 0.5 : 1,
          }}>
            {loading ? 'Processing...' : 'Submit Decision'}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ── Main Component ────────────────────────────────────────────────────────── */
export default function CTOChangeWorkflow() {
  const { isDark } = useTheme();
  const [crs, setCrs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('');
  const [reviewCR, setReviewCR] = useState(null);
  const [remarksCR, setRemarksCR] = useState(null);
  const [section, setSection] = useState('customer'); // 'customer' or 'ai'

  const fetchCRs = useCallback(async () => {
    try {
      const d = await apiGet('/api/cr/cto-list');
      setCrs(d?.crs || []);
    } catch { setCrs([]); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchCRs(); }, [fetchCRs]);
  useEffect(() => { const iv = setInterval(fetchCRs, 30000); return () => clearInterval(iv); }, [fetchCRs]);

  // Split CRs into customer complaint vs AI ticket
  const customerCRs = crs.filter(c => c.ticket_id && !c.network_issue_id);
  const aiCRs = crs.filter(c => c.network_issue_id);
  const activeCRs = section === 'customer' ? customerCRs : aiCRs;

  const filtered = filter ? activeCRs.filter(c => {
    if (filter === 'pending_cto') return c.status === 'pending_cto';
    if (filter === 'cto_approved') return c.cto_status === 'cto_approved';
    if (filter === 'cto_rejected') return c.cto_status === 'cto_rejected';
    return true;
  }) : activeCRs;

  const pendingCount = activeCRs.filter(c => c.status === 'pending_cto').length;
  const sectionColor = section === 'customer' ? '#00338D' : '#7c3aed';

  return (
    <div>
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <h1>Change Workflow</h1>
          {pendingCount > 0 && (
            <span style={{ background: '#dc2626', color: '#fff', fontSize: 12, fontWeight: 700, padding: '3px 10px', borderRadius: 12 }}>
              {pendingCount} pending approval
            </span>
          )}
        </div>
        <p>Urgent and Emergency Change Requests requiring CTO approval</p>
      </div>

      {/* Section Toggle */}
      <div style={{ display: 'flex', gap: 0, marginBottom: 20, borderRadius: 10, overflow: 'hidden', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, width: 'fit-content' }}>
        <button onClick={() => { setSection('customer'); setFilter(''); }} style={{
          padding: '10px 24px', fontSize: 13, fontWeight: 700, cursor: 'pointer', border: 'none',
          background: section === 'customer' ? '#00338D' : (isDark ? '#1e293b' : '#f8fafc'),
          color: section === 'customer' ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
          Customer Complaints ({customerCRs.length})
        </button>
        <button onClick={() => { setSection('ai'); setFilter(''); }} style={{
          padding: '10px 24px', fontSize: 13, fontWeight: 700, cursor: 'pointer', border: 'none',
          borderLeft: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
          background: section === 'ai' ? '#7c3aed' : (isDark ? '#1e293b' : '#f8fafc'),
          color: section === 'ai' ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 2a4 4 0 0 1 4 4v1h2a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h2V6a4 4 0 0 1 4-4z"/><circle cx="9" cy="14" r="1"/><circle cx="15" cy="14" r="1"/></svg>
          AI Tickets ({aiCRs.length})
        </button>
      </div>

      {/* Section Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <div style={{ width: 4, height: 16, borderRadius: 2, background: sectionColor }} />
        <span style={{ fontSize: 13, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
          {section === 'customer' ? 'Customer Complaint CRs' : 'AI-Detected Network Issue CRs'}
        </span>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 16 }}>
        {TABS.map(t => (
          <button key={t.key} onClick={() => setFilter(t.key)} style={{
            padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
            background: filter === t.key ? sectionColor : (isDark ? '#1e293b' : '#f8fafc'),
            color: filter === t.key ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
            border: `1px solid ${filter === t.key ? sectionColor : (isDark ? '#334155' : '#e2e8f0')}`,
          }}>{t.label} {t.key === 'pending_cto' && pendingCount > 0 ? `(${pendingCount})` : ''}</button>
        ))}
      </div>

      {/* CR List */}
      {loading ? (
        <div style={{ textAlign: 'center', padding: 40, color: '#94a3b8' }}>Loading...</div>
      ) : filtered.length === 0 ? (
        <div style={{ textAlign: 'center', padding: 40, color: '#94a3b8' }}>
          No {section === 'customer' ? 'customer complaint' : 'AI ticket'} change requests found.
        </div>
      ) : (
        filtered.map(cr => {
          const isClosed = ['closed', 'cto_rejected'].includes(cr.status) || cr.cto_status === 'cto_rejected';
          const ts = isClosed ? { bg: '#f1f5f9', color: '#94a3b8', border: '#e2e8f0', label: cr.change_type ? cr.change_type.charAt(0).toUpperCase() + cr.change_type.slice(1) : 'Urgent' } : (TYPE_STYLE[cr.change_type] || TYPE_STYLE.urgent);
          const meta = STATUS_META[cr.status] || STATUS_META[cr.cto_status] || { label: cr.status, color: '#64748b' };
          const isPending = cr.status === 'pending_cto';

          return (
            <div key={cr.id} style={{
              background: isDark ? '#1e293b' : '#fff', borderRadius: 12,
              border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
              borderLeft: `4px solid ${ts.color}`,
              marginBottom: 12, padding: '16px 20px',
              boxShadow: isPending ? (isDark ? '0 2px 8px rgba(0,0,0,0.2)' : '0 2px 8px rgba(234,88,12,0.1)') : 'none',
            }}>
              {/* Row 1 */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                  <span style={{ fontFamily: "'IBM Plex Mono',monospace", fontSize: 14, fontWeight: 800, color: isDark ? '#4da3ff' : '#00338D' }}>{cr.cr_number}</span>
                  <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: ts.bg, color: ts.color, border: `1px solid ${ts.border}` }}>{ts.label}</span>
                  <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: `${meta.color}15`, color: meta.color, border: `1px solid ${meta.color}30` }}>{meta.label}</span>
                </div>
                <SLATimer deadline={cr.cr_sla_deadline} slaHours={cr.cr_sla_hours} status={cr.status} />
              </div>

              {/* Title + Category */}
              <div style={{ fontSize: 14, fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a', marginBottom: 4 }}>{cr.title}</div>
              <div style={{ fontSize: 11, color: isDark ? '#94a3b8' : '#64748b', marginBottom: 8 }}>
                {cr.category && <span>{cr.category}{cr.subcategory ? ` > ${cr.subcategory}` : ''}</span>}
                {cr.zone && <span> | Zone: {cr.zone}</span>}
                {cr.raised_by_name && <span> | Agent: <strong>{cr.raised_by_name}</strong></span>}
              </div>

              {/* RF Params */}
              {cr.rf_params && Object.values(cr.rf_params).some(v => v.proposed != null) && (
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                  {Object.entries(cr.rf_params).map(([key, val]) => val.proposed != null ? (
                    <span key={key} style={{ fontSize: 10, padding: '2px 6px', borderRadius: 4, background: isDark ? '#152238' : '#f0fdf4', border: `1px solid ${isDark ? '#334155' : '#bbf7d0'}`, color: isDark ? '#94a3b8' : '#475569' }}>
                      {key.replace(/_/g, ' ')}: {val.current ?? 'N/A'} &rarr; <strong style={{ color: '#16a34a' }}>{val.proposed}</strong>
                    </span>
                  ) : null)}
                </div>
              )}

              {/* Actions */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 8 }}>
                {(cr.description || cr.justification || cr.approval_remark || cr.cto_remark || cr.implementation_notes || cr.closure_notes || cr.manager_proposed_changes) && (
                  <button onClick={() => setRemarksCR(cr)} style={{
                    background: 'none', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 6,
                    padding: '4px 12px', fontSize: 11, fontWeight: 600, cursor: 'pointer',
                    color: isDark ? '#94a3b8' : '#64748b',
                  }}>
                    View Remarks
                  </button>
                )}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 4 }}>
                <span style={{ fontSize: 10, color: '#94a3b8' }}>{cr.created_at ? new Date(cr.created_at).toLocaleString() : ''}</span>
                {isPending && (
                  <button onClick={() => setReviewCR(cr)} style={{
                    padding: '8px 20px', borderRadius: 7, fontSize: 13, fontWeight: 700, cursor: 'pointer',
                    background: 'linear-gradient(135deg, #00338D, #0047c9)', color: '#fff', border: 'none',
                  }}>
                    Review and Decide
                  </button>
                )}
                {cr.pdf_filename && !isPending && (
                  <button onClick={async () => {
                    try {
                      const token = localStorage.getItem('token');
                      const resp = await fetch(`/api/cr/${cr.id}/pdf`, { headers: { 'Authorization': `Bearer ${token}` } });
                      if (!resp.ok) throw new Error('Failed');
                      const blob = await resp.blob();
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement('a'); a.href = url; a.download = cr.pdf_filename; a.click();
                      URL.revokeObjectURL(url);
                    } catch (_) { alert('Download failed'); }
                  }} style={{ fontSize: 12, color: isDark ? '#4da3ff' : '#00338D', fontWeight: 600, background: 'none', border: `1px solid ${isDark ? '#334155' : '#bfdbfe'}`, borderRadius: 6, padding: '6px 12px', cursor: 'pointer' }}>
                    Download Report
                  </button>
                )}
              </div>
            </div>
          );
        })
      )}

      {/* Review Modal */}
      {reviewCR && <ReviewModal cr={reviewCR} onClose={() => setReviewCR(null)} onDone={fetchCRs} isDark={isDark} />}

      {/* Remarks Modal */}
      {remarksCR && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000, padding: 16 }} onClick={() => setRemarksCR(null)}>
          <div onClick={e => e.stopPropagation()} style={{ width: '100%', maxWidth: 500, background: isDark ? '#1e293b' : '#fff', borderRadius: 14, boxShadow: '0 24px 48px rgba(0,0,0,0.3)', padding: 24, maxHeight: '80vh', overflowY: 'auto' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <div>
                <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a' }}>Remarks and Notes</h3>
                <span style={{ fontSize: 12, fontFamily: "'IBM Plex Mono',monospace", color: isDark ? '#4da3ff' : '#00338D', fontWeight: 700 }}>{remarksCR.cr_number}</span>
              </div>
              <button onClick={() => setRemarksCR(null)} style={{ border: 'none', background: isDark ? '#334155' : '#f1f5f9', borderRadius: 6, width: 28, height: 28, cursor: 'pointer', fontSize: 14, color: '#94a3b8' }}>X</button>
            </div>

            {(remarksCR.justification || remarksCR.description) && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#00338D' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#00338D', textTransform: 'uppercase' }}>Agent Justification — {remarksCR.raised_by_name || 'Agent'}</span>
                </div>
                <div style={{ background: isDark ? '#0f172a' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                  {remarksCR.justification || remarksCR.description}
                </div>
              </div>
            )}

            {remarksCR.approval_remark && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#16a34a' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#16a34a', textTransform: 'uppercase' }}>Manager Approval — {remarksCR.approved_by_name || 'Manager'}</span>
                  {remarksCR.approved_at && <span style={{ fontSize: 10, color: '#94a3b8' }}>{new Date(remarksCR.approved_at).toLocaleString()}</span>}
                </div>
                <div style={{ background: isDark ? '#0f172a' : '#f0fdf4', border: `1px solid ${isDark ? '#334155' : '#bbf7d0'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                  {remarksCR.approval_remark}
                </div>
              </div>
            )}

            {remarksCR.manager_proposed_changes && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#7c3aed' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#7c3aed', textTransform: 'uppercase' }}>Manager Proposed Modifications</span>
                </div>
                <div style={{ background: isDark ? '#0f172a' : '#f5f3ff', border: `1px solid ${isDark ? '#334155' : '#c4b5fd'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                  {remarksCR.manager_proposed_changes}
                </div>
              </div>
            )}

            {remarksCR.cto_remark && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#ea580c' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#ea580c', textTransform: 'uppercase' }}>CTO Decision — {remarksCR.cto_approved_by_name || 'CTO'}</span>
                  {remarksCR.cto_approved_at && <span style={{ fontSize: 10, color: '#94a3b8' }}>{new Date(remarksCR.cto_approved_at).toLocaleString()}</span>}
                </div>
                <div style={{ background: isDark ? '#0f172a' : '#fff7ed', border: `1px solid ${isDark ? '#334155' : '#fed7aa'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                  {remarksCR.cto_remark}
                </div>
              </div>
            )}

            {remarksCR.implementation_notes && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#00338D' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#00338D', textTransform: 'uppercase' }}>Implementation — {remarksCR.implemented_by_name || 'Agent'}</span>
                </div>
                <div style={{ background: isDark ? '#0f172a' : '#eff6ff', border: `1px solid ${isDark ? '#334155' : '#bfdbfe'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                  {remarksCR.implementation_notes}
                </div>
              </div>
            )}

            {remarksCR.closure_notes && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#475569' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#475569', textTransform: 'uppercase' }}>Closure Notes</span>
                </div>
                <div style={{ background: isDark ? '#0f172a' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                  {remarksCR.closure_notes}
                </div>
              </div>
            )}

            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 16 }}>
              <button onClick={() => setRemarksCR(null)} style={{ padding: '8px 20px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer', background: '#00338D', color: '#fff', border: 'none' }}>Close</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
