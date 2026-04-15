import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';
import { useTheme } from '../../ThemeContext';

/* ── Constants ─────────────────────────────────────────────────────────────── */
const TYPE_STYLE = {
  standard:  { bg: 'rgba(22,163,106,0.1)', color: '#16a34a', border: 'rgba(22,163,106,0.3)', label: 'Standard' },
  normal:    { bg: 'rgba(37,99,235,0.1)', color: '#2563eb', border: 'rgba(37,99,235,0.3)', label: 'Normal' },
  urgent:    { bg: 'rgba(234,88,12,0.1)', color: '#ea580c', border: 'rgba(234,88,12,0.3)', label: 'Urgent' },
  emergency: { bg: 'rgba(220,38,38,0.1)', color: '#dc2626', border: 'rgba(220,38,38,0.3)', label: 'Emergency' },
};

const STATUS_META = {
  created:       { label: 'Created',        color: '#64748b' },
  classified:    { label: 'Classified',     color: '#2563eb' },
  invalid:       { label: 'Invalid',        color: '#dc2626' },
  auto_rejected: { label: 'Auto Rejected',  color: '#991b1b' },
  validated:     { label: 'Validated',       color: '#7c3aed' },
  approved:      { label: 'Approved',        color: '#16a34a' },
  rejected:      { label: 'Rejected',        color: '#dc2626' },
  pending_cto:   { label: 'Pending CTO',    color: '#ea580c' },
  cto_approved:  { label: 'CTO Approved',   color: '#16a34a' },
  cto_rejected:  { label: 'CTO Rejected',   color: '#dc2626' },
  implementing:  { label: 'Implementing',   color: '#d97706' },
  implemented:   { label: 'Implemented',    color: '#15803d' },
  failed:        { label: 'Failed',          color: '#dc2626' },
  rolled_back:   { label: 'Rolled Back',    color: '#9a3412' },
  closed:        { label: 'Closed',          color: '#475569' },
};

const TABS = [
  { key: '',              label: 'All' },
  { key: 'classified',    label: 'Classified' },
  { key: 'validated',     label: 'Validated' },
  { key: 'approved',      label: 'Approved' },
  { key: 'implemented',   label: 'Implemented' },
  { key: 'closed',        label: 'Closed' },
];

/* ── SLA Timer ─────────────────────────────────────────────────────────────── */
function SLATimer({ deadline, slaHours, status }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const iv = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(iv); }, []);

  const terminal = ['closed', 'cto_rejected', 'rejected', 'auto_rejected', 'failed'].includes(status);
  if (!deadline || terminal) {
    const terminalLabels = { closed: 'Closed', cto_rejected: 'Rejected', rejected: 'Rejected', auto_rejected: 'Auto Rejected', failed: 'Failed' };
    return terminal ? <span style={{ fontSize: 11, fontWeight: 600, color: status === 'closed' ? '#16a34a' : '#dc2626' }}>{terminalLabels[status] || ''}</span> : null;
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
      <div style={{ width: 80, height: 3, borderRadius: 2, background: 'var(--border)', marginTop: 3, marginLeft: 'auto' }}>
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

        <div style={{ background: isDark ? '#152238' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 8, padding: '12px 16px', marginBottom: 16 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a', marginBottom: 4 }}>{cr.title}</div>
          <div style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#64748b', lineHeight: 1.5, marginBottom: 8 }}>{cr.justification || cr.description}</div>
          {cr.category && <div style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>Category: {cr.category}{cr.subcategory ? ` > ${cr.subcategory}` : ''}</div>}
          {cr.zone && <div style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>Zone: {cr.zone} | {cr.location}</div>}
        </div>

        {cr.approved_by_name && (
          <div style={{ background: 'rgba(22,163,106,0.08)', border: '1px solid rgba(22,163,106,0.25)', borderRadius: 8, padding: '10px 14px', marginBottom: 12, fontSize: 12 }}>
            <div style={{ fontWeight: 700, color: '#16a34a', fontSize: 11, marginBottom: 2 }}>MANAGER APPROVED</div>
            <div style={{ color: isDark ? '#e2e8f0' : '#0f172a' }}>By: {cr.approved_by_name} {cr.approved_at ? `on ${new Date(cr.approved_at).toLocaleString()}` : ''}</div>
            {cr.approval_remark && <div style={{ color: isDark ? '#94a3b8' : '#475569', marginTop: 4 }}>{cr.approval_remark}</div>}
          </div>
        )}

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

        {cr.pdf_filename && (
          <div style={{ marginBottom: 14 }}>
            <button onClick={async () => {
              try {
                const token = sessionStorage.getItem('token');
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

/* ── Remarks Modal ────────────────────────────────────────────────────────── */
function RemarksModal({ cr, onClose, isDark }) {
  if (!cr) return null;
  const sections = [
    { key: 'justification', fallback: 'description', label: 'Agent Justification', name: cr.raised_by_name || 'Agent', color: '#00338D' },
    { key: 'validation_remark', label: 'Validation', name: cr.validated_by_name || 'Manager', color: '#7c3aed' },
    { key: 'approval_remark', label: 'Manager Approval', name: cr.approved_by_name || 'Manager', color: '#16a34a', at: cr.approved_at },
    { key: 'manager_proposed_changes', label: 'Manager Proposed Modifications', color: '#7c3aed' },
    { key: 'cto_remark', label: 'CTO Decision', name: cr.cto_approved_by_name || 'CTO', color: '#ea580c', at: cr.cto_approved_at },
    { key: 'implementation_notes', label: 'Implementation', name: cr.implemented_by_name || 'Agent', color: '#00338D' },
    { key: 'closure_notes', label: 'Closure Notes', color: '#475569' },
  ];

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000, padding: 16 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{ width: '100%', maxWidth: 500, background: isDark ? '#1e293b' : '#fff', borderRadius: 14, boxShadow: '0 24px 48px rgba(0,0,0,0.3)', padding: 24, maxHeight: '80vh', overflowY: 'auto' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <div>
            <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a' }}>Remarks and Notes</h3>
            <span style={{ fontSize: 12, fontFamily: "'IBM Plex Mono',monospace", color: isDark ? '#4da3ff' : '#00338D', fontWeight: 700 }}>{cr.cr_number}</span>
          </div>
          <button onClick={onClose} style={{ border: 'none', background: isDark ? '#334155' : '#f1f5f9', borderRadius: 6, width: 28, height: 28, cursor: 'pointer', fontSize: 14, color: '#94a3b8' }}>X</button>
        </div>
        {sections.map(sec => {
          const text = cr[sec.key] || (sec.fallback ? cr[sec.fallback] : '');
          if (!text) return null;
          return (
            <div key={sec.key} style={{ marginBottom: 14 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                <div style={{ width: 4, height: 4, borderRadius: '50%', background: sec.color }} />
                <span style={{ fontSize: 11, fontWeight: 700, color: sec.color, textTransform: 'uppercase' }}>
                  {sec.label}{sec.name ? ` — ${sec.name}` : ''}
                </span>
                {sec.at && <span style={{ fontSize: 10, color: '#94a3b8' }}>{new Date(sec.at).toLocaleString()}</span>}
              </div>
              <div style={{ background: isDark ? '#0f172a' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                {text}
              </div>
            </div>
          );
        })}
        <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 16 }}>
          <button onClick={onClose} style={{ padding: '8px 20px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer', background: 'var(--primary, #00338D)', color: '#fff', border: 'none' }}>Close</button>
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

  // Filter by status tab
  const filtered = filter ? activeCRs.filter(c => {
    if (filter === 'approved') return ['approved', 'pending_cto', 'cto_approved'].includes(c.status);
    if (filter === 'implemented') return ['implementing', 'implemented'].includes(c.status);
    return c.status === filter;
  }) : activeCRs;

  // Count for each tab
  const tabCounts = {};
  TABS.forEach(t => {
    if (!t.key) { tabCounts[''] = activeCRs.length; return; }
    if (t.key === 'approved') {
      tabCounts[t.key] = activeCRs.filter(c => ['approved', 'pending_cto', 'cto_approved'].includes(c.status)).length;
    } else if (t.key === 'implemented') {
      tabCounts[t.key] = activeCRs.filter(c => ['implementing', 'implemented'].includes(c.status)).length;
    } else {
      tabCounts[t.key] = activeCRs.filter(c => c.status === t.key).length;
    }
  });

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
        <p>All Change Requests — full lifecycle oversight</p>
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
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
        <div style={{ width: 4, height: 16, borderRadius: 2, background: sectionColor }} />
        <span style={{ fontSize: 13, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
          {section === 'customer' ? 'Customer Complaint CRs' : 'AI-Detected Network Issue CRs'}
        </span>
        <span style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>
          {section === 'customer' ? 'Raised from customer tickets (SR)' : 'Raised from AI-detected network issues'}
        </span>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 16, marginTop: 12, flexWrap: 'wrap' }}>
        {TABS.map(t => (
          <button key={t.key} onClick={() => setFilter(t.key)} style={{
            padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
            background: filter === t.key ? sectionColor : (isDark ? '#1e293b' : '#f8fafc'),
            color: filter === t.key ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
            border: `1px solid ${filter === t.key ? sectionColor : (isDark ? '#334155' : '#e2e8f0')}`,
          }}>
            {t.label}{tabCounts[t.key] > 0 ? ` (${tabCounts[t.key]})` : ''}
          </button>
        ))}
      </div>

      {/* CR List */}
      {loading ? (
        <div style={{ textAlign: 'center', padding: 40, color: '#94a3b8' }}>Loading...</div>
      ) : filtered.length === 0 ? (
        <div style={{ textAlign: 'center', padding: 40, color: '#94a3b8' }}>
          No {section === 'customer' ? 'customer complaint' : 'AI ticket'} change requests{filter ? ` with status "${filter}"` : ''} found.
        </div>
      ) : (
        filtered.map(cr => {
          const terminal = ['closed', 'cto_rejected', 'rejected', 'auto_rejected', 'failed'].includes(cr.status);
          const ts = terminal ? { bg: 'rgba(148,163,184,0.1)', color: '#94a3b8', border: 'rgba(148,163,184,0.3)', label: cr.change_type ? cr.change_type.charAt(0).toUpperCase() + cr.change_type.slice(1) : 'Standard' } : (TYPE_STYLE[cr.change_type] || TYPE_STYLE.standard);
          const meta = STATUS_META[cr.status] || { label: cr.status, color: '#64748b' };
          const isPending = cr.status === 'pending_cto';

          return (
            <div key={cr.id} style={{
              background: isDark ? '#1e293b' : '#fff', borderRadius: 12,
              border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
              borderLeft: `4px solid ${meta.color}`,
              marginBottom: 12, padding: '16px 20px',
              boxShadow: isPending ? (isDark ? '0 2px 8px rgba(0,0,0,0.2)' : '0 2px 8px rgba(234,88,12,0.1)') : 'none',
            }}>
              {/* Row 1 */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                  <span style={{ fontFamily: "'IBM Plex Mono',monospace", fontSize: 14, fontWeight: 800, color: isDark ? '#4da3ff' : '#00338D' }}>{cr.cr_number}</span>
                  {cr.change_type && <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: ts.bg, color: ts.color, border: `1px solid ${ts.border}` }}>{ts.label}</span>}
                  <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: `${meta.color}15`, color: meta.color, border: `1px solid ${meta.color}30` }}>{meta.label}</span>
                  {cr.ticket_ref && <span style={{ fontSize: 10, color: isDark ? '#64748b' : '#94a3b8' }}>Ticket: {cr.ticket_ref}</span>}
                </div>
                <SLATimer deadline={cr.cr_sla_deadline} slaHours={cr.cr_sla_hours} status={cr.status} />
              </div>

              {/* Title + Category */}
              <div style={{ fontSize: 14, fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a', marginBottom: 4 }}>{cr.title}</div>
              <div style={{ fontSize: 11, color: isDark ? '#94a3b8' : '#64748b', marginBottom: 8 }}>
                {cr.category && <span>{cr.category}{cr.subcategory ? ` > ${cr.subcategory}` : ''}</span>}
                {cr.zone && <span> | Zone: {cr.zone}</span>}
                {cr.raised_by_name && <span> | Agent: <strong>{cr.raised_by_name}</strong></span>}
                {cr.assigned_manager_name && <span> | Manager: <strong>{cr.assigned_manager_name}</strong></span>}
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
                <div style={{ display: 'flex', gap: 8 }}>
                  {(cr.description || cr.justification || cr.approval_remark || cr.cto_remark || cr.implementation_notes || cr.closure_notes || cr.manager_proposed_changes) && (
                    <button onClick={() => setRemarksCR(cr)} style={{
                      background: 'none', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 6,
                      padding: '4px 12px', fontSize: 11, fontWeight: 600, cursor: 'pointer',
                      color: isDark ? '#94a3b8' : '#64748b',
                    }}>
                      View Remarks
                    </button>
                  )}
                  {cr.pdf_filename && (
                    <button onClick={async () => {
                      try {
                        const token = sessionStorage.getItem('token');
                        const resp = await fetch(`/api/cr/${cr.id}/pdf`, { headers: { 'Authorization': `Bearer ${token}` } });
                        if (!resp.ok) throw new Error('Failed');
                        const blob = await resp.blob();
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a'); a.href = url; a.download = cr.pdf_filename; a.click();
                        URL.revokeObjectURL(url);
                      } catch (_) { alert('Download failed'); }
                    }} style={{ fontSize: 11, color: isDark ? '#4da3ff' : '#00338D', fontWeight: 600, background: 'none', border: `1px solid ${isDark ? '#334155' : '#bfdbfe'}`, borderRadius: 6, padding: '4px 12px', cursor: 'pointer' }}>
                      Download PDF
                    </button>
                  )}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: 10, color: '#94a3b8' }}>{cr.created_at ? new Date(cr.created_at).toLocaleString() : ''}</span>
                  {isPending && (
                    <button onClick={() => setReviewCR(cr)} style={{
                      padding: '8px 20px', borderRadius: 7, fontSize: 13, fontWeight: 700, cursor: 'pointer',
                      background: 'linear-gradient(135deg, #00338D, #0047c9)', color: '#fff', border: 'none',
                    }}>
                      Review and Decide
                    </button>
                  )}
                </div>
              </div>
            </div>
          );
        })
      )}

      {/* Modals */}
      {reviewCR && <ReviewModal cr={reviewCR} onClose={() => setReviewCR(null)} onDone={fetchCRs} isDark={isDark} />}
      {remarksCR && <RemarksModal cr={remarksCR} onClose={() => setRemarksCR(null)} isDark={isDark} />}
    </div>
  );
}
