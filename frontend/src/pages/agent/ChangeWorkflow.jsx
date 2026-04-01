import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';
import { useTheme } from '../../ThemeContext';

/* ── Constants ─────────────────────────────────────────────────────────────── */
const TYPE_STYLE = {
  standard:  { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0', label: 'Standard' },
  normal:    { bg: '#fffbeb', color: '#d97706', border: '#fde68a', label: 'Normal' },
  urgent:    { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa', label: 'Urgent' },
  emergency: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca', label: 'Emergency' },
};

const STATUS_META = {
  created:       { label: 'Created',       color: '#64748b' },
  classified:    { label: 'Classified',    color: '#7c3aed' },
  invalid:       { label: 'Invalid',       color: '#dc2626' },
  auto_rejected: { label: 'Auto-Rejected', color: '#991b1b' },
  validated:     { label: 'Validated',     color: '#0369a1' },
  approved:      { label: 'Approved',      color: '#16a34a' },
  rejected:      { label: 'Rejected',      color: '#dc2626' },
  pending_cto:   { label: 'Pending CTO',   color: '#ea580c' },
  cto_approved:  { label: 'CTO Approved',  color: '#16a34a' },
  cto_rejected:  { label: 'CTO Rejected',  color: '#dc2626' },
  implementing:  { label: 'Implementing',  color: '#d97706' },
  implemented:   { label: 'Implemented',   color: '#15803d' },
  failed:        { label: 'Failed',        color: '#dc2626' },
  rolled_back:   { label: 'Rolled Back',   color: '#92400e' },
  closed:        { label: 'Closed',        color: '#475569' },
};

// Standard/Normal: 6 steps. Urgent/Emergency: 7 steps (adds CTO Review).
const PIPELINE_STANDARD = [
  { key: 'raised',      label: 'CR Raised',   statuses: ['created', 'classified'] },
  { key: 'classified',  label: 'Classified',  statuses: ['classified'] },
  { key: 'validated',   label: 'Validated',   statuses: ['validated', 'invalid', 'auto_rejected'] },
  { key: 'approved',    label: 'Approved',    statuses: ['approved', 'rejected'] },
  { key: 'implemented', label: 'Implemented', statuses: ['implementing', 'implemented', 'failed', 'rolled_back'] },
  { key: 'closed',      label: 'Closed',      statuses: ['closed'] },
];
const PIPELINE_URGENT = [
  { key: 'raised',      label: 'CR Raised',   statuses: ['created', 'classified'] },
  { key: 'classified',  label: 'Classified',  statuses: ['classified'] },
  { key: 'validated',   label: 'Validated',   statuses: ['validated', 'invalid', 'auto_rejected'] },
  { key: 'approved',    label: 'Approved',    statuses: ['approved', 'rejected'] },
  { key: 'cto_review',  label: 'CTO Review',  statuses: ['pending_cto', 'cto_approved', 'cto_rejected'] },
  { key: 'implemented', label: 'Implemented', statuses: ['implementing', 'implemented', 'failed', 'rolled_back'] },
  { key: 'closed',      label: 'Closed',      statuses: ['closed'] },
];

const TABS = [
  { key: '',            label: 'All' },
  { key: 'classified',  label: 'Classified' },
  { key: 'validated',   label: 'Validated' },
  { key: 'approved',    label: 'Approved' },
  { key: 'implemented', label: 'Implemented' },
  { key: 'closed',      label: 'Closed' },
];

/* ── SLA Timer ─────────────────────────────────────────────────────────────── */
function SLATimer({ deadline, slaHours, status }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const iv = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(iv); }, []);

  if (!deadline || ['closed', 'rejected', 'auto_rejected', 'cto_rejected'].includes(status)) {
    const terminalLabels = { closed: 'Closed', rejected: 'Rejected', auto_rejected: 'Auto-Rejected', cto_rejected: 'CTO Rejected' };
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

/* ── Pipeline with Audit Info ──────────────────────────────────────────────── */
function PipelineVis({ cr, isDark }) {
  const isUrgent = cr.change_type === 'urgent' || cr.change_type === 'emergency';
  const steps = isUrgent ? PIPELINE_URGENT : PIPELINE_STANDARD;
  const status = cr.status;

  // Map each step to its completion info from the CR
  const stepInfo = {
    raised:      { done: true, by: cr.raised_by_name, at: cr.created_at },
    classified:  { done: !!cr.classified_at, by: cr.classified_by_name || cr.raised_by_name, at: cr.classified_at },
    validated:   { done: !!cr.validated_at, by: cr.validated_by_name, at: cr.validated_at, fail: status === 'invalid' || status === 'auto_rejected' },
    approved:    { done: !!cr.approved_at, by: cr.approved_by_name, at: cr.approved_at, fail: status === 'rejected' },
    cto_review:  { done: !!cr.cto_approved_at, by: cr.cto_approved_by_name, at: cr.cto_approved_at, fail: status === 'cto_rejected' },
    implemented: { done: !!cr.implemented_at, by: cr.implemented_by_name, at: cr.implemented_at, fail: status === 'failed' },
    closed:      { done: !!cr.closed_at, by: null, at: cr.closed_at },
  };

  // Find current step index
  const statusToStep = {
    created: 0, classified: 1, invalid: 2, auto_rejected: 2,
    validated: 2, approved: 3, rejected: 3,
    pending_cto: isUrgent ? 4 : 3, cto_approved: isUrgent ? 4 : 3, cto_rejected: isUrgent ? 4 : 3,
    implementing: isUrgent ? 5 : 4, implemented: isUrgent ? 5 : 4,
    failed: isUrgent ? 5 : 4, rolled_back: isUrgent ? 5 : 4,
    closed: isUrgent ? 6 : 5,
  };
  const currentIdx = statusToStep[status] ?? 0;

  return (
    <div style={{ margin: '6px 0 4px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 0 }}>
        {steps.map((step, i) => {
          const info = stepInfo[step.key] || {};
          const done = status === 'closed' ? true : i < currentIdx;
          const current = status === 'closed' ? false : i === currentIdx;
          const fail = current && (info.fail || false);
          const dotColor = done ? '#16a34a' : fail ? '#dc2626' : current ? '#00338D' : (isDark ? '#334155' : '#e2e8f0');
          const textColor = done ? '#16a34a' : fail ? '#dc2626' : current ? '#00338D' : '#94a3b8';
          const titleParts = [step.label];
          if (info.by) titleParts.push(info.by);
          if (info.at) titleParts.push(new Date(info.at).toLocaleString());

          return (
            <div key={step.key} style={{ display: 'flex', alignItems: 'center', flex: i < steps.length - 1 ? 1 : 'none' }}>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', minWidth: 52 }} title={titleParts.join('\n')}>
                <div style={{
                  width: 18, height: 18, borderRadius: '50%', background: dotColor,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 9, color: '#fff', fontWeight: 700,
                  boxShadow: current ? `0 0 0 2px ${dotColor}30` : 'none',
                }}>
                  {done ? '\u2713' : fail ? '\u2715' : i + 1}
                </div>
                <div style={{ fontSize: 7, fontWeight: 700, color: textColor, marginTop: 2, textAlign: 'center', textTransform: 'uppercase', letterSpacing: '0.03em', lineHeight: 1.1 }}>
                  {step.label}
                </div>
                {(done || current) && info.by && (
                  <div style={{ fontSize: 6.5, color: '#94a3b8', marginTop: 1, textAlign: 'center', maxWidth: 60, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{info.by}</div>
                )}
              </div>
              {i < steps.length - 1 && (
                <div style={{ flex: 1, height: 2, marginBottom: 14, background: done ? '#16a34a' : (isDark ? '#334155' : '#e2e8f0') }} />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ── Audit Trail ───────────────────────────────────────────────────────────── */
function AuditTrail({ crId, isDark }) {
  const [entries, setEntries] = useState([]);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (open && crId) {
      apiGet(`/api/cr/${crId}/audit-trail`).then(d => setEntries(d?.entries || [])).catch(() => {});
    }
  }, [open, crId]);

  const actionColor = (action) => {
    if (['validated', 'approved', 'cto_approved', 'implemented', 'closed'].includes(action)) return '#16a34a';
    if (['invalid', 'rejected', 'cto_rejected', 'failed', 'auto_rejected'].includes(action)) return '#dc2626';
    return isDark ? '#4da3ff' : '#00338D';
  };

  return (
    <div style={{ marginTop: 8 }}>
      <button onClick={() => setOpen(!open)} style={{
        background: 'none', border: 'none', cursor: 'pointer', fontSize: 11, fontWeight: 600,
        color: isDark ? '#4da3ff' : '#00338D', padding: 0,
      }}>
        {open ? 'Hide' : 'Show'} Audit Trail ({entries.length || '...'})
      </button>
      {open && entries.length > 0 && (
        <div style={{ position: 'relative', paddingLeft: 18, marginTop: 8 }}>
          <div style={{ position: 'absolute', left: 5, top: 4, bottom: 0, width: 2, background: isDark ? '#334155' : '#e2e8f0' }} />
          {entries.map((e, i) => (
            <div key={i} style={{ position: 'relative', paddingBottom: 12 }}>
              <div style={{
                position: 'absolute', left: -18, top: 3, width: 8, height: 8, borderRadius: '50%',
                background: actionColor(e.action), border: `2px solid ${isDark ? '#1e293b' : '#fff'}`,
              }} />
              <div style={{ fontSize: 10, fontWeight: 700, color: actionColor(e.action), textTransform: 'uppercase' }}>
                {e.action} {e.performed_by_name ? `- ${e.performed_by_name}` : ''}
              </div>
              <div style={{ fontSize: 10, color: '#94a3b8' }}>{e.created_at ? new Date(e.created_at).toLocaleString() : ''}</div>
              {e.notes && <div style={{ fontSize: 11, color: isDark ? '#94a3b8' : '#475569', marginTop: 2 }}>{e.notes}</div>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── Action Modal ──────────────────────────────────────────────────────────── */
function ActionModal({ cr, action, onClose, onDone, isDark }) {
  const [notes, setNotes] = useState('');
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');
  if (!cr || !action) return null;

  const submit = async () => {
    setLoading(true); setMsg('');
    try {
      if (action === 'implement') await apiPut(`/api/cr/${cr.id}/implement`, { notes });
      else if (action === 'close') await apiPut(`/api/cr/${cr.id}/close`, { notes });
      else if (action === 'resubmit') await apiPut(`/api/cr/${cr.id}/resubmit`, { justification: notes });
      onDone(); onClose();
    } catch (err) { setMsg(err?.message || 'Failed'); }
    setLoading(false);
  };

  const titles = { implement: 'Mark as Implemented', close: 'Close Change Request', resubmit: 'Resubmit CR' };
  const placeholders = {
    implement: 'Describe the implementation details, changes applied...',
    close: 'Closure notes / lessons learned...',
    resubmit: 'Updated justification for resubmission...',
  };

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000, padding: 16 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{ width: '100%', maxWidth: 480, background: isDark ? '#1e293b' : '#fff', borderRadius: 14, boxShadow: '0 24px 48px rgba(0,0,0,0.3)', padding: 24 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
          <div>
            <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a' }}>{titles[action]}</h3>
            <span style={{ fontSize: 12, fontFamily: 'monospace', color: isDark ? '#4da3ff' : '#00338D', fontWeight: 700 }}>{cr.cr_number}</span>
          </div>
          <button onClick={onClose} style={{ border: 'none', background: isDark ? '#334155' : '#f1f5f9', borderRadius: 6, width: 28, height: 28, cursor: 'pointer', fontSize: 14, color: '#94a3b8' }}>X</button>
        </div>

        {cr.manager_proposed_changes && action === 'implement' && (
          <div style={{ background: '#f5f3ff', border: '1px solid #c4b5fd', borderRadius: 8, padding: '10px 14px', marginBottom: 12, fontSize: 12, color: '#0f172a' }}>
            <div style={{ fontWeight: 700, color: '#7c3aed', fontSize: 11, marginBottom: 4 }}>MANAGER PROPOSED MODIFICATIONS</div>
            {cr.manager_proposed_changes}
          </div>
        )}

        <textarea value={notes} onChange={e => setNotes(e.target.value)} rows={4} placeholder={placeholders[action]}
          style={{ width: '100%', borderRadius: 8, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, padding: '8px 12px', fontSize: 13, resize: 'vertical', boxSizing: 'border-box', fontFamily: 'inherit', background: isDark ? '#0f172a' : '#fff', color: isDark ? '#e2e8f0' : '#0f172a' }}
        />
        {msg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{msg}</p>}
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
          <button onClick={onClose} disabled={loading} style={{ padding: '8px 18px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer', background: isDark ? '#152238' : '#f8fafc', color: isDark ? '#94a3b8' : '#475569', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>Cancel</button>
          <button onClick={submit} disabled={loading || (!notes.trim() && action === 'resubmit')} style={{ padding: '8px 18px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer', background: action === 'resubmit' ? '#7c3aed' : '#00338D', color: '#fff', border: 'none' }}>
            {loading ? 'Processing...' : titles[action]}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ── Main Component ────────────────────────────────────────────────────────── */
export default function ChangeWorkflow() {
  const { isDark } = useTheme();
  const [crs, setCrs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('');
  const [actionCR, setActionCR] = useState(null);
  const [remarksCR, setRemarksCR] = useState(null);
  const [actionType, setActionType] = useState('');
  const [expandedId, setExpandedId] = useState(null);

  const fetchCRs = useCallback(async () => {
    try {
      const d = await apiGet('/api/cr/agent-list');
      setCrs(d?.crs || []);
    } catch { setCrs([]); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchCRs(); }, [fetchCRs]);
  useEffect(() => { const iv = setInterval(fetchCRs, 30000); return () => clearInterval(iv); }, [fetchCRs]);

  const [section, setSection] = useState('customer'); // 'customer' or 'ai'

  // Split CRs into customer complaint vs AI ticket
  const customerCRs = crs.filter(c => c.ticket_id && !c.network_issue_id);
  const aiCRs = crs.filter(c => c.network_issue_id);
  const activeCRs = section === 'customer' ? customerCRs : aiCRs;

  const filtered = filter ? activeCRs.filter(c => {
    if (filter === 'approved') return ['approved', 'pending_cto', 'cto_approved'].includes(c.status);
    if (filter === 'implemented') return ['implementing', 'implemented'].includes(c.status);
    return c.status === filter;
  }) : activeCRs;

  const getAction = (status) => {
    if (status === 'approved' || status === 'cto_approved') return 'implement';
    if (status === 'implemented') return 'close';
    if (status === 'invalid' || status === 'cto_rejected') return 'resubmit';
    return null;
  };

  const sectionColor = section === 'customer' ? '#00338D' : '#7c3aed';

  return (
    <div>
      <div className="page-header">
        <h1>Change Workflow</h1>
        <p>Track your Change Requests through the ITIL lifecycle</p>
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

      {/* Stats */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12, marginBottom: 20 }}>
        {[
          { label: 'Total', count: activeCRs.length, color: sectionColor },
          { label: 'Approved', count: activeCRs.filter(c => ['approved', 'pending_cto', 'cto_approved'].includes(c.status)).length, color: '#16a34a' },
          { label: 'Implemented', count: activeCRs.filter(c => ['implementing', 'implemented'].includes(c.status)).length, color: '#7c3aed' },
          { label: 'Closed', count: activeCRs.filter(c => c.status === 'closed').length, color: '#64748b' },
        ].map(s => (
          <div key={s.label} style={{ background: isDark ? '#1e293b' : '#fff', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 10, padding: '12px 14px', borderTop: `3px solid ${s.color}` }}>
            <div style={{ fontSize: 22, fontWeight: 800, color: s.color }}>{s.count}</div>
            <div style={{ fontSize: 11, fontWeight: 600, color: isDark ? '#94a3b8' : '#64748b', textTransform: 'uppercase' }}>{s.label}</div>
          </div>
        ))}
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 16, flexWrap: 'wrap' }}>
        {TABS.map(t => {
          const cnt = !t.key ? activeCRs.length
            : t.key === 'approved' ? activeCRs.filter(c => ['approved', 'pending_cto', 'cto_approved'].includes(c.status)).length
            : t.key === 'implemented' ? activeCRs.filter(c => ['implementing', 'implemented'].includes(c.status)).length
            : activeCRs.filter(c => c.status === t.key).length;
          return (
            <button key={t.key} onClick={() => setFilter(t.key)} style={{
              padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
              background: filter === t.key ? sectionColor : (isDark ? '#1e293b' : '#f8fafc'),
              color: filter === t.key ? '#fff' : (isDark ? '#94a3b8' : '#64748b'),
              border: `1px solid ${filter === t.key ? sectionColor : (isDark ? '#334155' : '#e2e8f0')}`,
            }}>{t.label}{cnt > 0 ? ` (${cnt})` : ''}</button>
          );
        })}
      </div>

      {/* Section Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <div style={{ width: 4, height: 16, borderRadius: 2, background: sectionColor }} />
        <span style={{ fontSize: 13, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
          {section === 'customer' ? 'Customer Complaint CRs' : 'AI-Detected Network Issue CRs'}
        </span>
        {section === 'customer' && <span style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>Raised from customer tickets (SR)</span>}
        {section === 'ai' && <span style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>Raised from AI network analysis</span>}
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
          const meta = STATUS_META[cr.status] || STATUS_META.created;
          const isClosed = ['closed', 'rejected', 'auto_rejected', 'cto_rejected'].includes(cr.status);
          const ts = isClosed ? { bg: '#f1f5f9', color: '#94a3b8', border: '#e2e8f0', label: cr.change_type ? cr.change_type.charAt(0).toUpperCase() + cr.change_type.slice(1) : '' } : TYPE_STYLE[cr.change_type];
          const act = getAction(cr.status);
          const isExpanded = expandedId === cr.id;

          return (
            <div key={cr.id} style={{
              background: isDark ? '#1e293b' : '#fff', borderRadius: 12,
              border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`,
              borderLeft: `4px solid ${ts ? ts.color : '#64748b'}`,
              marginBottom: 12, overflow: 'hidden',
              boxShadow: act ? (isDark ? '0 2px 8px rgba(0,0,0,0.2)' : '0 2px 8px rgba(0,51,141,0.08)') : 'none',
            }}>
              <div style={{ padding: '14px 18px' }}>
                {/* Row 1: CR# + badges + SLA timer */}
                <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 6 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                    <span style={{ fontFamily: "'IBM Plex Mono',monospace", fontSize: 14, fontWeight: 800, color: isDark ? '#4da3ff' : '#00338D' }}>{cr.cr_number}</span>
                    {cr.category && <span style={{ fontSize: 11, color: isDark ? '#94a3b8' : '#64748b' }}>{cr.category}{cr.subcategory ? ` > ${cr.subcategory}` : ''}</span>}
                    {ts && <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: ts.bg, color: ts.color, border: `1px solid ${ts.border}` }}>{ts.label}</span>}
                    <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: `${meta.color}15`, color: meta.color, border: `1px solid ${meta.color}30` }}>{meta.label}</span>
                  </div>
                  <SLATimer deadline={cr.cr_sla_deadline} slaHours={cr.cr_sla_hours} status={cr.status} />
                </div>

                {/* Domain chips */}
                {cr.telecom_domain_primary && (
                  <div style={{ display: 'flex', gap: 4, marginBottom: 6 }}>
                    <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 8px', borderRadius: 4, background: '#00338D', color: '#fff' }}>{cr.telecom_domain_primary}</span>
                    {(cr.telecom_domain_secondary || '').split(',').filter(Boolean).map(d => (
                      <span key={d} style={{ fontSize: 9, fontWeight: 600, padding: '2px 8px', borderRadius: 4, background: isDark ? '#152238' : '#f1f5f9', color: isDark ? '#64748b' : '#94a3b8', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>{d.trim()}</span>
                    ))}
                  </div>
                )}

                {/* Source badge */}
                {cr.ticket_ref && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                    <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 4, background: isDark ? '#152238' : '#eff6ff', color: isDark ? '#4da3ff' : '#00338D', border: `1px solid ${isDark ? '#1e3a5f' : '#bfdbfe'}` }}>
                      {cr.ticket_ref}
                    </span>
                    {cr.ticket_user_name && <span style={{ fontSize: 10, color: isDark ? '#94a3b8' : '#64748b' }}>{cr.ticket_user_name}</span>}
                  </div>
                )}

                {/* Meta row */}
                <div style={{ fontSize: 11, color: isDark ? '#94a3b8' : '#64748b', marginBottom: 6 }}>
                  {cr.zone && <span>Zone: {cr.zone}</span>}
                  {cr.location && <span> | {cr.location}</span>}
                  {cr.customer_type && <span> | {cr.customer_type.charAt(0).toUpperCase() + cr.customer_type.slice(1)}</span>}
                  {cr.assigned_manager_name && <span> | Manager: <strong style={{ color: isDark ? '#cbd5e1' : '#374151' }}>{cr.assigned_manager_name}</strong></span>}
                </div>

                {/* RF Params summary (collapsed) */}
                {cr.rf_params && Object.values(cr.rf_params).some(v => v.proposed != null) && (
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                    {Object.entries(cr.rf_params).map(([key, val]) => val.proposed != null ? (
                      <span key={key} style={{ fontSize: 10, padding: '2px 6px', borderRadius: 4, background: isDark ? '#152238' : '#f0fdf4', border: `1px solid ${isDark ? '#334155' : '#bbf7d0'}`, color: isDark ? '#94a3b8' : '#475569' }}>
                        {key.replace(/_/g, ' ')}: {val.current ?? 'N/A'} &rarr; <strong style={{ color: '#16a34a' }}>{val.proposed}</strong>
                      </span>
                    ) : null)}
                  </div>
                )}

                {/* Pipeline */}
                <PipelineVis cr={cr} isDark={isDark} />

                {/* Remarks button — only show if there are remarks */}
                {(cr.approval_remark || cr.manager_proposed_changes || cr.cto_remark || cr.validation_remark) && (
                  <button onClick={() => setRemarksCR(cr)} style={{
                    background: 'none', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 6,
                    padding: '4px 12px', fontSize: 11, fontWeight: 600, cursor: 'pointer',
                    color: isDark ? '#94a3b8' : '#64748b', marginBottom: 6,
                  }}>
                    View Remarks
                  </button>
                )}

                {/* Actions + Audit trail */}
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: 4 }}>
                  <AuditTrail crId={cr.id} isDark={isDark} />
                  <div style={{ display: 'flex', gap: 8 }}>
                    <span style={{ fontSize: 10, color: '#94a3b8' }}>{cr.created_at ? new Date(cr.created_at).toLocaleString() : ''}</span>
                    {act && (
                      <button onClick={() => { setActionCR(cr); setActionType(act); }} style={{
                        padding: '6px 14px', borderRadius: 7, fontSize: 12, fontWeight: 600, cursor: 'pointer',
                        background: act === 'resubmit' ? '#7c3aed' : '#00338D', color: '#fff', border: 'none',
                      }}>
                        {act === 'implement' ? 'Mark Implemented' : act === 'close' ? 'Close CR' : 'Resubmit'}
                      </button>
                    )}
                    {!act && !['closed', 'rejected', 'auto_rejected'].includes(cr.status) && (
                      <span style={{ fontSize: 11, color: '#f59e0b', fontWeight: 600, padding: '4px 10px', background: '#fffbeb', borderRadius: 6, border: '1px solid #fde68a' }}>
                        {cr.status === 'classified' ? 'Awaiting Validation' : cr.status === 'validated' ? 'Awaiting Approval' : cr.status === 'pending_cto' ? 'Awaiting CTO' : 'Processing'}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            </div>
          );
        })
      )}

      {/* Action Modal */}
      {actionCR && actionType && (
        <ActionModal cr={actionCR} action={actionType} onClose={() => { setActionCR(null); setActionType(''); }} onDone={fetchCRs} isDark={isDark} />
      )}

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

            {remarksCR.validation_remark && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#0369a1' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#0369a1', textTransform: 'uppercase' }}>Validation — {remarksCR.validated_by_name || 'Manager'}</span>
                  {remarksCR.validated_at && <span style={{ fontSize: 10, color: '#94a3b8' }}>{new Date(remarksCR.validated_at).toLocaleString()}</span>}
                </div>
                <div style={{ background: isDark ? '#0f172a' : '#f8fafc', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 8, padding: '10px 14px', fontSize: 13, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: 1.5 }}>
                  {remarksCR.validation_remark}
                </div>
              </div>
            )}

            {remarksCR.approval_remark && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <div style={{ width: 4, height: 4, borderRadius: '50%', background: '#16a34a' }} />
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#16a34a', textTransform: 'uppercase' }}>Approval — {remarksCR.approved_by_name || 'Manager'}</span>
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
                  <span style={{ fontSize: 11, fontWeight: 700, color: '#ea580c', textTransform: 'uppercase' }}>CTO — {remarksCR.cto_approved_by_name || 'CTO'}</span>
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
              <div>
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
