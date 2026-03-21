import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';

/* ── Constants ─────────────────────────────────────────────────────────────── */
const PRIORITY_STYLE = {
  critical: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca' },
  high:     { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa' },
  medium:   { bg: '#fffbeb', color: '#d97706', border: '#fde68a' },
  low:      { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0' },
};

const TYPE_STYLE = {
  standard:  { bg: '#eff6ff', color: '#1d4ed8', border: '#bfdbfe', label: 'Standard'  },
  normal:    { bg: '#f5f3ff', color: '#6d28d9', border: '#c4b5fd', label: 'Normal'    },
  emergency: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca', label: 'Emergency' },
};

const STATUS_META = {
  created:       { label: 'Created',        color: '#64748b', stage: 0 },
  invalid:       { label: 'Invalid',        color: '#dc2626', stage: 0 },
  auto_rejected: { label: 'Auto-Rejected',  color: '#991b1b', stage: 0 },
  validated:     { label: 'Validated',      color: '#0369a1', stage: 1 },
  classified:    { label: 'Classified',     color: '#7c3aed', stage: 2 },
  approved:      { label: 'Approved',       color: '#16a34a', stage: 3 },
  rejected:      { label: 'Rejected',       color: '#dc2626', stage: 2 },
  implementing:  { label: 'Implementing',   color: '#d97706', stage: 3 },
  implemented:   { label: 'Implemented',    color: '#15803d', stage: 4 },
  failed:        { label: 'Failed',         color: '#dc2626', stage: 4 },
  rolled_back:   { label: 'Rolled Back',    color: '#92400e', stage: 4 },
  closed:        { label: 'Closed',         color: '#475569', stage: 5 },
};

const PIPELINE_STEPS = ['CR Raised', 'Validated', 'Classified', 'Approved', 'Implemented', 'Closed'];

/* ── Pipeline Component ─────────────────────────────────────────────────────── */
function Pipeline({ status }) {
  const meta    = STATUS_META[status] || STATUS_META.created;
  const stage   = meta.stage;
  const isFail  = ['invalid','auto_rejected','rejected','failed'].includes(status);
  const isRollB = status === 'rolled_back';

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 0, margin: '12px 0 10px' }}>
      {PIPELINE_STEPS.map((step, i) => {
        const done    = status === 'closed' ? true : i < stage;
        const current = status === 'closed' ? false : i === stage;
        const fail    = current && isFail;
        const rb      = current && isRollB;

        const dotColor = done    ? '#16a34a'
                       : fail    ? '#dc2626'
                       : rb      ? '#92400e'
                       : current ? '#00338D'
                       :           '#e2e8f0';

        const textColor = done    ? '#16a34a'
                        : fail    ? '#dc2626'
                        : rb      ? '#92400e'
                        : current ? '#00338D'
                        :           '#94a3b8';

        return (
          <div key={step} style={{ display: 'flex', alignItems: 'center', flex: i < PIPELINE_STEPS.length - 1 ? 1 : 'none' }}>
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', minWidth: 52 }}>
              <div style={{
                width: 22, height: 22, borderRadius: '50%',
                background: dotColor,
                border: `2px solid ${dotColor}`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 11, color: '#fff', fontWeight: 700,
                boxShadow: current ? `0 0 0 3px ${dotColor}30` : 'none',
                transition: 'all 0.2s',
              }}>
                {done ? '✓' : fail ? '✕' : rb ? '↩' : i + 1}
              </div>
              <div style={{
                fontSize: 9, fontWeight: 600, color: textColor,
                marginTop: 4, textAlign: 'center', whiteSpace: 'nowrap',
                textTransform: 'uppercase', letterSpacing: '0.04em',
              }}>
                {step}
              </div>
            </div>
            {i < PIPELINE_STEPS.length - 1 && (
              <div style={{
                flex: 1, height: 2, marginBottom: 14,
                background: done ? '#16a34a' : '#e2e8f0',
                transition: 'background 0.3s',
              }} />
            )}
          </div>
        );
      })}
    </div>
  );
}

/* ── Action Modal ───────────────────────────────────────────────────────────── */
function ActionModal({ cr, onClose, onDone }) {
  const [step,     setStep]     = useState('');     // validate|classify|approve|close
  const [typeVal,  setTypeVal]  = useState('standard');
  const [remark,   setRemark]   = useState('');
  const [loading,  setLoading]  = useState(false);
  const [errMsg,   setErrMsg]   = useState('');

  // Determine which action to show based on CR status
  useEffect(() => {
    if (!cr) return;
    const s = cr.status;
    if (s === 'created' || s === 'invalid')     setStep('validate');
    else if (s === 'validated')                  setStep('classify');
    else if (s === 'classified')                 setStep('approve');
    else if (s === 'implemented' || s === 'rolled_back') setStep('close');
    else setStep('');
  }, [cr]);

  if (!cr) return null;

  const submit = async (decision) => {
    setLoading(true);
    setErrMsg('');
    try {
      let endpoint, body;
      if (step === 'validate') {
        endpoint = `/api/manager/change-requests/${cr.id}/validate`;
        body     = { decision, remark };
      } else if (step === 'classify') {
        endpoint = `/api/manager/change-requests/${cr.id}/classify`;
        body     = { change_type: typeVal, note: remark };
      } else if (step === 'approve') {
        endpoint = `/api/manager/change-requests/${cr.id}/approve`;
        body     = { decision, remark };
      } else if (step === 'close') {
        endpoint = `/api/manager/change-requests/${cr.id}/close`;
        body     = { notes: remark };
      }
      await apiPut(endpoint, body);
      onDone();
      onClose();
    } catch (err) {
      setErrMsg(err?.message || 'Action failed. Please try again.');
    }
    setLoading(false);
  };

  const STEP_TITLES = {
    validate: 'Validate Change Request',
    classify: 'Classify Change Type',
    approve:  'Approve / Reject Change',
    close:    'Close Change Request',
  };

  return (
    <div style={{
      position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 1000, padding: 16,
    }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{
        width: '100%', maxWidth: 540, background: '#fff', borderRadius: 14,
        boxShadow: '0 24px 48px rgba(15,23,42,0.2)', padding: 28,
      }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 18 }}>
          <div>
            <h3 style={{ margin: '0 0 4px', fontSize: 16, fontWeight: 700, color: '#0f172a' }}>
              {STEP_TITLES[step]}
            </h3>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              <span style={{ fontSize: 12, fontFamily: 'monospace', color: '#00338D', fontWeight: 700 }}>
                {cr.cr_number}
              </span>
              <span style={{ fontSize: 11, color: '#94a3b8' }}>·</span>
              <span style={{ fontSize: 12, color: '#64748b' }}>{cr.ticket_ref}</span>
            </div>
          </div>
          <button onClick={onClose} style={{
            border: 'none', background: '#f1f5f9', borderRadius: 6, width: 28, height: 28,
            cursor: 'pointer', fontSize: 14, color: '#64748b', flexShrink: 0,
          }}>✕</button>
        </div>

        {/* CR summary box */}
        <div style={{
          background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8,
          padding: '10px 14px', marginBottom: 18,
        }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#0f172a', marginBottom: 4 }}>{cr.title}</div>
          <div style={{ fontSize: 12, color: '#64748b', lineHeight: 1.5 }}>{cr.description}</div>
          {cr.rejection_count > 0 && (
            <div style={{
              marginTop: 8, fontSize: 11, fontWeight: 700, color: '#dc2626',
              background: '#fef2f2', border: '1px solid #fecaca',
              borderRadius: 4, padding: '3px 8px', display: 'inline-block',
            }}>
              ⚠ {cr.rejection_count}/2 validation rejections used
            </div>
          )}
        </div>

        {/* Step-specific inputs */}
        {step === 'validate' && (
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: '#374151', display: 'block', marginBottom: 6 }}>
              Validation Remark (required if rejecting)
            </label>
            <textarea value={remark} onChange={e => setRemark(e.target.value)}
              rows={3} placeholder="Provide your validation comments..."
              style={{ width: '100%', borderRadius: 8, border: '1px solid #e2e8f0', padding: '8px 12px', fontSize: 13, resize: 'vertical', boxSizing: 'border-box', fontFamily: 'inherit' }}
            />
            {errMsg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{errMsg}</p>}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
              <button onClick={onClose} disabled={loading} style={btnStyle('ghost')}>Cancel</button>
              <button onClick={() => submit('invalid')} disabled={loading} style={btnStyle('danger')}>
                {loading ? '…' : `Reject (${cr.rejection_count + 1}/2)`}
              </button>
              <button onClick={() => submit('valid')} disabled={loading} style={btnStyle('success')}>
                {loading ? '…' : 'Mark Valid ✓'}
              </button>
            </div>
          </div>
        )}

        {step === 'classify' && (
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: '#374151', display: 'block', marginBottom: 8 }}>
              Change Classification
            </label>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 10, marginBottom: 14 }}>
              {['standard', 'normal', 'emergency'].map(t => {
                const ts = TYPE_STYLE[t];
                const sel = typeVal === t;
                return (
                  <button key={t} onClick={() => setTypeVal(t)} style={{
                    padding: '10px 6px', borderRadius: 8, cursor: 'pointer', textAlign: 'center',
                    background: sel ? ts.bg : '#f8fafc',
                    border: `2px solid ${sel ? ts.border : '#e2e8f0'}`,
                    color: sel ? ts.color : '#64748b',
                    fontWeight: sel ? 700 : 500, fontSize: 13,
                    transition: 'all 0.15s',
                  }}>
                    {ts.label}
                    <div style={{ fontSize: 10, marginTop: 3, fontWeight: 400, opacity: 0.8 }}>
                      {t === 'standard' ? 'Pre-approved type' : t === 'normal' ? 'Requires review' : 'Urgent / critical'}
                    </div>
                  </button>
                );
              })}
            </div>
            <textarea value={remark} onChange={e => setRemark(e.target.value)}
              rows={2} placeholder="Optional classification note..."
              style={{ width: '100%', borderRadius: 8, border: '1px solid #e2e8f0', padding: '8px 12px', fontSize: 13, resize: 'none', boxSizing: 'border-box', fontFamily: 'inherit' }}
            />
            {errMsg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{errMsg}</p>}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
              <button onClick={onClose} disabled={loading} style={btnStyle('ghost')}>Cancel</button>
              <button onClick={() => submit()} disabled={loading} style={btnStyle('primary')}>
                {loading ? '…' : 'Classify & Continue →'}
              </button>
            </div>
          </div>
        )}

        {step === 'approve' && (
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: '#374151', display: 'block', marginBottom: 6 }}>
              Approval Remark
            </label>
            <textarea value={remark} onChange={e => setRemark(e.target.value)}
              rows={3} placeholder="Optional note for the expert..."
              style={{ width: '100%', borderRadius: 8, border: '1px solid #e2e8f0', padding: '8px 12px', fontSize: 13, resize: 'vertical', boxSizing: 'border-box', fontFamily: 'inherit' }}
            />
            {errMsg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{errMsg}</p>}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
              <button onClick={onClose} disabled={loading} style={btnStyle('ghost')}>Cancel</button>
              <button onClick={() => submit('rejected')} disabled={loading} style={btnStyle('danger')}>
                {loading ? '…' : 'Reject ✕'}
              </button>
              <button onClick={() => submit('approved')} disabled={loading} style={btnStyle('success')}>
                {loading ? '…' : 'Approve ✓'}
              </button>
            </div>
          </div>
        )}

        {step === 'close' && (
          <div>
            <div style={{
              padding: '10px 14px', borderRadius: 8, marginBottom: 14,
              background: cr.status === 'implemented' ? '#f0fdf4' : '#fffbeb',
              border: `1px solid ${cr.status === 'implemented' ? '#bbf7d0' : '#fde68a'}`,
              fontSize: 13, color: cr.status === 'implemented' ? '#166534' : '#92400e',
            }}>
              {cr.status === 'implemented'
                ? '✓ Implementation was successful. Close this CR.'
                : '↩ Rollback was executed. Close this CR with notes.'}
            </div>
            <textarea value={remark} onChange={e => setRemark(e.target.value)}
              rows={3} placeholder="Closure notes / lessons learned..."
              style={{ width: '100%', borderRadius: 8, border: '1px solid #e2e8f0', padding: '8px 12px', fontSize: 13, resize: 'vertical', boxSizing: 'border-box', fontFamily: 'inherit' }}
            />
            {errMsg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{errMsg}</p>}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
              <button onClick={onClose} disabled={loading} style={btnStyle('ghost')}>Cancel</button>
              <button onClick={() => submit()} disabled={loading} style={btnStyle('primary')}>
                {loading ? '…' : 'Close Change Request'}
              </button>
            </div>
          </div>
        )}

        {!step && (
          <div style={{ textAlign: 'center', padding: '12px 0', color: '#94a3b8', fontSize: 13 }}>
            No action available for this CR in its current state.
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Detail Drawer ──────────────────────────────────────────────────────────── */
function DetailDrawer({ cr, onClose }) {
  if (!cr) return null;
  const Row = ({ label, value, mono }) => value ? (
    <div style={{ display: 'grid', gridTemplateColumns: '130px 1fr', gap: 8, padding: '7px 0', borderBottom: '1px solid #f1f5f9' }}>
      <span style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em', paddingTop: 1 }}>{label}</span>
      <span style={{ fontSize: 13, color: '#0f172a', fontFamily: mono ? 'monospace' : 'inherit' }}>{value}</span>
    </div>
  ) : null;

  const ts = TYPE_STYLE[cr.change_type];
  const meta = STATUS_META[cr.status] || STATUS_META.created;

  const timeline = [
    { label: 'Raised',      time: cr.created_at,      by: cr.raised_by_name,      note: cr.description },
    cr.validated_at && { label: 'Validated',   time: cr.validated_at,    by: cr.validated_by_name,   note: cr.validation_remark },
    cr.classified_at && { label: 'Classified',  time: cr.classified_at,   by: cr.classified_by_name,  note: cr.classification_note },
    cr.approved_at && { label: 'Approved',    time: cr.approved_at,     by: cr.approved_by_name,    note: cr.approval_remark },
    cr.implemented_at && { label: 'Implemented', time: cr.implemented_at,  by: cr.implemented_by_name, note: cr.implementation_notes },
    cr.rollback_at && { label: 'Rolled Back', time: cr.rollback_at,     by: cr.raised_by_name,      note: cr.rollback_notes },
    cr.closed_at && { label: 'Closed',      time: cr.closed_at,       by: null,                   note: cr.closure_notes },
  ].filter(Boolean);

  return (
    <div style={{
      position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.45)',
      display: 'flex', alignItems: 'flex-start', justifyContent: 'flex-end',
      zIndex: 1000,
    }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{
        width: 480, height: '100vh', background: '#fff', overflowY: 'auto',
        boxShadow: '-8px 0 24px rgba(15,23,42,0.15)', padding: 28,
      }}>
        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 20 }}>
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>
              Change Request
            </div>
            <div style={{ fontSize: 18, fontWeight: 800, color: '#00338D', fontFamily: 'monospace' }}>
              {cr.cr_number}
            </div>
          </div>
          <button onClick={onClose} style={{
            border: 'none', background: '#f1f5f9', borderRadius: 8, width: 32, height: 32,
            cursor: 'pointer', fontSize: 16, color: '#64748b',
          }}>✕</button>
        </div>

        {/* Status + Type badges */}
        <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
          <span style={{
            fontSize: 11, fontWeight: 700, padding: '3px 10px', borderRadius: 5,
            background: `${meta.color}15`, color: meta.color, border: `1px solid ${meta.color}30`,
          }}>{meta.label}</span>
          {ts && <span style={{
            fontSize: 11, fontWeight: 700, padding: '3px 10px', borderRadius: 5,
            background: ts.bg, color: ts.color, border: `1px solid ${ts.border}`,
          }}>{ts.label}</span>}
          {(() => { const p = PRIORITY_STYLE[cr.ticket_priority]; return p ? (
            <span style={{ fontSize: 11, fontWeight: 700, padding: '3px 10px', borderRadius: 5, background: p.bg, color: p.color, border: `1px solid ${p.border}` }}>
              {cr.ticket_priority}
            </span>
          ) : null; })()}
        </div>

        {/* Pipeline */}
        <Pipeline status={cr.status} />

        {/* Details */}
        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 14, fontWeight: 700, color: '#0f172a', marginBottom: 12 }}>Change Details</div>
          <Row label="Title"      value={cr.title} />
          <Row label="Ticket"     value={cr.ticket_ref} mono />
          <Row label="Customer"   value={cr.ticket_user_name} />
          <Row label="Domain"     value={cr.ticket_domain} />
          <Row label="Category"   value={`${cr.ticket_category}${cr.ticket_subcategory ? ' › ' + cr.ticket_subcategory : ''}`} />
          <Row label="Raised By"  value={cr.raised_by_name} />
          <Row label="Raised At"  value={cr.created_at ? new Date(cr.created_at).toLocaleString() : null} />
        </div>

        {/* Description */}
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
            Description
          </div>
          <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8, padding: '10px 14px', fontSize: 13, color: '#0f172a', lineHeight: 1.6 }}>
            {cr.description}
          </div>
        </div>

        {cr.impact_assessment && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
              Impact Assessment
            </div>
            <div style={{ background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 8, padding: '10px 14px', fontSize: 13, color: '#0f172a', lineHeight: 1.6 }}>
              {cr.impact_assessment}
            </div>
          </div>
        )}

        {cr.rollback_plan && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
              Rollback Plan
            </div>
            <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8, padding: '10px 14px', fontSize: 13, color: '#0f172a', lineHeight: 1.6 }}>
              {cr.rollback_plan}
            </div>
          </div>
        )}

        {/* Audit timeline */}
        <div>
          <div style={{ fontSize: 14, fontWeight: 700, color: '#0f172a', marginBottom: 12 }}>Audit Trail</div>
          <div style={{ position: 'relative', paddingLeft: 20 }}>
            <div style={{ position: 'absolute', left: 7, top: 6, bottom: 0, width: 2, background: '#e2e8f0' }} />
            {timeline.map((ev, i) => (
              <div key={i} style={{ position: 'relative', paddingBottom: 16 }}>
                <div style={{
                  position: 'absolute', left: -20, top: 4,
                  width: 10, height: 10, borderRadius: '50%',
                  background: '#00338D', border: '2px solid #fff',
                  boxShadow: '0 0 0 2px #00338D',
                }} />
                <div style={{ fontSize: 11, fontWeight: 700, color: '#00338D', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  {ev.label} {ev.by ? `— ${ev.by}` : ''}
                </div>
                <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: ev.note ? 4 : 0 }}>
                  {ev.time ? new Date(ev.time).toLocaleString() : ''}
                </div>
                {ev.note && (
                  <div style={{ fontSize: 12, color: '#475569', background: '#f8fafc', borderRadius: 6, padding: '6px 10px', lineHeight: 1.5 }}>
                    {ev.note}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Helpers ────────────────────────────────────────────────────────────────── */
function btnStyle(type) {
  const base = { padding: '8px 18px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer', border: '1px solid' };
  if (type === 'primary') return { ...base, background: '#00338D', color: '#fff', borderColor: '#00338D' };
  if (type === 'success') return { ...base, background: '#16a34a', color: '#fff', borderColor: '#16a34a' };
  if (type === 'danger')  return { ...base, background: '#fff', color: '#dc2626', borderColor: '#fca5a5' };
  return { ...base, background: '#f8fafc', color: '#475569', borderColor: '#e2e8f0' };
}

/* ── CR Card ────────────────────────────────────────────────────────────────── */
function CRCard({ cr, onAction, onDetail }) {
  const meta   = STATUS_META[cr.status] || STATUS_META.created;
  const p      = PRIORITY_STYLE[cr.ticket_priority] || PRIORITY_STYLE.medium;
  const ts     = TYPE_STYLE[cr.change_type];
  const needsAction = ['created','invalid','validated','classified','implemented','rolled_back'].includes(cr.status);

  return (
    <div style={{
      background: '#fff', borderRadius: 12, border: '1px solid #e2e8f0',
      boxShadow: needsAction ? '0 2px 8px rgba(0,51,141,0.08)' : '0 1px 3px rgba(0,0,0,0.04)',
      overflow: 'hidden', marginBottom: 12,
      borderLeft: `4px solid ${meta.color}`,
    }}>
      <div style={{ padding: '16px 20px' }}>
        {/* Row 1: CR# + Ticket + Badges + Time */}
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 8, gap: 12 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <span style={{ fontFamily: 'monospace', fontSize: 13, fontWeight: 800, color: '#00338D' }}>
              {cr.cr_number}
            </span>
            <span style={{ fontSize: 11, color: '#94a3b8' }}>·</span>
            <span style={{ fontFamily: 'monospace', fontSize: 11, color: '#64748b' }}>{cr.ticket_ref}</span>
            <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: p.bg, color: p.color, border: `1px solid ${p.border}`, textTransform: 'uppercase' }}>
              {cr.ticket_priority}
            </span>
            {ts && (
              <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: ts.bg, color: ts.color, border: `1px solid ${ts.border}`, textTransform: 'uppercase' }}>
                {ts.label}
              </span>
            )}
            <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: `${meta.color}15`, color: meta.color, border: `1px solid ${meta.color}30` }}>
              {meta.label}
            </span>
          </div>
          <span style={{ fontSize: 11, color: '#94a3b8', whiteSpace: 'nowrap', flexShrink: 0 }}>
            {cr.created_at ? new Date(cr.created_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : ''}
          </span>
        </div>

        {/* Title */}
        <div style={{ fontSize: 14, fontWeight: 600, color: '#0f172a', marginBottom: 4, lineHeight: 1.4 }}>
          {cr.title}
        </div>

        {/* Meta: raised by + customer */}
        <div style={{ fontSize: 12, color: '#64748b', marginBottom: 10 }}>
          Raised by <strong style={{ color: '#374151' }}>{cr.raised_by_name}</strong>
          {cr.ticket_user_name && <> · Customer: <strong style={{ color: '#374151' }}>{cr.ticket_user_name}</strong></>}
          {cr.ticket_domain && <> · <span style={{ textTransform: 'capitalize' }}>{cr.ticket_domain}</span></>}
        </div>

        {/* Description excerpt */}
        <div style={{
          background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 7,
          padding: '8px 12px', fontSize: 12, color: '#475569',
          lineHeight: 1.5, marginBottom: 12,
          overflow: 'hidden', display: '-webkit-box', WebkitLineClamp: 2,
          WebkitBoxOrient: 'vertical',
        }}>
          {cr.description}
        </div>

        {/* Rejection warning */}
        {cr.rejection_count >= 1 && cr.status !== 'auto_rejected' && (
          <div style={{
            fontSize: 11, fontWeight: 700, color: '#dc2626', marginBottom: 10,
            background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 5,
            padding: '4px 10px', display: 'inline-block',
          }}>
            ⚠ {cr.rejection_count}/2 rejection{cr.rejection_count > 1 ? 's' : ''} used
          </div>
        )}

        {/* Pipeline */}
        <Pipeline status={cr.status} />

        {/* Actions */}
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 4 }}>
          <button onClick={() => onDetail(cr)} style={btnStyle('ghost')}>
            View Details
          </button>
          {needsAction && (
            <button onClick={() => onAction(cr)} style={{
              ...btnStyle('primary'),
              background: 'linear-gradient(135deg, #00338D, #0047c9)',
            }}>
              {cr.status === 'created' || cr.status === 'invalid'     ? 'Validate →'
               : cr.status === 'validated'                             ? 'Classify →'
               : cr.status === 'classified'                            ? 'Approve / Reject →'
               : cr.status === 'implemented' || cr.status === 'rolled_back' ? 'Close CR →'
               : 'Action →'}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

/* ── Main Component ─────────────────────────────────────────────────────────── */
const FILTER_TABS = [
  { key: '',             label: 'All' },
  { key: 'needs_action', label: 'Needs Action' },
  { key: 'approved',     label: 'Approved' },
  { key: 'implementing', label: 'Implementing' },
  { key: 'closed',       label: 'Closed' },
  { key: 'rejected',     label: 'Rejected' },
];

export default function ManagerChangeWorkflow() {
  const [crs,     setCrs]     = useState([]);
  const [stats,   setStats]   = useState({ total: 0, needs_action: 0, approved: 0, closed: 0 });
  const [loading, setLoading] = useState(true);
  const [filter,  setFilter]  = useState('');
  const [actionCR, setActionCR] = useState(null);
  const [detailCR, setDetailCR] = useState(null);

  const fetchCRs = useCallback(async () => {
    try {
      const d = await apiGet(`/api/manager/change-requests${filter ? `?status=${filter}` : ''}`);
      setCrs(d?.change_requests || []);
      if (d?.stats) setStats(d.stats);
    } catch { /* ignore */ }
    setLoading(false);
  }, [filter]);

  useEffect(() => {
    setLoading(true);
    fetchCRs();
  }, [filter, fetchCRs]);

  useEffect(() => {
    const iv = setInterval(fetchCRs, 30000);
    return () => clearInterval(iv);
  }, [fetchCRs]);

  const STAT_CARDS = [
    { label: 'Total CRs',    value: stats.total,        color: '#00338D', icon: '📋' },
    { label: 'Needs Action', value: stats.needs_action, color: '#dc2626', icon: '⚡', highlight: stats.needs_action > 0 },
    { label: 'Approved',     value: stats.approved,     color: '#16a34a', icon: '✓'  },
    { label: 'Closed',       value: stats.closed,       color: '#64748b', icon: '✔'  },
  ];

  return (
    <div>
      {/* Page header */}
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <h1>Change Workflow</h1>
          {stats.needs_action > 0 && (
            <span style={{ background: '#dc2626', color: '#fff', fontSize: 12, fontWeight: 700, padding: '3px 10px', borderRadius: 12 }}>
              {stats.needs_action} need action
            </span>
          )}
        </div>
        <p>ITIL-aligned Change Request lifecycle — Validate · Classify · Approve · Implement · Close</p>
      </div>

      {/* Modals */}
      {actionCR && (
        <ActionModal cr={actionCR} onClose={() => setActionCR(null)} onDone={fetchCRs} />
      )}
      {detailCR && (
        <DetailDrawer cr={detailCR} onClose={() => setDetailCR(null)} />
      )}

      {/* Stats row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 14, marginBottom: 24 }}>
        {STAT_CARDS.map(s => (
          <div key={s.label} style={{
            background: '#fff', borderRadius: 12, border: `1px solid ${s.highlight ? '#fecaca' : '#e2e8f0'}`,
            padding: '16px 20px', boxShadow: s.highlight ? '0 2px 8px rgba(220,38,38,0.1)' : '0 1px 3px rgba(0,0,0,0.05)',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                {s.label}
              </div>
              <span style={{ fontSize: 18 }}>{s.icon}</span>
            </div>
            <div style={{ fontSize: 30, fontWeight: 800, color: s.highlight ? '#dc2626' : s.color }}>
              {s.value}
            </div>
          </div>
        ))}
      </div>

      {/* ITIL stage guide */}
      <div style={{
        background: 'linear-gradient(135deg, #f0f9ff, #e0f2fe)', border: '1px solid #bae6fd',
        borderRadius: 10, padding: '12px 18px', marginBottom: 22,
        display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap',
      }}>
        <span style={{ fontSize: 11, fontWeight: 700, color: '#0369a1', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
          Workflow Stages:
        </span>
        {['1. CR Raised', '2. Validate', '3. Classify (Std/Normal/Emrg)', '4. Approve', '5. Implement', '6. Close'].map((s, i) => (
          <span key={i} style={{ fontSize: 12, color: '#0369a1', fontWeight: 500 }}>
            {s}{i < 5 ? ' →' : ''}
          </span>
        ))}
      </div>

      {/* Filter tabs */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20, flexWrap: 'wrap' }}>
        {FILTER_TABS.map(t => (
          <button key={t.key} onClick={() => setFilter(t.key)} style={{
            padding: '7px 16px', borderRadius: 7, fontSize: 13, fontWeight: 600,
            border: '1px solid', cursor: 'pointer',
            background: filter === t.key ? '#00338D' : '#fff',
            color: filter === t.key ? '#fff' : '#475569',
            borderColor: filter === t.key ? '#00338D' : '#e2e8f0',
          }}>
            {t.label}
            {t.key === 'needs_action' && stats.needs_action > 0 && (
              <span style={{
                marginLeft: 6, background: filter === t.key ? 'rgba(255,255,255,0.3)' : '#dc2626',
                color: '#fff', fontSize: 10, fontWeight: 700, padding: '1px 6px', borderRadius: 10,
              }}>
                {stats.needs_action}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* CR list */}
      {loading ? (
        <div className="page-loader" style={{ minHeight: 200 }}><div className="spinner" /></div>
      ) : crs.length === 0 ? (
        <div style={{
          background: '#fff', borderRadius: 12, border: '1px solid #e2e8f0',
          padding: '60px 20px', textAlign: 'center',
        }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>📋</div>
          <h4 style={{ color: '#64748b', margin: '0 0 6px', fontSize: 16 }}>No Change Requests</h4>
          <p style={{ color: '#94a3b8', fontSize: 13, margin: 0 }}>
            {filter ? 'No CRs match this filter.' : 'Change requests raised by experts will appear here.'}
          </p>
        </div>
      ) : (
        <div>
          {crs.map(cr => (
            <CRCard
              key={cr.id}
              cr={cr}
              onAction={setActionCR}
              onDetail={setDetailCR}
            />
          ))}
        </div>
      )}

      <div style={{ marginTop: 14, fontSize: 12, color: '#94a3b8', textAlign: 'right' }}>
        {crs.length} change request{crs.length !== 1 ? 's' : ''} · Auto-refreshes every 30s
      </div>
    </div>
  );
}
