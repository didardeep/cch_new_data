import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPut } from '../../api';
import { useTheme } from '../../ThemeContext';

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
  urgent:    { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa', label: 'Urgent'    },
  emergency: { bg: '#fef2f2', color: '#dc2626', border: '#fecaca', label: 'Emergency' },
};

const STATUS_META = {
  created:       { label: 'Created',        color: '#64748b', stage: 0 },
  classified:    { label: 'Classified',     color: '#7c3aed', stage: 0 },
  invalid:       { label: 'Invalid',        color: '#dc2626', stage: 0 },
  auto_rejected: { label: 'Auto-Rejected',  color: '#991b1b', stage: 0 },
  validated:     { label: 'Validated',      color: '#0369a1', stage: 1 },
  approved:      { label: 'Approved',       color: '#16a34a', stage: 2 },
  rejected:      { label: 'Rejected',       color: '#dc2626', stage: 2 },
  pending_cto:   { label: 'Pending CTO',    color: '#ea580c', stage: 3 },
  cto_approved:  { label: 'CTO Approved',   color: '#16a34a', stage: 3 },
  cto_rejected:  { label: 'CTO Rejected',   color: '#dc2626', stage: 3 },
  implementing:  { label: 'Implementing',   color: '#d97706', stage: 4 },
  implemented:   { label: 'Implemented',    color: '#15803d', stage: 4 },
  failed:        { label: 'Failed',         color: '#dc2626', stage: 4 },
  rolled_back:   { label: 'Rolled Back',    color: '#92400e', stage: 4 },
  closed:        { label: 'Closed',         color: '#475569', stage: 5 },
};

const STEPS_STANDARD = ['CR Raised', 'Classified', 'Validated', 'Approved', 'Implemented', 'Closed'];
const STEPS_URGENT   = ['CR Raised', 'Classified', 'Validated', 'Approved', 'CTO Review', 'Implemented', 'Closed'];

function _stageIndex(status, isUrgent) {
  const map = {
    created: 0, classified: 1, invalid: 2, auto_rejected: 2,
    validated: 2, approved: 3, rejected: 3,
  };
  if (isUrgent) {
    return { ...map, pending_cto: 4, cto_approved: 4, cto_rejected: 4, implementing: 5, implemented: 5, failed: 5, rolled_back: 5, closed: 6 }[status] ?? 0;
  }
  return { ...map, implementing: 4, implemented: 4, failed: 4, rolled_back: 4, closed: 5 }[status] ?? 0;
}

/* ── Pipeline Component ─────────────────────────────────────────────────────── */
function Pipeline({ status, changeType, cr }) {
  const isUrgent = changeType === 'urgent' || changeType === 'emergency';
  const steps = isUrgent ? STEPS_URGENT : STEPS_STANDARD;
  const stage = _stageIndex(status, isUrgent);
  const isFail  = ['invalid','auto_rejected','rejected','cto_rejected','failed'].includes(status);
  const isRollB = status === 'rolled_back';

  // Audit info per step
  const stepAudit = cr ? [
    { by: cr.raised_by_name, at: cr.created_at },
    { by: cr.classified_by_name || cr.raised_by_name, at: cr.classified_at },
    { by: cr.validated_by_name, at: cr.validated_at },
    { by: cr.approved_by_name, at: cr.approved_at },
    ...(isUrgent ? [{ by: cr.cto_approved_by_name, at: cr.cto_approved_at }] : []),
    { by: cr.implemented_by_name, at: cr.implemented_at },
    { by: null, at: cr.closed_at },
  ] : [];

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 0, margin: '6px 0 4px' }}>
      {steps.map((step, i) => {
        const done    = status === 'closed' ? true : i < stage;
        const current = status === 'closed' ? false : i === stage;
        const fail    = current && isFail;
        const rb      = current && isRollB;

        const dotColor = done ? '#16a34a' : fail ? '#dc2626' : rb ? '#92400e' : current ? '#00338D' : '#e2e8f0';
        const textColor = done ? '#16a34a' : fail ? '#dc2626' : rb ? '#92400e' : current ? '#00338D' : '#94a3b8';
        const audit = stepAudit[i] || {};
        const titleParts = [step];
        if (audit.by) titleParts.push(audit.by);
        if (audit.at) titleParts.push(new Date(audit.at).toLocaleString());

        return (
          <div key={step} style={{ display: 'flex', alignItems: 'center', flex: i < steps.length - 1 ? 1 : 'none' }}>
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', minWidth: 52 }} title={titleParts.join('\n')}>
              <div style={{
                width: 18, height: 18, borderRadius: '50%', background: dotColor,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 9, color: '#fff', fontWeight: 700,
                boxShadow: current ? `0 0 0 2px ${dotColor}30` : 'none',
              }}>
                {done ? '\u2713' : fail ? '\u2715' : rb ? '\u21A9' : i + 1}
              </div>
              <div style={{ fontSize: 7, fontWeight: 700, color: textColor, marginTop: 2, textAlign: 'center', textTransform: 'uppercase', letterSpacing: '0.03em', lineHeight: 1.1 }}>
                {step}
              </div>
              {(done || current) && audit.by && (
                <div style={{ fontSize: 6.5, color: '#94a3b8', marginTop: 1, textAlign: 'center', maxWidth: 56, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{audit.by}</div>
              )}
            </div>
            {i < steps.length - 1 && (
              <div style={{ flex: 1, height: 2, marginBottom: 14, background: done ? '#16a34a' : '#e2e8f0' }} />
            )}
          </div>
        );
      })}
    </div>
  );
}

/* ── Action Modal ───────────────────────────────────────────────────────────── */
function ActionModal({ cr, onClose, onDone, isDark }) {
  const [step,     setStep]     = useState('');     // validate|approve|close
  const [typeVal,  setTypeVal]  = useState('standard');
  const [remark,   setRemark]   = useState('');
  const [loading,  setLoading]  = useState(false);
  const [errMsg,   setErrMsg]   = useState('');
  const [successMsg, setSuccessMsg] = useState(null); // {message, ctoName, ctoEmail} for urgent/emergency

  const [modifyMode, setModifyMode] = useState(false);
  const [modifyParams, setModifyParams] = useState('');
  const [mgrRf, setMgrRf] = useState({});

  // Determine which action to show based on CR status
  useEffect(() => {
    if (!cr) return;
    const s = cr.status;
    if (s === 'created' || s === 'classified' || s === 'invalid') setStep('validate');
    else if (s === 'validated')                                     setStep('approve');
    else if (s === 'implemented' || s === 'rolled_back')            setStep('close');
    else setStep('');
    setModifyMode(false);
    setModifyParams('');
    setMgrRf({});
  }, [cr]);

  if (!cr) return null;

  const submit = async (decision) => {
    setLoading(true);
    setErrMsg('');
    try {
      let endpoint, body;
      if (step === 'validate') {
        endpoint = `/api/cr/${cr.id}/validate`;
        body     = { decision, remark };
      } else if (step === 'approve') {
        if (modifyMode) {
          // Serialize manager's RF modifications as readable text
          const rfLines = Object.entries(mgrRf).filter(([,v]) => v).map(([k,v]) => `${k}: ${v}`);
          const changesText = rfLines.length > 0 ? rfLines.join(', ') : modifyParams;
          endpoint = `/api/cr/${cr.id}/approve`;
          body     = { decision: 'modified', remark, proposed_changes: changesText || remark };
        } else {
          endpoint = `/api/cr/${cr.id}/approve`;
          body     = { decision, remark };
        }
      } else if (step === 'close') {
        endpoint = `/api/cr/${cr.id}/close`;
        body     = { notes: remark };
      }
      const res = await apiPut(endpoint, body);
      onDone();
      // For urgent/emergency approval, show CTO transfer info before closing
      const updatedCr = res?.cr;
      if (step === 'approve' && updatedCr?.status === 'pending_cto') {
        // Find CTO user info
        try {
          const ctoList = await apiGet('/api/cr/cto-info');
          setSuccessMsg({
            message: `CR ${cr.cr_number} approved and transferred to CTO for final review.`,
            ctoName: ctoList?.cto_name || 'CTO',
            ctoEmail: ctoList?.cto_email || '',
          });
        } catch {
          setSuccessMsg({
            message: `CR ${cr.cr_number} approved and transferred to CTO for final review.`,
            ctoName: 'CTO', ctoEmail: '',
          });
        }
        return; // don't close — show success screen
      }
      onClose();
    } catch (err) {
      setErrMsg(err?.message || 'Action failed. Please try again.');
    }
    setLoading(false);
  };

  const STEP_TITLES = {
    validate: 'Validate Change Request',
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

        {/* CTO Transfer Success Screen */}
        {successMsg && (
          <div style={{ textAlign: 'center', padding: '12px 0' }}>
            <div style={{ width: 48, height: 48, borderRadius: '50%', background: '#16a34a', margin: '0 auto 12px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <span style={{ color: '#fff', fontSize: 20, fontWeight: 700 }}>{'\u2713'}</span>
            </div>
            <div style={{ fontSize: 14, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a', marginBottom: 8 }}>{successMsg.message}</div>
            <div style={{ background: '#fff7ed', border: '1px solid #fed7aa', borderRadius: 8, padding: '12px 16px', marginBottom: 16, textAlign: 'left' }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: '#9a3412', textTransform: 'uppercase', marginBottom: 4 }}>Transferred to CTO</div>
              <div style={{ fontSize: 14, fontWeight: 700, color: '#0f172a' }}>{successMsg.ctoName}</div>
              {successMsg.ctoEmail && <div style={{ fontSize: 12, color: '#64748b' }}>{successMsg.ctoEmail}</div>}
              <div style={{ fontSize: 11, color: '#9a3412', marginTop: 4 }}>CTO approval required before agent can implement this change.</div>
            </div>
            <button onClick={() => { setSuccessMsg(null); onClose(); }} style={btnStyle('primary', isDark)}>Close</button>
          </div>
        )}

        {!successMsg && <>
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
              <button onClick={onClose} disabled={loading} style={btnStyle('ghost', isDark)}>Cancel</button>
              <button onClick={() => submit('invalid')} disabled={loading} style={btnStyle('danger', isDark)}>
                {loading ? '…' : `Reject (${cr.rejection_count + 1}/2)`}
              </button>
              <button onClick={() => submit('valid')} disabled={loading} style={btnStyle('success', isDark)}>
                {loading ? '…' : 'Mark Valid ✓'}
              </button>
            </div>
          </div>
        )}

        {step === 'approve' && (
          <div>
            {/* Show RF params — only those with proposed changes */}
            {cr.rf_params && Object.values(cr.rf_params).some(v => v.proposed != null) && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: isDark ? '#94a3b8' : '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
                  RF Parameter Changes
                </div>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead>
                    <tr style={{ borderBottom: `2px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>
                      <th style={{ padding: '6px 10px', textAlign: 'left', fontSize: 10, fontWeight: 700, color: isDark ? '#64748b' : '#94a3b8', textTransform: 'uppercase' }}>Parameter</th>
                      <th style={{ padding: '6px 10px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: isDark ? '#64748b' : '#94a3b8', textTransform: 'uppercase' }}>Current</th>
                      <th style={{ padding: '6px 10px', textAlign: 'center', fontSize: 10, color: '#94a3b8' }}></th>
                      <th style={{ padding: '6px 10px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: '#16a34a', textTransform: 'uppercase' }}>Proposed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(cr.rf_params).filter(([, v]) => v.proposed != null).map(([key, val]) => (
                      <tr key={key} style={{ borderBottom: `1px solid ${isDark ? '#1e293b' : '#f1f5f9'}` }}>
                        <td style={{ padding: '6px 10px', fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a', textTransform: 'capitalize' }}>{key.replace(/_/g, ' ')}</td>
                        <td style={{ padding: '6px 10px', textAlign: 'right', fontFamily: "'IBM Plex Mono',monospace", color: isDark ? '#94a3b8' : '#64748b' }}>{val.current != null ? Number(val.current).toFixed(1) : '-'}</td>
                        <td style={{ padding: '6px 10px', textAlign: 'center', color: '#94a3b8' }}>&rarr;</td>
                        <td style={{ padding: '6px 10px', textAlign: 'right', fontFamily: "'IBM Plex Mono',monospace", fontWeight: 700, color: '#16a34a' }}>{Number(val.proposed).toFixed(1)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* PDF download — authenticated fetch */}
            {cr.pdf_filename && (
              <div style={{ marginBottom: 12 }}>
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
                }} style={{ background: 'none', border: `1px solid ${isDark ? '#334155' : '#bfdbfe'}`, borderRadius: 6, padding: '6px 12px', cursor: 'pointer', fontSize: 12, color: isDark ? '#4da3ff' : '#00338D', fontWeight: 600 }}>
                  Download Report: {cr.pdf_filename}
                </button>
              </div>
            )}

            {/* CTO note for urgent/emergency */}
            {(cr.change_type === 'urgent' || cr.change_type === 'emergency') && (
              <div style={{ padding: '8px 12px', borderRadius: 6, marginBottom: 12, background: '#fff7ed', border: '1px solid #fed7aa', fontSize: 11, color: '#9a3412', fontWeight: 600 }}>
                Note: {cr.change_type === 'emergency' ? 'Emergency' : 'Urgent'} CR — after your approval, this will require CTO approval before implementation.
              </div>
            )}

            {!modifyMode ? (
              <>
                <label style={{ fontSize: 12, fontWeight: 600, color: isDark ? '#cbd5e1' : '#374151', display: 'block', marginBottom: 6 }}>
                  Approval Remark
                </label>
                <textarea value={remark} onChange={e => setRemark(e.target.value)}
                  rows={3} placeholder="Optional note for the expert..."
                  style={{ width: '100%', borderRadius: 8, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, padding: '8px 12px', fontSize: 13, resize: 'vertical', boxSizing: 'border-box', fontFamily: 'inherit', background: isDark ? '#0f172a' : '#fff', color: isDark ? '#e2e8f0' : '#0f172a' }}
                />
                {errMsg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{errMsg}</p>}
                <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
                  <button onClick={onClose} disabled={loading} style={btnStyle('ghost', isDark)}>Cancel</button>
                  <button onClick={() => submit('rejected')} disabled={loading} style={btnStyle('danger', isDark)}>
                    {loading ? '...' : 'Reject with Reason'}
                  </button>
                  <button onClick={() => setModifyMode(true)} disabled={loading} style={{ ...btnStyle('ghost', isDark), borderColor: '#7c3aed', color: '#7c3aed' }}>
                    Modify Parameters
                  </button>
                  <button onClick={() => submit('approved')} disabled={loading} style={btnStyle('success', isDark)}>
                    {loading ? '...' : 'Approve with Notes'}
                  </button>
                </div>
              </>
            ) : (
              <>
                <div style={{ fontSize: 12, fontWeight: 700, color: '#7c3aed', marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                  Propose Modified Parameters
                </div>
                {/* RF Param Grid: Current | Agent Proposed | Manager Proposed */}
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, marginBottom: 12 }}>
                  <thead>
                    <tr style={{ borderBottom: `2px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>
                      <th style={{ padding: '5px 8px', textAlign: 'left', fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase' }}>Parameter</th>
                      <th style={{ padding: '5px 8px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: '#94a3b8' }}>Current</th>
                      <th style={{ padding: '5px 8px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: '#16a34a' }}>Agent Proposed</th>
                      <th style={{ padding: '5px 8px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: '#7c3aed' }}>Your Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {['bandwidth', 'antenna_gain', 'eirp', 'antenna_height', 'etilt', 'crs_gain'].map(key => {
                      const val = (cr.rf_params || {})[key] || {};
                      if (val.current == null && val.proposed == null) return null;
                      return (
                        <tr key={key} style={{ borderBottom: `1px solid ${isDark ? '#1e293b' : '#f1f5f9'}` }}>
                          <td style={{ padding: '6px 8px', fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a', textTransform: 'capitalize' }}>{key.replace(/_/g, ' ')}</td>
                          <td style={{ padding: '6px 8px', textAlign: 'right', fontFamily: "'IBM Plex Mono',monospace", color: '#94a3b8' }}>{val.current != null ? val.current : '-'}</td>
                          <td style={{ padding: '6px 8px', textAlign: 'right', fontFamily: "'IBM Plex Mono',monospace", color: '#16a34a', fontWeight: 700 }}>{val.proposed != null ? val.proposed : '-'}</td>
                          <td style={{ padding: '4px 4px', textAlign: 'right' }}>
                            <input
                              type="number" step="any"
                              value={mgrRf[key] ?? ''}
                              onChange={e => setMgrRf(r => ({ ...r, [key]: e.target.value }))}
                              placeholder={val.proposed != null ? String(val.proposed) : ''}
                              style={{ width: 70, padding: '4px 6px', borderRadius: 4, border: '1.5px solid #7c3aed', fontSize: 12, fontFamily: "'IBM Plex Mono',monospace", fontWeight: 700, color: '#7c3aed', textAlign: 'right', background: isDark ? '#0f172a' : '#fff', boxSizing: 'border-box' }}
                            />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                <label style={{ fontSize: 12, fontWeight: 600, color: isDark ? '#cbd5e1' : '#374151', display: 'block', marginBottom: 6 }}>
                  Notes (explain your modifications)
                </label>
                <textarea value={remark} onChange={e => setRemark(e.target.value)}
                  rows={2} placeholder="Explain why you are proposing different values..."
                  style={{ width: '100%', borderRadius: 8, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, padding: '8px 12px', fontSize: 13, resize: 'vertical', boxSizing: 'border-box', fontFamily: 'inherit', background: isDark ? '#0f172a' : '#fff', color: isDark ? '#e2e8f0' : '#0f172a' }}
                />
                {errMsg && <p style={{ margin: '6px 0 0', fontSize: 12, color: '#dc2626' }}>{errMsg}</p>}
                <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 16 }}>
                  <button onClick={() => setModifyMode(false)} disabled={loading} style={btnStyle('ghost', isDark)}>Back</button>
                  <button onClick={() => submit('modified')} disabled={loading} style={{ ...btnStyle('primary', isDark), background: '#7c3aed', borderColor: '#7c3aed' }}>
                    {loading ? '...' : 'Submit Modified Approval'}
                  </button>
                </div>
              </>
            )}
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
              <button onClick={onClose} disabled={loading} style={btnStyle('ghost', isDark)}>Cancel</button>
              <button onClick={() => submit()} disabled={loading} style={btnStyle('primary', isDark)}>
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
        </>}
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
        <Pipeline status={cr.status} changeType={cr.change_type} cr={cr} />

        {/* Details */}
        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 14, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a', marginBottom: 12 }}>Change Details</div>
          <Row label="Title"         value={cr.title} />
          <Row label="Ticket"        value={cr.ticket_ref || (cr.network_issue_id ? `NI-${cr.network_issue_id}` : '')} mono />
          <Row label="Customer"      value={cr.ticket_user_name} />
          <Row label="Category"      value={cr.category ? `${cr.category}${cr.subcategory ? ' > ' + cr.subcategory : ''}` : (cr.ticket_category || '')} />
          <Row label="Domain"        value={cr.telecom_domain_primary ? `${cr.telecom_domain_primary}${cr.telecom_domain_secondary ? ' + ' + cr.telecom_domain_secondary : ''}` : (cr.ticket_domain || '')} />
          <Row label="Zone"          value={cr.zone} />
          <Row label="Location"      value={cr.location} />
          <Row label="Customer Type" value={cr.customer_type ? cr.customer_type.charAt(0).toUpperCase() + cr.customer_type.slice(1) : ''} />
          <Row label="Nearest Site"  value={cr.nearest_site_id} mono />
          <Row label="Raised By"     value={cr.raised_by_name} />
          <Row label="Manager"       value={cr.assigned_manager_name} />
          <Row label="Raised At"     value={cr.created_at ? new Date(cr.created_at).toLocaleString() : null} />
          {cr.cr_sla_deadline && <Row label="SLA Deadline" value={new Date(cr.cr_sla_deadline).toLocaleString()} />}
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

        {/* RF Parameters — only show params with proposed changes */}
        {cr.rf_params && Object.values(cr.rf_params).some(v => v.proposed != null) && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: isDark ? '#94a3b8' : '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
              RF Parameter Changes
            </div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${isDark ? '#334155' : '#e2e8f0'}` }}>
                  <th style={{ padding: '5px 8px', textAlign: 'left', fontSize: 10, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase' }}>Parameter</th>
                  <th style={{ padding: '5px 8px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: '#94a3b8' }}>Current</th>
                  <th style={{ padding: '5px 8px', textAlign: 'center', color: '#94a3b8' }}></th>
                  <th style={{ padding: '5px 8px', textAlign: 'right', fontSize: 10, fontWeight: 700, color: '#16a34a' }}>Proposed</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(cr.rf_params).filter(([, v]) => v.proposed != null).map(([key, val]) => (
                  <tr key={key} style={{ borderBottom: `1px solid ${isDark ? '#1e293b' : '#f1f5f9'}` }}>
                    <td style={{ padding: '5px 8px', fontWeight: 600, color: isDark ? '#e2e8f0' : '#0f172a', textTransform: 'capitalize' }}>{key.replace(/_/g, ' ')}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: "'IBM Plex Mono',monospace", color: isDark ? '#94a3b8' : '#64748b' }}>{val.current != null ? Number(val.current).toFixed(1) : '-'}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'center', color: '#94a3b8' }}>&rarr;</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: "'IBM Plex Mono',monospace", fontWeight: 700, color: '#16a34a' }}>{Number(val.proposed).toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Manager proposed changes */}
        {cr.manager_proposed_changes && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: '#7c3aed', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
              Manager Proposed Modifications
            </div>
            <div style={{ background: '#f5f3ff', border: '1px solid #c4b5fd', borderRadius: 8, padding: '10px 14px', fontSize: 13, color: '#0f172a', lineHeight: 1.6 }}>
              {cr.manager_proposed_changes}
            </div>
          </div>
        )}

        {/* CTO status */}
        {cr.cto_approval_required && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: '#ea580c', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
              CTO Approval
            </div>
            <div style={{ background: '#fff7ed', border: '1px solid #fed7aa', borderRadius: 8, padding: '10px 14px', fontSize: 13 }}>
              <div style={{ fontWeight: 600, color: cr.cto_status === 'cto_approved' ? '#16a34a' : cr.cto_status === 'cto_rejected' ? '#dc2626' : '#ea580c' }}>
                Status: {cr.cto_status === 'cto_approved' ? 'Approved by CTO' : cr.cto_status === 'cto_rejected' ? 'Rejected by CTO' : 'Pending CTO Review'}
              </div>
              {cr.cto_approved_by_name && <div style={{ fontSize: 12, color: '#64748b', marginTop: 4 }}>By: {cr.cto_approved_by_name} {cr.cto_approved_at ? `on ${new Date(cr.cto_approved_at).toLocaleString()}` : ''}</div>}
              {cr.cto_remark && <div style={{ fontSize: 12, color: '#0f172a', marginTop: 4 }}>{cr.cto_remark}</div>}
            </div>
          </div>
        )}

        {/* PDF download — authenticated */}
        {cr.pdf_filename && (
          <div style={{ marginBottom: 16 }}>
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
              display: 'inline-flex', alignItems: 'center', gap: 6, padding: '8px 14px', borderRadius: 8,
              background: isDark ? '#152238' : '#eff6ff', border: `1px solid ${isDark ? '#334155' : '#bfdbfe'}`,
              fontSize: 12, fontWeight: 600, color: isDark ? '#4da3ff' : '#00338D', cursor: 'pointer',
            }}>
              Download: {cr.pdf_filename}
            </button>
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
function btnStyle(type, isDark) {
  const base = { padding: '8px 18px', borderRadius: 7, fontSize: 13, fontWeight: 600, cursor: 'pointer', border: '1px solid' };
  if (type === 'primary') return { ...base, background: '#00338D', color: '#fff', borderColor: '#00338D' };
  if (type === 'success') return { ...base, background: '#16a34a', color: '#fff', borderColor: '#16a34a' };
  if (type === 'danger')  return { ...base, background: isDark ? '#1e293b' : '#fff', color: '#dc2626', borderColor: '#fca5a5' };
  return { ...base, background: isDark ? '#152238' : '#f8fafc', color: isDark ? '#94a3b8' : '#475569', borderColor: isDark ? '#334155' : '#e2e8f0' };
}

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

/* ── CR Card ────────────────────────────────────────────────────────────────── */
function CRCard({ cr, onAction, onDetail, onRemarks, isDark }) {
  const meta   = STATUS_META[cr.status] || STATUS_META.created;
  const p      = PRIORITY_STYLE[cr.ticket_priority] || PRIORITY_STYLE.medium;
  const isClosed = ['closed', 'rejected', 'auto_rejected', 'cto_rejected'].includes(cr.status);
  const ts     = isClosed ? { bg: '#f1f5f9', color: '#94a3b8', border: '#e2e8f0', label: cr.change_type ? cr.change_type.charAt(0).toUpperCase() + cr.change_type.slice(1) : '' } : TYPE_STYLE[cr.change_type];
  const needsAction = ['created','classified','invalid','validated','implemented','rolled_back'].includes(cr.status);

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
            {cr.ticket_ref && <>
              <span style={{ fontSize: 11, color: isDark ? '#64748b' : '#94a3b8' }}>·</span>
              <span style={{ fontFamily: 'monospace', fontSize: 11, color: isDark ? '#94a3b8' : '#64748b' }}>{cr.ticket_ref}</span>
            </>}
            {cr.ticket_id && !cr.network_issue_id && (
              <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: p.bg, color: p.color, border: `1px solid ${p.border}`, textTransform: 'uppercase' }}>
                {cr.ticket_priority}
              </span>
            )}
            {cr.network_issue_id && (
              <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: '#f5f3ff', color: '#7c3aed', border: '1px solid #c4b5fd', textTransform: 'uppercase' }}>
                AI Detected
              </span>
            )}
            {ts && (
              <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: ts.bg, color: ts.color, border: `1px solid ${ts.border}`, textTransform: 'uppercase' }}>
                {ts.label}
              </span>
            )}
            <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4, background: `${meta.color}15`, color: meta.color, border: `1px solid ${meta.color}30` }}>
              {meta.label}
            </span>
          </div>
          <SLATimer deadline={cr.cr_sla_deadline} slaHours={cr.cr_sla_hours} status={cr.status} />
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
        <Pipeline status={cr.status} changeType={cr.change_type} cr={cr} />

        {/* Actions */}
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', alignItems: 'center', marginTop: 4 }}>
          {(cr.description || cr.justification || cr.approval_remark || cr.manager_proposed_changes || cr.cto_remark || cr.validation_remark || cr.implementation_notes || cr.closure_notes) && (
            <button onClick={() => onRemarks(cr)} style={{
              background: 'none', border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 6,
              padding: '4px 12px', fontSize: 11, fontWeight: 600, cursor: 'pointer',
              color: isDark ? '#94a3b8' : '#64748b', marginRight: 'auto',
            }}>
              View Remarks
            </button>
          )}
          <button onClick={() => onDetail(cr)} style={btnStyle('ghost', isDark)}>
            View Details
          </button>
          {needsAction && (
            <button onClick={() => onAction(cr)} style={{
              ...btnStyle('primary', isDark),
              background: 'linear-gradient(135deg, #00338D, #0047c9)',
            }}>
              {cr.status === 'created' || cr.status === 'classified' || cr.status === 'invalid' ? 'Validate →'
               : cr.status === 'validated'                                                     ? 'Approve / Reject / Modify →'
               : cr.status === 'implemented' || cr.status === 'rolled_back'                    ? 'Close CR →'
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
  { key: '',              label: 'All' },
  { key: 'classified',    label: 'Classified' },
  { key: 'validated',     label: 'Validated' },
  { key: 'approved',      label: 'Approved' },
  { key: 'implemented',   label: 'Implemented' },
  { key: 'closed',        label: 'Closed' },
];

export default function ManagerChangeWorkflow() {
  const { isDark } = useTheme();
  const [crs,     setCrs]     = useState([]);
  const [stats,   setStats]   = useState({ total: 0, needs_action: 0, approved: 0, closed: 0 });
  const [loading, setLoading] = useState(true);
  const [filter,  setFilter]  = useState('');
  const [actionCR, setActionCR] = useState(null);
  const [detailCR, setDetailCR] = useState(null);
  const [remarksCR, setRemarksCR] = useState(null);
  const [section, setSection] = useState('customer'); // 'customer' or 'ai'

  const fetchCRs = useCallback(async () => {
    try {
      const d = await apiGet('/api/cr/manager-list');
      setCrs(d?.change_requests || d?.crs || []);
      if (d?.stats) setStats(d.stats);
    } catch { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { fetchCRs(); }, [fetchCRs]);

  useEffect(() => {
    const iv = setInterval(fetchCRs, 30000);
    return () => clearInterval(iv);
  }, [fetchCRs]);

  // Split CRs into customer complaint vs AI ticket
  const customerCRs = crs.filter(c => c.ticket_id && !c.network_issue_id);
  const aiCRs = crs.filter(c => c.network_issue_id);
  const activeCRs = section === 'customer' ? customerCRs : aiCRs;

  // Client-side filtering by tab
  const filteredCRs = filter ? activeCRs.filter(c => {
    if (filter === 'approved') return ['approved', 'pending_cto', 'cto_approved'].includes(c.status);
    if (filter === 'implemented') return ['implementing', 'implemented'].includes(c.status);
    return c.status === filter;
  }) : activeCRs;

  const sectionColor = section === 'customer' ? '#00338D' : '#7c3aed';

  // Compute stats for active section
  const activeStats = {
    total: activeCRs.length,
    needs_action: activeCRs.filter(c => ['created','classified','invalid','validated','implemented','rolled_back'].includes(c.status)).length,
    approved: activeCRs.filter(c => ['approved', 'pending_cto', 'cto_approved'].includes(c.status)).length,
    closed: activeCRs.filter(c => c.status === 'closed').length,
  };

  const _ic = (d, color = '#94a3b8') => (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      {Array.isArray(d) ? d.map((p, i) => <path key={i} d={p} />) : <path d={d} />}
    </svg>
  );
  const STAT_CARDS = [
    { label: 'Total CRs',    value: activeStats.total,        color: sectionColor, icon: _ic(["M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z", "M14 2v6h6", "M8 13h8", "M8 17h8", "M8 9h2"], sectionColor) },
    { label: 'Needs Action', value: activeStats.needs_action, color: '#dc2626', icon: _ic(["M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z", "M12 9v4", "M12 17h.01"], '#dc2626'), highlight: activeStats.needs_action > 0 },
    { label: 'Approved',     value: activeStats.approved,     color: '#16a34a', icon: _ic(["M22 11.08V12a10 10 0 1 1-5.93-9.14", "M22 4L12 14.01l-3-3"], '#16a34a') },
    { label: 'Closed',       value: activeStats.closed,       color: '#64748b', icon: _ic(["M9 11l3 3L22 4", "M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"], '#64748b') },
  ];

  return (
    <div>
      {/* Page header */}
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <h1>Change Workflow</h1>
          {activeStats.needs_action > 0 && (
            <span style={{ background: '#dc2626', color: '#fff', fontSize: 12, fontWeight: 700, padding: '3px 10px', borderRadius: 12 }}>
              {activeStats.needs_action} need action
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
        <DetailDrawer cr={detailCR} onClose={() => setDetailCR(null)} isDark={isDark} />
      )}

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

      {/* Stats row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 14, marginBottom: 24 }}>
        {STAT_CARDS.map(s => (
          <div key={s.label} style={{
            background: isDark ? '#1e293b' : '#fff', borderRadius: 12, border: `1px solid ${s.highlight ? '#fecaca' : (isDark ? '#334155' : '#e2e8f0')}`,
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
            {s}{i < 5 ? ' \u2192' : ''}
          </span>
        ))}
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

      {/* Filter tabs */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20, flexWrap: 'wrap' }}>
        {FILTER_TABS.map(t => {
          const cnt = !t.key ? activeCRs.length
            : t.key === 'approved' ? activeCRs.filter(c => ['approved', 'pending_cto', 'cto_approved'].includes(c.status)).length
            : t.key === 'implemented' ? activeCRs.filter(c => ['implementing', 'implemented'].includes(c.status)).length
            : activeCRs.filter(c => c.status === t.key).length;
          return (
            <button key={t.key} onClick={() => setFilter(t.key)} style={{
              padding: '7px 16px', borderRadius: 7, fontSize: 13, fontWeight: 600,
              border: '1px solid', cursor: 'pointer',
              background: filter === t.key ? (isDark ? '#4da3ff' : sectionColor) : (isDark ? '#1e293b' : '#fff'),
              color: filter === t.key ? '#fff' : (isDark ? '#94a3b8' : '#475569'),
              borderColor: filter === t.key ? (isDark ? '#4da3ff' : sectionColor) : (isDark ? '#334155' : '#e2e8f0'),
            }}>
              {t.label}{cnt > 0 ? ` (${cnt})` : ''}
            </button>
          );
        })}
      </div>

      {/* CR list */}
      {loading ? (
        <div className="page-loader" style={{ minHeight: 200 }}><div className="spinner" /></div>
      ) : filteredCRs.length === 0 ? (
        <div style={{
          background: '#fff', borderRadius: 12, border: '1px solid #e2e8f0',
          padding: '60px 20px', textAlign: 'center',
        }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>{section === 'customer' ? '\ud83d\udccb' : '\ud83e\udd16'}</div>
          <h4 style={{ color: isDark ? '#94a3b8' : '#64748b', margin: '0 0 6px', fontSize: 16 }}>No {section === 'customer' ? 'Customer Complaint' : 'AI Ticket'} CRs</h4>
          <p style={{ color: isDark ? '#64748b' : '#94a3b8', fontSize: 13, margin: 0 }}>
            {filter ? 'No CRs match this filter.' : `${section === 'customer' ? 'Customer complaint' : 'AI ticket'} change requests will appear here.`}
          </p>
        </div>
      ) : (
        <div>
          {filteredCRs.map(cr => (
            <CRCard
              key={cr.id}
              cr={cr}
              onAction={setActionCR}
              onDetail={setDetailCR}
              onRemarks={setRemarksCR}
              isDark={isDark}
            />
          ))}
        </div>
      )}

      <div style={{ marginTop: 14, fontSize: 12, color: isDark ? '#64748b' : '#94a3b8', textAlign: 'right' }}>
        {activeCRs.length} change request{activeCRs.length !== 1 ? 's' : ''} · Auto-refreshes every 30s
      </div>

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
