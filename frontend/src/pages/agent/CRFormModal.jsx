import { useState, useEffect } from 'react';
import { apiGet, apiPost } from '../../api';
import { useTheme } from '../../ThemeContext';

const DOMAINS = ['RAN', 'Core', 'Transport', 'Transmission', 'IMS'];

const RF_PARAMS = [
  { key: 'bandwidth', label: 'Bandwidth', unit: 'MHz', currentField: 'rf_bandwidth_current', proposedField: 'rf_bandwidth_proposed' },
  { key: 'antenna_gain', label: 'Antenna Gain', unit: 'dBi', currentField: 'rf_antenna_gain_current', proposedField: 'rf_antenna_gain_proposed' },
  { key: 'eirp', label: 'RF Power (EIRP)', unit: 'dBm', currentField: 'rf_eirp_current', proposedField: 'rf_eirp_proposed' },
  { key: 'antenna_height', label: 'Antenna Height (AGL)', unit: 'M', currentField: 'rf_antenna_height_current', proposedField: 'rf_antenna_height_proposed' },
  { key: 'etilt', label: 'E-tilt (Electrical Tilt)', unit: '\u00b0', currentField: 'rf_etilt_current', proposedField: 'rf_etilt_proposed' },
  { key: 'crs_gain', label: 'CRS Gain', unit: 'dB', currentField: 'rf_crs_gain_current', proposedField: 'rf_crs_gain_proposed' },
];

const CLASSIFICATIONS = [
  { key: 'standard', label: 'Standard', sla: '30%', desc: '30% of remaining SLA', color: '#16a34a' },
  { key: 'normal', label: 'Normal', sla: '20%', desc: '20% of remaining SLA', color: '#d97706' },
  { key: 'urgent', label: 'Urgent', sla: '10%', desc: '10% of remaining SLA', color: '#ea580c' },
  { key: 'emergency', label: 'Emergency', sla: '10%', desc: '10% of remaining SLA', color: '#dc2626' },
];

const CUSTOMER_TYPES = ['Platinum', 'Gold', 'Silver', 'Bronze'];

export default function CRFormModal({ open, onClose, ticket, networkIssue }) {
  const { isDark } = useTheme();
  const [categories, setCategories] = useState({});
  const [form, setForm] = useState({
    category: '', subcategory: '', zone: '', location: '', nearest_site_id: '',
    customer_type: '', justification: '', change_type: 'standard', title: 'Parameter Change Request',
  });
  const [rfCurrent, setRfCurrent] = useState({});
  const [rfProposed, setRfProposed] = useState({});
  const [selectedRfParams, setSelectedRfParams] = useState([]);
  const [rfDropdownOpen, setRfDropdownOpen] = useState(false);
  const [primaryDomain, setPrimaryDomain] = useState('');
  const [secondaryDomains, setSecondaryDomains] = useState([]);
  const [pdfFile, setPdfFile] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [success, setSuccess] = useState(null);
  const [crDraft, setCrDraft] = useState(null);  // holds CR after phase 1 submit
  const [phase, setPhase] = useState('form');     // form → classify → success
  const [classifyType, setClassifyType] = useState('standard');
  const [classifying, setClassifying] = useState(false);
  const [error, setError] = useState('');
  const [tempPcr] = useState(() => `PCR-${String(Math.floor(Math.random() * 9000) + 1000)}`);

  // Fetch categories
  useEffect(() => {
    if (!open) return;
    apiGet('/api/cr/categories').then(d => setCategories(d?.categories || {})).catch(() => {});
  }, [open]);

  // Pre-fill from ticket/networkIssue
  useEffect(() => {
    if (!open) return;

    // ── Network Issue (AI Ticket) ──
    if (networkIssue) {
      const updates = {
        zone: networkIssue.zone || '',
        location: networkIssue.location || '',
        nearest_site_id: networkIssue.site_id || '',
        category: 'Network Performance (AI Detected)',
      };
      const drop = networkIssue.avg_drop_rate || 0;
      const cssr = networkIssue.avg_cssr || 100;
      const tput = networkIssue.avg_tput || 999;
      if (drop > 2) updates.subcategory = 'Worst Cell / Drop Rate Degradation';
      else if (cssr < 95) updates.subcategory = 'CSSR Failure / Access Issues';
      else if (tput < 5) updates.subcategory = 'Throughput Degradation / Low SINR';
      else updates.subcategory = 'RF Parameter Optimization';
      setForm(f => ({ ...f, ...updates }));

      // Fetch RF params
      if (networkIssue.site_id) {
        apiGet(`/api/cr/site-rf-params?site_id=${networkIssue.site_id}`).then(d => {
          if (d?.params) setRfCurrent(d.params);
        }).catch(() => {});
      }
    }

    // ── Customer Complaint Ticket ──
    if (ticket) {
      const updates = {
        category: ticket.category || '',
        subcategory: ticket.subcategory || '',
      };
      setForm(f => ({ ...f, ...updates }));

      // Fetch nearest site for the customer ticket
      if (ticket.id) {
        apiGet(`/api/agent/tickets/${ticket.id}/nearest-sites`)
          .then(d => {
            console.log('[CRForm] nearest-sites response:', d);
            const sites = d?.nearest_sites || d?.sites || [];
            // Auto-fill customer_type from the ticket's customer tier
            const custType = d?.customer_type || ticket.user_type || '';
            if (sites.length > 0) {
              const site = sites[0];
              // Build site location from city/state
              const locParts = [site.city, site.state].filter(Boolean);
              const siteLocation = locParts.length > 0 ? locParts.join(', ') : `${site.latitude?.toFixed(4)}, ${site.longitude?.toFixed(4)}`;
              setForm(f => ({
                ...f,
                nearest_site_id: site.site_id || f.nearest_site_id,
                zone: site.zone || f.zone,
                location: siteLocation,
                customer_type: custType || f.customer_type,
              }));
              // Use RF params directly from nearest-sites response (already averaged)
              setRfCurrent({
                bandwidth: site.bandwidth_mhz,
                antenna_gain: site.antenna_gain_dbi,
                eirp: site.rf_power_eirp_dbm,
                antenna_height: site.antenna_height_agl_m,
                etilt: site.e_tilt_degree,
                crs_gain: site.crs_gain,
              });
            } else if (custType) {
              setForm(f => ({ ...f, customer_type: custType || f.customer_type }));
            }
          })
          .catch(err => {
            console.warn('[CRForm] nearest-sites failed:', err?.message);
            // Fallback: still auto-fill customer_type and any known location info
            setForm(f => ({
              ...f,
              customer_type: ticket.user_type || f.customer_type,
              zone: ticket.zone || f.zone,
              location: ticket.location || f.location,
            }));
          });
      }
    }
  }, [open, ticket, networkIssue]);

  // Reset on close
  useEffect(() => {
    if (!open) {
      setForm({ category: '', subcategory: '', zone: '', location: '', nearest_site_id: '', customer_type: '', justification: '', change_type: 'standard', title: 'Parameter Change Request' });
      setRfCurrent({}); setRfProposed({}); setSelectedRfParams([]); setRfDropdownOpen(false);
      setPdfFile(null); setSuccess(null); setError('');
      setPrimaryDomain(''); setSecondaryDomains([]);
      setCrDraft(null); setPhase('form'); setClassifyType('standard'); setClassifying(false);
    }
  }, [open]);

  // Auto-map domains on category change
  useEffect(() => {
    const catInfo = categories[form.category];
    if (catInfo) {
      setPrimaryDomain(catInfo.primary_domain || '');
      setSecondaryDomains(catInfo.secondary_domains || []);
    } else {
      setPrimaryDomain(''); setSecondaryDomains([]);
    }
  }, [form.category, categories]);

  const updateForm = (key, val) => setForm(f => ({ ...f, [key]: val }));
  const subcategories = categories[form.category]?.subcategories || [];

  // Phase 1: Submit CR form (without classification)
  const handleSubmit = async () => {
    console.log('[CRForm] handleSubmit called, form:', form);
    if (!form.category) { setError('Category is required. Please select a category.'); return; }
    if (!form.justification.trim()) { setError('Justification is required. Please fill in the justification field.'); return; }
    setSubmitting(true); setError('');

    try {
      const body = {
        title: form.title || 'Parameter Change Request',
        description: form.justification,
        justification: form.justification,
        category: form.category,
        subcategory: form.subcategory || '',
        zone: form.zone || '',
        location: form.location || '',
        nearest_site_id: form.nearest_site_id || '',
        customer_type: form.customer_type || '',
        change_type: 'standard',
        ticket_id: ticket?.id || null,
        network_issue_id: networkIssue?.id || null,
        telecom_domain_primary: primaryDomain || '',
        telecom_domain_secondary: secondaryDomains.join(','),
      };
      RF_PARAMS.forEach(p => {
        body[p.currentField] = rfCurrent[p.key] != null ? Number(rfCurrent[p.key]) : null;
        body[p.proposedField] = rfProposed[p.key] ? parseFloat(rfProposed[p.key]) : null;
      });

      console.log('[CRForm] Posting to /api/cr/create:', body);
      const res = await apiPost('/api/cr/create', body);
      console.log('[CRForm] Response:', res);

      // Upload PDF if selected
      if (pdfFile && res?.cr?.id) {
        const fd = new FormData();
        fd.append('file', pdfFile);
        try {
          await fetch(`/api/cr/${res.cr.id}/upload-pdf`, {
            method: 'POST', body: fd,
            headers: { 'Authorization': `Bearer ${localStorage.getItem('token')}` },
          });
        } catch (_) { /* PDF upload optional */ }
      }

      setCrDraft(res?.cr || {});
      setPhase('classify');
    } catch (err) {
      console.error('[CRForm] Error:', err);
      setError(err?.message || 'Failed to create CR.');
    }
    setSubmitting(false);
  };

  // Phase 2: Classify the CR
  const handleClassify = async () => {
    if (!crDraft?.id) return;
    setClassifying(true); setError('');
    try {
      const res = await apiPost(`/api/cr/${crDraft.id}/classify`, { change_type: classifyType });
      setSuccess(res?.cr || crDraft);
      setPhase('success');
    } catch (err) {
      setError(err?.message || 'Classification failed.');
    }
    setClassifying(false);
  };

  if (!open) return null;

  const T = {
    bg: isDark ? '#0f172a' : '#f8fafc',
    surface: isDark ? '#1e293b' : '#fff',
    border: isDark ? '#334155' : '#e2e8f0',
    text: isDark ? '#e2e8f0' : '#0f172a',
    muted: isDark ? '#64748b' : '#94a3b8',
    textSub: isDark ? '#94a3b8' : '#64748b',
    input: isDark ? '#0f172a' : '#fff',
    accent: '#00838f',
  };

  // Phase 2: Classification screen
  if (phase === 'classify' && crDraft) {
    return (
      <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.7)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000, padding: 16 }}>
        <div onClick={e => e.stopPropagation()} style={{ width: '100%', maxWidth: 500, background: T.surface, borderRadius: 16, padding: 28 }}>
          {/* Header with CR number */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20 }}>
            <div>
              <h3 style={{ margin: '0 0 4px', fontSize: 18, fontWeight: 800, color: T.text }}>Classify Change Request</h3>
              <span style={{ fontSize: 12, color: T.muted }}>Select the priority classification for this CR</span>
            </div>
            <span style={{ fontSize: 13, fontWeight: 800, fontFamily: "'IBM Plex Mono',monospace", color: '#fff', background: '#00338D', padding: '4px 12px', borderRadius: 6 }}>
              {crDraft.cr_number}
            </span>
          </div>

          {/* Classification buttons */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10, marginBottom: 20 }}>
            {CLASSIFICATIONS.filter(c => c.key !== 'emergency').map(c => {
              const sel = classifyType === c.key;
              return (
                <button key={c.key} onClick={() => setClassifyType(c.key)} style={{
                  padding: '14px 8px', borderRadius: 10, cursor: 'pointer', textAlign: 'center',
                  background: sel ? `${c.color}12` : T.bg,
                  border: `2px solid ${sel ? c.color : T.border}`,
                  color: sel ? c.color : T.muted, fontWeight: sel ? 700 : 500, fontSize: 14,
                  transition: 'all 0.15s',
                }}>
                  {c.label}
                  <div style={{ fontSize: 10, marginTop: 4, fontWeight: 400, opacity: 0.8 }}>{c.desc}</div>
                </button>
              );
            })}
          </div>
          {/* Emergency as separate full-width option */}
          {(() => {
            const c = CLASSIFICATIONS.find(x => x.key === 'emergency');
            const sel = classifyType === 'emergency';
            return (
              <button onClick={() => setClassifyType('emergency')} style={{
                width: '100%', padding: '12px', borderRadius: 10, cursor: 'pointer', textAlign: 'center', marginBottom: 20,
                background: sel ? '#dc262612' : T.bg,
                border: `2px solid ${sel ? '#dc2626' : T.border}`,
                color: sel ? '#dc2626' : T.muted, fontWeight: sel ? 700 : 500, fontSize: 14,
              }}>
                Emergency <span style={{ fontSize: 10, fontWeight: 400 }}> — {c.desc} (requires CTO approval)</span>
              </button>
            );
          })()}

          {/* Assigned manager info */}
          {crDraft.assigned_manager_name && (
            <div style={{ background: isDark ? '#152238' : '#eff6ff', border: `1px solid ${isDark ? '#334155' : '#bfdbfe'}`, borderRadius: 8, padding: '10px 14px', marginBottom: 16, fontSize: 12 }}>
              <div style={{ fontWeight: 600, color: T.text }}>Your CR is being assigned to:</div>
              <div style={{ color: isDark ? '#4da3ff' : '#00338D', fontWeight: 700, marginTop: 2 }}>
                {crDraft.assigned_manager_name} {crDraft.assigned_manager_email && `(${crDraft.assigned_manager_email})`}
              </div>
              <div style={{ color: T.muted, marginTop: 2 }}>for validation and approval</div>
            </div>
          )}

          {error && <div style={{ padding: '8px 12px', borderRadius: 6, background: '#fef2f2', border: '1px solid #fecaca', color: '#dc2626', fontSize: 12, marginBottom: 12 }}>{error}</div>}

          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button onClick={handleClassify} disabled={classifying} style={{
              padding: '10px 28px', borderRadius: 8, fontSize: 14, fontWeight: 700, cursor: 'pointer',
              background: '#00338D', color: '#fff', border: 'none', opacity: classifying ? 0.6 : 1,
            }}>
              {classifying ? 'Classifying...' : 'Raise CR'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Phase 3: Success screen
  if (phase === 'success' && success) {
    return (
      <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.7)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000, padding: 16 }} onClick={onClose}>
        <div onClick={e => e.stopPropagation()} style={{ width: '100%', maxWidth: 440, background: T.surface, borderRadius: 16, padding: 32, textAlign: 'center' }}>
          <div style={{ width: 56, height: 56, borderRadius: '50%', background: '#16a34a', margin: '0 auto 16px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12" /></svg>
          </div>
          <h3 style={{ margin: '0 0 8px', fontSize: 18, fontWeight: 700, color: T.text }}>CR Raised Successfully</h3>
          <div style={{ fontSize: 22, fontWeight: 800, fontFamily: "'IBM Plex Mono',monospace", color: isDark ? '#4da3ff' : '#00338D', marginBottom: 12 }}>{success.cr_number}</div>
          {success.assigned_manager_name && (
            <div style={{ fontSize: 13, color: T.textSub, marginBottom: 4 }}>
              Assigned to: <strong style={{ color: T.text }}>{success.assigned_manager_name}</strong>
              {success.assigned_manager_email && <span> ({success.assigned_manager_email})</span>}
            </div>
          )}
          <div style={{ fontSize: 12, color: T.muted, marginBottom: 4 }}>
            Classification: <strong>{success.change_type?.charAt(0).toUpperCase() + success.change_type?.slice(1)}</strong>
          </div>
          {success.cr_sla_deadline && (
            <div style={{ fontSize: 12, color: T.muted, marginBottom: 16 }}>
              SLA Deadline: {new Date(success.cr_sla_deadline).toLocaleString()}
            </div>
          )}
          <button onClick={onClose} style={{ padding: '10px 28px', borderRadius: 8, fontSize: 14, fontWeight: 600, cursor: 'pointer', background: '#00338D', color: '#fff', border: 'none' }}>Close</button>
        </div>
      </div>
    );
  }

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.7)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000, padding: 16 }} onClick={onClose}>
      <div onClick={e => e.stopPropagation()} style={{
        width: '100%', maxWidth: 700, maxHeight: '92vh', overflowY: 'auto',
        background: T.bg, borderRadius: 16, boxShadow: '0 24px 48px rgba(0,0,0,0.4)',
      }}>
        {/* Header */}
        <div style={{ padding: '20px 24px 16px', borderBottom: `1px solid ${T.border}`, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', position: 'sticky', top: 0, background: T.bg, zIndex: 10 }}>
          <div>
            <h2 style={{ margin: 0, fontSize: 18, fontWeight: 800, color: T.text }}>Parameter Change Request</h2>
            <span style={{ fontSize: 10, fontWeight: 600, color: T.muted, textTransform: 'uppercase', letterSpacing: '0.1em' }}>Network Configuration Modification</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 12, fontWeight: 800, fontFamily: "'IBM Plex Mono',monospace", color: '#fff', background: '#00338D', padding: '4px 12px', borderRadius: 6 }}>
              {tempPcr}
            </span>
            <button onClick={onClose} style={{ border: 'none', background: isDark ? '#334155' : '#f1f5f9', borderRadius: 8, width: 32, height: 32, cursor: 'pointer', fontSize: 16, color: T.muted, flexShrink: 0 }}>X</button>
          </div>
        </div>

        <div style={{ padding: '16px 24px 24px' }}>

          {/* Error at top */}
          {error && <div style={{ padding: '10px 14px', borderRadius: 8, background: '#fef2f2', border: '1px solid #fecaca', color: '#dc2626', fontSize: 13, fontWeight: 600, marginBottom: 14 }}>{error}</div>}

          {/* ── COMPLAINT CLASSIFICATION ─── */}
          <SectionLabel text="Complaint Classification" color={T.accent} />
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
            <Field label="Category" required>
              <select value={form.category} onChange={e => { updateForm('category', e.target.value); updateForm('subcategory', ''); }} style={selectStyle(T)}>
                <option value="">Select Category</option>
                {Object.keys(categories).map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </Field>
            <Field label="Subcategory">
              <select value={form.subcategory} onChange={e => updateForm('subcategory', e.target.value)} style={selectStyle(T)} disabled={!form.category}>
                <option value="">Select Subcategory</option>
                {subcategories.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </Field>
          </div>

          {/* ── TELECOM DOMAIN ─── */}
          {primaryDomain && (
            <>
              <SectionLabel text="Telecom Domain" color={T.accent} />
              <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
                {DOMAINS.map(d => {
                  const isPrimary = d === primaryDomain;
                  const isSecondary = secondaryDomains.includes(d);
                  if (!isPrimary && !isSecondary) return null;
                  return (
                    <span key={d} style={{
                      padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 700,
                      background: isPrimary ? '#00338D' : (isDark ? '#152238' : '#f1f5f9'),
                      color: isPrimary ? '#fff' : T.muted,
                      border: `1.5px solid ${isPrimary ? '#00338D' : T.border}`,
                      opacity: isSecondary && !isPrimary ? 0.6 : 1,
                    }}>
                      {d} {isPrimary ? '(Primary)' : ''}
                    </span>
                  );
                })}
              </div>
            </>
          )}

          {/* ── NETWORK DOMAIN & LOCATION ─── */}
          <SectionLabel text="Network Domain & Location" color={T.accent} />
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
            <Field label="Zone">
              <input value={form.zone} onChange={e => updateForm('zone', e.target.value)} style={inputStyle(T)} placeholder="e.g., North, Edge, Urban" />
            </Field>
            <Field label="Site Location">
              <input value={form.location} onChange={e => updateForm('location', e.target.value)} style={inputStyle(T)} placeholder="e.g., Sector 14, Gurugram" />
            </Field>
            {!networkIssue && (
              <Field label="Customer Type">
                <select value={form.customer_type} onChange={e => updateForm('customer_type', e.target.value)} style={selectStyle(T)}>
                  <option value="">Select Type</option>
                  {CUSTOMER_TYPES.map(t => <option key={t} value={t.toLowerCase()}>{t}</option>)}
                </select>
              </Field>
            )}
            <Field label="Site ID">
              <input value={form.nearest_site_id} onChange={e => updateForm('nearest_site_id', e.target.value)} style={inputStyle(T)} placeholder="e.g., GUR_LTE_0900" />
            </Field>
          </div>

          {/* ── RF / NETWORK PARAMETERS ─── */}
          <SectionLabel text="RF / Network Parameters" color={T.accent} />
          {/* Multi-select dropdown for RF parameters */}
          <div style={{ position: 'relative', marginBottom: 12 }}>
            <div onClick={() => setRfDropdownOpen(!rfDropdownOpen)} style={{
              ...inputStyle(T), cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              minHeight: 38, flexWrap: 'wrap', gap: 4, paddingRight: 32,
            }}>
              {selectedRfParams.length === 0 ? (
                <span style={{ color: T.muted, fontSize: 12 }}>Select RF parameters to modify...</span>
              ) : (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                  {selectedRfParams.map(key => {
                    const p = RF_PARAMS.find(r => r.key === key);
                    return (
                      <span key={key} style={{
                        display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px',
                        borderRadius: 4, fontSize: 11, fontWeight: 600,
                        background: isDark ? '#0f3b3f' : '#e0f7fa', color: T.accent,
                      }}>
                        {p?.label}
                        <span onClick={e => { e.stopPropagation(); setSelectedRfParams(s => s.filter(k => k !== key)); }}
                          style={{ cursor: 'pointer', fontSize: 13, fontWeight: 700, lineHeight: 1, color: T.muted }}>×</span>
                      </span>
                    );
                  })}
                </div>
              )}
              <span style={{ position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)', fontSize: 10, color: T.muted }}>
                {rfDropdownOpen ? '▲' : '▼'}
              </span>
            </div>
            {rfDropdownOpen && (
              <div style={{
                position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 20,
                background: T.surface, border: `1px solid ${T.border}`, borderRadius: 8,
                boxShadow: '0 8px 24px rgba(0,0,0,0.12)', marginTop: 4, maxHeight: 220, overflowY: 'auto',
              }}>
                {RF_PARAMS.map(p => {
                  const checked = selectedRfParams.includes(p.key);
                  return (
                    <div key={p.key} onClick={() => {
                      setSelectedRfParams(s => checked ? s.filter(k => k !== p.key) : [...s, p.key]);
                      // Auto-fetch RF params when first parameter is selected and site_id exists
                      if (!checked && Object.keys(rfCurrent).length === 0 && form.nearest_site_id) {
                        apiGet(`/api/cr/site-rf-params?site_id=${form.nearest_site_id}`).then(d => {
                          if (d?.params) setRfCurrent(d.params);
                        }).catch(() => {});
                      }
                    }} style={{
                      display: 'flex', alignItems: 'center', gap: 10, padding: '9px 14px',
                      cursor: 'pointer', fontSize: 12, color: T.text,
                      background: checked ? (isDark ? '#0f3b3f' : '#e0f7fa') : 'transparent',
                      borderBottom: `1px solid ${T.border}`,
                    }}>
                      <span style={{
                        width: 16, height: 16, borderRadius: 3, border: `2px solid ${checked ? T.accent : T.border}`,
                        background: checked ? T.accent : 'transparent', display: 'flex', alignItems: 'center', justifyContent: 'center',
                        flexShrink: 0,
                      }}>
                        {checked && <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3"><polyline points="20 6 9 17 4 12"/></svg>}
                      </span>
                      <span style={{ fontWeight: 600 }}>{p.label}</span>
                      <span style={{ fontSize: 9, fontWeight: 700, color: T.accent, background: isDark ? '#0f3b3f' : '#e0f7fa', padding: '1px 5px', borderRadius: 3, marginLeft: 'auto' }}>{p.unit}</span>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Selected RF parameter cards with current + proposed */}
          {selectedRfParams.length > 0 && (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 16 }}>
              {selectedRfParams.map(key => {
                const p = RF_PARAMS.find(r => r.key === key);
                if (!p) return null;
                return (
                  <div key={p.key} style={{ background: T.surface, border: `1px solid ${T.border}`, borderRadius: 10, padding: '10px 14px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                      <span style={{ fontSize: 12, fontWeight: 700, color: T.text }}>{p.label}</span>
                      <span style={{ fontSize: 9, fontWeight: 700, color: T.accent, background: isDark ? '#0f3b3f' : '#e0f7fa', padding: '2px 6px', borderRadius: 4 }}>{p.unit}</span>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                      <div>
                        <div style={{ fontSize: 9, fontWeight: 700, color: T.muted, textTransform: 'uppercase', marginBottom: 2 }}>Current</div>
                        <input value={rfCurrent[p.key] ?? ''} readOnly style={{ ...inputStyle(T), background: isDark ? '#0f172a' : '#f1f5f9', cursor: 'default', fontSize: 13, fontWeight: 700 }} />
                      </div>
                      <div>
                        <div style={{ fontSize: 9, fontWeight: 700, color: '#16a34a', textTransform: 'uppercase', marginBottom: 2 }}>Proposed</div>
                        <input value={rfProposed[p.key] ?? ''} onChange={e => setRfProposed(r => ({ ...r, [p.key]: e.target.value }))}
                          style={{ ...inputStyle(T), borderColor: '#16a34a', fontSize: 13, fontWeight: 700, color: '#16a34a' }} placeholder="New value" type="number" step="any" />
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {/* ── PDF Upload ─── */}
          <div style={{ marginBottom: 16 }}>
            <label style={{ fontSize: 11, fontWeight: 700, color: T.textSub, display: 'block', marginBottom: 4 }}>Upload Report (PDF)</label>
            <input type="file" accept=".pdf" onChange={e => setPdfFile(e.target.files?.[0] || null)}
              style={{ fontSize: 12, color: T.text }} />
            {pdfFile && <span style={{ fontSize: 11, color: '#16a34a', marginLeft: 8 }}>{pdfFile.name}</span>}
          </div>

          {/* ── JUSTIFICATION ─── */}
          <Field label="Justification / Remarks" required>
            <textarea value={form.justification} onChange={e => updateForm('justification', e.target.value)} rows={3}
              placeholder="Explain the reason for this parameter change..."
              style={{ ...inputStyle(T), resize: 'vertical', fontFamily: 'inherit' }} />
          </Field>

          {/* Error */}
          {error && <div style={{ padding: '8px 12px', borderRadius: 6, background: '#fef2f2', border: '1px solid #fecaca', color: '#dc2626', fontSize: 12, marginBottom: 12 }}>{error}</div>}

          {/* ── Footer ─── */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', paddingTop: 16, borderTop: `1px solid ${T.border}` }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <div style={{ width: 8, height: 8, borderRadius: '50%', background: form.category && form.justification ? '#16a34a' : '#f59e0b' }} />
              <span style={{ fontSize: 11, fontWeight: 600, color: T.muted, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                {form.category && form.justification ? 'Ready to Submit' : 'Fill Required Fields'}
              </span>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button type="button" onClick={(e) => { e.preventDefault(); e.stopPropagation(); onClose(); }} disabled={submitting} style={{ padding: '10px 20px', borderRadius: 8, fontSize: 13, fontWeight: 600, cursor: 'pointer', background: T.surface, color: T.textSub, border: `1px solid ${T.border}` }}>Cancel</button>
              <button type="button" onClick={(e) => { e.preventDefault(); e.stopPropagation(); handleSubmit(); }} disabled={submitting} style={{
                padding: '10px 28px', borderRadius: 8, fontSize: 14, fontWeight: 700, cursor: 'pointer',
                background: 'linear-gradient(135deg, #00838f, #16a34a)', color: '#fff', border: 'none',
                opacity: submitting ? 0.6 : 1,
              }}>
                {submitting ? 'Submitting...' : 'Submit CR \u2192'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Shared Styles ─────────────────────────────────────────────────────────── */
function SectionLabel({ text, color }) {
  return (
    <div style={{ fontSize: 11, fontWeight: 700, color: color || '#00838f', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{ width: 6, height: 6, borderRadius: '50%', background: color || '#00838f' }} />
      {text}
    </div>
  );
}

function Field({ label, required, children }) {
  return (
    <div style={{ marginBottom: 8 }}>
      <label style={{ fontSize: 11, fontWeight: 700, color: '#64748b', display: 'block', marginBottom: 3, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
        {label} {required && <span style={{ color: '#dc2626' }}>*</span>}
      </label>
      {children}
    </div>
  );
}

function inputStyle(T) {
  return {
    width: '100%', padding: '8px 12px', borderRadius: 6, fontSize: 13,
    border: `1px solid ${T.border}`, background: T.input, color: T.text,
    boxSizing: 'border-box', outline: 'none',
  };
}

function selectStyle(T) {
  return {
    ...inputStyle(T), cursor: 'pointer', appearance: 'auto',
  };
}
