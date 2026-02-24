import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiGet, apiPost, apiPut } from '../../api';

/* ── SVG Icons ───────────────────────────────────────────────────────────────── */
const IC = {
  chat:    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>,
  cpu:     <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg>,
  user360: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>,
  check:   <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>,
  clock:   <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>,
  phone:   <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 13a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3.6 2h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z"/></svg>,
  refresh: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>,
  x:       <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  chart:   <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>,
};

/* ── Priority config ─────────────────────────────────────────────────────────── */
const P_CFG = {
  critical: { bar: '#dc2626', badgeClass: 'badge-critical', label: 'Critical' },
  high:     { bar: '#f97316', badgeClass: 'badge-high',     label: 'High'     },
  medium:   { bar: '#f59e0b', badgeClass: 'badge-medium',   label: 'Medium'   },
  low:      { bar: '#10b981', badgeClass: 'badge-low',      label: 'Low'      },
};

/* ── Live SLA Timer ──────────────────────────────────────────────────────────── */
function SlaTimer({ deadline, slaHours, status }) {
  const [remaining, setRemaining] = useState(null);
  const [pct, setPct] = useState(0);

  useEffect(() => {
    if (!deadline || status === 'resolved') { setRemaining(null); return; }
    const total = slaHours ? slaHours * 3600 * 1000 : null;
    const tick = () => {
      const left = new Date(deadline).getTime() - Date.now();
      setRemaining(left);
      if (total) setPct(Math.min(((total - left) / total) * 100, 100));
    };
    tick();
    const iv = setInterval(tick, 1000);
    return () => clearInterval(iv);
  }, [deadline, slaHours, status]);

  if (status === 'resolved') return <span className="badge badge-resolved">Resolved</span>;
  if (remaining === null) return <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>No SLA</span>;

  const breached  = remaining <= 0;
  const critical  = !breached && pct >= 87.5;
  const warning   = !breached && pct >= 62.5;
  const color     = breached ? 'var(--danger)' : critical ? '#ef4444' : warning ? 'var(--warning)' : 'var(--success)';

  const abs = Math.abs(remaining);
  const h   = String(Math.floor(abs / 3600000)).padStart(2, '0');
  const m   = String(Math.floor((abs % 3600000) / 60000)).padStart(2, '0');
  const s   = String(Math.floor((abs % 60000) / 1000)).padStart(2, '0');

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 4 }}>
        <span style={{ color, flexShrink: 0 }}>{IC.clock}</span>
        <span style={{ fontSize: 13, fontWeight: 700, color, fontFamily: 'monospace', letterSpacing: 1 }}>
          {breached ? '+' : ''}{h}:{m}:{s}
        </span>
      </div>
      <div style={{ background: 'var(--border)', borderRadius: 4, height: 4, overflow: 'hidden', width: 120 }}>
        <div style={{ height: '100%', width: `${Math.min(pct, 100)}%`, background: color, borderRadius: 4, transition: 'width 1s linear' }} />
      </div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
        {breached ? 'SLA Breached' : `${Math.round(pct)}% elapsed`}
      </div>
    </div>
  );
}

/* ── Modal wrapper ───────────────────────────────────────────────────────────── */
function Modal({ title, onClose, width = 560, children }) {
  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
      <div style={{ background: '#fff', borderRadius: 'var(--radius-lg)', width, maxWidth: '92vw', maxHeight: '88vh', overflowY: 'auto', boxShadow: 'var(--shadow-lg)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '18px 24px', borderBottom: '1px solid var(--border)' }}>
          <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: 'var(--text)' }}>{title}</h3>
          <button onClick={onClose} style={{ border: 'none', background: 'var(--bg)', borderRadius: 6, width: 28, height: 28, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-secondary)' }}>{IC.x}</button>
        </div>
        <div style={{ padding: 24 }}>{children}</div>
      </div>
    </div>
  );
}

/* ── Customer 360 Modal ──────────────────────────────────────────────────────── */
function Customer360Modal({ customerId, onClose }) {
  const [data, setData]   = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiGet(`/api/agent/customer360/${customerId}`).then(d => { setData(d); setLoading(false); }).catch(() => setLoading(false));
  }, [customerId]);

  return (
    <Modal title="Customer 360°" onClose={onClose} width={620}>
      {loading ? (
        <div className="page-loader" style={{ height: 180 }}><div className="spinner" /></div>
      ) : !data ? (
        <div className="form-error">Failed to load customer data.</div>
      ) : (
        <>
          {/* Customer info */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px 20px', padding: '14px 16px', background: 'var(--bg)', borderRadius: 'var(--radius-sm)', marginBottom: 16 }}>
            {[['Name', data.customer?.name], ['Email', data.customer?.email], ['Phone', data.customer?.phone || '—'], ['Member Since', data.plan_info?.account_since]].map(([k, v]) => (
              <div key={k}>
                <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 2 }}>{k}</div>
                <div style={{ fontSize: 13, color: 'var(--text)', fontWeight: 500 }}>{v || '—'}</div>
              </div>
            ))}
          </div>

          {/* Scores row */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 12, marginBottom: 16 }}>
            {[
              { label: 'Loyalty Score',       value: `${data.loyalty_score}/100`, color: 'var(--primary)' },
              { label: 'Avg Rating',          value: `${data.avg_rating} / 5`,    color: 'var(--warning)' },
              { label: 'Total Interactions',  value: data.plan_info?.total_interactions, color: 'var(--success)' },
            ].map(({ label, value, color }) => (
              <div key={label} style={{ textAlign: 'center', padding: '14px 10px', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)' }}>
                <div style={{ fontSize: 20, fontWeight: 700, color }}>{value}</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 3 }}>{label}</div>
              </div>
            ))}
          </div>

          {/* Plan info */}
          <div style={{ padding: '10px 14px', background: 'var(--primary-glow)', border: '1px solid rgba(0,51,141,0.12)', borderRadius: 'var(--radius-sm)', marginBottom: 14 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--primary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>Most Used Service</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text)' }}>{data.plan_info?.most_used_service}</div>
          </div>

          {/* Location */}
          {data.location && (
            <div style={{ padding: '10px 14px', background: 'var(--success-bg)', border: '1px solid #a7f3d0', borderRadius: 'var(--radius-sm)', marginBottom: 14 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--success)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>Last Known Location</div>
              <div style={{ fontSize: 13, color: 'var(--text)' }}>Lat: {data.location.latitude?.toFixed(4)}, Lng: {data.location.longitude?.toFixed(4)}</div>
            </div>
          )}

          {/* Category breakdown */}
          {(data.category_breakdown || []).length > 0 && (
            <div style={{ marginBottom: 14 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 10 }}>Issue Breakdown</div>
              {data.category_breakdown.map(({ category, count }) => {
                const max = Math.max(...data.category_breakdown.map(c => c.count));
                return (
                  <div key={category} style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 7 }}>
                    <span style={{ fontSize: 12, color: 'var(--text-secondary)', width: 170, flexShrink: 0 }}>{category}</span>
                    <div style={{ flex: 1, background: 'var(--bg)', borderRadius: 4, height: 7, overflow: 'hidden' }}>
                      <div style={{ height: '100%', width: `${(count / max) * 100}%`, background: 'var(--primary)', borderRadius: 4 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--text)', width: 20, textAlign: 'right' }}>{count}</span>
                  </div>
                );
              })}
            </div>
          )}

          {/* Past complaints */}
          {(data.recent_sessions || []).length > 0 && (
            <div>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 10 }}>Recent Complaints</div>
              {data.recent_sessions.slice(0, 5).map(s => (
                <div key={s.id} style={{ padding: '10px 14px', background: 'var(--bg)', borderRadius: 'var(--radius-sm)', marginBottom: 8 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{s.sector} — {s.subprocess}</span>
                    <span className={`badge badge-${s.status}`}>{s.status}</span>
                  </div>
                  {s.summary && <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 }}>{s.summary.slice(0, 120)}…</div>}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </Modal>
  );
}

/* ── AI Diagnosis Modal (6-tab for network/signal, placeholder for others) ──── */
function DiagnoseModal({ ticket, onClose }) {
  const isNetwork = ticket.category?.toLowerCase().includes('mobile services') &&
    ticket.subcategory?.toLowerCase().includes('network');

  if (!isNetwork) {
    return (
      <Modal title="AI Diagnostic Report" onClose={onClose} width={560}>
        <div style={{ textAlign: 'center', padding: '40px 20px' }}>
          <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" strokeWidth="1.5" style={{ marginBottom: 16 }}>
            <rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/>
          </svg>
          <h3 style={{ margin: '0 0 8px', color: 'var(--text)', fontSize: 16 }}>Coming Soon</h3>
          <p style={{ color: 'var(--text-muted)', fontSize: 14, margin: 0, lineHeight: 1.6 }}>
            AI Diagnosis for <strong>{ticket.category}</strong> / <strong>{ticket.subcategory}</strong> will be available in a future update.
          </p>
        </div>
      </Modal>
    );
  }

  return <NetworkDiagnosisModal ticket={ticket} onClose={onClose} />;
}

/* ── Network Diagnosis: 6-tab modal ──────────────────────────────────────────── */
function NetworkDiagnosisModal({ ticket, onClose }) {
  const [tab, setTab] = useState('map');
  const [sites, setSites] = useState(null);
  const [customer, setCustomer] = useState(null);
  const [sitesLoading, setSitesLoading] = useState(true);
  const [sitesError, setSitesError] = useState('');
  const [trends, setTrends] = useState(null);
  const [trendPeriod, setTrendPeriod] = useState('day');
  const [trendsLoading, setTrendsLoading] = useState(false);
  const [rootCause, setRootCause] = useState('');
  const [rcLoading, setRcLoading] = useState(false);
  const [recommendation, setRecommendation] = useState('');
  const [recLoading, setRecLoading] = useState(false);
  const [pdfLoading, setPdfLoading] = useState(false);

  // Fetch nearest sites on mount
  useEffect(() => {
    apiGet(`/api/agent/tickets/${ticket.id}/nearest-sites`)
      .then(d => {
        if (d.error) { setSitesError(d.error); }
        else { setSites(d.nearest_sites); setCustomer(d.customer); }
        setSitesLoading(false);
      })
      .catch(() => { setSitesError('Failed to fetch site data.'); setSitesLoading(false); });
  }, [ticket.id]);

  // Fetch trends when tab=trend or period changes
  useEffect(() => {
    if (tab !== 'trend' || !sites?.length) return;
    setTrendsLoading(true);
    apiGet(`/api/agent/sites/${sites[0].site_id}/kpi-trends?period=${trendPeriod}`)
      .then(d => { setTrends(d.trends || {}); setTrendsLoading(false); })
      .catch(() => { setTrends({}); setTrendsLoading(false); });
  }, [tab, trendPeriod, sites]);

  const runRootCause = async () => {
    setRcLoading(true);
    try {
      const d = await apiPost(`/api/agent/tickets/${ticket.id}/root-cause`, {});
      setRootCause(d.analysis || 'No analysis available.');
    } catch { setRootCause('Root cause analysis failed.'); }
    setRcLoading(false);
  };

  const runRecommendation = async () => {
    setRecLoading(true);
    try {
      const d = await apiPost(`/api/agent/tickets/${ticket.id}/recommendation`, { root_cause: rootCause });
      setRecommendation(d.recommendation || 'No recommendation available.');
    } catch { setRecommendation('Recommendation failed.'); }
    setRecLoading(false);
  };

  const downloadPdf = async () => {
    setPdfLoading(true);
    try {
      const { default: jsPDF } = await import('jspdf');
      await import('jspdf-autotable');
      const doc = new jsPDF('p', 'mm', 'a4');
      let y = 15;
      const pageW = doc.internal.pageSize.getWidth();

      // Header
      doc.setFillColor(0, 51, 141);
      doc.rect(0, 0, pageW, 30, 'F');
      doc.setTextColor(255);
      doc.setFontSize(16);
      doc.text('AI Network Diagnosis Report', 14, 12);
      doc.setFontSize(10);
      doc.text(`Ticket: ${ticket.reference_number} | ${ticket.category} / ${ticket.subcategory} | Priority: ${ticket.priority?.toUpperCase()}`, 14, 22);
      y = 38;

      doc.setTextColor(0);

      // Site Information
      if (sites?.length) {
        doc.setFontSize(13);
        doc.text('Site Information', 14, y);
        y += 6;
        doc.autoTable({
          startY: y,
          head: [['#', 'Site ID', 'Latitude', 'Longitude', 'Zone', 'Distance (km)']],
          body: sites.map((s, i) => [i + 1, s.site_id, s.latitude?.toFixed(5), s.longitude?.toFixed(5), s.zone, s.distance_km]),
          styles: { fontSize: 9 },
          headStyles: { fillColor: [0, 51, 141] },
          margin: { left: 14, right: 14 },
        });
        y = doc.lastAutoTable.finalY + 10;
      }

      // Root Cause
      if (rootCause) {
        if (y > 240) { doc.addPage(); y = 15; }
        doc.setFontSize(13);
        doc.text('Root Cause Analysis', 14, y);
        y += 6;
        doc.setFontSize(9);
        const rcLines = doc.splitTextToSize(rootCause, pageW - 28);
        for (const line of rcLines) {
          if (y > 280) { doc.addPage(); y = 15; }
          doc.text(line, 14, y);
          y += 4.5;
        }
        y += 6;
      }

      // Recommendations
      if (recommendation) {
        if (y > 240) { doc.addPage(); y = 15; }
        doc.setFontSize(13);
        doc.text('Final Recommendations', 14, y);
        y += 6;
        doc.setFontSize(9);
        const recLines = doc.splitTextToSize(recommendation, pageW - 28);
        for (const line of recLines) {
          if (y > 280) { doc.addPage(); y = 15; }
          doc.text(line, 14, y);
          y += 4.5;
        }
      }

      doc.save(`Diagnosis_${ticket.reference_number}.pdf`);
    } catch (e) {
      alert('PDF generation failed: ' + e.message);
    }
    setPdfLoading(false);
  };

  const TABS = [
    { key: 'map', label: 'Map Visualization' },
    { key: 'sites', label: 'Site Information' },
    { key: 'trend', label: 'Trend Analysis' },
    { key: 'rca', label: 'Root Cause Analysis' },
    { key: 'rec', label: 'Final Recommendation' },
    { key: 'pdf', label: 'Download Report' },
  ];

  return (
    <Modal title="AI Network Diagnosis" onClose={onClose} width={1050}>
      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20, flexWrap: 'wrap' }}>
        {TABS.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)} style={{
            padding: '7px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600,
            border: '1px solid', cursor: 'pointer',
            background: tab === t.key ? '#00338D' : '#fff',
            color: tab === t.key ? '#fff' : '#475569',
            borderColor: tab === t.key ? '#00338D' : '#e2e8f0',
          }}>
            {t.label}
          </button>
        ))}
      </div>

      {sitesLoading ? (
        <div className="page-loader" style={{ height: 200 }}><div className="spinner" /></div>
      ) : sitesError ? (
        <div style={{ textAlign: 'center', padding: 40, color: '#dc2626' }}>
          <p style={{ fontWeight: 600, margin: '0 0 8px' }}>Unable to load site data</p>
          <p style={{ fontSize: 13, color: '#64748b', margin: 0 }}>{sitesError}</p>
        </div>
      ) : (
        <>
          {/* ── Tab: Map ─────────────────────────────────────── */}
          {tab === 'map' && <MapTab customer={customer} sites={sites} />}

          {/* ── Tab: Site Info ────────────────────────────────── */}
          {tab === 'sites' && (
            <div>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                <thead>
                  <tr style={{ background: '#f8fafc', borderBottom: '2px solid #e2e8f0' }}>
                    {['#', 'Site ID', 'Latitude', 'Longitude', 'Zone', 'Distance (km)'].map(h => (
                      <th key={h} style={{ padding: '10px 12px', textAlign: 'left', fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sites.map((s, i) => (
                    <tr key={s.site_id} style={{ borderBottom: '1px solid #f1f5f9', background: i === 0 ? '#eff6ff' : '#fff' }}>
                      <td style={{ padding: '10px 12px', fontWeight: 700 }}>{i + 1}</td>
                      <td style={{ padding: '10px 12px', fontWeight: 600, color: '#00338D' }}>{s.site_id}</td>
                      <td style={{ padding: '10px 12px' }}>{s.latitude?.toFixed(5)}</td>
                      <td style={{ padding: '10px 12px' }}>{s.longitude?.toFixed(5)}</td>
                      <td style={{ padding: '10px 12px' }}>{s.zone || '—'}</td>
                      <td style={{ padding: '10px 12px', fontWeight: 700, color: i === 0 ? '#16a34a' : '#475569' }}>{s.distance_km} km</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {customer && (
                <div style={{ marginTop: 12, fontSize: 12, color: '#64748b' }}>
                  Customer Location: {customer.latitude?.toFixed(5)}, {customer.longitude?.toFixed(5)}
                </div>
              )}
            </div>
          )}

          {/* ── Tab: Trend ────────────────────────────────────── */}
          {tab === 'trend' && (
            <div>
              <div style={{ display: 'flex', gap: 6, marginBottom: 16 }}>
                {['month', 'week', 'day', 'hour'].map(p => (
                  <button key={p} onClick={() => setTrendPeriod(p)} style={{
                    padding: '5px 12px', borderRadius: 5, fontSize: 11, fontWeight: 600,
                    border: '1px solid', cursor: 'pointer', textTransform: 'capitalize',
                    background: trendPeriod === p ? '#00338D' : '#fff',
                    color: trendPeriod === p ? '#fff' : '#475569',
                    borderColor: trendPeriod === p ? '#00338D' : '#e2e8f0',
                  }}>
                    {p}ly
                  </button>
                ))}
              </div>
              <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12 }}>
                Showing trends for nearest site: <strong style={{ color: '#00338D' }}>{sites[0]?.site_id}</strong>
              </div>
              {trendsLoading ? (
                <div className="page-loader" style={{ height: 160 }}><div className="spinner" /></div>
              ) : !trends || Object.keys(trends).length === 0 ? (
                <div style={{ textAlign: 'center', padding: 40, color: '#64748b' }}>
                  No KPI data available for this site. Ask admin to upload KPI data.
                </div>
              ) : (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, maxHeight: 500, overflowY: 'auto' }}>
                  {Object.entries(trends).map(([kpiName, data]) => (
                    <TrendMiniChart key={kpiName} kpiName={kpiName} data={data} />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* ── Tab: Root Cause ───────────────────────────────── */}
          {tab === 'rca' && (
            <div>
              {!rootCause && !rcLoading && (
                <div style={{ textAlign: 'center', padding: 30 }}>
                  <button className="btn btn-primary btn-sm" onClick={runRootCause}>Run Root Cause Analysis</button>
                  <p style={{ fontSize: 12, color: '#64748b', marginTop: 10 }}>Uses AI to analyze KPI trends for nearest site: <strong>{sites[0]?.site_id}</strong></p>
                </div>
              )}
              {rcLoading && <div className="page-loader" style={{ height: 160 }}><div className="spinner" /></div>}
              {rootCause && (
                <div style={{ fontSize: 13, lineHeight: 1.8, color: 'var(--text)', whiteSpace: 'pre-wrap', background: 'var(--bg)', padding: 16, borderRadius: 8, maxHeight: 450, overflowY: 'auto' }}>
                  {rootCause}
                </div>
              )}
            </div>
          )}

          {/* ── Tab: Recommendation ──────────────────────────── */}
          {tab === 'rec' && (
            <div>
              {!recommendation && !recLoading && (
                <div style={{ textAlign: 'center', padding: 30 }}>
                  <button className="btn btn-primary btn-sm" onClick={runRecommendation}>Get Recommendations</button>
                  <p style={{ fontSize: 12, color: '#64748b', marginTop: 10 }}>
                    {rootCause ? 'Based on the root cause analysis' : 'Run root cause analysis first for better results'}
                  </p>
                </div>
              )}
              {recLoading && <div className="page-loader" style={{ height: 160 }}><div className="spinner" /></div>}
              {recommendation && (
                <div style={{ fontSize: 13, lineHeight: 1.8, color: 'var(--text)', whiteSpace: 'pre-wrap', background: 'var(--bg)', padding: 16, borderRadius: 8, maxHeight: 450, overflowY: 'auto' }}>
                  {recommendation}
                </div>
              )}
            </div>
          )}

          {/* ── Tab: Download PDF ─────────────────────────────── */}
          {tab === 'pdf' && (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#00338D" strokeWidth="1.5" style={{ marginBottom: 16 }}>
                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="12" y1="18" x2="12" y2="12"/><polyline points="9 15 12 18 15 15"/>
              </svg>
              <h3 style={{ margin: '0 0 8px', fontSize: 16, color: 'var(--text)' }}>Generate PDF Report</h3>
              <p style={{ fontSize: 13, color: '#64748b', marginBottom: 20, lineHeight: 1.6 }}>
                Includes: Site Information, Root Cause Analysis, and Final Recommendations.<br/>
                {!rootCause && <span style={{ color: '#d97706' }}>Tip: Run Root Cause Analysis and Recommendations first for a complete report.</span>}
              </p>
              <button className="btn btn-primary" onClick={downloadPdf} disabled={pdfLoading}>
                {pdfLoading ? 'Generating...' : 'Download PDF Report'}
              </button>
            </div>
          )}
        </>
      )}
    </Modal>
  );
}

/* ── Map Tab (Leaflet) ───────────────────────────────────────────────────────── */
function MapTab({ customer, sites }) {
  const [mapReady, setMapReady] = useState(false);
  const [Leaflet, setLeaflet] = useState(null);

  useEffect(() => {
    // Dynamic import to avoid SSR issues
    Promise.all([
      import('leaflet'),
      import('react-leaflet'),
      import('leaflet/dist/leaflet.css'),
    ]).then(([L, RL]) => {
      // Fix default marker icons
      delete L.Icon.Default.prototype._getIconUrl;
      L.Icon.Default.mergeOptions({
        iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
        iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
        shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
      });
      setLeaflet({ L, ...RL });
      setMapReady(true);
    });
  }, []);

  if (!mapReady || !Leaflet) return <div className="page-loader" style={{ height: 350 }}><div className="spinner" /></div>;

  const { MapContainer, TileLayer, Marker, Popup, Tooltip } = Leaflet;

  const center = customer ? [customer.latitude, customer.longitude] : [20.5937, 78.9629];
  const allPoints = [center, ...(sites || []).map(s => [s.latitude, s.longitude])];
  const bounds = allPoints.length > 1 ? allPoints : undefined;

  return (
    <div style={{ height: 400, borderRadius: 8, overflow: 'hidden', border: '1px solid #e2e8f0' }}>
      <MapContainer center={center} zoom={13} style={{ height: '100%', width: '100%' }} bounds={bounds} boundsOptions={{ padding: [40, 40] }}>
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" attribution='&copy; OSM' />
        {customer && (
          <Marker position={[customer.latitude, customer.longitude]}>
            <Popup>Customer Location<br/>{customer.latitude?.toFixed(5)}, {customer.longitude?.toFixed(5)}</Popup>
            <Tooltip permanent direction="top" offset={[0, -30]}>Customer</Tooltip>
          </Marker>
        )}
        {(sites || []).map((s, i) => (
          <Marker key={s.site_id} position={[s.latitude, s.longitude]}>
            <Popup>
              <strong>{s.site_id}</strong><br/>
              Zone: {s.zone || 'N/A'}<br/>
              Distance: {s.distance_km} km
            </Popup>
            <Tooltip direction="top" offset={[0, -30]}>
              {s.site_id} — {s.distance_km} km
            </Tooltip>
          </Marker>
        ))}
      </MapContainer>
    </div>
  );
}

/* ── Trend Mini Chart (using recharts) ────────────────────────────────────────── */
function TrendMiniChart({ kpiName, data }) {
  const [RC, setRC] = useState(null);
  useEffect(() => {
    import('recharts').then(m => setRC(m));
  }, []);

  if (!RC || !data?.length) return (
    <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8, padding: 12, height: 140 }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: '#475569', marginBottom: 6 }}>{kpiName}</div>
      <div style={{ fontSize: 11, color: '#94a3b8' }}>{data?.length ? 'Loading...' : 'No data'}</div>
    </div>
  );

  const { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip } = RC;
  return (
    <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8, padding: 12, height: 180 }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: '#475569', marginBottom: 4 }}>{kpiName}</div>
      <ResponsiveContainer width="100%" height={130}>
        <LineChart data={data}>
          <XAxis dataKey="label" tick={{ fontSize: 8 }} interval="preserveStartEnd" />
          <YAxis tick={{ fontSize: 8 }} width={35} />
          <Tooltip contentStyle={{ fontSize: 11 }} />
          <Line type="monotone" dataKey="avg" stroke="#00338D" strokeWidth={1.5} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

/* ── Resolve Modal ───────────────────────────────────────────────────────────── */
function ResolveModal({ ticket, onClose, onResolved }) {
  const [notes, setNotes]       = useState('');
  const [submitting, setSubmitting] = useState(false);
  const handle = async () => {
    setSubmitting(true);
    try {
      await apiPut(`/api/agent/tickets/${ticket.id}/resolve`, { resolution_notes: notes });
      onResolved(ticket.id);
      onClose();
    } catch (_) { alert('Failed to resolve ticket. Please try again.'); }
    finally { setSubmitting(false); }
  };
  return (
    <Modal title="Resolve Ticket" onClose={onClose} width={480}>
      <div style={{ marginBottom: 16 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 16px', padding: '12px 14px', background: 'var(--bg)', borderRadius: 'var(--radius-sm)', marginBottom: 16 }}>
          {[['Reference', ticket.reference_number], ['Category', ticket.category], ['Priority', ticket.priority], ['Customer', ticket.user_name]].map(([k, v]) => (
            <div key={k}>
              <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 2 }}>{k}</div>
              <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{v}</div>
            </div>
          ))}
        </div>
        <div className="form-group">
          <label>Resolution Notes <span style={{ color: 'var(--text-muted)', fontWeight: 400 }}>(optional)</span></label>
          <textarea
            className="feedback-textarea"
            rows={4}
            value={notes}
            onChange={e => setNotes(e.target.value)}
            placeholder="Describe the resolution steps taken..."
          />
        </div>
      </div>
      <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end' }}>
        <button className="btn btn-ghost btn-sm" onClick={onClose}>Cancel</button>
        <button className="btn btn-success btn-sm" onClick={handle} disabled={submitting}>
          {submitting ? 'Resolving…' : 'Mark as Resolved'}
        </button>
      </div>
    </Modal>
  );
}

/* ── Action button ───────────────────────────────────────────────────────────── */
function ActionBtn({ onClick, icon, label, variant = 'ghost' }) {
  return (
    <button className={`btn btn-${variant} btn-sm`} onClick={onClick} style={{ display: 'inline-flex', alignItems: 'center', gap: 5, fontSize: 12 }}>
      {icon}{label}
    </button>
  );
}

/* ── Main Component ──────────────────────────────────────────────────────────── */
export default function AgentTicketBucket() {
  const [tickets, setTickets]   = useState([]);
  const [loading, setLoading]   = useState(true);
  const [modal, setModal]       = useState(null);
  const [filterStatus, setFilterStatus] = useState('all');
  const navigate = useNavigate();

  const fetchTickets = useCallback(() => {
    apiGet('/api/agent/tickets').then(d => { setTickets(d.tickets || []); setLoading(false); }).catch(() => setLoading(false));
  }, []);

  useEffect(() => { fetchTickets(); const iv = setInterval(fetchTickets, 30000); return () => clearInterval(iv); }, [fetchTickets]);

  const handleResolved = id => setTickets(prev => prev.map(t => t.id === id ? { ...t, status: 'resolved', resolved_at: new Date().toISOString() } : t));

  const filtered = filterStatus === 'all' ? tickets : tickets.filter(t => t.status === filterStatus);
  const openCount     = tickets.filter(t => t.status !== 'resolved').length;
  const resolvedCount = tickets.filter(t => t.status === 'resolved').length;

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  return (
    <div>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 24 }}>
        <div className="page-header" style={{ margin: 0 }}>
          <h1>Assigned Ticket Bucket</h1>
          <p>{openCount} open &middot; {resolvedCount} resolved &middot; {tickets.length} total</p>
        </div>
        <button className="btn btn-ghost btn-sm" onClick={fetchTickets} style={{ display: 'inline-flex', alignItems: 'center', gap: 6, marginTop: 4 }}>
          {IC.refresh} Refresh
        </button>
      </div>

      {/* Filter tabs */}
      <div className="rpt-tabs" style={{ marginBottom: 20 }}>
        {[['all','All'], ['pending','Pending'], ['in_progress','In Progress'], ['resolved','Resolved']].map(([v, l]) => (
          <button key={v} className={`rpt-tab${filterStatus === v ? ' active' : ''}`} onClick={() => setFilterStatus(v)}>{l}</button>
        ))}
      </div>

      {/* Priority legend */}
      <div style={{ display: 'flex', gap: 20, marginBottom: 16 }}>
        {Object.entries(P_CFG).map(([k, c]) => (
          <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 10, height: 10, borderRadius: 2, background: c.bar, display: 'inline-block' }} />
            <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{c.label}</span>
          </div>
        ))}
      </div>

      {/* Ticket list */}
      {filtered.length === 0 ? (
        <div className="table-card">
          <div className="empty-state">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M15 5v2M15 11v2M15 17v2M5 5h14a2 2 0 0 1 2 2v3a2 2 0 0 0 0 4v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-3a2 2 0 0 0 0-4V7a2 2 0 0 1 2-2z"/></svg>
            <h4>No tickets found</h4>
            <p>{filterStatus === 'all' ? 'No tickets assigned to you yet.' : `No ${filterStatus.replace('_', ' ')} tickets.`}</p>
          </div>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {filtered.map(ticket => {
            const pc        = P_CFG[ticket.priority] || P_CFG.low;
            const isResolved = ticket.status === 'resolved';
            return (
              <div
                key={ticket.id}
                style={{
                  background: '#fff',
                  border: '1px solid var(--border)',
                  borderLeft: `4px solid ${isResolved ? 'var(--border)' : pc.bar}`,
                  borderRadius: 'var(--radius)',
                  boxShadow: 'var(--shadow-sm)',
                  opacity: isResolved ? 0.85 : 1,
                  transition: 'box-shadow 0.2s',
                }}
                onMouseEnter={e => { if (!isResolved) e.currentTarget.style.boxShadow = 'var(--shadow-md)'; }}
                onMouseLeave={e => { e.currentTarget.style.boxShadow = 'var(--shadow-sm)'; }}
              >
                {/* Metadata row */}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '10px 24px', padding: '16px 20px', borderBottom: '1px solid var(--border-light)', alignItems: 'flex-start' }}>
                  {/* Reference */}
                  <div style={{ minWidth: 130 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 3 }}>Reference</div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--primary)' }}>{ticket.reference_number}</div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>ID #{ticket.id}</div>
                  </div>

                  {/* Category */}
                  <div style={{ flex: 1, minWidth: 160 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 3 }}>Problem Category</div>
                    <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{ticket.category}</div>
                    <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{ticket.subcategory}</div>
                  </div>

                  {/* Priority + Status */}
                  <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start', flexWrap: 'wrap' }}>
                    <div>
                      <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>Priority</div>
                      <span className={`badge badge-${ticket.priority}`}>{ticket.priority}</span>
                    </div>
                    <div>
                      <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>Status</div>
                      <span className={`badge badge-${ticket.status}`}>{ticket.status.replace('_', ' ')}</span>
                    </div>
                  </div>

                  {/* SLA Timer */}
                  <div>
                    <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>SLA Timer</div>
                    <SlaTimer deadline={ticket.sla_deadline} slaHours={ticket.sla_hours} status={ticket.status} />
                  </div>

                  {/* Customer */}
                  <div>
                    <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 3 }}>Customer</div>
                    <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{ticket.user_name}</div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12, color: 'var(--text-secondary)', marginTop: 2 }}>
                      <span style={{ color: 'var(--primary)' }}>{IC.phone}</span>
                      {ticket.user_phone || '—'}
                    </div>
                  </div>
                </div>

                {/* Description */}
                <div style={{ padding: '10px 20px', borderBottom: '1px solid var(--border-light)' }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>Issue Description</div>
                  <div style={{ fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
                    {(ticket.description || '').slice(0, 220)}{(ticket.description || '').length > 220 ? '…' : ''}
                  </div>
                </div>

                {/* Action buttons */}
                <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8, padding: '12px 20px' }}>
                  {ticket.chat_session_id && (
                    <ActionBtn onClick={() => navigate(`/agent/chat/${ticket.chat_session_id}?ticketId=${ticket.id}`)} icon={IC.chat} label="AI Chat Log" />
                  )}
                  <ActionBtn onClick={() => setModal({ type: 'diagnose', ticket })} icon={IC.cpu} label="AI Diagnosis" />
                  <ActionBtn onClick={() => setModal({ type: 'c360', customerId: ticket.user_id })} icon={IC.user360} label="Customer 360" />
                  {!isResolved && (
                    <ActionBtn onClick={() => setModal({ type: 'resolve', ticket })} icon={IC.check} label="Mark Resolved" variant="success" />
                  )}
                  <div style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-muted)' }}>
                    Created {ticket.created_at ? new Date(ticket.created_at).toLocaleString() : '—'}
                    {ticket.resolved_at && <>&nbsp;&middot;&nbsp;Resolved {new Date(ticket.resolved_at).toLocaleString()}</>}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Modals */}
      {modal?.type === 'c360'     && <Customer360Modal customerId={modal.customerId} onClose={() => setModal(null)} />}
      {modal?.type === 'diagnose' && <DiagnoseModal    ticket={modal.ticket}         onClose={() => setModal(null)} />}
      {modal?.type === 'resolve'  && <ResolveModal     ticket={modal.ticket}         onClose={() => setModal(null)} onResolved={handleResolved} />}
    </div>
  );
}
