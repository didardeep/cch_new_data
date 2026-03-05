import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiGet, apiPost, apiPut } from '../../api';

/* â”€â”€ SVG Icons â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
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
  tune:    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/><line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/><line x1="17" y1="16" x2="23" y2="16"/></svg>,
};

/* â”€â”€ Priority config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
const P_CFG = {
  critical: { bar: '#dc2626', badgeClass: 'badge-critical', label: 'Critical' },
  high:     { bar: '#f97316', badgeClass: 'badge-high',     label: 'High'     },
  medium:   { bar: '#f59e0b', badgeClass: 'badge-medium',   label: 'Medium'   },
  low:      { bar: '#10b981', badgeClass: 'badge-low',      label: 'Low'      },
};

/* â”€â”€ Live SLA Timer â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
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

/* â”€â”€ Modal wrapper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
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

/* â”€â”€ Customer 360 Modal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
function Customer360Modal({ customerId, onClose }) {
  const [data, setData]   = useState(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    apiGet(`/api/agent/customer360/${customerId}`).then(d => { setData(d); setLoading(false); }).catch(() => setLoading(false));
  }, [customerId]);

  return (
    <Modal title="Customer 360Â°" onClose={onClose} width={620}>
      {loading ? (
        <div className="page-loader" style={{ height: 180 }}><div className="spinner" /></div>
      ) : !data ? (
        <div className="form-error">Failed to load customer data.</div>
      ) : (
        <>
          {/* Customer info */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px 20px', padding: '14px 16px', background: 'var(--bg)', borderRadius: 'var(--radius-sm)', marginBottom: 16 }}>
            {[['Name', data.customer?.name], ['Email', data.customer?.email], ['Phone', data.customer?.phone || ' - '], ['Member Since', data.plan_info?.account_since]].map(([k, v]) => (
              <div key={k}>
                <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 2 }}>{k}</div>
                <div style={{ fontSize: 13, color: 'var(--text)', fontWeight: 500 }}>{v || ' - '}</div>
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
                    <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{s.sector}  -  {s.subprocess}</span>
                    <span className={`badge badge-${s.status}`}>{s.status}</span>
                  </div>
                  {s.summary && <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 }}>{s.summary.slice(0, 120)}...</div>}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </Modal>
  );
}

/* â”€â”€ AI Diagnosis Modal (6-tab for network/signal, placeholder for others) â”€â”€â”€â”€ */
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

/* â”€â”€ Network Diagnosis: 6-tab modal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
const PERIOD_LABELS = { month: 'Monthly', week: 'Weekly', day: 'Daily', hour: 'Hourly' };

function NetworkDiagnosisModal({ ticket, onClose }) {
  const [tab, setTab] = useState('map');
  const [sites, setSites] = useState(null);
  const [solutionSite, setSolutionSite] = useState(null);
  const [customer, setCustomer] = useState(null);
  const [sitesLoading, setSitesLoading] = useState(true);
  const [sitesError, setSitesError] = useState('');
  // Trend state  -  separate for site and cell level
  const [trendLevel, setTrendLevel] = useState('site'); // 'site' or 'cell'
  const [trendPeriod, setTrendPeriod] = useState('day');
  const [trends, setTrends] = useState(null);
  const [trendsLoading, setTrendsLoading] = useState(false);
  // Root cause & recommendation
  const [rootCause, setRootCause] = useState('');
  const [rcLoading, setRcLoading] = useState(false);
  const [recommendation, setRecommendation] = useState('');
  const [recLoading, setRecLoading] = useState(false);
  const [pdfLoading, setPdfLoading] = useState(false);
  // Cache trend data for PDF
  const [trendCache, setTrendCache] = useState({});
  const trendCacheRef = useRef({});

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

  // Fetch trends when tab=trend or level/period changes
  useEffect(() => {
    if (tab !== 'trend' || !sites?.length) return;
    const cacheKey = `${trendLevel}_${trendPeriod}`;
    if (trendCacheRef.current[cacheKey]) {
      setTrends(trendCacheRef.current[cacheKey]);
      return;
    }
    setTrendsLoading(true);
    apiGet(`/api/agent/sites/${sites[0].site_id}/kpi-trends?period=${trendPeriod}&data_level=${trendLevel}`)
      .then(d => {
        const t = d.trends || {};
        setTrends(t);
        trendCacheRef.current[cacheKey] = t;
        setTrendCache(prev => ({ ...prev, [cacheKey]: t }));
        setTrendsLoading(false);
      })
      .catch(() => { setTrends({}); setTrendsLoading(false); });
  }, [tab, trendLevel, trendPeriod, sites]);

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
      // Build trend summary from cached data
      let trendSummary = '';
      for (const [key, data] of Object.entries(trendCache)) {
        const [level, period] = key.split('_');
        trendSummary += `\n${level.toUpperCase()} LEVEL (${PERIOD_LABELS[period] || period}):\n`;
        for (const [kpiName, points] of Object.entries(data)) {
          if (points.length > 0) {
            const avgVals = points.map(p => p.avg);
            const overall = (avgVals.reduce((a, b) => a + b, 0) / avgVals.length).toFixed(4);
            trendSummary += `- ${kpiName}: overall avg=${overall}, points=${points.length}\n`;
          }
        }
      }
      const d = await apiPost(`/api/agent/tickets/${ticket.id}/recommendation`, {
        root_cause: rootCause,
        trend_summary: trendSummary,
      });
      setRecommendation(d.recommendation || 'No recommendation available.');
    } catch { setRecommendation('Recommendation failed.'); }
    setRecLoading(false);
  };

  const formatAlarms = (alarmText) => {
    if (!alarmText || !alarmText.trim()) return 'No active alarms';
    return alarmText.length > 90 ? `${alarmText.slice(0, 90)}...` : alarmText;
  };

  const cleanAiText = (raw = '') => (
    String(raw)
      .replace(/\r/g, '')
      .replace(/^#{1,6}\s*/gm, '')
      .replace(/\*\*(.*?)\*\*/g, '$1')
      .replace(/\*(.*?)\*/g, '$1')
      .replace(/`/g, '')
      .replace(/^\s*[-•]\s+/gm, '')
      .replace(/^\s*\*+\s*/gm, '')
      .replace(/\\rightarrow/g, '->')
      .replace(/\$/g, '')
      .replace(/[ \t]+/g, ' ')
      .replace(/\n{3,}/g, '\n\n')
      .trim()
  );

  const isSectionHeading = (line = '') =>
    /^(\d+\.\s+.+|Recommendations?:?|Final Recommendations?:?|Root Cause Analysis:?|Capacity Expansion:?|Immediate Actions:?|Short-term Fixes:?|Long-term Recommendations:?|Escalation Path:?|Customer Communication:?|Impact Assessment:?|Correlation Analysis:?)/i.test(line.trim());

  const isImportantLine = (line = '') =>
    /(critical|root cause|congestion|throughput|latency|handover|prb|on air|off air|severity|impact|scope|site-wide|bottleneck|\b\d+(?:\.\d+)?\s?(?:%|mbps|ms|km)\b)/i.test(line);

  const highlightInline = (line) => {
    const tokenRe = /(\b\d+(?:\.\d+)?\s?(?:%|Mbps|ms|km)\b|\b(?:Critical|Root Cause|Primary Cause|Secondary Cause|Congestion|Throughput|Latency|Handover|PRB Utilization|ON AIR|OFF AIR|Severity|Impact|Scope|Site-wide)\b)/gi;
    return line.split(tokenRe).map((part, i) => (
      /^(?:\d+(?:\.\d+)?\s?(?:%|Mbps|ms|km)|Critical|Root Cause|Primary Cause|Secondary Cause|Congestion|Throughput|Latency|Handover|PRB Utilization|ON AIR|OFF AIR|Severity|Impact|Scope|Site-wide)$/i.test(part)
        ? <strong key={`${part}-${i}`}>{part}</strong>
        : <span key={`${part}-${i}`}>{part}</span>
    ));
  };

  const renderProfessionalText = (raw) => {
    const linesText = cleanAiText(raw).split('\n').map(l => l.trim()).filter(Boolean);
    return linesText.map((line, idx) => {
      const heading = isSectionHeading(line);
      const important = isImportantLine(line);
      return (
        <p
          key={`${idx}-${line.slice(0, 20)}`}
          style={{
            margin: heading ? '14px 0 8px' : '0 0 10px',
            fontSize: heading ? 15 : 14,
            lineHeight: heading ? 1.5 : 1.72,
            color: '#1e293b',
            fontFamily: "'Segoe UI', Arial, sans-serif",
            fontWeight: heading || important ? 600 : 400,
          }}
        >
          {highlightInline(line)}
        </p>
      );
    });
  };

  const summarizeForPdf = (raw, { min = 4, max = 5 } = {}) => {
    const lines = cleanAiText(raw)
      .split('\n')
      .map(l => l.trim().replace(/^\d+\.\s*/, ''))
      .filter(Boolean)
      .filter(l => !isSectionHeading(l));

    const ranked = lines
      .map(line => ({ line, score: (isImportantLine(line) ? 2 : 0) + Math.min(line.length / 90, 1) }))
      .sort((a, b) => b.score - a.score)
      .map(x => x.line);

    const unique = [];
    for (const line of ranked) {
      if (!unique.some(u => u.toLowerCase() === line.toLowerCase())) unique.push(line);
      if (unique.length >= max) break;
    }

    if (unique.length < min) {
      for (const line of lines) {
        if (!unique.some(u => u.toLowerCase() === line.toLowerCase())) unique.push(line);
        if (unique.length >= min) break;
      }
    }

    return unique.slice(0, max).map(l => l.length > 180 ? `${l.slice(0, 177)}...` : l);
  };

  const drawTrendChart = (doc, { x, y, w, h, title, points, color = [0, 51, 141] }) => {
    const vals = points.map(p => Number(p?.avg)).filter(v => Number.isFinite(v));
    if (vals.length < 2) return;

    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const span = Math.max(max - min, 1e-6);
    const px = 8;
    const py = 8;
    const plotX = x + px;
    const plotY = y + py;
    const plotW = w - px * 2;
    const plotH = h - py * 2;

    doc.setDrawColor(226, 232, 240);
    doc.roundedRect(x, y, w, h, 1.5, 1.5, 'S');
    doc.setFontSize(8.5);
    doc.setTextColor(30, 41, 59);
    doc.text(title.length > 28 ? `${title.slice(0, 28)}...` : title, x + 2, y + 5);

    doc.setDrawColor(203, 213, 225);
    doc.line(plotX, plotY + plotH, plotX + plotW, plotY + plotH);
    doc.line(plotX, plotY, plotX, plotY + plotH);

    doc.setDrawColor(...color);
    for (let i = 1; i < vals.length; i++) {
      const x1 = plotX + ((i - 1) / (vals.length - 1)) * plotW;
      const x2 = plotX + (i / (vals.length - 1)) * plotW;
      const y1 = plotY + (1 - (vals[i - 1] - min) / span) * plotH;
      const y2 = plotY + (1 - (vals[i] - min) / span) * plotH;
      doc.line(x1, y1, x2, y2);
    }

    doc.setFontSize(7.5);
    doc.setTextColor(71, 85, 105);
    doc.text(`min ${min.toFixed(2)}`, x + 2, y + h - 2);
    doc.text(`max ${max.toFixed(2)}`, x + w - 18, y + h - 2);
  };

  const downloadPdf = async () => {
    setPdfLoading(true);
    try {
      const { default: jsPDF } = await import('jspdf');
      const { default: autoTable } = await import('jspdf-autotable');
      const html2canvas = (await import('html2canvas')).default;
      const doc = new jsPDF('p', 'mm', 'a4');
      let y = 15;
      const pageW = doc.internal.pageSize.getWidth();
      const previousTab = tab;

      // â”€â”€ Header â”€â”€
      doc.setFillColor(0, 51, 141);
      doc.rect(0, 0, pageW, 32, 'F');
      doc.setTextColor(255);
      doc.setFontSize(18);
      doc.text('AI Network Diagnosis Report', 14, 14);
      doc.setFontSize(10);
      doc.text(`Ticket: ${ticket.reference_number} | ${ticket.category} / ${ticket.subcategory}`, 14, 22);
      doc.text(`Priority: ${ticket.priority?.toUpperCase()} | Generated: ${new Date().toLocaleString()}`, 14, 28);
      y = 40;
      doc.setTextColor(0);

      // â”€â”€ Map Screenshot â”€â”€
      if (previousTab !== 'map') {
        setTab('map');
        await new Promise(resolve => setTimeout(resolve, 900));
      }
      const mapEl = document.getElementById('diagnosis-map-container');
      if (mapEl) {
        try {
          const canvas = await html2canvas(mapEl, { useCORS: true, allowTaint: true, scale: 2 });
          const imgData = canvas.toDataURL('image/png');
          doc.setFontSize(14);
          doc.setFont(undefined, 'bold');
          doc.text('1. Map Visualization', 14, y);
          y += 6;
          const imgW = pageW - 28;
          const imgH = (canvas.height / canvas.width) * imgW;
          doc.addImage(imgData, 'PNG', 14, y, imgW, Math.min(imgH, 100));
          y += Math.min(imgH, 100) + 8;
        } catch { /* map screenshot failed, skip */ }
      }
      if (previousTab !== 'map') {
        setTab(previousTab);
      }

      // â”€â”€ Site Information Table â”€â”€
      if (sites?.length) {
        if (y > 230) { doc.addPage(); y = 15; }
        doc.setFontSize(14);
        doc.setFont(undefined, 'bold');
        doc.text('2. Site Information', 14, y);
        y += 6;
        doc.setFont(undefined, 'normal');
        autoTable(doc, {
          startY: y,
          head: [['#', 'Site ID', 'Latitude', 'Longitude', 'Zone', 'Distance (km)', 'Status', 'Alarms']],
          body: sites.map((s, i) => [
            i + 1,
            s.site_id,
            s.latitude?.toFixed(5),
            s.longitude?.toFixed(5),
            s.zone || 'N/A',
            s.distance_km,
            s.site_status || 'on_air',
            s.alarms || 'No active alarms',
          ]),
          styles: { fontSize: 9 },
          headStyles: { fillColor: [0, 51, 141] },
          margin: { left: 14, right: 14 },
        });
        y = (doc.lastAutoTable?.finalY || y) + 10;
        if (customer) {
          doc.setFontSize(9);
          doc.text(`Customer Location: ${customer.latitude?.toFixed(5)}, ${customer.longitude?.toFixed(5)}`, 14, y);
          y += 8;
        }
      }

      // â”€â”€ Trend Analysis Crux â”€â”€
      if (Object.keys(trendCache).length > 0) {
        if (y > 220) { doc.addPage(); y = 15; }
        doc.setFontSize(14);
        doc.setFont(undefined, 'bold');
        doc.text('3. Trend Analysis Summary', 14, y);
        y += 6;
        doc.setFont(undefined, 'normal');

        for (const [key, data] of Object.entries(trendCache)) {
          const [level, period] = key.split('_');
          if (y > 245) { doc.addPage(); y = 15; }
          doc.setFontSize(11);
          doc.setFont(undefined, 'bold');
          doc.text(`${level.charAt(0).toUpperCase() + level.slice(1)} Level - ${PERIOD_LABELS[period] || period}`, 14, y);
          y += 4;
          doc.setFont(undefined, 'normal');

          const chartEntries = Object.entries(data)
            .filter(([, points]) => Array.isArray(points) && points.length > 1)
            .slice(0, 4);

          if (chartEntries.length === 0) {
            doc.setFontSize(9);
            doc.setTextColor(100, 116, 139);
            doc.text('No trend chart data available.', 14, y + 5);
            doc.setTextColor(0);
            y += 12;
            continue;
          }

          const chartW = (pageW - 14 * 2 - 6) / 2;
          const chartH = 34;
          const rowCount = Math.ceil(chartEntries.length / 2);
          const blockHeight = rowCount * (chartH + 5) + 2;
          if (y + blockHeight > 285) {
            doc.addPage();
            y = 15;
          }

          chartEntries.forEach(([kpiName, points], idx) => {
            const col = idx % 2;
            const row = Math.floor(idx / 2);
            const cx = 14 + col * (chartW + 6);
            const cy = y + row * (chartH + 5);
            drawTrendChart(doc, {
              x: cx,
              y: cy,
              w: chartW,
              h: chartH,
              title: kpiName,
              points,
              color: level === 'cell' ? [124, 58, 237] : [0, 51, 141],
            });
          });

          y += blockHeight + 4;
        }
      }

      // â”€â”€ Root Cause Analysis â”€â”€
      if (rootCause) {
        if (y > 220) { doc.addPage(); y = 15; }
        doc.setFontSize(14);
        doc.setFont(undefined, 'bold');
        doc.text('4. Root Cause Analysis', 14, y);
        y += 7;
        const rcLines = summarizeForPdf(rootCause, { min: 4, max: 5 });
        rcLines.forEach((rawLine, idx) => {
          const wrapped = doc.splitTextToSize(rawLine, pageW - 28);
          const prefix = `${idx + 1}. `;
          let first = true;
          for (const line of wrapped) {
            if (y > 280) { doc.addPage(); y = 15; }
            doc.setFont(undefined, isSectionHeading(rawLine) || isImportantLine(rawLine) ? 'bold' : 'normal');
            doc.setFontSize(isSectionHeading(rawLine) ? 11 : (isImportantLine(rawLine) ? 10 : 9));
            doc.text(first ? `${prefix}${line}` : `   ${line}`, 14, y);
            first = false;
            y += 4.8;
          }
          y += 0.6;
        });
        y += 6;
      }

      // â”€â”€ Final Recommendations â”€â”€
      if (recommendation) {
        if (y > 220) { doc.addPage(); y = 15; }
        doc.setFontSize(14);
        doc.setFont(undefined, 'bold');
        doc.text('5. Final Recommendations', 14, y);
        y += 7;
        const recLines = summarizeForPdf(recommendation, { min: 3, max: 5 });
        recLines.forEach((rawLine, idx) => {
          const wrapped = doc.splitTextToSize(rawLine, pageW - 28);
          const prefix = `${idx + 1}. `;
          let first = true;
          for (const line of wrapped) {
            if (y > 280) { doc.addPage(); y = 15; }
            doc.setFont(undefined, isSectionHeading(rawLine) || isImportantLine(rawLine) ? 'bold' : 'normal');
            doc.setFontSize(isSectionHeading(rawLine) ? 11 : (isImportantLine(rawLine) ? 10 : 9));
            doc.text(first ? `${prefix}${line}` : `   ${line}`, 14, y);
            first = false;
            y += 4.8;
          }
          y += 0.6;
        });
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
          {/* â”€â”€ Tab: Map â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */}
          {tab === 'map' && <MapTab customer={customer} sites={sites} />}

          {/* â”€â”€ Tab: Site Info â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */}
          {tab === 'sites' && (
            <div>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                <thead>
                  <tr style={{ background: '#f8fafc', borderBottom: '2px solid #e2e8f0' }}>
                    {['#', 'Site ID', 'Latitude', 'Longitude', 'Zone', 'Status', 'Alarms', 'Solution'].map(h => (
                      <th key={h} style={{ padding: '10px 12px', textAlign: 'left', fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sites.map((s, i) => {
                    const isOffAir = (s.site_status || '').toLowerCase() === 'off_air';
                    return (
                      <tr key={s.site_id} style={{ borderBottom: '1px solid #f1f5f9', background: i === 0 ? '#eff6ff' : '#fff' }}>
                        <td style={{ padding: '10px 12px', fontWeight: 700 }}>{i + 1}</td>
                        <td style={{ padding: '10px 12px', fontWeight: 600, color: '#00338D' }}>{s.site_id}</td>
                        <td style={{ padding: '10px 12px' }}>{s.latitude?.toFixed(5)}</td>
                        <td style={{ padding: '10px 12px' }}>{s.longitude?.toFixed(5)}</td>
                        <td style={{ padding: '10px 12px' }}>{s.zone || 'N/A'}</td>
                        <td style={{ padding: '10px 12px' }}>
                          <span className={`badge badge-${isOffAir ? 'critical' : 'resolved'}`}>
                            {(s.site_status || 'on_air').replace('_', ' ')}
                          </span>
                        </td>
                        <td style={{ padding: '10px 12px', maxWidth: 300, whiteSpace: 'normal', lineHeight: 1.45 }}>
                          {formatAlarms(s.alarms)}
                        </td>
                        <td style={{ padding: '10px 12px' }}>
                          <button
                            className="btn btn-outline btn-sm"
                            onClick={() => setSolutionSite(s)}
                            disabled={!s.solution}
                            title={s.solution ? 'View solution details' : 'No solution available'}
                          >
                            {s.solution ? 'View Solution' : 'No Solution'}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              {customer && (
                <div style={{ marginTop: 12, fontSize: 12, color: '#64748b' }}>
                  Customer Location: {customer.latitude?.toFixed(5)}, {customer.longitude?.toFixed(5)}
                </div>
              )}
            </div>
          )}

          {/* â”€â”€ Tab: Trend Analysis (Site Level / Cell Level) â”€â”€ */}
          {tab === 'trend' && (
            <div>
              {/* Data level toggle */}
              <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
                {[{ key: 'site', label: 'Site Level Data Analysis' }, { key: 'cell', label: 'Cell Level Data Analysis' }].map(lv => (
                  <button key={lv.key} onClick={() => { setTrendLevel(lv.key); setTrends(null); }} style={{
                    padding: '8px 18px', borderRadius: 8, fontSize: 13, fontWeight: 700,
                    border: '2px solid', cursor: 'pointer',
                    background: trendLevel === lv.key ? '#00338D' : '#fff',
                    color: trendLevel === lv.key ? '#fff' : '#00338D',
                    borderColor: '#00338D',
                  }}>
                    {lv.label}
                  </button>
                ))}
              </div>

              {/* Period buttons */}
              <div style={{ display: 'flex', gap: 6, marginBottom: 16 }}>
                {['month', 'week', 'day', 'hour'].map(p => (
                  <button key={p} onClick={() => setTrendPeriod(p)} style={{
                    padding: '5px 12px', borderRadius: 5, fontSize: 11, fontWeight: 600,
                    border: '1px solid', cursor: 'pointer',
                    background: trendPeriod === p ? '#0f172a' : '#fff',
                    color: trendPeriod === p ? '#fff' : '#475569',
                    borderColor: trendPeriod === p ? '#0f172a' : '#e2e8f0',
                  }}>
                    {PERIOD_LABELS[p]}
                  </button>
                ))}
              </div>
              <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12 }}>
                Showing <strong style={{ color: trendLevel === 'site' ? '#00338D' : '#7c3aed' }}>{trendLevel}-level</strong> trends for nearest site: <strong style={{ color: '#00338D' }}>{sites[0]?.site_id}</strong>
              </div>
              {trendsLoading ? (
                <div className="page-loader" style={{ height: 160 }}><div className="spinner" /></div>
              ) : !trends || Object.keys(trends).length === 0 ? (
                <div style={{ textAlign: 'center', padding: 40, color: '#64748b' }}>
                  No {trendLevel}-level KPI data available for this site. Ask admin to upload {trendLevel}-level KPI data.
                </div>
              ) : (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, maxHeight: 500, overflowY: 'auto' }}>
                  {Object.entries(trends).map(([kpiName, data]) => (
                    <TrendMiniChart key={kpiName} kpiName={kpiName} data={data} color={trendLevel === 'cell' ? '#7c3aed' : '#00338D'} />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* â”€â”€ Tab: Root Cause â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */}
          {tab === 'rca' && (
            <div>
              {!rootCause && !rcLoading && (
                <div style={{ textAlign: 'center', padding: 30 }}>
                  <button className="btn btn-primary btn-sm" onClick={runRootCause}>Run Root Cause Analysis</button>
                  <p style={{ fontSize: 12, color: '#64748b', marginTop: 10 }}>
                    Uses AI to analyze both site-level and cell-level KPI trends for nearest site: <strong>{sites[0]?.site_id}</strong>
                  </p>
                </div>
              )}
              {rcLoading && <div className="page-loader" style={{ height: 160 }}><div className="spinner" /></div>}
              {rootCause && (
                <div style={{ background: 'var(--bg)', padding: 16, borderRadius: 8, maxHeight: 450, overflowY: 'auto', border: '1px solid #e2e8f0' }}>
                  {renderProfessionalText(rootCause)}
                </div>
              )}
            </div>
          )}

          {/* â”€â”€ Tab: Recommendation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */}
          {tab === 'rec' && (
            <div>
              {!recommendation && !recLoading && (
                <div style={{ textAlign: 'center', padding: 30 }}>
                  <button className="btn btn-primary btn-sm" onClick={runRecommendation}>Get Recommendations</button>
                  <p style={{ fontSize: 12, color: '#64748b', marginTop: 10 }}>
                    {rootCause ? 'Based on the entire trend analysis and root cause analysis' : 'Run root cause analysis first for better results'}
                  </p>
                </div>
              )}
              {recLoading && <div className="page-loader" style={{ height: 160 }}><div className="spinner" /></div>}
              {recommendation && (
                <div style={{ background: 'var(--bg)', padding: 16, borderRadius: 8, maxHeight: 450, overflowY: 'auto', border: '1px solid #e2e8f0' }}>
                  {renderProfessionalText(recommendation)}
                </div>
              )}
            </div>
          )}

          {/* â”€â”€ Tab: Download PDF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */}
          {tab === 'pdf' && (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#00338D" strokeWidth="1.5" style={{ marginBottom: 16 }}>
                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="12" y1="18" x2="12" y2="12"/><polyline points="9 15 12 18 15 15"/>
              </svg>
              <h3 style={{ margin: '0 0 8px', fontSize: 16, color: 'var(--text)' }}>Generate PDF Report</h3>
              <p style={{ fontSize: 13, color: '#64748b', marginBottom: 10, lineHeight: 1.6 }}>
                The PDF includes: Map Screenshot, Site Information Table, Trend Analysis Summary with Data,
                Root Cause Analysis, and Final Recommendations.
              </p>
              <div style={{ fontSize: 12, color: '#94a3b8', marginBottom: 20 }}>
                {!rootCause && <span style={{ color: '#d97706', display: 'block', marginBottom: 4 }}>Tip: Run Root Cause Analysis first.</span>}
                {!recommendation && <span style={{ color: '#d97706', display: 'block', marginBottom: 4 }}>Tip: Get Recommendations first.</span>}
                {Object.keys(trendCache).length === 0 && <span style={{ color: '#d97706', display: 'block' }}>Tip: View Trend Analysis tabs first to include trend data in the report.</span>}
              </div>
              <button className="btn btn-primary" onClick={downloadPdf} disabled={pdfLoading} style={{ fontSize: 14, padding: '10px 28px' }}>
                {pdfLoading ? 'Generating PDF...' : 'Download PDF Report'}
              </button>
            </div>
          )}
        </>
      )}

      {solutionSite && (
        <Modal
          title={`Site Solution - ${solutionSite.site_id}`}
          onClose={() => setSolutionSite(null)}
          width={620}
        >
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px 14px', marginBottom: 14 }}>
            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Site Status</div>
              <div style={{ marginTop: 4, fontSize: 13, fontWeight: 600 }}>
                {(solutionSite.site_status || 'on_air').replace('_', ' ')}
              </div>
            </div>
            <div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Zone</div>
              <div style={{ marginTop: 4, fontSize: 13, fontWeight: 600 }}>{solutionSite.zone || 'N/A'}</div>
            </div>
          </div>
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>Active Alarms</div>
            <div style={{ background: '#fff7ed', border: '1px solid #fed7aa', borderRadius: 8, padding: 12, fontSize: 13, lineHeight: 1.6 }}>
              {solutionSite.alarms || 'No active alarms'}
            </div>
          </div>
          <div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>Recommended Solution</div>
            <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: 12, fontSize: 13, lineHeight: 1.7, whiteSpace: 'pre-wrap' }}>
              {solutionSite.solution || 'No solution available for this site.'}
            </div>
          </div>
        </Modal>
      )}
    </Modal>
  );
}

/* â”€â”€ Map Tab (Leaflet) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
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
    <div id="diagnosis-map-container" style={{ height: 400, borderRadius: 8, overflow: 'hidden', border: '1px solid #e2e8f0' }}>
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
              {s.site_id}  -  {s.distance_km} km
            </Tooltip>
          </Marker>
        ))}
      </MapContainer>
    </div>
  );
}

/* â”€â”€ Trend Mini Chart (using recharts) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
function TrendMiniChart({ kpiName, data, color = '#00338D' }) {
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
          <Line type="monotone" dataKey="avg" stroke={color} strokeWidth={1.5} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

/* â”€â”€ Resolve Modal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
function ParameterChangeModal({ ticket, onClose }) {
  const [proposed, setProposed] = useState('');
  const [change, setChange] = useState(null);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [msg, setMsg] = useState('');

  const loadStatus = useCallback(async () => {
    setLoading(true);
    try {
      const d = await apiGet(`/api/agent/tickets/${ticket.id}/parameter-change`);
      setChange(d?.change || null);
    } catch (_) {
      setChange(null);
    }
    setLoading(false);
  }, [ticket.id]);

  useEffect(() => {
    loadStatus();
  }, [loadStatus]);

  const submit = async () => {
    if (!proposed.trim()) return;
    setSubmitting(true);
    setMsg('');
    try {
      const d = await apiPost(`/api/agent/tickets/${ticket.id}/parameter-change`, {
        proposed_change: proposed.trim(),
      });
      setMsg(d?.message || 'Request submitted.');
      setProposed('');
      await loadStatus();
    } catch (_) {
      setMsg('Failed to submit request.');
    }
    setSubmitting(false);
  };

  return (
    <Modal title="Parameter Change Request" onClose={onClose} width={560}>
      <div style={{ marginBottom: 12, fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.55 }}>
        Submit a technical parameter change for manager approval for ticket <strong>{ticket.reference_number}</strong>.
      </div>

      {loading ? (
        <div className="page-loader" style={{ height: 100 }}><div className="spinner" /></div>
      ) : change ? (
        <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', padding: 12, marginBottom: 14 }}>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>Latest Request Status</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
            <span className={`badge badge-${change.status === 'approved' ? 'resolved' : change.status === 'disapproved' ? 'critical' : 'pending'}`}>
              {change.status}
            </span>
            {change.reviewed_at && (
              <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                Reviewed {new Date(change.reviewed_at).toLocaleString()}
              </span>
            )}
          </div>
          <div style={{ fontSize: 12, color: 'var(--text)' }}><strong>Proposed:</strong> {change.proposed_change}</div>
          {change.manager_note && (
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 6 }}>
              <strong>Manager Note:</strong> {change.manager_note}
            </div>
          )}
        </div>
      ) : null}

      <div className="form-group">
        <label>Proposed Change</label>
        <textarea
          className="feedback-textarea"
          rows={4}
          value={proposed}
          onChange={e => setProposed(e.target.value)}
          placeholder="Describe the parameter/configuration change and expected impact..."
        />
      </div>

      {msg && <div style={{ marginTop: 8, fontSize: 12, color: '#475569' }}>{msg}</div>}

      <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end', marginTop: 16 }}>
        <button className="btn btn-ghost btn-sm" onClick={onClose}>Close</button>
        <button className="btn btn-primary btn-sm" onClick={submit} disabled={submitting || !proposed.trim()}>
          {submitting ? 'Submitting...' : 'Submit for Approval'}
        </button>
      </div>
    </Modal>
  );
}

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
          {submitting ? 'Resolving...' : 'Mark as Resolved'}
        </button>
      </div>
    </Modal>
  );
}

/* â”€â”€ Action button â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
function ActionBtn({ onClick, icon, label, variant = 'ghost' }) {
  return (
    <button className={`btn btn-${variant} btn-sm`} onClick={onClick} style={{ display: 'inline-flex', alignItems: 'center', gap: 5, fontSize: 12 }}>
      {icon}{label}
    </button>
  );
}

/* â”€â”€ Main Component â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
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
                      {ticket.user_phone || ' - '}
                    </div>
                  </div>
                </div>

                {/* Description */}
                <div style={{ padding: '10px 20px', borderBottom: '1px solid var(--border-light)' }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>Issue Description</div>
                  <div style={{ fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
                    {(ticket.description || '').slice(0, 220)}{(ticket.description || '').length > 220 ? '...' : ''}
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
                    <ActionBtn onClick={() => setModal({ type: 'param', ticket })} icon={IC.tune} label="Parameter Change" />
                  )}
                  {!isResolved && (
                    <ActionBtn onClick={() => setModal({ type: 'resolve', ticket })} icon={IC.check} label="Mark Resolved" variant="success" />
                  )}
                  <div style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-muted)' }}>
                    Created {ticket.created_at ? new Date(ticket.created_at).toLocaleString() : ' - '}
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
      {modal?.type === 'param'    && <ParameterChangeModal ticket={modal.ticket}      onClose={() => setModal(null)} />}
      {modal?.type === 'resolve'  && <ResolveModal     ticket={modal.ticket}         onClose={() => setModal(null)} onResolved={handleResolved} />}
    </div>
  );
}








