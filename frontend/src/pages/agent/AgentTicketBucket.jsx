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

/* ── AI Diagnosis Modal ──────────────────────────────────────────────────────── */
function DiagnoseModal({ ticketId, onClose }) {
  const [diagnosis, setDiagnosis] = useState('');
  const [loading, setLoading]     = useState(true);
  useEffect(() => {
    apiPost(`/api/agent/tickets/${ticketId}/diagnose`, {})
      .then(d => { setDiagnosis(d.diagnosis); setLoading(false); })
      .catch(() => { setDiagnosis('AI diagnosis unavailable at this time.'); setLoading(false); });
  }, [ticketId]);
  return (
    <Modal title="AI Diagnostic Report" onClose={onClose} width={680}>
      {loading ? (
        <div className="page-loader" style={{ height: 160 }}><div className="spinner" /></div>
      ) : (
        <div style={{ fontSize: 14, lineHeight: 1.8, color: 'var(--text)', whiteSpace: 'pre-wrap', background: 'var(--bg)', padding: 16, borderRadius: 'var(--radius-sm)' }}>
          {diagnosis}
        </div>
      )}
    </Modal>
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
                  <ActionBtn onClick={() => setModal({ type: 'diagnose', ticketId: ticket.id })} icon={IC.cpu} label="AI Diagnosis" />
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
      {modal?.type === 'diagnose' && <DiagnoseModal    ticketId={modal.ticketId}     onClose={() => setModal(null)} />}
      {modal?.type === 'resolve'  && <ResolveModal     ticket={modal.ticket}         onClose={() => setModal(null)} onResolved={handleResolved} />}
    </div>
  );
}
