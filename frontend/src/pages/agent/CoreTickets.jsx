import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { apiGet, apiCall, apiPost } from '../../api';
import { useAuth } from '../../AuthContext';

const COMPONENTS = ['MME', 'SGW', 'PGW', 'HSS', 'PCRF'];

// KPMG-aligned, high-contrast component palette so each tab/strip stands apart
const COMP_COLOR = {
  MME:  '#00338D',  // KPMG blue
  HSS:  '#DC2626',  // strong red
  PGW:  '#7C3AED',  // violet
  SGW:  '#059669',  // emerald green
  PCRF: '#EA580C',  // orange
};

const PRIORITY_COLOR = {
  Critical: '#dc2626', High: '#f97316', Medium: '#f59e0b', Low: '#10b981',
};

const ICONS = {
  bell: <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>,
  refresh: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>,
  x: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  check: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>,
  clock: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>,
  zap: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>,
};

function fmt(v, d = 3) {
  if (v == null || isNaN(+v)) return '—';
  return (+v).toFixed(d);
}

function SlaTimer({ deadline, slaHours, status }) {
  const [rem, setRem] = useState(0);
  useEffect(() => {
    if (!deadline || status === 'resolved' || status === 'closed') { setRem(0); return; }
    const t = () => setRem(new Date(deadline).getTime() - Date.now());
    t();
    const iv = setInterval(t, 1000);
    return () => clearInterval(iv);
  }, [deadline, status]);
  if (status === 'resolved' || status === 'closed') {
    return <span style={{ padding: '2px 10px', borderRadius: 12, fontSize: 10, fontWeight: 700, background: '#dcfce7', color: '#16a34a' }}>{status}</span>;
  }
  if (!deadline) return <span style={{ fontSize: 11, color: '#94a3b8' }}>No SLA</span>;
  const total = (slaHours || 1) * 3600000;
  // Real countdown — backend pins deadline to wall-clock now+sla_hours, so we
  // don't have to cap. Negative ⇒ breached.
  const breached = rem < 0;
  const pct = Math.max(Math.min(((total - rem) / total) * 100, 100), 0);
  const abs = Math.abs(rem);
  const totalHours = Math.floor(abs / 3600000);
  const m = String(Math.floor((abs % 3600000) / 60000)).padStart(2, '0');
  const s = String(Math.floor((abs % 60000) / 1000)).padStart(2, '0');
  let display;
  if (totalHours >= 24) {
    const d = Math.floor(totalHours / 24);
    const hh = String(totalHours % 24).padStart(2, '0');
    display = `${d}d ${hh}:${m}`;
  } else {
    const h = String(totalHours).padStart(2, '0');
    display = `${h}:${m}:${s}`;
  }
  const color = breached ? '#dc2626' : pct >= 75 ? '#ef4444' : pct >= 50 ? '#f59e0b' : '#16a34a';
  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
        <span style={{ color }}>{ICONS.clock}</span>
        <span style={{ fontSize: 12, fontWeight: 700, color, fontFamily: 'monospace' }}>{breached ? '+' : ''}{display}</span>
        <span style={{ fontSize: 9, color: '#94a3b8', marginLeft: 4 }}>SLA {slaHours}h</span>
      </div>
      <div style={{ background: '#e2e8f0', borderRadius: 4, height: 3, marginTop: 2, overflow: 'hidden', width: 110 }}>
        <div style={{ height: '100%', width: `${pct}%`, background: color, transition: 'width 1s linear' }} />
      </div>
    </div>
  );
}

/* ─── Big trend chart with hover tooltip — KPMG palette ───────────────────── */
const KPMG_BLUE = '#00338D';
const KPMG_LIGHT_BLUE = '#0091DA';
const KPMG_VIOLET = '#6D2077';

// SVG ids must be valid CSS identifiers — strip spaces / punctuation
const safeId = (s) => String(s || 'x').replace(/[^a-zA-Z0-9_-]/g, '_');

function TrendChart({ points, color, kpiName, unit = '', thresholdLow, thresholdHigh, direction, isPrimary }) {
  const [hoverIdx, setHoverIdx] = useState(null);
  const svgRef = useRef(null);
  // Primary line uses KPMG deep blue; siblings use the per-KPI colour from
  // the catalogue (still contrasting between cards thanks to the new palette).
  const lineColor = isPrimary ? KPMG_BLUE : (color || KPMG_LIGHT_BLUE);
  const gid = safeId(kpiName);

  if (!points || points.length === 0) {
    return <div style={{ fontSize: 12, color: '#94a3b8', padding: 16, textAlign: 'center' }}>No data in window.</div>;
  }
  const W = 880, H = 240, padL = 60, padR = 24, padT = 24, padB = 46;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const vals = points.map(p => +p.avg);
  const refValues = [thresholdLow, thresholdHigh].filter(v => v != null && !isNaN(+v)).map(v => +v);
  const allVals = [...vals, ...refValues];
  let yMin = Math.min(...allVals);
  let yMax = Math.max(...allVals);
  if (yMin === yMax) { yMin -= 1; yMax += 1; }
  const yPad = (yMax - yMin) * 0.08;
  yMin -= yPad; yMax += yPad;

  const xAt = i => padL + (i / Math.max(points.length - 1, 1)) * innerW;
  const yAt = v => padT + innerH - ((v - yMin) / (yMax - yMin)) * innerH;

  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${xAt(i)} ${yAt(+p.avg)}`).join(' ');

  const gridY = [];
  for (let g = 0; g <= 4; g++) {
    const v = yMin + (yMax - yMin) * (g / 4);
    gridY.push({ v, y: yAt(v) });
  }

  // X-axis hour labels — only show every 4th hour for 24h windows so they don't overlap
  const xTicks = [];
  const stride = Math.max(1, Math.ceil(points.length / 6));
  for (let i = 0; i < points.length; i += stride) {
    const ts = points[i]?.ts;
    if (!ts) continue;
    const d = new Date(ts);
    xTicks.push({
      x: xAt(i),
      label: `${String(d.getHours()).padStart(2, '0')}:00`,
      sublabel: `${d.getMonth() + 1}/${d.getDate()}`,
    });
  }

  // Coloured bands
  const bands = [];
  if (direction === 'higher_is_better' && thresholdHigh != null && thresholdLow != null) {
    bands.push({ from: yMin, to: +thresholdLow, color: '#fee2e230' });
    bands.push({ from: +thresholdLow, to: +thresholdHigh, color: '#fef3c730' });
  } else if (direction === 'lower_is_better' && thresholdHigh != null && thresholdLow != null) {
    bands.push({ from: +thresholdHigh, to: yMax, color: '#fee2e230' });
    bands.push({ from: +thresholdLow, to: +thresholdHigh, color: '#fef3c730' });
  }

  // Position threshold labels so they don't overlap. Critical (red) goes
  // ABOVE its line; degrade-edge (amber) goes BELOW its line. If the two
  // values are very close, push them apart vertically.
  const yLow = thresholdLow != null ? yAt(+thresholdLow) : null;
  const yHigh = thresholdHigh != null ? yAt(+thresholdHigh) : null;
  const sameThresh = yLow != null && yHigh != null && Math.abs(yLow - yHigh) < 14;

  // Hover handling
  const handleMouseMove = (e) => {
    const rect = svgRef.current.getBoundingClientRect();
    const x = ((e.clientX - rect.left) / rect.width) * W;
    if (x < padL || x > W - padR) { setHoverIdx(null); return; }
    const t = (x - padL) / innerW;
    const idx = Math.max(0, Math.min(points.length - 1, Math.round(t * (points.length - 1))));
    setHoverIdx(idx);
  };

  const last = points[points.length - 1];
  const hover = hoverIdx != null ? points[hoverIdx] : null;
  const hoverDate = hover ? new Date(hover.ts) : null;
  const hoverX = hover ? xAt(hoverIdx) : null;
  const hoverY = hover ? yAt(+hover.avg) : null;

  return (
    <div style={{ position: 'relative' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
        <div style={{ fontSize: 13, fontWeight: 800, color: '#0f172a', display: 'flex', alignItems: 'center', gap: 8 }}>
          {isPrimary && <span style={{ padding: '2px 9px', borderRadius: 10, fontSize: 10, fontWeight: 800, background: KPMG_BLUE, color: '#fff' }}>DEGRADED KPI</span>}
          <span>{kpiName}</span>
          <span style={{ fontSize: 11, color: '#64748b' }}>· last: <b style={{ color: lineColor }}>{(+last.avg).toFixed(2)}{unit}</b></span>
        </div>
        <div style={{ fontSize: 10, color: '#94a3b8' }}>{points.length} hourly points</div>
      </div>
      <svg ref={svgRef} width={W} height={H} viewBox={`0 0 ${W} ${H}`}
           style={{
             display: 'block', width: '100%', height: 'auto', maxWidth: '100%',
             cursor: 'crosshair',
             borderRadius: 12,
             background: '#FFFFFF',
             boxShadow: '0 6px 18px rgba(0,51,141,.10), 0 1px 3px rgba(0,51,141,.05)',
           }}
           onMouseMove={handleMouseMove} onMouseLeave={() => setHoverIdx(null)}>
        <defs>
          <linearGradient id={`bgGrad-${gid}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#F4F8FF" />
            <stop offset="100%" stopColor="#FFFFFF" />
          </linearGradient>
          <linearGradient id={`fillGrad-${gid}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={lineColor} stopOpacity="0.30" />
            <stop offset="100%" stopColor={lineColor} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        <rect x="0" y="0" width={W} height={H} fill={`url(#bgGrad-${gid})`} rx="12" />
        <rect x="0.5" y="0.5" width={W - 1} height={H - 1} fill="none" stroke="#DCE7FB" strokeWidth="1" rx="12" />
        {/* Bands */}
        {bands.map((b, i) => (
          <rect key={i} x={padL} y={Math.min(yAt(b.to), yAt(b.from))}
                width={innerW} height={Math.abs(yAt(b.from) - yAt(b.to))}
                fill={b.color} />
        ))}
        {/* Grid */}
        {gridY.map((g, i) => (
          <g key={i}>
            <line x1={padL} y1={g.y} x2={W - padR} y2={g.y} stroke="#e2e8f0" strokeWidth="1" strokeDasharray="2 4" />
            <text x={padL - 8} y={g.y + 3} textAnchor="end" fontSize="10" fill="#94a3b8">
              {g.v.toFixed(g.v >= 100 ? 0 : 2)}
            </text>
          </g>
        ))}
        {/* Threshold: critical (red dashed line) — label OUTSIDE the chart on the right */}
        {yLow != null && (
          <g>
            <line x1={padL} y1={yLow} x2={W - padR} y2={yLow}
                  stroke="#dc2626" strokeWidth="1.4" strokeDasharray="6 4" opacity="0.85" />
            <text x={W - padR - 4} y={yLow - 4} textAnchor="end" fontSize="10" fontWeight="700" fill="#dc2626">
              critical {(+thresholdLow).toFixed(2)}{unit}
            </text>
          </g>
        )}
        {/* Threshold: degrade edge (amber dashed line) */}
        {yHigh != null && +thresholdHigh !== +thresholdLow && (
          <g>
            <line x1={padL} y1={yHigh} x2={W - padR} y2={yHigh}
                  stroke="#f59e0b" strokeWidth="1.4" strokeDasharray="6 4" opacity="0.85" />
            <text x={W - padR - 4} y={yHigh + (sameThresh ? 14 : -4)} textAnchor="end" fontSize="10" fontWeight="700" fill="#b45309">
              normal/degrade {(+thresholdHigh).toFixed(2)}{unit}
            </text>
          </g>
        )}
        {/* Trend line — gradient area fill */}
        <path d={pathD + ` L ${xAt(points.length - 1)} ${padT + innerH} L ${xAt(0)} ${padT + innerH} Z`}
              fill={`url(#fillGrad-${gid})`} />
        <path d={pathD} fill="none" stroke={lineColor} strokeWidth={isPrimary ? 3 : 2}
              strokeLinejoin="round" strokeLinecap="round"
              style={{ filter: isPrimary ? 'drop-shadow(0 1.5px 2px rgba(0,51,141,.25))' : 'none' }} />
        {/* Points */}
        {points.map((p, i) => (
          <circle key={i} cx={xAt(i)} cy={yAt(+p.avg)} r={isPrimary ? 2.6 : 2} fill={lineColor} />
        ))}
        <circle cx={xAt(points.length - 1)} cy={yAt(+last.avg)} r="4.5" fill="#fff" stroke={lineColor} strokeWidth="2.5" />
        {/* X-axis */}
        <line x1={padL} y1={padT + innerH} x2={W - padR} y2={padT + innerH} stroke="#cbd5e1" strokeWidth="1" />
        {xTicks.map((tk, i) => (
          <g key={i}>
            <line x1={tk.x} y1={padT + innerH} x2={tk.x} y2={padT + innerH + 4} stroke="#cbd5e1" />
            <text x={tk.x} y={padT + innerH + 16} textAnchor="middle" fontSize="10" fill="#64748b">{tk.label}</text>
            <text x={tk.x} y={padT + innerH + 28} textAnchor="middle" fontSize="9" fill="#94a3b8">{tk.sublabel}</text>
          </g>
        ))}
        {/* Hover crosshair */}
        {hover && (
          <g>
            <line x1={hoverX} y1={padT} x2={hoverX} y2={padT + innerH} stroke="#94a3b8" strokeWidth="1" strokeDasharray="3 3" />
            <circle cx={hoverX} cy={hoverY} r="5" fill="#fff" stroke={color} strokeWidth="2.5" />
          </g>
        )}
      </svg>
      {/* Tooltip */}
      {hover && (
        <div style={{
          position: 'absolute',
          left: Math.min(Math.max(hoverX - 70, 0), W - 140),
          top: Math.max(hoverY - 60, 0) + 30,
          background: '#0f172a', color: '#fff',
          fontSize: 11, padding: '6px 10px', borderRadius: 6,
          pointerEvents: 'none', whiteSpace: 'nowrap', zIndex: 5,
          boxShadow: '0 4px 12px rgba(0,0,0,.25)',
        }}>
          <div style={{ fontWeight: 700, color: '#facc15' }}>
            {hoverDate.toLocaleDateString()} {String(hoverDate.getHours()).padStart(2, '0')}:00
          </div>
          <div>{kpiName}: <b>{(+hover.avg).toFixed(3)}{unit}</b></div>
        </div>
      )}
    </div>
  );
}

/* ─── RCA point renderer ─────────────────────────────────────────────────── */
const RCA_HEADERS = [
  'Failure Signature', 'Component-Specific Mechanism', 'Cross-KPI Correlation',
  'Likely Trigger', 'Customer Impact', 'Most Probable Parameter Root Cause',
];
function RcaPoints({ text }) {
  if (!text) return null;
  // Strip residual markdown/symbols
  const clean = text
    .replace(/\*{1,3}/g, '')
    .replace(/^#{1,6}\s*/gm, '')
    .replace(/^[•●◦▪]\s*/gm, '')
    .trim();
  // Try splitting both on newline-prefixed numbers AND on inline numbers.
  let parts = clean.split(/(?=(?:^|\n)\s*\d+\s*[.)]\s)/g)
    .map(s => s.trim())
    .filter(s => /^\d+\s*[.)]/.test(s) && s.length > 4);
  // If only one match, retry with a global scan that ignores newline anchor
  if (parts.length < 2) {
    parts = clean.split(/\s+(?=\d+\s*[.)]\s)/g)
      .map(s => s.trim())
      .filter(s => /^\d+\s*[.)]/.test(s) && s.length > 4);
  }
  // Final fallback: split into paragraphs / blank-line groups and treat each as a point
  if (parts.length < 2) {
    parts = clean.split(/\n{2,}/).map(s => s.trim()).filter(Boolean);
  }
  // Ensure each part has a leading number for renderer, even if not present
  parts = parts.map((p, i) => {
    if (/^\d+\s*[.)]/.test(p)) return p;
    return `${i + 1}. ${p}`;
  });

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {parts.map((part, i) => {
        const m = part.match(/^(\d+)\s*[.)]\s*(.*)$/s);
        const num = m ? m[1] : String(i + 1);
        let body = m ? m[2].trim() : part;
        const headerMatch = body.match(/^([^—:.\n]{4,80})\s*[—:-]\s+(.*)/s);
        const header = headerMatch ? headerMatch[1].trim() : (RCA_HEADERS[+num - 1] || null);
        body = headerMatch ? headerMatch[2].trim() : body;
        return (
          <div key={i} style={{
            background: '#fff', border: '1px solid #e2e8f0', borderLeft: '4px solid #00338D',
            borderRadius: 10, padding: '14px 18px',
            boxShadow: '0 1px 3px rgba(0,51,141,.06)',
          }}>
            <div style={{ display: 'flex', gap: 14 }}>
              <span style={{
                width: 30, height: 30, borderRadius: '50%',
                background: 'linear-gradient(135deg,#00338D,#0091DA)',
                color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 13, fontWeight: 800, flexShrink: 0,
              }}>{num}</span>
              <div style={{ fontSize: 13.5, lineHeight: 1.75, color: '#1f2937', flex: 1 }}>
                {header && (
                  <div style={{
                    fontSize: 11.5, fontWeight: 800, color: '#00338D',
                    textTransform: 'uppercase', letterSpacing: '.8px', marginBottom: 6,
                    display: 'inline-block',
                    background: '#E6F0FF', padding: '3px 10px', borderRadius: 12,
                  }}>{header}</div>
                )}
                <div style={{ whiteSpace: 'pre-wrap' }}>{body}</div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function Modal({ title, onClose, children, width = 1000 }) {
  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
      <div style={{ background: '#fff', borderRadius: 12, width, maxWidth: '95vw', maxHeight: '90vh', overflowY: 'auto', boxShadow: '0 20px 60px rgba(0,0,0,.2)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 22px', borderBottom: '1px solid #e2e8f0', position: 'sticky', top: 0, background: '#fff', zIndex: 1 }}>
          <h3 style={{ margin: 0, fontSize: 16, fontWeight: 800, color: '#0f172a' }}>{title}</h3>
          <button onClick={onClose} style={{ border: 'none', background: '#f1f5f9', borderRadius: 6, width: 28, height: 28, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>{ICONS.x}</button>
        </div>
        <div style={{ padding: 22 }}>{children}</div>
      </div>
    </div>
  );
}

/* ─── Alert Box (per component, opens via the bell) ─────────────────────── */
function AlertBox({ alerts, onAck }) {
  if (!alerts || alerts.length === 0) {
    return <div style={{ padding: 18, color: '#94a3b8', fontSize: 13, textAlign: 'center' }}>No active alerts.</div>;
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {alerts.map(a => (
        <div key={a.id} style={{
          background: '#fff', border: '1px solid #e2e8f0', borderLeft: `4px solid ${a.color || '#f59e0b'}`,
          borderRadius: 8, padding: '12px 14px',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <span style={{ padding: '2px 8px', borderRadius: 10, fontSize: 10, fontWeight: 800, background: a.color + '22', color: a.color }}>
              DEGRADATION
            </span>
            <span style={{ fontSize: 13, fontWeight: 800, color: '#0f172a' }}>{a.component_id} — {a.kpi_name}</span>
            <span style={{ fontSize: 11, color: '#64748b', marginLeft: 'auto' }}>{a.hour_date} · {a.hour_value}:00</span>
          </div>
          <div style={{ fontSize: 12, color: '#475569', marginBottom: 8 }}>{a.forecast_message}</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 11, color: '#475569' }}>Hourly avg: <b>{fmt(a.avg_value, 3)}{a.unit}</b></span>
            <span style={{ fontSize: 11, color: '#dc2626' }}>{ICONS.zap} <b>~{a.forecast_minutes} min</b> to outage</span>
            <button onClick={() => onAck(a)} style={{
              marginLeft: 'auto', padding: '5px 12px', fontSize: 11, fontWeight: 700,
              background: '#16a34a', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer',
              display: 'flex', alignItems: 'center', gap: 4,
            }}>{ICONS.check} Acknowledge</button>
          </div>
        </div>
      ))}
    </div>
  );
}

/* ─── Ticket detail (Trend / RCA / Recommendation / Change Param / Resolve) ── */
function TicketDetail({ ticket, onClose, onMutated }) {
  const [tab, setTab] = useState('trend');
  const [trend, setTrend] = useState(null);
  const [rcaText, setRcaText] = useState(ticket.root_cause || '');
  const [rec, setRec] = useState(() => {
    try { return ticket.recommendation ? JSON.parse(ticket.recommendation) : null; } catch { return null; }
  });
  const [paramList, setParamList] = useState([]);
  const [paramMatchTier, setParamMatchTier] = useState(null);
  const [paramTierLabel, setParamTierLabel] = useState('');
  const [paramSel, setParamSel] = useState({ parameter_group: '', parameter_name: '', current_value: '', proposed_value: '', unit: '', reason: '' });
  const [routePreview, setRoutePreview] = useState(null);
  const [resolveNotes, setResolveNotes] = useState('');
  const [busy, setBusy] = useState('');
  const [msg, setMsg] = useState('');

  // Load trend
  useEffect(() => {
    if (tab !== 'trend' || trend) return;
    apiGet(`/api/core/tickets/${ticket.id}/trend`).then(setTrend).catch(() => setTrend({ series: {} }));
  }, [tab, ticket.id, trend]);

  // Load parameter list + manager/cto routing preview when Change Parameter tab opens
  useEffect(() => {
    if (tab !== 'param') return;
    apiGet(`/api/admin/core-parameters?component_type=${ticket.component_type}&component_id=${encodeURIComponent(ticket.component_id)}&kpi_name=${encodeURIComponent(ticket.kpi_name)}`)
      .then(d => {
        setParamList(d.parameters || []);
        setParamMatchTier(d.match_tier ?? null);
        setParamTierLabel(d.tier_label || '');
      })
      .catch(() => { setParamList([]); setParamMatchTier(null); });
    apiGet(`/api/core/parameter-change/preview-routing?core_ticket_id=${ticket.id}`)
      .then(setRoutePreview)
      .catch(() => setRoutePreview(null));
  }, [tab, ticket.id, ticket.component_type, ticket.component_id, ticket.kpi_name]);

  const runRca = async () => {
    setBusy('rca'); setMsg('');
    try {
      const r = await apiCall(`/api/core/tickets/${ticket.id}/rca`, { method: 'POST' });
      setRcaText(r.root_cause);
    } catch (e) { setMsg(e.message); }
    setBusy('');
  };

  const runRecommendation = async () => {
    setBusy('rec'); setMsg('');
    try {
      const r = await apiCall(`/api/core/tickets/${ticket.id}/recommendation`, { method: 'POST' });
      setRec(r.recommendation);
    } catch (e) { setMsg(e.message); }
    setBusy('');
  };

  const submitChange = async () => {
    if (!paramSel.parameter_name || !paramSel.proposed_value) { setMsg('Pick a parameter and proposed value.'); return; }
    setBusy('change'); setMsg('');
    try {
      await apiPost('/api/core/parameter-change', {
        core_ticket_id: ticket.id,
        component_type: ticket.component_type,
        component_id: ticket.component_id,
        parameter_group: paramSel.parameter_group,
        parameter_name: paramSel.parameter_name,
        current_value: paramSel.current_value,
        proposed_value: paramSel.proposed_value,
        unit: paramSel.unit,
        reason: paramSel.reason,
      });
      setMsg('Change request submitted to manager.');
    } catch (e) { setMsg(e.message); }
    setBusy('');
  };

  const markResolved = async () => {
    if (!window.confirm('Mark this ticket resolved?')) return;
    setBusy('resolve'); setMsg('');
    try {
      await apiPost(`/api/core/tickets/${ticket.id}/resolve`, { notes: resolveNotes });
      onMutated && onMutated();
      onClose();
    } catch (e) { setMsg(e.message); }
    setBusy('');
  };

  const tabs = [
    ['trend', 'Trend Analysis'],
    ['rca', 'Root Cause Analysis'],
    ['rec', 'Final Recommendation'],
    ['param', 'Change Parameter'],
    ['resolve', 'Mark Resolved'],
  ];

  const stripColor = ticket.color || COMP_COLOR[ticket.component_type] || '#64748b';

  return (
    <Modal title={`Core Ticket — ${ticket.reference_number}`} onClose={onClose} width={1100}>
      {/* Header strip */}
      <div style={{ borderLeft: `5px solid ${stripColor}`, padding: '8px 14px', background: '#f8fafc', borderRadius: 6, marginBottom: 14 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <span style={{ padding: '3px 10px', borderRadius: 12, fontSize: 11, fontWeight: 800, background: stripColor + '22', color: stripColor }}>{ticket.component_type}</span>
          <span style={{ fontSize: 14, fontWeight: 800, color: '#0f172a' }}>{ticket.component_id}</span>
          <span style={{ fontSize: 12, color: '#475569' }}>KPI: <b>{ticket.kpi_name}</b></span>
          <span style={{ padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 700, background: PRIORITY_COLOR[ticket.priority] + '22', color: PRIORITY_COLOR[ticket.priority] }}>{ticket.priority}</span>
          <span style={{ fontSize: 11, color: '#dc2626' }}>Avg <b>{fmt(ticket.avg_value, 3)}{ticket.unit}</b> vs threshold {fmt(ticket.threshold_value, 2)}{ticket.unit}</span>
          <span style={{ marginLeft: 'auto' }}>
            <SlaTimer deadline={ticket.sla_deadline} slaHours={ticket.sla_hours} status={ticket.status} />
          </span>
        </div>
        <div style={{ display: 'flex', gap: 14, marginTop: 6, fontSize: 11, color: '#64748b' }}>
          <span>Hour: {ticket.hour_date} · {String(ticket.hour_value).padStart(2, '0')}:00–{String(ticket.hour_value + 1).padStart(2, '0')}:00</span>
          <span>Routed to: <b>{ticket.agent_name || 'Unassigned'}</b> {ticket.agent_email && <span style={{ color: '#94a3b8' }}>({ticket.agent_email})</span>}</span>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 0, borderBottom: '2px solid #e2e8f0', marginBottom: 14 }}>
        {tabs.map(([k, l]) => (
          <button key={k} onClick={() => setTab(k)} style={{
            padding: '8px 16px', fontSize: 12, fontWeight: 700, cursor: 'pointer', border: 'none', background: 'transparent',
            color: tab === k ? '#00338D' : '#64748b', borderBottom: tab === k ? '3px solid #00338D' : '3px solid transparent', marginBottom: -2,
          }}>{l}</button>
        ))}
      </div>

      {msg && <div style={{ padding: '8px 12px', background: '#fef3c7', color: '#92400e', borderRadius: 6, marginBottom: 12, fontSize: 12 }}>{msg}</div>}

      {/* Trend */}
      {tab === 'trend' && (
        <div>
          {!trend ? (
            <div style={{ padding: 30, textAlign: 'center', color: '#64748b' }}>Loading trend…</div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
              {(() => {
                // Render the degraded KPI first, siblings after
                const entries = Object.entries(trend.series || {});
                entries.sort(([a], [b]) => (a === ticket.kpi_name ? -1 : b === ticket.kpi_name ? 1 : 0));
                return entries.map(([kpi, points]) => {
                  const isPrimary = kpi === ticket.kpi_name;
                  const cfg = trend.config?.[kpi] || {};
                  const color = isPrimary ? '#dc2626' : (cfg.color || '#475569');
                  return (
                    <div key={kpi} style={{
                      background: '#fff', border: '1px solid #e2e8f0',
                      borderLeft: isPrimary ? `4px solid ${color}` : '1px solid #e2e8f0',
                      borderRadius: 10, padding: 14,
                      boxShadow: isPrimary ? '0 2px 6px rgba(220,38,38,.08)' : 'none',
                    }}>
                      <TrendChart
                        points={points}
                        color={color}
                        kpiName={kpi}
                        unit={cfg.unit || ''}
                        thresholdLow={cfg.threshold_low}
                        thresholdHigh={cfg.threshold_high}
                        direction={cfg.direction}
                        isPrimary={isPrimary}
                      />
                    </div>
                  );
                });
              })()}
            </div>
          )}
        </div>
      )}

      {/* RCA */}
      {tab === 'rca' && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
            <span style={{ fontSize: 12, color: '#64748b' }}>A senior 4G/5G core network engineer interprets the hourly trend and explains the outage cause.</span>
            <button onClick={runRca} disabled={busy === 'rca'} style={{ padding: '6px 14px', fontSize: 12, fontWeight: 700, background: '#00338D', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
              {busy === 'rca' ? 'Generating…' : (rcaText ? 'Re-run RCA' : 'Run RCA')}
            </button>
          </div>
          {rcaText ? <RcaPoints text={rcaText} /> : (
            <div style={{ padding: 30, textAlign: 'center', color: '#94a3b8', background: '#f8fafc', borderRadius: 8, fontSize: 13 }}>
              Click "Run RCA" to generate the engineer-style analysis.
            </div>
          )}
        </div>
      )}

      {/* Recommendation */}
      {tab === 'rec' && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
            <span style={{ fontSize: 12, color: '#64748b' }}>OpenAI selects the most appropriate core parameter to change.</span>
            <button onClick={runRecommendation} disabled={busy === 'rec'} style={{ padding: '6px 14px', fontSize: 12, fontWeight: 700, background: '#7c3aed', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
              {busy === 'rec' ? 'Thinking…' : 'Generate Recommendation'}
            </button>
          </div>
          {rec ? (
            <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: 14 }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                <Field label="Parameter Group" v={rec.parameter_group} />
                <Field label="Parameter Name" v={rec.parameter_name} />
                <Field label="Current Value" v={`${rec.current_value || ''} ${rec.unit || ''}`} />
                <Field label="Proposed Value" v={`${rec.proposed_value || ''} ${rec.unit || ''}`} highlight />
                <Field label="Direction" v={rec.direction || ''} />
                <Field label="Unit" v={rec.unit || ''} />
              </div>
              {Array.isArray(rec.bullets) && rec.bullets.length > 0 && (
                <div style={{ marginTop: 14, display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {rec.bullets.map((b, i) => (
                    <div key={i} style={{
                      display: 'flex', gap: 10, padding: '10px 14px',
                      background: '#f8fafc', borderLeft: '3px solid #7c3aed', borderRadius: 6,
                    }}>
                      <span style={{
                        flexShrink: 0, width: 22, height: 22, borderRadius: '50%',
                        background: '#7c3aed', color: '#fff', display: 'flex',
                        alignItems: 'center', justifyContent: 'center',
                        fontSize: 11, fontWeight: 800,
                      }}>{i + 1}</span>
                      <div style={{ fontSize: 13, lineHeight: 1.6, color: '#334155', whiteSpace: 'pre-wrap' }}>{b}</div>
                    </div>
                  ))}
                </div>
              )}
              {rec.reason && (
                <div style={{ marginTop: 10, padding: 10, background: '#f1f5f9', borderRadius: 6, fontSize: 12, color: '#475569', lineHeight: 1.6 }}>
                  <b>Summary:</b> {rec.reason}
                </div>
              )}
              <div style={{ marginTop: 10 }}>
                <button onClick={() => { setTab('param'); setParamSel({
                  parameter_group: rec.parameter_group || '',
                  parameter_name: rec.parameter_name || '',
                  current_value: rec.current_value || '',
                  proposed_value: rec.proposed_value || '',
                  unit: rec.unit || '',
                  reason: rec.reason || '',
                }); }} style={{ padding: '6px 14px', fontSize: 12, fontWeight: 700, background: '#16a34a', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
                  Apply via Change Request →
                </button>
              </div>
            </div>
          ) : <div style={{ padding: 24, textAlign: 'center', color: '#94a3b8', background: '#f8fafc', borderRadius: 8 }}>Click "Generate Recommendation".</div>}
        </div>
      )}

      {/* Change Parameter */}
      {tab === 'param' && (
        <div>
          <div style={{ marginBottom: 10, fontSize: 12, color: '#64748b' }}>
            Pick a parameter from the uploaded core parameter set for this component, propose a new value,
            and submit for manager approval.
          </div>
          {routePreview && (
            <div style={{
              marginBottom: 12, padding: '10px 14px', borderRadius: 8,
              background: routePreview.cto_required ? '#fef2f2' : '#eff6ff',
              border: '1px solid ' + (routePreview.cto_required ? '#fecaca' : '#bfdbfe'),
              fontSize: 12,
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <span style={{
                  padding: '2px 8px', borderRadius: 10, fontSize: 10, fontWeight: 800,
                  background: routePreview.cto_required ? '#dc2626' : '#1d4ed8',
                  color: '#fff',
                }}>
                  {(routePreview.change_type || 'standard').toUpperCase()}
                </span>
                <span style={{ color: '#475569', fontWeight: 600 }}>
                  Approval flow: {routePreview.cto_required ? 'Manager → CTO' : 'Manager only'}
                </span>
              </div>
              {routePreview.manager && (
                <div style={{ color: '#0f172a' }}>
                  Routes to manager: <b>{routePreview.manager.name}</b> <span style={{ color: '#64748b' }}>({routePreview.manager.email})</span>
                </div>
              )}
              {routePreview.cto_required && routePreview.cto && (
                <div style={{ color: '#0f172a' }}>
                  Then CTO: <b>{routePreview.cto.name}</b> <span style={{ color: '#64748b' }}>({routePreview.cto.email})</span>
                </div>
              )}
            </div>
          )}
          {paramMatchTier && paramMatchTier > 1 && paramList.length > 0 && (
            <div style={{
              marginBottom: 10, padding: '8px 12px', background: '#fef3c7', border: '1px solid #fde68a',
              borderRadius: 6, fontSize: 11, color: '#92400e', display: 'flex', alignItems: 'center', gap: 8,
            }}>
              <span style={{ fontWeight: 700 }}>Note:</span>
              No parameters were uploaded for the exact ({ticket.component_type}/{ticket.component_id} · "{ticket.kpi_name}") combination.
              Showing widened match: <b>{paramTierLabel}</b> ({paramList.length} params).
            </div>
          )}
          {paramList.length === 0 ? (
            <div style={{ padding: 16, background: '#fef3c7', borderRadius: 6, fontSize: 12, color: '#92400e' }}>
              No parameters uploaded for {ticket.component_type}/{ticket.component_id} yet. Ask admin to upload the Core Parameter sheet.
            </div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <div>
                <label style={{ fontSize: 11, color: '#64748b', fontWeight: 700 }}>Parameter</label>
                <select value={paramSel.parameter_name} onChange={e => {
                  const p = paramList.find(x => x.parameter_name === e.target.value);
                  setParamSel({
                    parameter_group: p?.parameter_group || '',
                    parameter_name: p?.parameter_name || '',
                    current_value: p?.current_value || '',
                    proposed_value: paramSel.proposed_value,
                    unit: p?.unit || '',
                    reason: paramSel.reason,
                  });
                }} style={{ width: '100%', padding: '6px 8px', fontSize: 12, border: '1px solid #cbd5e1', borderRadius: 6, marginTop: 4 }}>
                  <option value="">— Select parameter —</option>
                  {paramList.map(p => <option key={p.id} value={p.parameter_name}>{p.parameter_group ? `[${p.parameter_group}] ` : ''}{p.parameter_name}</option>)}
                </select>
              </div>
              <Input label="Parameter Group" v={paramSel.parameter_group} ro />
              <Input label="Current Value" v={paramSel.current_value} ro />
              <Input label="Proposed Value" v={paramSel.proposed_value} onChange={v => setParamSel(p => ({ ...p, proposed_value: v }))} />
              <Input label="Unit" v={paramSel.unit} ro />
            </div>
          )}
          <div style={{ marginTop: 12 }}>
            <label style={{ fontSize: 11, color: '#64748b', fontWeight: 700 }}>Reason</label>
            <textarea value={paramSel.reason} onChange={e => setParamSel(p => ({ ...p, reason: e.target.value }))}
              style={{ width: '100%', padding: 8, fontSize: 12, border: '1px solid #cbd5e1', borderRadius: 6, marginTop: 4, minHeight: 60 }} />
          </div>
          <button onClick={submitChange} disabled={busy === 'change' || paramList.length === 0}
            style={{ marginTop: 12, padding: '8px 16px', fontSize: 12, fontWeight: 700, background: '#16a34a', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
            {busy === 'change' ? 'Submitting…' : 'Submit Change Request'}
          </button>
        </div>
      )}

      {/* Resolve */}
      {tab === 'resolve' && (
        <div>
          <textarea placeholder="Resolution notes…" value={resolveNotes} onChange={e => setResolveNotes(e.target.value)}
            style={{ width: '100%', padding: 10, fontSize: 12, border: '1px solid #cbd5e1', borderRadius: 6, minHeight: 100 }} />
          <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>
            <button onClick={markResolved} disabled={busy === 'resolve'}
              style={{ padding: '8px 16px', fontSize: 12, fontWeight: 700, background: '#16a34a', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
              {busy === 'resolve' ? 'Resolving…' : 'Mark Resolved'}
            </button>
            <button onClick={onClose}
              style={{ padding: '8px 16px', fontSize: 12, fontWeight: 700, background: '#f1f5f9', color: '#475569', border: '1px solid #cbd5e1', borderRadius: 6, cursor: 'pointer' }}>
              Cancel
            </button>
          </div>
        </div>
      )}
    </Modal>
  );
}

function Field({ label, v, highlight }) {
  return (
    <div>
      <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.5px' }}>{label}</div>
      <div style={{ fontSize: 13, color: highlight ? '#16a34a' : '#0f172a', fontWeight: highlight ? 800 : 600, marginTop: 2 }}>{v || '—'}</div>
    </div>
  );
}
function Input({ label, v, onChange, ro }) {
  return (
    <div>
      <label style={{ fontSize: 11, color: '#64748b', fontWeight: 700 }}>{label}</label>
      <input value={v || ''} readOnly={ro} onChange={e => onChange && onChange(e.target.value)}
        style={{ width: '100%', padding: '6px 8px', fontSize: 12, border: '1px solid #cbd5e1', borderRadius: 6, marginTop: 4, background: ro ? '#f8fafc' : '#fff' }} />
    </div>
  );
}

/* ─── Hourly routing panel — redesigned for clarity ─────────────────────── */
function HourlyRouting({ data, onClose }) {
  const [jobLog, setJobLog] = useState(null);
  const [diag, setDiag] = useState(null);
  const [openHour, setOpenHour] = useState(null); // { date, hour }
  const [hourDetail, setHourDetail] = useState(null);
  const [hourLoading, setHourLoading] = useState(false);
  useEffect(() => {
    apiGet('/api/core/job-log').then(setJobLog).catch(() => setJobLog({ logs: [], activity: [] }));
    apiGet('/api/core/diagnose-today').then(setDiag).catch(() => setDiag(null));
  }, []);
  // Fetch hour detail when an hour card is clicked
  useEffect(() => {
    if (!openHour) { setHourDetail(null); return; }
    setHourLoading(true);
    apiGet(`/api/core/hour-detail?date=${openHour.date}&hour=${openHour.hour}`)
      .then(d => { setHourDetail(d); setHourLoading(false); })
      .catch(() => { setHourDetail(null); setHourLoading(false); });
  }, [openHour]);
  if (!data) return null;

  const fmtDt = iso => iso ? new Date(iso).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '—';
  const logs = jobLog?.logs || [];
  const activity = jobLog?.activity || [];

  // Derived totals
  const totalTickets = COMPONENTS.reduce((s, ct) => s + (data.by_type[ct] || []).length, 0);
  const totalAlertsLogged = logs.reduce((s, l) => s + (l.alerts_created || 0), 0);
  const totalTicketsLogged = logs.reduce((s, l) => s + (l.tickets_created || 0), 0);
  const newToday = activity.filter(a => a.is_new).length;
  const updatedToday = activity.length - newToday;

  // Group job log entries by data date (since data can span past/today/future)
  const logsByDate = logs.reduce((acc, l) => {
    (acc[l.hour_date] = acc[l.hour_date] || []).push(l);
    return acc;
  }, {});
  const dateKeys = Object.keys(logsByDate).sort().reverse();

  // Wall-clock progress: which hour did the scheduler last process today?
  const todayLogs = logs.filter(l => l.hour_date === data.date);
  const lastTodayHour = todayLogs.length ? Math.max(...todayLogs.map(l => l.hour_value)) : null;
  const nowDt = new Date();
  const nowHour = nowDt.getHours();
  const nowMin = nowDt.getMinutes();
  const nextFire = nowMin >= 55 ? `right now (MM:55 has hit for ${nowHour}:00)`
    : `${55 - nowMin} min (at ${nowHour}:55 — will process hour ${nowHour}:00)`;

  return (
    <Modal title={`Hourly Routing — ${data.date}`} onClose={onClose} width={1200}>
      {/* ── Wall-clock progress banner ──────────────────────────────────── */}
      <div style={{
        background: 'linear-gradient(90deg,#00338D,#0091DA)', color: '#fff',
        borderRadius: 8, padding: '12px 16px', marginBottom: 16,
        display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap',
      }}>
        <div>
          <div style={{ fontSize: 11, fontWeight: 700, opacity: .8, textTransform: 'uppercase', letterSpacing: '.5px' }}>Now</div>
          <div style={{ fontSize: 18, fontWeight: 800 }}>{nowDt.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</div>
        </div>
        <div style={{ width: 1, height: 36, background: 'rgba(255,255,255,.3)' }} />
        <div>
          <div style={{ fontSize: 11, fontWeight: 700, opacity: .8, textTransform: 'uppercase', letterSpacing: '.5px' }}>Last hour processed today</div>
          <div style={{ fontSize: 18, fontWeight: 800 }}>{lastTodayHour != null ? `${String(lastTodayHour).padStart(2, '0')}:00` : '—'}</div>
        </div>
        <div style={{ width: 1, height: 36, background: 'rgba(255,255,255,.3)' }} />
        <div>
          <div style={{ fontSize: 11, fontWeight: 700, opacity: .8, textTransform: 'uppercase', letterSpacing: '.5px' }}>Next hourly run in</div>
          <div style={{ fontSize: 14, fontWeight: 700 }}>{nextFire}</div>
        </div>
        <div style={{ marginLeft: 'auto', fontSize: 11, opacity: .9, maxWidth: 360, lineHeight: 1.4 }}>
          The job runs at MM:55 every hour and only processes hours that have already happened in real time. Tickets for future-dated data are NOT created in advance.
        </div>
      </div>

      {/* ── Today's Health Snapshot (verifies "are KPIs healthy or not?") ─ */}
      {diag && diag.by_component && (() => {
        let totalN = 0, totalD = 0, totalO = 0, totalHrs = 0, missing = 0;
        const flags = [];
        for (const ct of Object.keys(diag.by_component)) {
          for (const cid of Object.keys(diag.by_component[ct])) {
            for (const k of diag.by_component[ct][cid]) {
              totalN += k.counts.normal;
              totalD += k.counts.degradation;
              totalO += k.counts.outage;
              totalHrs += k.hours_with_data;
              if (k.counts.outage > 0) flags.push({ ct, cid, kpi: k.kpi, c: k.counts });
              if (k.hours_with_data === 0) missing++;
            }
          }
        }
        const allHealthy = totalD === 0 && totalO === 0 && totalHrs > 0;
        const verdictColor = allHealthy ? '#16a34a' : (totalO > 0 ? '#dc2626' : '#d97706');
        const verdictText = allHealthy
          ? `✓ ALL MONITORED KPIs HEALTHY for today (${totalHrs} hour-readings scanned, all in NORMAL band).`
          : (totalO > 0
              ? `${totalO} hour-reading(s) in OUTAGE band → ticket(s) created. ${totalD} in degradation. ${totalN} normal.`
              : `${totalD} hour-reading(s) in DEGRADATION band → alert(s) created. ${totalN} normal.`);
        return (
          <div style={{
            background: allHealthy ? '#f0fdf4' : (totalO > 0 ? '#fef2f2' : '#fef3c7'),
            border: '1px solid ' + (allHealthy ? '#bbf7d0' : (totalO > 0 ? '#fecaca' : '#fde68a')),
            borderLeft: `4px solid ${verdictColor}`,
            borderRadius: 8, padding: '12px 16px', marginBottom: 16,
            fontSize: 12, color: '#0f172a',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
              <span style={{ fontSize: 13, fontWeight: 800, color: verdictColor }}>Today's KPI Health Verdict</span>
              <span style={{ fontSize: 10, color: '#64748b' }}>(verifies whether "0 tickets" means data is healthy or there's a gap)</span>
            </div>
            <div style={{ fontSize: 13, fontWeight: 600, color: verdictColor, marginBottom: 8 }}>{verdictText}</div>
            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 11, color: '#475569' }}>
              <span>🟢 normal: <b>{totalN}</b></span>
              <span>🟡 degradation: <b>{totalD}</b></span>
              <span>🔴 outage: <b>{totalO}</b></span>
              <span>📊 total hour-readings: <b>{totalHrs}</b></span>
              {missing > 0 && <span style={{ color: '#92400e' }}>⚠ KPIs with no data today: <b>{missing}</b></span>}
            </div>
            {flags.length > 0 && (
              <div style={{ marginTop: 8, fontSize: 11, color: '#7f1d1d' }}>
                <b>Outage hits:</b>{' '}
                {flags.slice(0, 6).map((f, i) => (
                  <span key={i} style={{ display: 'inline-block', padding: '1px 7px', borderRadius: 8, background: '#fee2e2', marginRight: 4 }}>
                    {f.ct}/{f.cid}/{f.kpi} ×{f.c.outage}h
                  </span>
                ))}
                {flags.length > 6 && <span> + {flags.length - 6} more</span>}
              </div>
            )}
          </div>
        );
      })()}

      {/* ── Today's Summary ─────────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 10, marginBottom: 18 }}>
        <SummaryCard label="Tickets routed today" value={totalTickets} color="#00338D"
          hint="Count of ticket records visible on this page (today's data + today's activity)." />
        <SummaryCard label="Tickets created today" value={newToday} color="#16a34a"
          hint="Brand-new tickets the job inserted in the last 24h." />
        <SummaryCard label="Tickets updated today" value={updatedToday} color="#0091DA"
          hint="Existing tickets the dedup logic rolled forward today." />
        <SummaryCard label="Hours processed (recent)" value={logs.length} color="#7c3aed"
          hint="Hours of KPI data the scheduler has scanned (latest 48 entries in core_job_log)." />
      </div>

      {/* ── 1) Job activity by data date ────────────────────────────────── */}
      <SectionHeader color="#00338D" title="Step 1 · Hourly job activity"
        subtitle={`Each card = 1 hour of KPI data. "new" = first-time tickets created at this hour. "upd" = existing tickets rolled forward (same KPI on the same component is still in outage). "alerts" = degradation alerts raised that hour. A 0/0/0 hour means nothing was in the degradation or outage band.`} />
      {dateKeys.length === 0 ? (
        <Empty>No completed hours yet. Click "Run Today's Job" to process.</Empty>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10, marginBottom: 18 }}>
          {dateKeys.map(d => {
            const day = logsByDate[d];
            const dayNew = day.reduce((s, l) => s + (l.tickets_new || l.tickets_created || 0), 0);
            const dayUpd = day.reduce((s, l) => s + (l.tickets_updated || 0), 0);
            const dayA = day.reduce((s, l) => s + (l.alerts_created || 0), 0);
            return (
              <div key={d} style={{ border: '1px solid #e2e8f0', borderRadius: 8 }}>
                <div style={{ padding: '8px 14px', background: '#f8fafc', borderBottom: '1px solid #e2e8f0', display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                  <span style={{ fontSize: 12, fontWeight: 800, color: '#0f172a' }}>Data date {d}</span>
                  <span style={{ fontSize: 11, color: '#64748b' }}>{day.length} hour{day.length !== 1 ? 's' : ''} processed</span>
                  <span style={{ fontSize: 11, color: '#16a34a', fontWeight: 700 }}>+{dayNew} new tickets</span>
                  <span style={{ fontSize: 11, color: '#1d4ed8', fontWeight: 700 }}>+{dayUpd} updated</span>
                  <span style={{ fontSize: 11, color: '#d97706', fontWeight: 700 }}>+{dayA} alerts</span>
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: 10 }}>
                  {day.sort((a, b) => b.hour_value - a.hour_value).map((l, i) => {
                    const newCnt = l.tickets_new ?? l.tickets_created ?? 0;
                    const updCnt = l.tickets_updated ?? 0;
                    const alertCnt = l.alerts_created ?? 0;
                    const noActivity = !newCnt && !updCnt && !alertCnt;
                    const onlyUpdates = !newCnt && !alertCnt && updCnt > 0;
                    const isOpen = openHour && openHour.date === d && openHour.hour === l.hour_value;
                    const clickable = !noActivity;
                    return (
                      <button
                        key={i}
                        type="button"
                        title={clickable ? 'Click to see tickets / alerts created or updated this hour'
                                          : `Hour ${l.hour_value}:00 — no activity (KPIs in normal band)`}
                        disabled={!clickable}
                        onClick={() => clickable && setOpenHour(isOpen ? null : { date: d, hour: l.hour_value })}
                        style={{
                          padding: '6px 10px', fontSize: 10, borderRadius: 6, fontFamily: 'inherit',
                          border: '1px solid ' + (isOpen ? '#00338D' : noActivity ? '#e2e8f0' : newCnt > 0 ? '#bbf7d0' : onlyUpdates ? '#bfdbfe' : '#fde68a'),
                          background: isOpen ? '#dbeafe' : noActivity ? '#ffffff' : newCnt > 0 ? '#f0fdf4' : onlyUpdates ? '#eff6ff' : '#fffbeb',
                          minWidth: 140, textAlign: 'left',
                          cursor: clickable ? 'pointer' : 'default',
                          outline: 'none',
                          boxShadow: isOpen ? '0 0 0 2px rgba(0,51,141,.25)' : 'none',
                          transition: 'background .12s, box-shadow .12s',
                        }}>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                          <span style={{ fontWeight: 800, color: '#0f172a' }}>{String(l.hour_value).padStart(2, '0')}:00</span>
                          {clickable && <span style={{ fontSize: 9, color: isOpen ? '#00338D' : '#94a3b8', fontWeight: 700 }}>{isOpen ? '▲' : '▼'}</span>}
                        </div>
                        <div style={{ fontSize: 10, marginTop: 2 }}>
                          <span style={{ color: newCnt > 0 ? '#16a34a' : '#94a3b8', fontWeight: newCnt > 0 ? 700 : 400 }}>new {newCnt}</span>
                          <span style={{ color: '#94a3b8' }}> · </span>
                          <span style={{ color: updCnt > 0 ? '#1d4ed8' : '#94a3b8', fontWeight: updCnt > 0 ? 700 : 400 }}>upd {updCnt}</span>
                          <span style={{ color: '#94a3b8' }}> · </span>
                          <span style={{ color: alertCnt > 0 ? '#d97706' : '#94a3b8', fontWeight: alertCnt > 0 ? 700 : 400 }}>{alertCnt}A</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
                {/* Detail panel for the selected hour, shown inline under its day */}
                {openHour && openHour.date === d && (
                  <HourDetailPanel
                    detail={hourDetail}
                    loading={hourLoading}
                    date={openHour.date}
                    hour={openHour.hour}
                    onClose={() => setOpenHour(null)}
                  />
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* ── 2) Recent created/updated activity ──────────────────────────── */}
      <SectionHeader color="#0091DA" title="Step 2 · Recent ticket activity (last 24h)"
        subtitle="Every ticket the job touched in the last 24 hours. NEW = first time created. UPDATED = existing ticket rolled forward to a newer hour because the same KPI on the same component is still in outage." />
      {activity.length === 0 ? (
        <Empty>No tickets created or updated in the last 24 hours.</Empty>
      ) : (
        <div style={{ maxHeight: 220, overflowY: 'auto', border: '1px solid #e2e8f0', borderRadius: 6, marginBottom: 18 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead style={{ position: 'sticky', top: 0, background: '#f8fafc', zIndex: 1 }}><tr>
              {['Activity time', 'Action', 'Ref', 'Component', 'KPI', 'Hour of data', 'Priority', 'Routed to'].map(h =>
                <th key={h} style={{ textAlign: 'left', padding: '8px 10px', fontSize: 10, color: '#64748b', fontWeight: 700, borderBottom: '1px solid #e2e8f0' }}>{h}</th>)}
            </tr></thead>
            <tbody>{activity.map((a, i) => (
              <tr key={i} style={{ borderTop: '1px solid #f1f5f9' }}>
                <td style={{ padding: '6px 10px', color: '#475569' }}>{fmtDt(a.updated_at)}</td>
                <td style={{ padding: '6px 10px' }}>
                  <span style={{
                    padding: '2px 8px', borderRadius: 9, fontSize: 9, fontWeight: 800,
                    background: a.is_new ? '#dcfce7' : '#dbeafe',
                    color: a.is_new ? '#16a34a' : '#1d4ed8',
                  }}>{a.is_new ? 'NEW TICKET' : 'UPDATED'}</span>
                </td>
                <td style={{ padding: '6px 10px', fontFamily: 'monospace', fontSize: 10, color: '#00338D' }}>{a.reference_number}</td>
                <td style={{ padding: '6px 10px', fontWeight: 700 }}>
                  <span style={{ padding: '1px 6px', borderRadius: 6, fontSize: 10, background: COMP_COLOR[a.component_type] + '22', color: COMP_COLOR[a.component_type], fontWeight: 800 }}>{a.component_type}</span>
                  <span style={{ marginLeft: 6 }}>{a.component_id}</span>
                </td>
                <td style={{ padding: '6px 10px' }}>{a.kpi_name}</td>
                <td style={{ padding: '6px 10px', color: '#64748b' }}>{a.hour_date} {String(a.hour_value).padStart(2, '0')}:00</td>
                <td style={{ padding: '6px 10px' }}>
                  <span style={{ padding: '1px 7px', borderRadius: 9, fontSize: 9, fontWeight: 700, background: PRIORITY_COLOR[a.priority] + '22', color: PRIORITY_COLOR[a.priority] }}>{a.priority}</span>
                </td>
                <td style={{ padding: '6px 10px' }}>
                  <div style={{ fontWeight: 600, color: '#0f172a' }}>{a.agent_name}</div>
                  {a.agent_email && <div style={{ fontSize: 9, color: '#94a3b8' }}>{a.agent_email}</div>}
                </td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {/* ── 3) Per-component breakdown ──────────────────────────────────── */}
      <SectionHeader color="#7c3aed" title="Step 3 · Today's tickets by component"
        subtitle="The tickets currently visible on the dashboard, grouped by which Core component they affect. Each row shows the assigned agent, the KPI in outage, and the hour of data that triggered the ticket." />
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        {COMPONENTS.map(ct => {
          const items = data.by_type[ct] || [];
          return (
            <div key={ct} style={{ border: '1px solid #e2e8f0', borderRadius: 8 }}>
              <div style={{ padding: '10px 14px', borderBottom: items.length ? '1px solid #e2e8f0' : 'none',
                            background: COMP_COLOR[ct] + '11', display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 4, height: 18, borderRadius: 2, background: COMP_COLOR[ct] }} />
                <span style={{ fontWeight: 800, color: '#0f172a', fontSize: 13 }}>{ct}</span>
                <span style={{ padding: '1px 8px', borderRadius: 10, fontSize: 10, fontWeight: 700,
                               background: items.length ? COMP_COLOR[ct] + '22' : '#f1f5f9',
                               color: items.length ? COMP_COLOR[ct] : '#94a3b8' }}>
                  {items.length} ticket{items.length !== 1 ? 's' : ''}
                </span>
              </div>
              {items.length === 0 ? (
                <div style={{ padding: '10px 14px', fontSize: 12, color: '#94a3b8' }}>
                  No {ct} tickets today — every monitored KPI on every {ct} instance is in the normal band.
                </div>
              ) : (
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead><tr style={{ background: '#f8fafc' }}>
                    {['Ref', 'Instance', 'KPI in outage', 'Hour of data', 'Priority', 'Status', 'Routed to'].map(h =>
                      <th key={h} style={{ textAlign: 'left', padding: '6px 10px', fontSize: 10, color: '#64748b', fontWeight: 700 }}>{h}</th>)}
                  </tr></thead>
                  <tbody>{items.map(t => (
                    <tr key={t.ticket_id} style={{ borderTop: '1px solid #f1f5f9' }}>
                      <td style={{ padding: '6px 10px', fontFamily: 'monospace', fontSize: 11, color: '#00338D' }}>{t.reference_number}</td>
                      <td style={{ padding: '6px 10px', fontWeight: 700, color: '#0f172a' }}>{t.component_id}</td>
                      <td style={{ padding: '6px 10px', color: '#475569' }}>
                        <span style={{ display: 'inline-block', width: 6, height: 6, borderRadius: 3, background: t.color, marginRight: 6 }} />
                        {t.kpi_name}
                      </td>
                      <td style={{ padding: '6px 10px', color: '#64748b' }}>{t.hour_iso ? new Date(t.hour_iso).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : `${String(t.hour).padStart(2, '0')}:00`}</td>
                      <td style={{ padding: '6px 10px' }}><span style={{ padding: '1px 8px', borderRadius: 10, fontSize: 10, fontWeight: 700, background: PRIORITY_COLOR[t.priority] + '22', color: PRIORITY_COLOR[t.priority] }}>{t.priority}</span></td>
                      <td style={{ padding: '6px 10px' }}><span style={{ fontSize: 10, fontWeight: 600, color: '#475569', textTransform: 'capitalize' }}>{t.status}</span></td>
                      <td style={{ padding: '6px 10px' }}>
                        <div style={{ fontWeight: 600, color: '#0f172a' }}>{t.agent_name || 'Unassigned'}</div>
                        {t.agent_email && <div style={{ fontSize: 9, color: '#94a3b8' }}>{t.agent_email}</div>}
                      </td>
                    </tr>
                  ))}</tbody>
                </table>
              )}
            </div>
          );
        })}
      </div>
    </Modal>
  );
}

function SummaryCard({ label, value, color, hint }) {
  return (
    <div title={hint} style={{
      background: '#fff', border: '1px solid #e2e8f0', borderTop: `3px solid ${color}`,
      borderRadius: 8, padding: '12px 14px',
    }}>
      <div style={{ fontSize: 24, fontWeight: 800, color }}>{value}</div>
      <div style={{ fontSize: 11, fontWeight: 600, color: '#64748b', marginTop: 2 }}>{label}</div>
    </div>
  );
}

function SectionHeader({ color, title, subtitle }) {
  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ width: 4, height: 18, background: color, borderRadius: 2 }} />
        <span style={{ fontSize: 13, fontWeight: 800, color: '#0f172a' }}>{title}</span>
      </div>
      {subtitle && (
        <div style={{ fontSize: 11, color: '#64748b', marginTop: 4, marginLeft: 12, lineHeight: 1.5 }}>{subtitle}</div>
      )}
    </div>
  );
}

function Empty({ children }) {
  return (
    <div style={{ padding: '20px 14px', fontSize: 12, color: '#94a3b8', background: '#f8fafc', borderRadius: 6, marginBottom: 18 }}>
      {children}
    </div>
  );
}

/* ─── Inline detail panel for a clicked hour card ──────────────────────── */
function HourDetailPanel({ detail, loading, date, hour, onClose }) {
  if (loading) {
    return (
      <div style={{ borderTop: '1px solid #e2e8f0', padding: '12px 14px', background: '#f8fafc', fontSize: 12, color: '#64748b' }}>
        Loading hour {String(hour).padStart(2, '0')}:00 detail…
      </div>
    );
  }
  if (!detail) return null;
  const t_new = detail.tickets_new || [];
  const t_upd = detail.tickets_updated || [];
  const al = detail.alerts || [];
  const totalRows = t_new.length + t_upd.length + al.length;

  return (
    <div style={{ borderTop: '2px solid #00338D', background: '#fafdff', padding: '14px 16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
        <span style={{ fontSize: 13, fontWeight: 800, color: '#00338D' }}>Hour {String(hour).padStart(2, '0')}:00 — {date}</span>
        <span style={{ fontSize: 11, color: '#64748b' }}>
          {t_new.length} new ticket{t_new.length !== 1 ? 's' : ''} · {t_upd.length} updated · {al.length} alert{al.length !== 1 ? 's' : ''}
        </span>
        <button onClick={onClose} style={{ marginLeft: 'auto', fontSize: 10, fontWeight: 700, padding: '3px 10px', borderRadius: 5, background: '#fff', color: '#475569', border: '1px solid #cbd5e1', cursor: 'pointer' }}>Close</button>
      </div>

      {totalRows === 0 ? (
        <div style={{ fontSize: 12, color: '#94a3b8' }}>No tickets or alerts touched at this hour.</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {/* New tickets */}
          {t_new.length > 0 && (
            <DetailTable title="New tickets created" color="#16a34a" badge="NEW" badgeBg="#dcfce7" badgeColor="#16a34a"
              rows={t_new} columns={['ref', 'component', 'kpi', 'reading', 'priority', 'agent']} />
          )}
          {/* Updated tickets */}
          {t_upd.length > 0 && (
            <DetailTable title="Existing tickets rolled forward" color="#1d4ed8" badge="UPDATED" badgeBg="#dbeafe" badgeColor="#1d4ed8"
              rows={t_upd} columns={['ref', 'component', 'kpi', 'reading', 'priority', 'agent']} />
          )}
          {/* Alerts */}
          {al.length > 0 && (
            <DetailTable title="Degradation alerts" color="#d97706" badge="ALERT" badgeBg="#fef3c7" badgeColor="#d97706"
              rows={al} columns={['component', 'kpi', 'reading', 'forecast', 'agent']} alertMode />
          )}
        </div>
      )}
    </div>
  );
}

function DetailTable({ title, color, badge, badgeBg, badgeColor, rows, columns, alertMode }) {
  const headers = {
    ref: 'Ref',
    component: 'Component',
    kpi: 'KPI',
    reading: 'Reading',
    priority: 'Priority',
    forecast: 'Forecast',
    agent: 'Routed to',
  };
  return (
    <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderLeft: `3px solid ${color}`, borderRadius: 6 }}>
      <div style={{ padding: '6px 12px', display: 'flex', alignItems: 'center', gap: 8, background: badgeBg + '40', borderBottom: '1px solid #e2e8f0' }}>
        <span style={{ padding: '2px 8px', borderRadius: 9, fontSize: 9, fontWeight: 800, background: badgeBg, color: badgeColor }}>{badge}</span>
        <span style={{ fontSize: 12, fontWeight: 700, color: '#0f172a' }}>{title}</span>
        <span style={{ fontSize: 10, color: '#64748b' }}>({rows.length})</span>
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead><tr>
          {columns.map(c => <th key={c} style={{ textAlign: 'left', padding: '6px 10px', fontSize: 10, fontWeight: 700, color: '#64748b', borderBottom: '1px solid #f1f5f9' }}>{headers[c]}</th>)}
        </tr></thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.id} style={{ borderTop: '1px solid #f5f7fa' }}>
              {columns.map(c => {
                if (c === 'ref') return <td key={c} style={{ padding: '6px 10px', fontFamily: 'monospace', fontSize: 10, color: '#00338D' }}>{r.reference_number}</td>;
                if (c === 'component') return (
                  <td key={c} style={{ padding: '6px 10px' }}>
                    <span style={{ padding: '1px 7px', borderRadius: 8, fontSize: 10, fontWeight: 800, background: COMP_COLOR[r.component_type] + '22', color: COMP_COLOR[r.component_type] }}>{r.component_type}</span>
                    <span style={{ marginLeft: 6, fontWeight: 700, color: '#0f172a' }}>{r.component_id}</span>
                  </td>
                );
                if (c === 'kpi') return <td key={c} style={{ padding: '6px 10px', color: '#475569' }}>{r.kpi_name}</td>;
                if (c === 'reading') {
                  const v = r.avg_value;
                  const t = r.threshold_value;
                  return (
                    <td key={c} style={{ padding: '6px 10px', color: '#475569' }}>
                      <b style={{ color: '#dc2626' }}>{v != null ? (+v).toFixed(3) : '—'}{r.unit || ''}</b>
                      {t != null && !alertMode && <span style={{ color: '#94a3b8' }}> vs {(+t).toFixed(2)}{r.unit || ''}</span>}
                    </td>
                  );
                }
                if (c === 'priority') return (
                  <td key={c} style={{ padding: '6px 10px' }}>
                    <span style={{ padding: '1px 7px', borderRadius: 8, fontSize: 10, fontWeight: 700, background: PRIORITY_COLOR[r.priority] + '22', color: PRIORITY_COLOR[r.priority] }}>{r.priority}</span>
                  </td>
                );
                if (c === 'forecast') return (
                  <td key={c} style={{ padding: '6px 10px', fontSize: 10, color: '#92400e' }}>~{r.forecast_minutes} min to outage</td>
                );
                if (c === 'agent') return (
                  <td key={c} style={{ padding: '6px 10px' }}>
                    <div style={{ fontWeight: 700, color: '#0f172a' }}>{r.agent_name || 'Unassigned'}</div>
                    {r.agent_email && <div style={{ fontSize: 9, color: '#94a3b8' }}>{r.agent_email}</div>}
                  </td>
                );
                return <td key={c} />;
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ─── Main page ────────────────────────────────────────────────────────── */
export default function CoreTickets() {
  const { user } = useAuth();
  const [activeComp, setActiveComp] = useState('MME');
  const [alerts, setAlerts] = useState({});       // { MME: [...], SGW: [...], ... }
  const [tickets, setTickets] = useState({});     // same shape
  const [loading, setLoading] = useState(true);
  const [openAlertBox, setOpenAlertBox] = useState(false);
  const [hourly, setHourly] = useState(null);
  const [showHourly, setShowHourly] = useState(false);
  const [activeTicket, setActiveTicket] = useState(null);
  const [running, setRunning] = useState(false);
  const [statusFilter, setStatusFilter] = useState('all');

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [aRes, tRes] = await Promise.all([
        Promise.all(COMPONENTS.map(c => apiGet(`/api/core/alerts?component_type=${c}&only_open=true`).catch(() => ({ alerts: [] })))),
        Promise.all(COMPONENTS.map(c => apiGet(`/api/core/tickets?component_type=${c}`).catch(() => ({ tickets: [] })))),
      ]);
      const aMap = {}, tMap = {};
      COMPONENTS.forEach((c, i) => { aMap[c] = aRes[i].alerts || []; tMap[c] = tRes[i].tickets || []; });
      setAlerts(aMap);
      setTickets(tMap);
    } catch (e) { /* swallow */ }
    setLoading(false);
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  const ackAlert = async (a) => {
    try {
      await apiPost(`/api/core/alerts/${a.id}/ack`, {});
      setAlerts(prev => ({ ...prev, [a.component_type]: prev[a.component_type].filter(x => x.id !== a.id) }));
    } catch (e) { /* ignore */ }
  };

  const fetchHourly = async () => {
    try {
      const d = await apiGet('/api/core/hourly-routing');
      setHourly(d);
      setShowHourly(true);
    } catch (e) { /* ignore */ }
  };

  const runJob = async () => {
    setRunning(true);
    try {
      const r = await apiPost('/api/core/run-job', {});
      await refresh();
      alert(`Job done. Alerts: ${r.alerts_created}, Tickets: ${r.tickets_created}.`);
    } catch (e) { alert(e.message); }
    setRunning(false);
  };

  const runTodaysJob = async () => {
    setRunning(true);
    try {
      const r = await apiPost('/api/core/run-todays-job', { force: true });
      await refresh();
      alert(`Today's job done (${r.date}).\nAlerts: ${r.alerts_created}\nTickets: ${r.tickets_created}\n\n(Watch the terminal for per-hour TICKET CREATED / UPDATED / ALERT CREATED lines.)`);
    } catch (e) { alert(e.message); }
    setRunning(false);
  };

  const cleanFuture = async () => {
    if (!window.confirm('Delete every ticket / alert whose hour is in the future relative to right now?\n\nThis enforces the rule "tickets only exist for hours that have actually happened in real time".')) return;
    try {
      const r = await apiPost('/api/core/cleanup-future', {});
      await refresh();
      alert(`Future cleanup done.\nTickets deleted: ${r.tickets_deleted}\nAlerts deleted: ${r.alerts_deleted}\nLog entries deleted: ${r.log_entries_deleted}`);
    } catch (e) { alert(e.message); }
  };

  const diagnoseToday = async () => {
    try {
      const d = await apiGet('/api/core/diagnose-today');
      const lines = [`Today's KPI Diagnosis — ${d.date}`, ''];
      const types = Object.keys(d.by_component || {});
      if (types.length === 0) {
        lines.push('No monitored data found for today.');
      } else {
        let totalNormal = 0, totalDegrade = 0, totalOutage = 0, totalHrs = 0;
        for (const ct of types) {
          lines.push(`── ${ct} ──`);
          const insts = d.by_component[ct];
          for (const cid of Object.keys(insts)) {
            lines.push(`  ${ct}/${cid}`);
            for (const k of insts[cid]) {
              totalNormal += k.counts.normal;
              totalDegrade += k.counts.degradation;
              totalOutage += k.counts.outage;
              totalHrs += k.hours_with_data;
              const flag = k.counts.outage > 0 ? '🔴' : k.counts.degradation > 0 ? '🟡' : '🟢';
              const lastAvg = k.samples.length ? k.samples[k.samples.length - 1].avg : null;
              lines.push(`    ${flag} ${k.kpi.padEnd(34)} hours=${k.hours_with_data} N=${k.counts.normal} D=${k.counts.degradation} O=${k.counts.outage}` +
                         (lastAvg != null ? `  last=${(+lastAvg).toFixed(3)}${k.unit}` : ''));
            }
          }
          lines.push('');
        }
        lines.push('═'.repeat(60));
        lines.push(`TOTAL hours scanned: ${totalHrs}`);
        lines.push(`  normal: ${totalNormal}   degradation: ${totalDegrade}   outage: ${totalOutage}`);
        lines.push('');
        lines.push('Job log entries for today:');
        for (const j of (d.job_log || [])) {
          lines.push(`  hour ${String(j.hour).padStart(2, '0')}:00 — ${j.tickets_created} tickets, ${j.alerts_created} alerts ${j.finished_at ? '(finished)' : '(pending)'}`);
        }
      }
      const w = window.open('', '_blank', 'width=900,height=700');
      if (w) {
        w.document.write(`<pre style="font-family:Menlo,Consolas,monospace;font-size:12px;line-height:1.6;padding:14px;white-space:pre-wrap">${lines.join('\n').replace(/&/g, '&amp;').replace(/</g, '&lt;')}</pre>`);
        w.document.title = `Diagnose Today — ${d.date}`;
      } else {
        alert(lines.join('\n'));
      }
    } catch (e) { alert(e.message); }
  };

  const runJobRange = async () => {
    const fromD = window.prompt('Backfill FROM date (YYYY-MM-DD):');
    if (!fromD) return;
    const toD = window.prompt('Backfill TO date (YYYY-MM-DD):', fromD);
    if (!toD) return;
    const force = window.confirm('Re-process hours that were already run?\n\nOK = force re-run · Cancel = skip already-finished hours');
    setRunning(true);
    try {
      const r = await apiPost('/api/core/run-job', { from_date: fromD, to_date: toD, force });
      await refresh();
      alert(`Backfill ${r.from_date} → ${r.to_date} done${force ? ' (forced)' : ''}.\nAlerts: ${r.alerts_created}, Tickets: ${r.tickets_created}.`);
    } catch (e) { alert(e.message); }
    setRunning(false);
  };

  const reconcile = async () => {
    if (!window.confirm('Re-evaluate every open ticket/alert against current thresholds and DELETE any whose KPI no longer breaches critical or is no longer monitored?')) return;
    try {
      const r = await apiPost('/api/core/reconcile-tickets', {});
      await refresh();
      alert(`Reconciled.\nTickets: kept=${r.tickets_kept}, deleted=${r.tickets_deleted}\nAlerts: kept=${r.alerts_kept}, deleted=${r.alerts_deleted}`);
    } catch (e) { alert(e.message); }
  };

  const rebuild = async () => {
    if (!window.confirm('REBUILD will:\n1. Reconcile open tickets/alerts vs current thresholds\n2. Wipe core_job_log within data range\n3. Replay every hour with the current thresholds and recreate tickets accordingly\n\nProceed?')) return;
    setRunning(true);
    try {
      const r = await apiPost('/api/core/rebuild-tickets', {});
      await refresh();
      alert(`Rebuild complete.\n${r.rebuild?.from_date} → ${r.rebuild?.to_date}\nNew alerts: ${r.rebuild?.alerts_created}\nNew tickets: ${r.rebuild?.tickets_created}\n\n(Reconcile first deleted ${r.reconciliation?.tickets_deleted} stale tickets, ${r.reconciliation?.alerts_deleted} alerts.)`);
    } catch (e) { alert(e.message); }
    setRunning(false);
  };

  const showCoverage = async () => {
    try {
      const c = await apiGet('/api/core/coverage');
      const lines = [
        `core_component_kpi rows: ${c.rows_total}`,
        `Date range in DB: ${c.min_date || '—'} → ${c.max_date || '—'}`,
        `Job log: ${c.logs_finished}/${c.logs_total} hours finished`,
        c.last_log ? `Last log: ${c.last_log.hour_date} ${c.last_log.hour_value}:00 (a=${c.last_log.alerts_created}, t=${c.last_log.tickets_created})` : 'Last log: none',
        '',
        '── Component-type rows ──',
        ...(c.by_type || []).map(t => `  ${t.component_type}: ${t.rows} rows · ${t.component_ids} components · ${t.min_date} → ${t.max_date}`),
        '',
        '── KPI-name audit (matched? / current avg / band) ──',
      ];
      const audit = c.kpi_audit || [];
      if (audit.length === 0) {
        lines.push('  (no KPI rows found)');
      } else {
        for (const k of audit) {
          const ok = k.matched_in_thresholds ? 'OK' : 'NO MATCH';
          const tag = k.matched_in_thresholds ? (k.avg_band || 'normal') : '—';
          const resolved = k.resolved_to && k.resolved_to !== k.kpi_name ? ` → "${k.resolved_to}"` : '';
          lines.push(`  [${ok}] ${k.component_type} / "${k.kpi_name}"${resolved}  avg=${k.avg_value?.toFixed?.(3) ?? k.avg_value}  band=${tag}  rows=${k.rows}`);
          if (!k.matched_in_thresholds && k.expected_kpi_names) {
            lines.push(`        Expected one of: ${k.expected_kpi_names.join(', ')}`);
          }
        }
      }
      // Show in a roomier window than alert()
      const w = window.open('', '_blank', 'width=980,height=720');
      if (w) {
        w.document.write(`<pre style="font-family:Menlo,Consolas,monospace;font-size:12px;line-height:1.5;padding:14px;white-space:pre-wrap">${lines.map(l => l.replace(/&/g, '&amp;').replace(/</g, '&lt;')).join('\n')}</pre>`);
        w.document.title = 'Core Coverage';
      } else {
        alert(lines.join('\n'));
      }
    } catch (e) { alert(e.message); }
  };

  const compTickets = (tickets[activeComp] || []).filter(t => statusFilter === 'all' || t.status === statusFilter);
  const compAlerts = alerts[activeComp] || [];
  const totalAlerts = Object.values(alerts).reduce((s, arr) => s + arr.length, 0);

  return (
    <div>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 800, color: '#0f172a', display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ width: 4, height: 28, background: COMP_COLOR[activeComp], borderRadius: 2 }} />
            Core Tickets — Predictive Monitoring
          </h2>
          <p style={{ margin: '4px 0 0', fontSize: 12, color: '#64748b' }}>
            MME · SGW · PGW · HSS · PCRF · Hourly job at MM:55
          </p>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {/* Bell with count */}
          <button onClick={() => setOpenAlertBox(v => !v)} style={{
            position: 'relative', padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700,
            background: openAlertBox ? '#00338D' : '#f8fafc', color: openAlertBox ? '#fff' : '#475569',
            border: '1px solid ' + (openAlertBox ? '#00338D' : '#e2e8f0'), cursor: 'pointer',
            display: 'flex', alignItems: 'center', gap: 6,
          }}>
            {ICONS.bell} Alerts
            {totalAlerts > 0 && <span style={{
              minWidth: 18, height: 18, borderRadius: 9, padding: '0 5px',
              background: '#dc2626', color: '#fff', fontSize: 10, fontWeight: 800,
              display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
            }}>{totalAlerts}</span>}
          </button>
          <button onClick={fetchHourly} style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#f8fafc', color: '#475569', border: '1px solid #e2e8f0', cursor: 'pointer' }}>
            Hourly Routing
          </button>
          {(user?.role === 'admin' || user?.role === 'cto' || user?.role === 'manager') && (
            <>
              <button onClick={showCoverage} title="Show what data is available + last job log" style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#f8fafc', color: '#475569', border: '1px solid #e2e8f0', cursor: 'pointer' }}>
                Coverage
              </button>
              <button onClick={runJob} disabled={running} style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#7c3aed', color: '#fff', border: 'none', cursor: 'pointer' }}>
                {running ? 'Running…' : 'Run Hourly Job'}
              </button>
              <button onClick={runTodaysJob} disabled={running} title="Force-replay today's completed hours and (re)create tickets/alerts"
                style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#16a34a', color: '#fff', border: 'none', cursor: 'pointer' }}>
                Run Today's Job
              </button>
              <button onClick={diagnoseToday} title="Show every monitored KPI for today + band classification"
                style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#f59e0b', color: '#fff', border: 'none', cursor: 'pointer' }}>
                Diagnose Today
              </button>
              <button onClick={runJobRange} disabled={running} title="Backfill a specific date range" style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#0EA5E9', color: '#fff', border: 'none', cursor: 'pointer' }}>
                Backfill Range
              </button>
              {(user?.role === 'admin' || user?.role === 'cto') && (
                <>
                  <button onClick={reconcile} title="Re-check open tickets/alerts vs current thresholds; delete those that no longer breach"
                    style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#dc2626', color: '#fff', border: 'none', cursor: 'pointer' }}>
                    Reconcile
                  </button>
                  <button onClick={rebuild} disabled={running} title="Wipe job log + replay all hours with current thresholds (recreates tickets, capped at wall-clock now)"
                    style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#16a34a', color: '#fff', border: 'none', cursor: 'pointer' }}>
                    Rebuild from Data
                  </button>
                  <button onClick={cleanFuture} title="Delete any future-dated tickets / alerts (those whose hour hasn't happened yet)"
                    style={{ padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#92400e', color: '#fff', border: 'none', cursor: 'pointer' }}>
                    Clean Future
                  </button>
                </>
              )}
            </>
          )}
          <button onClick={refresh} style={{ padding: '7px 12px', borderRadius: 8, fontSize: 12, fontWeight: 700, background: '#f8fafc', color: '#475569', border: '1px solid #e2e8f0', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
            {ICONS.refresh} Refresh
          </button>
        </div>
      </div>

      {/* Component tabs */}
      <div style={{ display: 'flex', gap: 0, marginBottom: 14, borderBottom: '2px solid #e2e8f0' }}>
        {COMPONENTS.map(c => {
          const aN = (alerts[c] || []).length;
          const tN = (tickets[c] || []).filter(x => x.status === 'open' || x.status === 'in_progress').length;
          const active = activeComp === c;
          return (
            <button key={c} onClick={() => setActiveComp(c)} style={{
              padding: '10px 18px', fontSize: 13, fontWeight: 800, cursor: 'pointer', border: 'none', background: 'transparent',
              color: active ? COMP_COLOR[c] : '#64748b',
              borderBottom: active ? `3px solid ${COMP_COLOR[c]}` : '3px solid transparent', marginBottom: -2,
              display: 'flex', alignItems: 'center', gap: 6,
            }}>
              {c}
              {tN > 0 && <span style={{ padding: '0 6px', fontSize: 10, fontWeight: 700, background: COMP_COLOR[c] + '22', color: COMP_COLOR[c], borderRadius: 8 }}>{tN}</span>}
              {aN > 0 && <span style={{ padding: '0 6px', fontSize: 10, fontWeight: 700, background: '#dc2626', color: '#fff', borderRadius: 8 }}>{aN} alert{aN !== 1 ? 's' : ''}</span>}
            </button>
          );
        })}
      </div>

      {/* Alert box for current component (toggles via the bell) */}
      {openAlertBox && (
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: 14, marginBottom: 14, boxShadow: '0 4px 14px rgba(0,0,0,.05)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
            <div style={{ fontSize: 13, fontWeight: 800, color: '#0f172a' }}>Alert Box — {activeComp} ({compAlerts.length})</div>
            <span style={{ fontSize: 11, color: '#64748b' }}>Acknowledge to clear from your inbox.</span>
          </div>
          <AlertBox alerts={compAlerts} onAck={ackAlert} />
        </div>
      )}

      {/* Status filter */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 12 }}>
        {['all', 'open', 'in_progress', 'resolved'].map(s => (
          <button key={s} onClick={() => setStatusFilter(s)} style={{
            padding: '5px 12px', fontSize: 11, fontWeight: 700, border: 'none', cursor: 'pointer', borderRadius: 6,
            background: statusFilter === s ? COMP_COLOR[activeComp] : '#f1f5f9',
            color: statusFilter === s ? '#fff' : '#475569',
            textTransform: 'capitalize',
          }}>{s.replace('_', ' ')}</button>
        ))}
        <span style={{ marginLeft: 'auto', fontSize: 11, color: '#64748b' }}>{compTickets.length} tickets</span>
      </div>

      {/* Tickets list */}
      {loading ? (
        <div style={{ padding: 60, textAlign: 'center', color: '#94a3b8' }}>Loading…</div>
      ) : compTickets.length === 0 ? (
        <div style={{ padding: 60, textAlign: 'center', color: '#94a3b8', fontSize: 13, background: '#f8fafc', borderRadius: 8 }}>
          No {activeComp} tickets in this view.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {compTickets.map(t => (
            <div key={t.id} onClick={() => setActiveTicket(t)} style={{
              background: '#fff', border: '1px solid #e2e8f0', borderLeft: `4px solid ${t.color || COMP_COLOR[activeComp]}`,
              borderRadius: 8, padding: '12px 16px', cursor: 'pointer', transition: 'box-shadow .15s',
              display: 'grid', gridTemplateColumns: '170px 1fr 90px 120px 150px',
              alignItems: 'center', gap: 10,
            }} onMouseEnter={e => e.currentTarget.style.boxShadow = '0 3px 10px rgba(0,0,0,.06)'} onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}>
              <div>
                <div style={{ fontSize: 10, fontWeight: 700, color: '#00338D', fontFamily: 'monospace' }}>{t.reference_number}</div>
                <div style={{ fontSize: 14, fontWeight: 800, color: '#0f172a' }}>{t.component_id}</div>
                <div style={{ fontSize: 10, color: '#64748b' }}>Hour {String(t.hour_value).padStart(2, '0')}:00 · {t.hour_date}</div>
              </div>
              <div>
                <div style={{ fontSize: 13, fontWeight: 700, color: '#0f172a' }}>
                  <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: 4, background: t.color, marginRight: 6 }} />
                  {t.kpi_name}
                </div>
                <div style={{ fontSize: 11, color: '#dc2626', marginTop: 2 }}>
                  Avg <b>{fmt(t.avg_value, 3)}{t.unit}</b> vs threshold {fmt(t.threshold_value, 2)}{t.unit}
                </div>
              </div>
              <div style={{ textAlign: 'center' }}>
                <span style={{ padding: '3px 10px', borderRadius: 12, fontSize: 10, fontWeight: 700, background: PRIORITY_COLOR[t.priority] + '22', color: PRIORITY_COLOR[t.priority] }}>{t.priority}</span>
              </div>
              <SlaTimer deadline={t.sla_deadline} slaHours={t.sla_hours} status={t.status} />
              <div style={{ fontSize: 11 }}>
                <div style={{ fontWeight: 700, color: '#0f172a' }}>{t.agent_name || 'Unassigned'}</div>
                <div style={{ fontSize: 10, color: '#94a3b8' }}>{t.agent_email}</div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Modals */}
      {activeTicket && <TicketDetail ticket={activeTicket} onClose={() => setActiveTicket(null)} onMutated={refresh} />}
      {showHourly && <HourlyRouting data={hourly} onClose={() => setShowHourly(false)} />}
    </div>
  );
}
