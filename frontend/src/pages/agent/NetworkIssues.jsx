import { useState, useEffect, useCallback, useRef } from 'react';
import { apiGet, apiCall } from '../../api';
import { useTheme } from '../../ThemeContext';
import CRFormModal from './CRFormModal';

/* ── Icons ──────────────────────────────────────────────────────────────────── */
const IC = {
  clock: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>,
  cpu:   <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg>,
  check: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>,
  tune:  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/></svg>,
  refresh:<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>,
  x:     <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
};

const P_CFG = {
  Critical: { bar: '#dc2626', label: 'Critical' },
  High:     { bar: '#f97316', label: 'High' },
  Medium:   { bar: '#f59e0b', label: 'Medium' },
  Low:      { bar: '#10b981', label: 'Low' },
};

const f = (v, d = 1) => (v == null || isNaN(+v)) ? '—' : (+v).toFixed(d);

/* ── SLA Timer (same as AgentTicketBucket) ──────────────────────────────────── */
function SlaTimer({ deadline, slaHours, status }) {
  const [remaining, setRemaining] = useState(null);
  const [pct, setPct] = useState(0);
  useEffect(() => {
    if (!deadline || status === 'resolved') { setRemaining(null); return; }
    const total = slaHours ? slaHours * 3600000 : null;
    const tick = () => {
      const left = new Date(deadline).getTime() - Date.now();
      setRemaining(left);
      if (total) setPct(Math.min(((total - left) / total) * 100, 100));
    };
    tick();
    const iv = setInterval(tick, 1000);
    return () => clearInterval(iv);
  }, [deadline, slaHours, status]);

  if (status === 'resolved') return <span style={{padding:'2px 10px',borderRadius:12,fontSize:10,fontWeight:700,background:'#dcfce7',color:'#16a34a'}}>Resolved</span>;
  if (remaining === null) return <span style={{fontSize:12,color:'#94a3b8'}}>No SLA</span>;
  const breached = remaining <= 0;
  const color = breached ? '#dc2626' : pct >= 87.5 ? '#ef4444' : pct >= 62.5 ? '#f59e0b' : '#16a34a';
  const abs = Math.abs(remaining);
  const h = String(Math.floor(abs / 3600000)).padStart(2, '0');
  const m = String(Math.floor((abs % 3600000) / 60000)).padStart(2, '0');
  const s = String(Math.floor((abs % 60000) / 1000)).padStart(2, '0');
  return (
    <div>
      <div style={{display:'flex',alignItems:'center',gap:5,marginBottom:3}}>
        <span style={{color}}>{IC.clock}</span>
        <span style={{fontSize:13,fontWeight:700,color,fontFamily:'monospace',letterSpacing:1}}>{breached?'+':''}{h}:{m}:{s}</span>
      </div>
      <div style={{background:'var(--border)',borderRadius:4,height:4,overflow:'hidden',width:120}}>
        <div style={{height:'100%',width:`${Math.min(pct,100)}%`,background:color,borderRadius:4,transition:'width 1s linear'}}/>
      </div>
      <div style={{fontSize:10,color:'#94a3b8',marginTop:2}}>{breached?'SLA Breached':`${Math.round(pct)}% elapsed`}</div>
    </div>
  );
}

/* ── Modal ───────────────────────────────────────────────────────────────────── */
function Modal({ title, onClose, children }) {
  return (
    <div style={{position:'fixed',inset:0,background:'rgba(15,23,42,0.45)',display:'flex',alignItems:'center',justifyContent:'center',zIndex:1000}}>
      <div style={{background:'var(--bg-card)',borderRadius:12,width:900,maxWidth:'95vw',maxHeight:'90vh',overflowY:'auto',boxShadow:'0 20px 60px rgba(0,0,0,.15)'}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'16px 24px',borderBottom:'1px solid var(--border)'}}>
          <h3 style={{margin:0,fontSize:16,fontWeight:700,color:'var(--text)'}}>{title}</h3>
          <button onClick={onClose} style={{border:'none',background:'var(--bg)',borderRadius:6,width:28,height:28,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center'}}>{IC.x}</button>
        </div>
        <div style={{padding:24}}>{children}</div>
      </div>
    </div>
  );
}

/* ── RCA Points Renderer ─────────────────────────────────────────────────────── */
function RcaPoints({ text }) {
  if (!text) return <div style={{color:'#94a3b8',fontSize:12,padding:16}}>Click "Run Analysis" to generate</div>;

  // Clean all markdown symbols
  const clean = text.replace(/\*{1,3}/g, '').replace(/#{1,4}\s*/g, '').replace(/^[•●◦▪]\s*/gm, '').replace(/^[\-─━═]{3,}$/gm, '').trim();

  // Split into numbered points: "1. ...", "2. ...", etc. Works even if they're in one big paragraph
  const parts = clean.split(/(?=(?:^|\n)\s*\d+[\.\)]\s)/);
  const points = [];
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed || trimmed.length < 10) continue;
    // Remove the leading number
    const content = trimmed.replace(/^\d+[\.\)]\s*/, '').trim();
    if (content.length < 10) continue;

    // Try to extract a short title before first period or colon
    let title = null, body = content;
    const colonMatch = content.match(/^([^:.]{5,60})[.:]\s*(.+)/s);
    if (colonMatch) {
      title = colonMatch[1].trim();
      body = colonMatch[2].trim();
    }
    points.push({ title, body });
  }

  // Fallback: if no numbered points found, split by newlines
  if (points.length === 0) {
    clean.split('\n').filter(l => l.trim().length > 15).forEach(line => {
      const l = line.replace(/^\d+[\.\)]\s*/, '').trim();
      const cm = l.match(/^([^:.]{5,60})[.:]\s*(.+)/s);
      points.push({ title: cm ? cm[1].trim() : null, body: cm ? cm[2].trim() : l });
    });
  }

  return (
    <div style={{display:'flex',flexDirection:'column',gap:10}}>
      {points.map((pt, i) => (
        <div key={i} style={{display:'flex',gap:12,padding:'14px 16px',background:'#f8fafc',borderLeft:'3px solid #00338D',borderRadius:6,border:'1px solid #e2e8f0'}}>
          <span style={{width:26,height:26,borderRadius:'50%',background:'#00338D',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontSize:11,fontWeight:800,flexShrink:0,marginTop:2}}>{i+1}</span>
          <div style={{fontSize:12.5,lineHeight:1.75,color:'#334155'}}>
            {pt.title ? <><b style={{color:'#0f172a'}}>{pt.title}:</b> {pt.body}</> : pt.body}
          </div>
        </div>
      ))}
    </div>
  );
}

/* ── Trend Chart (lazy loads recharts) ────────────────────────────────────────── */
function TrendChart({ kpiName, data, color = '#00338D' }) {
  const [RC, setRC] = useState(null);
  useEffect(() => { import('recharts').then(m => setRC(m)); }, []);
  if (!RC || !data?.length) return <div style={{background:'var(--bg)',border:'1px solid var(--border)',borderRadius:8,padding:12,height:180}}>
    <div style={{fontSize:11,fontWeight:700,color:'var(--text-secondary)',marginBottom:6}}>{kpiName}</div>
    <div style={{fontSize:11,color:'#94a3b8'}}>{data?.length?'Loading...':'No data'}</div>
  </div>;
  const { ResponsiveContainer, ComposedChart, Area, Line, XAxis, YAxis, Tooltip, CartesianGrid, ReferenceLine } = RC;
  const vals = data.map(d => Number(d?.avg)).filter(v => Number.isFinite(v));
  const avg = vals.length ? vals.reduce((a,b)=>a+b,0)/vals.length : 0;
  const minV = vals.length ? Math.min(...vals) : 0;
  const maxV = vals.length ? Math.max(...vals) : 0;
  const gid = `nig_${kpiName.replace(/[^a-zA-Z0-9]/g,'')}`;
  // Only flag the most significant drops (>60% below baseline AND in bottom 10% of all values)
  const dropIdx = new Set();
  if (vals.length >= 8) {
    const sorted = [...vals].sort((a,b)=>a-b);
    const threshold10 = sorted[Math.floor(sorted.length * 0.1)] || minV;
    for (let i=4;i<data.length;i++){
      const c=Number(data[i]?.avg);if(!Number.isFinite(c))continue;
      const prior=data.slice(Math.max(0,i-6),i).map(d=>Number(d?.avg)).filter(v=>Number.isFinite(v));
      if(prior.length<4)continue;
      const bl=prior.reduce((a,b)=>a+b,0)/prior.length;
      if(bl===0)continue;
      const dropPct=(bl-c)/Math.abs(bl);
      if(dropPct>0.6 && c<=threshold10)dropIdx.add(i);
    }
    // Max 3 drops per chart
    if(dropIdx.size>3){
      const arr=[...dropIdx].map(i=>{const c=Number(data[i]?.avg)||0;const p=data.slice(Math.max(0,i-6),i).map(d=>Number(d?.avg)).filter(v=>Number.isFinite(v));const bl=p.length?p.reduce((a,b)=>a+b,0)/p.length:1;return{i,sev:Math.abs((bl-c)/Math.abs(bl||1))}}).sort((a,b)=>b.sev-a.sev).slice(0,3);
      dropIdx.clear();arr.forEach(s=>dropIdx.add(s.i));
    }
  }
  const Dot=(p)=>{if(!dropIdx.has(p.index))return null;return<g><circle cx={p.cx} cy={p.cy} r={5} fill="#dc262644"/><circle cx={p.cx} cy={p.cy} r={3} fill="#dc2626" stroke="#fff" strokeWidth={1.5}/></g>;};
  return (
    <div style={{background:'var(--bg-card)',border:'1px solid var(--border)',borderRadius:10,padding:'10px 12px',height:200,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:4}}>
        <span style={{fontSize:11,fontWeight:700,color:'var(--text)'}}>{kpiName}</span>
        {dropIdx.size>0&&<span style={{fontSize:8,padding:'1px 6px',borderRadius:8,background:'#fef2f2',color:'#dc2626',fontWeight:700}}>{dropIdx.size} drop{dropIdx.size>1?'s':''}</span>}
      </div>
      <div style={{display:'flex',gap:10,marginBottom:3,fontSize:9,color:'var(--text-muted)'}}>
        <span>Avg: <b style={{color:'var(--text)'}}>{avg.toFixed(1)}</b></span>
        <span>Min: <b style={{color:'#dc2626'}}>{minV.toFixed(1)}</b></span>
        <span>Max: <b style={{color:'#16a34a'}}>{maxV.toFixed(1)}</b></span>
      </div>
      <ResponsiveContainer width="100%" height={140}>
        <ComposedChart data={data} margin={{top:5,right:5,bottom:0,left:-5}}>
          <defs><linearGradient id={gid} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={color} stopOpacity={.25}/><stop offset="100%" stopColor={color} stopOpacity={.02}/></linearGradient></defs>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false}/>
          <XAxis dataKey="label" tick={{fontSize:7,fill:'#94a3b8'}} axisLine={false} tickLine={false} interval="preserveStartEnd" tickFormatter={v=>v?.length>10?v.slice(5,10):v}/>
          <YAxis tick={{fontSize:8,fill:'#94a3b8'}} axisLine={false} tickLine={false} width={36}/>
          <Tooltip contentStyle={{fontSize:10,borderRadius:8}}/>
          <ReferenceLine y={avg} stroke={color} strokeDasharray="4 3" strokeOpacity={.3}/>
          <Area type="monotone" dataKey="avg" fill={`url(#${gid})`} stroke="none"/>
          <Line type="monotone" dataKey="avg" stroke={color} strokeWidth={2} dot={Dot} activeDot={{r:4,fill:color}}/>
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

/* ── Multi-Site Overlay Trend Chart (target + neighbors) ─────────────────────── */
function MultiTrendChart({ kpiName, series }) {
  // series: [{ site_id, label, color, data: [{label, avg}, ...], isTarget }]
  const [RC, setRC] = useState(null);
  useEffect(() => { import('recharts').then(m => setRC(m)); }, []);
  if (!RC) return <div style={{background:'#f8fafc',border:'1px solid #e2e8f0',borderRadius:8,padding:12,height:210}}><div style={{fontSize:11,fontWeight:700,color:'#475569',marginBottom:6}}>{kpiName}</div><div style={{fontSize:11,color:'#94a3b8'}}>Loading...</div></div>;
  const { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, Legend } = RC;

  // Merge all data points by label
  const labelSet = new Set();
  series.forEach(s => (s.data||[]).forEach(d => labelSet.add(d.label)));
  const labels = [...labelSet].sort();
  const merged = labels.map(l => {
    const row = { label: l };
    series.forEach(s => {
      const hit = (s.data||[]).find(d => d.label === l);
      row[s.site_id] = hit ? hit.avg : null;
    });
    return row;
  });
  const hasData = series.some(s => (s.data||[]).length > 0);
  if (!hasData) return <div style={{background:'#f8fafc',border:'1px solid #e2e8f0',borderRadius:8,padding:12,height:210}}><div style={{fontSize:11,fontWeight:700,color:'#475569',marginBottom:6}}>{kpiName}</div><div style={{fontSize:11,color:'#94a3b8'}}>No data</div></div>;

  return (
    <div style={{background:'#fff',border:'1px solid #e2e8f0',borderRadius:10,padding:'10px 12px',height:230,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
      <div style={{fontSize:11,fontWeight:700,color:'#1e293b',marginBottom:4}}>{kpiName}</div>
      <ResponsiveContainer width="100%" height={195}>
        <LineChart data={merged} margin={{top:5,right:5,bottom:20,left:-5}}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false}/>
          <XAxis dataKey="label" tick={{fontSize:7,fill:'#94a3b8'}} axisLine={false} tickLine={false} interval="preserveStartEnd" tickFormatter={v=>v?.length>10?v.slice(5,10):v}/>
          <YAxis tick={{fontSize:8,fill:'#94a3b8'}} axisLine={false} tickLine={false} width={36}/>
          <Tooltip contentStyle={{fontSize:10,borderRadius:8}}/>
          <Legend wrapperStyle={{fontSize:9,bottom:-5}} iconSize={8}/>
          {series.map(s => (
            <Line key={s.site_id} type="monotone" dataKey={s.site_id} name={s.label}
              stroke={s.color} strokeWidth={s.isTarget ? 2.5 : 1.3}
              strokeDasharray={s.isTarget ? '' : '4 3'}
              dot={false} activeDot={{r:3,fill:s.color}} connectNulls/>
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

/* ── Nearest Sites Map (Leaflet dynamic import) ──────────────────────────────── */
function NearestSitesMap({ target, neighbors }) {
  const [L, setL] = useState(null);
  useEffect(() => { Promise.all([import('leaflet'), import('leaflet/dist/leaflet.css')]).then(([mod])=>setL(mod.default||mod)); }, []);
  const ref = useRef(null);
  const mapRef = useRef(null);
  useEffect(() => {
    if (!L || !ref.current || !target?.latitude || !target?.longitude) return;
    if (mapRef.current) { mapRef.current.remove(); mapRef.current = null; }
    const pts = [[target.latitude, target.longitude], ...neighbors.map(n=>[n.latitude,n.longitude]).filter(p=>p[0]&&p[1])];
    const map = L.map(ref.current).setView([target.latitude, target.longitude], 12);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'&copy; OSM',maxZoom:18}).addTo(map);
    // Target (red, larger)
    L.circleMarker([target.latitude, target.longitude], {radius:11,color:'#dc2626',fillColor:'#dc2626',fillOpacity:.9,weight:2}).addTo(map).bindPopup(`<b>${target.site_id}</b><br/>Overutilized<br/>PRB ${target.avg_prb}%`);
    // Neighbors (green)
    neighbors.forEach(n => {
      if (!n.latitude || !n.longitude) return;
      const color = n.avg_prb >= 80 ? '#f97316' : '#16a34a';
      L.circleMarker([n.latitude, n.longitude], {radius:8,color,fillColor:color,fillOpacity:.85,weight:2}).addTo(map)
        .bindPopup(`<b>${n.site_id}</b><br/>${n.distance_km} km away<br/>PRB ${n.avg_prb}% (spare ${n.spare_capacity_pct}%)<br/>RRC ${n.avg_rrc} users`);
      // Line from target to neighbor with distance label
      L.polyline([[target.latitude,target.longitude],[n.latitude,n.longitude]], {color:'#64748b',weight:1.5,opacity:.5,dashArray:'4 4'}).addTo(map);
    });
    if (pts.length > 1) map.fitBounds(pts, {padding:[30,30]});
    mapRef.current = map;
    return () => { if (mapRef.current) { mapRef.current.remove(); mapRef.current = null; } };
  }, [L, target, neighbors]);

  if (!target?.latitude || !target?.longitude) return <div style={{padding:16,background:'#fef2f2',color:'#991b1b',borderRadius:8,fontSize:12}}>Target site lat/lng missing — cannot render map.</div>;
  return <div ref={ref} style={{height:320,width:'100%',borderRadius:8,border:'1px solid #e2e8f0'}}/>;
}

/* ── AI Diagnosis Modal ──────────────────────────────────────────────────────── */
function AIDiagnosisModal({ ticket, onClose }) {
  const [tab, setTab] = useState('trends');
  const [target, setTarget] = useState('site');
  const [period, setPeriod] = useState('day');
  const [trends, setTrends] = useState(null);
  const [neighborTrends, setNeighborTrends] = useState({});
  const [neighbors, setNeighbors] = useState([]);
  const [trendsLoading, setTrendsLoading] = useState(false);
  const [rca, setRca] = useState({});
  const [rcaLoading, setRcaLoading] = useState(false);
  const [rec, setRec] = useState({});
  const [recLoading, setRecLoading] = useState(false);
  // Nearest-sites state (overutilized only)
  const [nearest, setNearest] = useState(null);
  const [nearestLoading, setNearestLoading] = useState(false);

  // Route to correct API based on ticket type
  const isOU = ticket.isOverutilized;
  const apiBase = isOU ? `/api/network-issues/overutilized/${ticket.id}` : `/api/network-issues/${ticket.id}`;

  const cells = ticket.cells ? ticket.cells : (ticket.cells_affected||'').split(',').filter(Boolean);
  const cellSiteIds = ticket.cell_site_id_list ? ticket.cell_site_id_list : (ticket.cell_site_ids||'').split(',').filter(Boolean);

  // Deterministic neighbor colors
  const NEIGHBOR_COLORS = ['#16a34a', '#0ea5e9', '#f97316', '#7c3aed'];

  const fetchTrends = useCallback(async (t, p) => {
    setTrendsLoading(true);
    try {
      // For overutilized tickets, also pass site_id and include_neighbors=1 so
      // the backend returns {trends, neighbors, neighbor_trends}.
      const extra = isOU ? `&site_id=${encodeURIComponent(ticket.site_id)}&include_neighbors=1` : '';
      const d = await apiGet(`${apiBase}/trends?target=${t}&period=${p}${extra}`);
      setTrends(d.trends || {});
      if (isOU) {
        setNeighborTrends(d.neighbor_trends || {});
        setNeighbors(Array.isArray(d.neighbors) ? d.neighbors : []);
      }
    } catch (_) { setTrends({}); setNeighborTrends({}); setNeighbors([]); }
    setTrendsLoading(false);
  }, [ticket.id, ticket.site_id, isOU, apiBase]);

  useEffect(() => { fetchTrends(target, period); }, [target, period, fetchTrends]);

  // Fetch nearest sites for overutilized tickets (map tab)
  useEffect(() => {
    if (!isOU) return;
    setNearestLoading(true);
    apiGet(`${apiBase}/nearest-sites?site_id=${encodeURIComponent(ticket.site_id)}`)
      .then(d => { setNearest(d); setNearestLoading(false); })
      .catch(() => { setNearest({ target: null, neighbors: [] }); setNearestLoading(false); });
  }, [isOU, apiBase, ticket.site_id]);

  // For overutilized tickets, the 'site' button means "this specific site
  // in the clubbed ticket". Pass site_id explicitly so the backend doesn't
  // mistake the literal "site" string for an actual site identifier.
  const buildBody = (t, extra = {}) => {
    const body = { target: t, ...extra };
    if (isOU) body.site_id = (t === 'site' || !t) ? ticket.site_id : t;
    return body;
  };

  const runRCA = async (t) => {
    setRcaLoading(true);
    try {
      const r = await apiCall(`${apiBase}/rca`, { method: 'POST', body: JSON.stringify(buildBody(t)) });
      setRca(prev => ({ ...prev, [t]: r.root_cause }));
    } catch (_) {}
    setRcaLoading(false);
  };

  const runRec = async (t) => {
    setRecLoading(true);
    try {
      // Auto-run RCA first if not done, so recommendations have context
      let rcaText = rca[t] || '';
      if (!rcaText) {
        const rcaRes = await apiCall(`${apiBase}/rca`, { method: 'POST', body: JSON.stringify(buildBody(t)) });
        rcaText = rcaRes.root_cause || '';
        setRca(prev => ({ ...prev, [t]: rcaText }));
      }
      const r = await apiCall(`${apiBase}/recommendations`, { method: 'POST', body: JSON.stringify(buildBody(t, { root_cause: rcaText })) });
      setRec(prev => ({ ...prev, [t]: r.recommendation }));
    } catch (_) {}
    setRecLoading(false);
  };

  // First button ALWAYS shows the site identifier (site_abs_id → site_id).
  // All subsequent buttons show the Cell Name column (kpi_data.cell_id / cells_affected),
  // falling back to cell_site_id only if cell_name is missing. This applies to
  // Trend Analysis, Root Cause Analysis and Final Recommendations.
  const siteLabel = ticket.site_abs_id || ticket.site_id;
  const TargetButtons = ({ onClick, current }) => (
    <div style={{display:'flex',gap:5,marginBottom:12,flexWrap:'wrap'}}>
      <button onClick={()=>onClick('site')} style={{padding:'5px 14px',borderRadius:16,fontSize:10,fontWeight:700,cursor:'pointer',
        background:current==='site'?'#00338D':'#f1f5f9',color:current==='site'?'#fff':'#475569',border:current==='site'?'none':'1px solid #e2e8f0'}}>
         {siteLabel}
      </button>
      {cells.map((c, i) => {
        const cellLabel = c || cellSiteIds[i] || `Cell ${i+1}`;
        return (
          <button key={c||i} onClick={()=>onClick(c)} style={{padding:'5px 14px',borderRadius:16,fontSize:10,fontWeight:700,cursor:'pointer',
            background:current===c?'#7C3AED':'#f1f5f9',color:current===c?'#fff':'#475569',border:current===c?'none':'1px solid #e2e8f0'}}>
             {cellLabel}
          </button>
        );
      })}
    </div>
  );

  // Tab list — overutilized tickets get an extra "Nearest Sites" tab
  const tabList = isOU
    ? [['nearest','Nearest Sites'],['trends','Trend Analysis'],['rca','Root Cause Analysis'],['rec','Final Recommendations']]
    : [['trends','Trend Analysis'],['rca','Root Cause Analysis'],['rec','Final Recommendations']];

  return (
    <Modal title={`AI Network Diagnosis — ${ticket.site_id}`} onClose={onClose}>
      {/* Tabs */}
      <div style={{display:'flex',gap:4,marginBottom:16,borderBottom:'2px solid #e2e8f0',paddingBottom:8,flexWrap:'wrap'}}>
        {tabList.map(([k,l])=>(
          <button key={k} onClick={()=>setTab(k)} style={{padding:'7px 18px',borderRadius:8,fontSize:12,fontWeight:700,cursor:'pointer',
            background:tab===k?'var(--primary)':'transparent',color:tab===k?'#fff':'var(--text-secondary)',border:'none'}}>{l}</button>
        ))}
      </div>

      {/* Nearest Sites (overutilized only) */}
      {isOU && tab==='nearest' && (
        <div>
          <div style={{fontSize:12,color:'#64748b',marginBottom:10}}>
            Map shows the overutilized site (red) and its 3 nearest neighbours (green/orange by PRB load).
            Use this to decide if traffic can be routed off the overutilized site.
          </div>
          {nearestLoading ? <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Locating nearest sites...</div> : (
            <>
              <NearestSitesMap target={nearest?.target} neighbors={nearest?.neighbors || []}/>
              <div style={{marginTop:12}}>
                <table style={{width:'100%',borderCollapse:'collapse',fontSize:11}}>
                  <thead><tr style={{borderBottom:'1px solid #e2e8f0',background:'#f8fafc'}}>
                    <th style={{textAlign:'left',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>Site</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>Distance</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>PRB %</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>Spare</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>RRC Users</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>Tput</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>BW</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>E-tilt</th>
                    <th style={{textAlign:'center',padding:'6px 8px',fontSize:10,color:'#475569',fontWeight:700}}>EIRP</th>
                  </tr></thead>
                  <tbody>
                    {(nearest?.neighbors || []).map(n => (
                      <tr key={n.site_id} style={{borderBottom:'1px solid #f1f5f9'}}>
                        <td style={{padding:'6px 8px',fontWeight:700,color:'#0f172a'}}>{n.site_id}<div style={{fontSize:9,color:'#94a3b8',fontWeight:500}}>{n.city||n.zone}</div></td>
                        <td style={{textAlign:'center',padding:'6px 8px'}}>{n.distance_km} km</td>
                        <td style={{textAlign:'center',padding:'6px 8px',color:n.avg_prb>=80?'#dc2626':'#16a34a',fontWeight:700}}>{n.avg_prb}%</td>
                        <td style={{textAlign:'center',padding:'6px 8px',color:'#475569'}}>{n.spare_capacity_pct}%</td>
                        <td style={{textAlign:'center',padding:'6px 8px'}}>{n.avg_rrc}</td>
                        <td style={{textAlign:'center',padding:'6px 8px'}}>{n.avg_tput} Mbps</td>
                        <td style={{textAlign:'center',padding:'6px 8px'}}>{n.bandwidth_mhz ?? '—'}</td>
                        <td style={{textAlign:'center',padding:'6px 8px'}}>{n.e_tilt_degree ?? '—'}°</td>
                        <td style={{textAlign:'center',padding:'6px 8px'}}>{n.rf_power_eirp_dbm ?? '—'} dBm</td>
                      </tr>
                    ))}
                    {(!nearest?.neighbors || nearest.neighbors.length===0) && (
                      <tr><td colSpan={9} style={{padding:20,textAlign:'center',color:'#94a3b8'}}>No neighbours found with lat/lng in DB.</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      )}

      {/* Trend Analysis */}
      {tab==='trends'&&(
        <div>
          <TargetButtons onClick={t=>{setTarget(t);}} current={target}/>
          <div style={{display:'flex',gap:5,marginBottom:12,alignItems:'center'}}>
            {['month','week','day','hour'].map(p=>(
              <button key={p} onClick={()=>setPeriod(p)} style={{padding:'4px 12px',borderRadius:14,fontSize:10,fontWeight:700,cursor:'pointer',
                background:period===p?'var(--text)':'var(--bg)',color:period===p?'var(--bg)':'var(--text-muted)',border:period===p?'none':'1px solid var(--border)'}}>
                {p==='day'?'Daily':p==='hour'?'Hourly':p==='week'?'Weekly':'Monthly'}
              </button>
            ))}
            {isOU && neighbors.length > 0 && (
              <div style={{marginLeft:12,display:'flex',gap:10,alignItems:'center',flexWrap:'wrap'}}>
                <span style={{fontSize:10,color:'#64748b',fontWeight:600}}>Overlay:</span>
                <span style={{display:'inline-flex',alignItems:'center',gap:4,fontSize:10,color:'#475569'}}>
                  <span style={{width:16,height:3,background:'#dc2626',display:'inline-block'}}/>{ticket.site_id} (target)
                </span>
                {neighbors.map((n, i) => (
                  <span key={n.site_id} style={{display:'inline-flex',alignItems:'center',gap:4,fontSize:10,color:'#475569'}}>
                    <span style={{width:16,height:2,background:NEIGHBOR_COLORS[i%NEIGHBOR_COLORS.length],borderTop:`1px dashed ${NEIGHBOR_COLORS[i%NEIGHBOR_COLORS.length]}`,display:'inline-block'}}/>
                    {n.site_id} ({n.distance_km}km)
                  </span>
                ))}
              </div>
            )}
          </div>
          {trendsLoading ? <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Loading trends...</div> : (
            isOU && target === 'site' && neighbors.length > 0 ? (
              <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10}}>
                {Object.entries(trends||{}).map(([kpi,data])=>{
                  const series = [
                    { site_id: ticket.site_id, label: `${ticket.site_id} (target)`, color: '#dc2626', data, isTarget: true },
                    ...neighbors.map((n, i) => ({
                      site_id: n.site_id,
                      label: `${n.site_id} (${n.distance_km}km)`,
                      color: NEIGHBOR_COLORS[i%NEIGHBOR_COLORS.length],
                      data: (neighborTrends[n.site_id] || {})[kpi] || [],
                      isTarget: false,
                    })),
                  ];
                  return <MultiTrendChart key={kpi} kpiName={kpi} series={series}/>;
                })}
              </div>
            ) : (
              <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:10}}>
                {Object.entries(trends||{}).map(([kpi,data])=>(
                  <TrendChart key={kpi} kpiName={kpi} data={data} color={target==='site'?'#00338D':'#7C3AED'}/>
                ))}
              </div>
            )
          )}
        </div>
      )}

      {/* Root Cause Analysis */}
      {tab==='rca'&&(
        <div>
          <TargetButtons onClick={t=>{setTarget(t);if(!rca[t])runRCA(t);}} current={target}/>
          {rcaLoading ? <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Analyzing...</div> : (
            <div>
              {!rca[target] && <button onClick={()=>runRCA(target)} style={{padding:'8px 20px',borderRadius:8,fontSize:12,fontWeight:700,background:'var(--primary)',color:'#fff',border:'none',cursor:'pointer',marginBottom:12}}> Run Root Cause Analysis</button>}
              <RcaPoints text={rca[target]}/>
            </div>
          )}
        </div>
      )}

      {/* Final Recommendations */}
      {tab==='rec'&&(
        <div>
          <TargetButtons onClick={t=>{setTarget(t);if(!rec[t])runRec(t);}} current={target}/>
          {recLoading ? <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Generating recommendations...</div> : (
            <div>
              {!rec[target] && <button onClick={()=>runRec(target)} style={{padding:'8px 20px',borderRadius:8,fontSize:12,fontWeight:700,background:'#16a34a',color:'#fff',border:'none',cursor:'pointer',marginBottom:12}}> Get Recommendations</button>}
              <RcaPoints text={rec[target]}/>
            </div>
          )}
        </div>
      )}
    </Modal>
  );
}

/* ── ITIL Pipeline Mini ────────────────────────────────────────────────────── */
const CR_STAGES = ['Created','Validated','Classified','Approved','Implemented','Closed'];
const CR_STAGE_MAP = {created:0,invalid:0,auto_rejected:0,validated:1,classified:2,approved:3,rejected:2,implementing:3,implemented:4,failed:4,rolled_back:4,closed:5};
const CR_STATUS_LABEL = {created:'Pending Validation',invalid:'Validation Rejected',auto_rejected:'Permanently Rejected',validated:'Validated',classified:'Classified',approved:'Approved',rejected:'Approval Rejected',implementing:'Implementing',implemented:'Implemented',failed:'Failed',rolled_back:'Rolled Back',closed:'Closed'};
const CR_STATUS_COLOR = {created:'#00338D',invalid:'#dc2626',auto_rejected:'#991b1b',validated:'#0369a1',classified:'#7c3aed',approved:'#16a34a',rejected:'#dc2626',implementing:'#d97706',implemented:'#15803d',failed:'#dc2626',rolled_back:'#92400e',closed:'#475569'};

function MiniPipeline({ status }) {
  const stage = CR_STAGE_MAP[status] ?? 0;
  const isFail = ['invalid','auto_rejected','rejected','failed'].includes(status);
  return (
    <div style={{display:'flex',alignItems:'center',gap:0,margin:'8px 0'}}>
      {CR_STAGES.map((s,i) => (
        <div key={s} style={{display:'flex',alignItems:'center',flex:1}}>
          <div style={{width:24,height:24,borderRadius:'50%',display:'flex',alignItems:'center',justifyContent:'center',fontSize:9,fontWeight:800,
            background:i<=stage?(isFail&&i===stage?'#dc2626':'var(--primary)'):'var(--border)',color:i<=stage?'#fff':'#94a3b8'}}>{i+1}</div>
          {i<5&&<div style={{flex:1,height:2,background:i<stage?'var(--primary)':'var(--border)'}}/>}
          <div style={{position:'absolute',marginTop:32,fontSize:7,color:'#94a3b8',textTransform:'uppercase',width:60,textAlign:'center',marginLeft:-18}}></div>
        </div>
      ))}
    </div>
  );
}

/* ── Parameter Change Modal (ITIL workflow — same as AgentTicketBucket) ───── */
function ParamChangeModal({ ticket, onClose }) {
  const [proposed, setProposed] = useState('');
  const [impact, setImpact] = useState('');
  const [rollback, setRollback] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [errMsg, setErrMsg] = useState('');
  const [change, setChange] = useState(null);
  const [cr, setCr] = useState(null);
  const [loading, setLoading] = useState(true);
  const [managerInfo, setManagerInfo] = useState(null);

  const loadStatus = useCallback(async () => {
    setLoading(true);
    try {
      const d = await apiGet(`/api/network-issues/${ticket.id}/parameter-change`);
      setChange(d.change || null);
      setCr(d.cr || null);
    } catch (_) {}
    setLoading(false);
  }, [ticket.id]);

  useEffect(() => { loadStatus(); }, [loadStatus]);

  const submit = async () => {
    if (!proposed.trim()) return;
    setSubmitting(true); setErrMsg('');
    try {
      const r = await apiCall(`/api/network-issues/${ticket.id}/parameter-change`, {
        method: 'POST', body: JSON.stringify({ proposed_change: proposed.trim(), impact_assessment: impact.trim(), rollback_plan: rollback.trim() })
      });
      setManagerInfo(r.assigned_manager);
      setSubmitted(true);
      loadStatus();
    } catch (e) { setErrMsg(e?.message || 'Failed to submit'); }
    setSubmitting(false);
  };

  return (
    <Modal title={`Parameter Change Request — ${ticket.site_id}`} onClose={onClose}>
      {/* Success screen */}
      {submitted ? (
        <div style={{textAlign:'center',padding:'24px 16px'}}>
          <div style={{width:64,height:64,borderRadius:'50%',background:'linear-gradient(135deg,#ecfdf5,#d1fae5)',display:'flex',alignItems:'center',justifyContent:'center',fontSize:28,margin:'0 auto 16px',boxShadow:'0 4px 12px rgba(22,163,74,0.2)'}}></div>
          <h3 style={{margin:'0 0 6px',fontSize:17,fontWeight:700,color:'var(--text)'}}>Change Request Raised!</h3>
          <p style={{margin:'0 0 8px',fontSize:13,color:'var(--text-secondary)'}}>
            Routed to <b>{managerInfo ? `Manager ${managerInfo.name}` : 'manager'}</b> for approval.
          </p>
          {managerInfo && <p style={{margin:'0 0 4px',fontSize:11,color:'var(--text-muted)'}}>Email: {managerInfo.email}</p>}
          {cr && <p style={{fontSize:12,color:'var(--primary)',fontFamily:'monospace',fontWeight:700,margin:'8px 0'}}>{cr.cr_number}</p>}
          {cr && <MiniPipeline status={cr.status}/>}
          <p style={{fontSize:11,color:'#94a3b8',margin:'12px 0'}}>Approval deadline: 30% of remaining SLA time</p>
          <button onClick={onClose} style={{padding:'8px 24px',borderRadius:8,fontSize:12,fontWeight:700,background:'var(--primary)',color:'#fff',border:'none',cursor:'pointer',marginTop:8}}>Done</button>
        </div>
      ) : loading ? (
        <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Loading...</div>
      ) : cr ? (
        /* Show existing CR status */
        <div>
          <div style={{background:'var(--bg)',border:'1px solid var(--border)',borderRadius:10,padding:'14px 16px',marginBottom:16}}>
            <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:6}}>
              <span style={{fontFamily:'monospace',fontSize:12,fontWeight:800,color:'var(--primary)'}}>{cr.cr_number}</span>
              <span style={{fontSize:11,fontWeight:700,padding:'2px 8px',borderRadius:4,
                background:`${CR_STATUS_COLOR[cr.status]||'#64748b'}15`,color:CR_STATUS_COLOR[cr.status]||'#64748b',
                border:`1px solid ${CR_STATUS_COLOR[cr.status]||'#64748b'}30`}}>
                {CR_STATUS_LABEL[cr.status] || cr.status}
              </span>
            </div>
            <MiniPipeline status={cr.status}/>
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:6,marginTop:10,fontSize:11}}>
              <div><span style={{color:'#94a3b8'}}>Raised by: </span><b>{cr.raised_by_name || 'Agent'}</b></div>
              <div><span style={{color:'#94a3b8'}}>Created: </span>{cr.created_at ? new Date(cr.created_at).toLocaleString() : '—'}</div>
            </div>
            {change?.approval_deadline && (
              <div style={{fontSize:10,color:'#d97706',marginTop:6,padding:'4px 8px',background:'rgba(245,158,11,0.08)',borderRadius:4}}>
                Approval deadline: {new Date(change.approval_deadline).toLocaleString()}
              </div>
            )}
            {cr.approval_remark && <div style={{fontSize:11,color:'#374151',background:'#f0fdf4',border:'1px solid #bbf7d0',borderRadius:6,padding:'6px 10px',marginTop:8}}><b>Manager Note:</b> {cr.approval_remark}</div>}
          </div>
          {change && (
            <div style={{background:'var(--bg)',borderRadius:8,padding:'10px 14px',border:'1px solid var(--border)'}}>
              <div style={{fontSize:10,color:'#94a3b8',fontWeight:700,textTransform:'uppercase',marginBottom:4}}>Proposed Change</div>
              <div style={{fontSize:12,color:'#334155',lineHeight:1.5}}>{change.proposed_change}</div>
            </div>
          )}
          <button onClick={onClose} style={{marginTop:16,padding:'8px 24px',borderRadius:8,fontSize:12,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:'pointer'}}>Close</button>
        </div>
      ) : (
        /* Submit form */
        <div>
          <div style={{fontSize:12,fontWeight:700,color:'var(--text)',marginBottom:6}}>Propose Parameter Change</div>
          <div style={{fontSize:10,color:'var(--text-muted)',marginBottom:10}}>
            Site: <b>{ticket.site_id}</b> · Cells: {ticket.cell_count} · SLA: {ticket.sla_hours}h · Priority: {ticket.priority}
          </div>
          <label style={{fontSize:11,fontWeight:600,color:'#334155',display:'block',marginBottom:3}}>Proposed Change *</label>
          <textarea value={proposed} onChange={e=>setProposed(e.target.value)} rows={3}
            placeholder="e.g., Increase E-tilt from 3° to 5° for GUR_LTE_0900 to reduce overshooting..."
            style={{width:'100%',padding:'8px 10px',borderRadius:8,border:'1px solid var(--border)',fontSize:12,fontFamily:'inherit',resize:'vertical',outline:'none',boxSizing:'border-box',marginBottom:10}}/>
          <label style={{fontSize:11,fontWeight:600,color:'#334155',display:'block',marginBottom:3}}>Impact Assessment</label>
          <textarea value={impact} onChange={e=>setImpact(e.target.value)} rows={2}
            placeholder="Expected impact on network KPIs..."
            style={{width:'100%',padding:'8px 10px',borderRadius:8,border:'1px solid var(--border)',fontSize:12,fontFamily:'inherit',resize:'vertical',outline:'none',boxSizing:'border-box',marginBottom:10}}/>
          <label style={{fontSize:11,fontWeight:600,color:'#334155',display:'block',marginBottom:3}}>Rollback Plan</label>
          <textarea value={rollback} onChange={e=>setRollback(e.target.value)} rows={2}
            placeholder="Steps to revert if change fails..."
            style={{width:'100%',padding:'8px 10px',borderRadius:8,border:'1px solid var(--border)',fontSize:12,fontFamily:'inherit',resize:'vertical',outline:'none',boxSizing:'border-box',marginBottom:10}}/>
          {errMsg && <div style={{color:'#dc2626',fontSize:11,marginBottom:8}}>{errMsg}</div>}
          <div style={{display:'flex',gap:8}}>
            <button onClick={submit} disabled={submitting||!proposed.trim()}
              style={{padding:'8px 20px',borderRadius:8,fontSize:12,fontWeight:700,background:'var(--primary)',color:'#fff',border:'none',cursor:'pointer'}}>
              {submitting?' Submitting...':'Submit & Route to Manager'}
            </button>
            <button onClick={onClose} style={{padding:'8px 20px',borderRadius:8,fontSize:12,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:'pointer'}}>Cancel</button>
          </div>
          <div style={{fontSize:9,color:'#94a3b8',marginTop:8}}>Approval deadline = 30% of remaining SLA · Routes to least-loaded manager</div>
        </div>
      )}
    </Modal>
  );
}

/* ═══════════════════════════════════════════════════════════════════════════════
   MAIN PAGE
   ═══════════════════════════════════════════════════════════════════════════════ */
export default function NetworkIssues() {
  const { isDark } = useTheme();
  const [tickets, setTickets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('All');
  const [stats, setStats] = useState({});
  const [diagTicket, setDiagTicket] = useState(null);
  const [paramTicket, setParamTicket] = useState(null);
  const [triggering, setTriggering] = useState(false);
  const [resolving, setResolving] = useState(null);
  const [pdfLoading, setPdfLoading] = useState(null);
  const [showRouting, setShowRouting] = useState(false);
  const [routingData, setRoutingData] = useState([]);
  const [routingLoading, setRoutingLoading] = useState(false);
  // Overutilized sites
  const [issueTab, setIssueTab] = useState('worst'); // 'worst' | 'overutilized'
  const [ouTickets, setOuTickets] = useState([]);
  const [ouLoading, setOuLoading] = useState(false);
  const [ouExpanded, setOuExpanded] = useState({});  // {ticketId: true/false}

  /* ── PDF Trend Chart Helper ────────────────────────────────────────────────── */
  const drawTrendChart = (doc, { x, y, w, h, title, points, color = [0, 51, 141] }) => {
    const vals = points.map(p => Number(p?.avg)).filter(v => Number.isFinite(v));
    if (vals.length < 2) return;
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const span = Math.max(max - min, 1e-6);
    const px = 8, py = 8;
    const plotX = x + px, plotY = y + py, plotW = w - px * 2, plotH = h - py * 2;

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

  /* ── PDF RCA/Recommendation Section Helper ─────────────────────────────────── */
  const drawRcaSectionPdf = (doc, sectionTitle, text, startY, pageW) => {
    let y = startY;
    if (y > 220) { doc.addPage(); y = 15; }
    doc.setFontSize(14);
    doc.setFont(undefined, 'bold');
    doc.setTextColor(0, 51, 141);
    doc.text(sectionTitle, 14, y);
    doc.setTextColor(0);
    y += 8;

    const lines = (text || '').split('\n').map(l => l.trim()).filter(Boolean);
    lines.forEach((rawLine, idx) => {
      if (y > 275) { doc.addPage(); y = 15; }
      const stripped = rawLine.replace(/^\d+[\.\)]\s*/, '').replace(/\*\*([^*]+)\*\*/g, '$1').trim();
      const colonIdx = stripped.indexOf(':');
      const hasTitle = colonIdx > 0 && colonIdx < 70;
      const titlePart = hasTitle ? stripped.slice(0, colonIdx).trim() : '';
      const bodyPart = hasTitle ? stripped.slice(colonIdx + 1).trim() : stripped;
      const prefix = `${idx + 1}.  `;

      if (hasTitle && titlePart) {
        doc.setFont(undefined, 'bold');
        doc.setFontSize(10.5);
        doc.setTextColor(15, 23, 42);
        const tw = doc.splitTextToSize(`${prefix}${titlePart}`, pageW - 28);
        doc.text(tw, 14, y);
        y += tw.length * 5.5;
        if (bodyPart) {
          if (y > 278) { doc.addPage(); y = 15; }
          doc.setFont(undefined, 'normal');
          doc.setFontSize(10);
          doc.setTextColor(51, 65, 85);
          const bw = doc.splitTextToSize(bodyPart, pageW - 32);
          doc.text(bw, 18, y);
          y += bw.length * 5.2;
        }
      } else {
        doc.setFont(undefined, 'normal');
        doc.setFontSize(10);
        doc.setTextColor(51, 65, 85);
        const wrapped = doc.splitTextToSize(`${prefix}${bodyPart}`, pageW - 28);
        doc.text(wrapped, 14, y);
        y += wrapped.length * 5.2;
      }
      y += 4;
    });
    return y;
  };

  /* ── Download PDF Report ───────────────────────────────────────────────────── */
  const downloadPdf = async (ticket) => {
    setPdfLoading(ticket.id);
    try {
      const { default: jsPDF } = await import('jspdf');
      const { default: autoTable } = await import('jspdf-autotable');
      const doc = new jsPDF('p', 'mm', 'a4');
      let y = 15;
      const pageW = doc.internal.pageSize.getWidth();

      // Fetch PDF data, trends, RCA, recommendations in parallel
      const isOU = ticket.isOverutilized;
      const pdfBase = isOU ? `/api/network-issues/overutilized/${ticket.id}` : `/api/network-issues/${ticket.id}`;
      const cells = (ticket.cells_affected || '').split(',').filter(Boolean);
      const firstCell = cells[0] || null;
      const [pdfData, siteTrends, cellTrends, siteRca, cellRca] = await Promise.all([
        apiGet(`${pdfBase}/pdf-data`),
        apiGet(`${pdfBase}/trends?target=site&period=day`),
        firstCell ? apiGet(`${pdfBase}/trends?target=${firstCell}&period=day`) : Promise.resolve(null),
        apiCall(`${pdfBase}/rca`, { method: 'POST', body: JSON.stringify(isOU ? { target: 'site', site_id: ticket.site_id } : { target: 'site' }) }).catch(() => null),
        firstCell ? apiCall(`${pdfBase}/rca`, { method: 'POST', body: JSON.stringify({ target: firstCell }) }).catch(() => null) : Promise.resolve(null),
      ]);

      // Get recommendations (needs RCA)
      const siteRcaText = siteRca?.root_cause || pdfData?.ticket?.root_cause || '';
      const cellRcaText = cellRca?.root_cause || '';
      const [siteRec, cellRec] = await Promise.all([
        siteRcaText ? apiCall(`${pdfBase}/recommendations`, { method: 'POST', body: JSON.stringify(isOU ? { target: 'site', site_id: ticket.site_id, root_cause: siteRcaText } : { target: 'site', root_cause: siteRcaText }) }).catch(() => null) : Promise.resolve(null),
        firstCell && cellRcaText ? apiCall(`${pdfBase}/recommendations`, { method: 'POST', body: JSON.stringify({ target: firstCell, root_cause: cellRcaText }) }).catch(() => null) : Promise.resolve(null),
      ]);

      const tkt = pdfData?.ticket || ticket;
      const siteInfo = pdfData?.site_info;
      const cellsInfo = pdfData?.cells_info || [];
      const cellKpis = pdfData?.cell_kpis || [];
      const cellSiteIds = (ticket.cell_site_ids || '').split(',').filter(Boolean);

      // ── Header ──────────────────────────────────────────────────────────────
      doc.setFillColor(0, 51, 141);
      doc.rect(0, 0, pageW, 32, 'F');
      doc.setTextColor(255);
      doc.setFontSize(18);
      doc.text('Network Issue Diagnosis Report', 14, 14);
      doc.setFontSize(10);
      doc.text(`Site: ${tkt.site_id} | ${tkt.category} | Zone: ${tkt.zone || 'N/A'}`, 14, 22);
      doc.text(`Priority: ${tkt.priority?.toUpperCase()} | Generated: ${new Date().toLocaleString()}`, 14, 28);
      y = 40;
      doc.setTextColor(0);

      // ── Section 1: Site Information ──────────────────────────────────────────
      doc.setFontSize(14);
      doc.setFont(undefined, 'bold');
      doc.setTextColor(0, 51, 141);
      doc.text('1. Site Information', 14, y);
      doc.setTextColor(0);
      y += 6;
      doc.setFont(undefined, 'normal');

      if (siteInfo) {
        autoTable(doc, {
          startY: y,
          head: [['Property', 'Value']],
          body: [
            ['Site ID', siteInfo.site_id],
            ['Location', `${siteInfo.latitude?.toFixed(5)}, ${siteInfo.longitude?.toFixed(5)}`],
            ['Province', siteInfo.province || siteInfo.zone || 'N/A'],
            ['City', siteInfo.city || 'N/A'],
            ['State', siteInfo.state || 'N/A'],
            ['Status', siteInfo.site_status || 'on_air'],
            ['Alarms', siteInfo.alarms || 'No active alarms'],
            ['Bandwidth (MHz)', siteInfo.bandwidth_mhz != null ? String(siteInfo.bandwidth_mhz) : 'N/A'],
            ['Antenna Gain (dBi)', siteInfo.antenna_gain_dbi != null ? String(siteInfo.antenna_gain_dbi) : 'N/A'],
            ['RF Power EIRP (dBm)', siteInfo.rf_power_eirp_dbm != null ? String(siteInfo.rf_power_eirp_dbm) : 'N/A'],
            ['Antenna Height AGL (m)', siteInfo.antenna_height_agl_m != null ? String(siteInfo.antenna_height_agl_m) : 'N/A'],
            ['E-Tilt (deg)', siteInfo.e_tilt_degree != null ? String(siteInfo.e_tilt_degree) : 'N/A'],
            ['CRS Gain', siteInfo.crs_gain != null ? String(siteInfo.crs_gain) : 'N/A'],
          ],
          styles: { fontSize: 9 },
          headStyles: { fillColor: [0, 51, 141] },
          columnStyles: { 0: { fontStyle: 'bold', cellWidth: 55 } },
          margin: { left: 14, right: 14 },
        });
        y = (doc.lastAutoTable?.finalY || y) + 10;
      }

      // Site-level KPIs summary
      if (y > 240) { doc.addPage(); y = 15; }
      doc.setFontSize(11);
      doc.setFont(undefined, 'bold');
      doc.setTextColor(0, 51, 141);
      doc.text('Site KPI Summary (7-day avg)', 14, y);
      doc.setTextColor(0);
      y += 5;
      doc.setFont(undefined, 'normal');
      autoTable(doc, {
        startY: y,
        head: [['E-RAB Drop Rate %', 'CSSR %', 'DL Throughput Mbps', 'Avg RRC Users', 'Max RRC Users', 'Revenue (L)']],
        body: [[
          f(tkt.avg_drop_rate, 2) + '%',
          f(tkt.avg_cssr, 1) + '%',
          f(tkt.avg_tput, 1),
          f(tkt.avg_rrc, 0),
          f(tkt.max_rrc, 0),
          f(tkt.revenue_total, 0) + 'L',
        ]],
        styles: { fontSize: 9, halign: 'center' },
        headStyles: { fillColor: [0, 51, 141] },
        margin: { left: 14, right: 14 },
      });
      y = (doc.lastAutoTable?.finalY || y) + 10;

      // ── Section 2: Cell Information ─────────────────────────────────────────
      if (cells.length > 0) {
        if (y > 230) { doc.addPage(); y = 15; }
        doc.setFontSize(14);
        doc.setFont(undefined, 'bold');
        doc.setTextColor(0, 51, 141);
        doc.text(`2. Cell Information (${cells.length} affected cells)`, 14, y);
        doc.setTextColor(0);
        y += 6;
        doc.setFont(undefined, 'normal');

        // Cell KPI table
        if (cellKpis.length > 0) {
          autoTable(doc, {
            startY: y,
            head: [['#', 'Cell Name', 'Cell Site ID', 'Drop Rate %', 'CSSR %', 'DL Tput Mbps', 'RRC Users']],
            body: cellKpis.map((ck, i) => [
              i + 1,
              ck.cell_id,
              cellSiteIds[cells.indexOf(ck.cell_id)] || 'N/A',
              ck.drop_rate + '%',
              ck.cssr + '%',
              ck.tput,
              ck.rrc,
            ]),
            styles: { fontSize: 8.5, halign: 'center' },
            headStyles: { fillColor: [0, 51, 141] },
            columnStyles: { 1: { halign: 'left' }, 2: { halign: 'left' } },
            margin: { left: 14, right: 14 },
          });
          y = (doc.lastAutoTable?.finalY || y) + 8;
        } else {
          // Fallback: just list cell IDs
          autoTable(doc, {
            startY: y,
            head: [['#', 'Cell Name', 'Cell Site ID']],
            body: cells.map((c, i) => [i + 1, c, cellSiteIds[i] || 'N/A']),
            styles: { fontSize: 9 },
            headStyles: { fillColor: [0, 51, 141] },
            margin: { left: 14, right: 14 },
          });
          y = (doc.lastAutoTable?.finalY || y) + 8;
        }

        // Cell RF parameters table (if available)
        const affectedCellsInfo = cellsInfo.filter(ci => cells.includes(ci.cell_id));
        if (affectedCellsInfo.length > 0 && affectedCellsInfo.some(ci => ci.bandwidth_mhz != null)) {
          if (y > 240) { doc.addPage(); y = 15; }
          doc.setFontSize(11);
          doc.setFont(undefined, 'bold');
          doc.text('Cell RF Parameters', 14, y);
          y += 5;
          doc.setFont(undefined, 'normal');
          autoTable(doc, {
            startY: y,
            head: [['Cell Name', 'BW MHz', 'Gain dBi', 'EIRP dBm', 'Height m', 'E-Tilt', 'CRS']],
            body: affectedCellsInfo.map(ci => [
              ci.cell_id,
              ci.bandwidth_mhz ?? 'N/A',
              ci.antenna_gain_dbi ?? 'N/A',
              ci.rf_power_eirp_dbm ?? 'N/A',
              ci.antenna_height_agl_m ?? 'N/A',
              ci.e_tilt_degree ?? 'N/A',
              ci.crs_gain ?? 'N/A',
            ]),
            styles: { fontSize: 8, halign: 'center' },
            headStyles: { fillColor: [100, 116, 139] },
            margin: { left: 14, right: 14 },
          });
          y = (doc.lastAutoTable?.finalY || y) + 10;
        }
      }

      // ── Section 3: Trend Analysis (Site + Cell Daily Charts) ─────────────────
      const sectionNum = cells.length > 0 ? 3 : 2;
      const siteTrendData = siteTrends?.trends || {};
      const cellTrendData = cellTrends?.trends || {};
      const hasTrends = Object.values(siteTrendData).some(d => d?.length > 1) ||
                        Object.values(cellTrendData).some(d => d?.length > 1);

      if (hasTrends) {
        if (y > 220) { doc.addPage(); y = 15; }
        doc.setFontSize(14);
        doc.setFont(undefined, 'bold');
        doc.setTextColor(0, 51, 141);
        doc.text(`${sectionNum}. Trend Analysis (Daily KPI Charts)`, 14, y);
        doc.setTextColor(0);
        y += 6;
        doc.setFont(undefined, 'normal');

        for (const [level, data] of [['Site', siteTrendData], ['Cell', cellTrendData]]) {
          const chartEntries = Object.entries(data).filter(([, pts]) => Array.isArray(pts) && pts.length > 1);
          if (chartEntries.length === 0) continue;

          if (y > 245) { doc.addPage(); y = 15; }
          doc.setFontSize(11);
          doc.setFont(undefined, 'bold');
          doc.setTextColor(30, 41, 59);
          doc.text(`${level} Level - Daily${level === 'Cell' && firstCell ? ` (${cellSiteIds[0] || firstCell})` : ''}`, 14, y);
          doc.setTextColor(0);
          y += 5;
          doc.setFont(undefined, 'normal');

          const chartW = (pageW - 14 * 2 - 6) / 2;
          const chartH = 34;
          for (let idx = 0; idx < chartEntries.length; idx += 2) {
            if (y + chartH > 285) { doc.addPage(); y = 15; }
            const [name1, pts1] = chartEntries[idx];
            drawTrendChart(doc, { x: 14, y, w: chartW, h: chartH, title: name1, points: pts1, color: level === 'Cell' ? [124, 58, 237] : [0, 51, 141] });
            if (chartEntries[idx + 1]) {
              const [name2, pts2] = chartEntries[idx + 1];
              drawTrendChart(doc, { x: 14 + chartW + 6, y, w: chartW, h: chartH, title: name2, points: pts2, color: level === 'Cell' ? [124, 58, 237] : [0, 51, 141] });
            }
            y += chartH + 5;
          }
          y += 3;
        }
      }

      // ── Section 4: Root Cause Analysis (Site + Cell) ─────────────────────────
      const rcaSectionNum = sectionNum + 1;
      if (siteRcaText || cellRcaText) {
        if (siteRcaText) {
          y = drawRcaSectionPdf(doc, `${rcaSectionNum}. Root Cause Analysis - Site (${tkt.site_id})`, siteRcaText, y, pageW);
          y += 4;
        }
        if (cellRcaText) {
          y = drawRcaSectionPdf(doc, `${rcaSectionNum}${siteRcaText ? 'b' : ''}. Root Cause Analysis - Cell (${cellSiteIds[0] || firstCell})`, cellRcaText, y, pageW);
          y += 4;
        }
      }

      // ── Section 5: Recommendations (Site + Cell) ────────────────────────────
      const recSectionNum = rcaSectionNum + 1;
      const siteRecText = siteRec?.recommendation || pdfData?.ticket?.recommendation || '';
      const cellRecText = cellRec?.recommendation || '';
      if (siteRecText || cellRecText) {
        if (siteRecText) {
          y = drawRcaSectionPdf(doc, `${recSectionNum}. Recommendations - Site (${tkt.site_id})`, siteRecText, y, pageW);
          y += 4;
        }
        if (cellRecText) {
          y = drawRcaSectionPdf(doc, `${recSectionNum}${siteRecText ? 'b' : ''}. Recommendations - Cell (${cellSiteIds[0] || firstCell})`, cellRecText, y, pageW);
        }
      }

      doc.save(`NetworkIssue_${tkt.site_id}_${tkt.id}.pdf`);
      alert('Report has been downloaded successfully');
    } catch (e) {
      console.error('PDF generation failed:', e);
      alert('PDF generation failed: ' + e.message);
    }
    setPdfLoading(null);
  };

  const fetchAll = useCallback(async () => {
    try {
      const [tData, sData] = await Promise.all([
        apiGet('/api/network-issues/list'),
        apiGet('/api/network-issues/stats'),
      ]);
      setTickets(tData.tickets || []);
      setStats(sData);
    } catch (_) {}
    setLoading(false);
  }, []);

  const fetchOverutilized = useCallback(async () => {
    setOuLoading(true);
    try {
      const d = await apiGet('/api/network-issues/overutilized/list');
      setOuTickets(d.tickets || []);
    } catch (_) {}
    setOuLoading(false);
  }, []);

  useEffect(() => { fetchAll(); fetchOverutilized(); }, [fetchAll, fetchOverutilized]);
  useEffect(() => { const iv = setInterval(() => { fetchAll(); fetchOverutilized(); }, 30000); return () => clearInterval(iv); }, [fetchAll, fetchOverutilized]);

  const triggerJob = async () => {
    setTriggering(true);
    try { await apiCall('/api/network-issues/trigger-job', { method: 'POST' }); fetchAll(); } catch (_) {}
    setTriggering(false);
  };

  const markResolved = async (id) => {
    setResolving(id);
    try { await apiCall(`/api/network-issues/${id}/status`, { method: 'PUT', body: JSON.stringify({ status: 'resolved' }) }); fetchAll(); } catch (_) {}
    setResolving(null);
  };

  // Only show tickets assigned to this agent
  const myTickets = tickets.filter(t => t.is_mine);
  const myOuTickets = ouTickets.filter(t => t.is_mine);

  const filtered = filter === 'All' ? myTickets
    : filter === 'Pending' ? myTickets.filter(t => t.status === 'open')
    : filter === 'In Progress' ? myTickets.filter(t => t.status === 'in_progress')
    : myTickets.filter(t => t.status === 'resolved');

  const openCount = myTickets.filter(t => t.status === 'open').length;
  const resolvedCount = myTickets.filter(t => t.status === 'resolved').length;

  const fetchRouting = async () => {
    setRoutingLoading(true);
    try {
      const d = await apiGet('/api/network-issues/todays-routing');
      setRoutingData(d.routing || []);
    } catch (_) {}
    setRoutingLoading(false);
    setShowRouting(true);
  };

  const fetchLogs = async () => {
    setLogsLoading(true);
    try {
      const d = await apiGet('/api/network-issues/logs');
      setLogsData(d.logs || []);
    } catch (_) {}
    setLogsLoading(false);
    setShowLogs(true);
  };

  if (loading) return <div style={{display:'flex',alignItems:'center',justifyContent:'center',height:400}}><div className="spinner"/></div>;

  return (
    <div>
      {/* Issue Type Tabs */}
      <div style={{display:'flex',gap:0,marginBottom:16,borderBottom:'2px solid #e2e8f0'}}>
        <button onClick={()=>setIssueTab('worst')} style={{padding:'10px 24px',fontSize:13,fontWeight:700,cursor:'pointer',border:'none',background:'transparent',
          color:issueTab==='worst'?'#00338D':'#94a3b8',borderBottom:issueTab==='worst'?'3px solid #00338D':'3px solid transparent',marginBottom:-2}}>
          Worst Cell Offenders ({myTickets.length})
        </button>
        <button onClick={()=>setIssueTab('overutilized')} style={{padding:'10px 24px',fontSize:13,fontWeight:700,cursor:'pointer',border:'none',background:'transparent',
          color:issueTab==='overutilized'?'#E65100':'#94a3b8',borderBottom:issueTab==='overutilized'?'3px solid #E65100':'3px solid transparent',marginBottom:-2}}>
          Overutilized Sites ({myOuTickets.length})
        </button>
      </div>

      {/* Header */}
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16}}>
        <div>
          <h2 style={{margin:0,fontSize:20,fontWeight:800,color:'#0f172a',display:'flex',alignItems:'center',gap:8}}>
            <span style={{width:4,height:28,background:issueTab==='worst'?'#00338D':'#E65100',borderRadius:2,display:'inline-block'}}/>
            {issueTab==='worst' ? 'Worst Cell Offenders' : 'Overutilized Sites (PRB > 92%)'}
          </h2>
          <p style={{margin:'4px 0 0',fontSize:12,color:'#64748b'}}>
            {issueTab==='worst' ? `${openCount} open · ${resolvedCount} resolved · ${myTickets.length} total` : `${myOuTickets.filter(t=>t.status==='open').length} open · ${myOuTickets.filter(t=>t.status==='resolved').length} resolved · ${myOuTickets.length} assigned to you`}
          </p>
        </div>
        <div style={{display:'flex',gap:8}}>
          <button onClick={fetchLogs} style={{display:'flex',alignItems:'center',gap:5,padding:'7px 14px',borderRadius:8,fontSize:12,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:'pointer'}}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
            System Logs
          </button>
          <button onClick={fetchRouting} style={{display:'flex',alignItems:'center',gap:5,padding:'7px 14px',borderRadius:8,fontSize:12,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:'pointer'}}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg>
            Today's Routing
          </button>
          <button onClick={triggerJob} disabled={triggering} style={{display:'flex',alignItems:'center',gap:5,padding:'7px 16px',borderRadius:8,fontSize:12,fontWeight:600,background:'var(--primary)',color:'#fff',border:'none',cursor:'pointer'}}>
            {triggering?' Scanning...':' Refresh Worst Cells'}
          </button>
          <button onClick={fetchAll} style={{display:'flex',alignItems:'center',gap:5,padding:'7px 12px',borderRadius:8,fontSize:12,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:'pointer'}}>
            {IC.refresh} Refresh
          </button>
        </div>
      </div>

      {/* ── OVERUTILIZED SITES TAB ── */}
      {issueTab === 'overutilized' && (
        <div>
          {ouLoading ? <div style={{textAlign:'center',padding:40}}><div className="spinner"/></div> :
           myOuTickets.length === 0 ? (
            <div style={{textAlign:'center',padding:40,color:'#94a3b8',fontSize:14}}>
              No overutilized tickets assigned to you. Check Today's Routing for all active tickets.
            </div>
           ) : (
            <div style={{display:'flex',flexDirection:'column',gap:14}}>
              {myOuTickets.map(t => {
                const pc = t.priority==='Critical'?{bg:'#FEE2E2',bar:'#DC2626',text:'#991B1B'}:t.priority==='High'?{bg:'#FEF3C7',bar:'#F59E0B',text:'#92400E'}:t.priority==='Medium'?{bg:'#DBEAFE',bar:'#3B82F6',text:'#1E40AF'}:{bg:'#F1F5F9',bar:'#94A3B8',text:'#475569'};
                const sites = t.sites || [];
                const siteStatuses = t.site_statuses || {};
                const resolvedCount = Object.values(siteStatuses).filter(v=>v==='resolved').length;
                const expanded = ouExpanded[t.id] || false;
                const toggleExpand = () => setOuExpanded(prev => ({...prev, [t.id]: !prev[t.id]}));
                return (
                  <div key={t.id} style={{border:'1px solid #e2e8f0',borderLeft:`3px solid ${pc.bar}`,borderRadius:8,background:'#fff',overflow:'hidden'}}>
                    {/* Compact header — single row */}
                    <div style={{padding:'8px 12px',display:'flex',alignItems:'center',gap:8,flexWrap:'wrap',cursor:'pointer'}} onClick={toggleExpand}>
                      <span style={{fontSize:10,fontWeight:700,color:'#E65100',fontFamily:'monospace'}}>OU-{String(t.id).padStart(4,'0')}</span>
                      <span style={{fontSize:13,fontWeight:800,color:'#0f172a'}}>{t.zone||t.site_id}</span>
                      <span style={{padding:'1px 6px',borderRadius:10,fontSize:9,fontWeight:700,background:pc.bg,color:pc.text}}>{t.priority}</span>
                      <span style={{padding:'1px 6px',borderRadius:10,fontSize:9,fontWeight:700,background:'#FFF7ED',color:'#E65100'}}>{sites.length} sites · PRB {t.avg_prb_util}%</span>
                      <span style={{padding:'1px 6px',borderRadius:10,fontSize:9,fontWeight:700,
                        background:t.status==='resolved'?'#DCFCE7':t.status==='in_progress'?'#DBEAFE':'#FEF3C7',
                        color:t.status==='resolved'?'#166534':t.status==='in_progress'?'#1E40AF':'#92400E'}}>{t.status.replace('_',' ')}</span>
                      <span style={{fontSize:9,color:'#64748b'}}>RRC:{t.avg_rrc} · Tput:{t.avg_dl_tput}Mbps · Rev:${t.revenue_total}</span>
                      <span style={{marginLeft:'auto',fontSize:9,color:'#64748b'}}>{t.agent_name||'Unassigned'} · {resolvedCount}/{sites.length} done</span>
                      <span style={{fontSize:10,color:'#94a3b8',transform:expanded?'rotate(180deg)':'rotate(0)',transition:'transform .2s'}}>&#9660;</span>
                    </div>

                    {/* Expandable site list */}
                    {expanded && sites.length > 0 && (
                      <div style={{padding:'6px 12px 10px',borderTop:'1px solid #f1f5f9'}}>
                        <table style={{width:'100%',borderCollapse:'collapse',fontSize:11}}>
                          <thead><tr style={{borderBottom:'1px solid #e2e8f0'}}>
                            <th style={{textAlign:'left',padding:'4px 6px',fontSize:9,color:'#94a3b8',fontWeight:600}}>Site</th>
                            <th style={{textAlign:'center',padding:'4px 6px',fontSize:9,color:'#94a3b8',fontWeight:600}}>PRB %</th>
                            <th style={{textAlign:'center',padding:'4px 6px',fontSize:9,color:'#94a3b8',fontWeight:600}}>Tput</th>
                            <th style={{textAlign:'center',padding:'4px 6px',fontSize:9,color:'#94a3b8',fontWeight:600}}>RRC</th>
                            <th style={{textAlign:'right',padding:'4px 6px',fontSize:9,color:'#94a3b8',fontWeight:600}}>Actions</th>
                          </tr></thead>
                          <tbody>{sites.map(s=>{
                            const resolved=siteStatuses[s.site_id]==='resolved';
                            const st={...t,site_id:s.site_id,site_abs_id:s.site_abs_id||'',site_name:s.site_name||'',avg_prb_util:s.avg_prb,avg_dl_tput:s.avg_tput,avg_rrc:s.avg_rrc,avg_drop_rate:s.avg_drop,isOverutilized:true};
                            return(
                              <tr key={s.site_id} style={{borderBottom:'1px solid #f8fafc',background:resolved?'#F0FDF408':'transparent'}}>
                                <td style={{padding:'5px 6px'}}><span style={{width:6,height:6,borderRadius:'50%',background:resolved?'#16A34A':'#E65100',display:'inline-block',marginRight:6}}/><b>{s.site_id}</b></td>
                                <td style={{textAlign:'center',padding:'5px 6px',color:'#E65100',fontWeight:700}}>{s.avg_prb}%</td>
                                <td style={{textAlign:'center',padding:'5px 6px',color:'#475569'}}>{s.avg_tput} Mbps</td>
                                <td style={{textAlign:'center',padding:'5px 6px',color:'#475569'}}>{s.avg_rrc}</td>
                                <td style={{textAlign:'right',padding:'3px 6px'}}>
                                  <div style={{display:'flex',gap:3,justifyContent:'flex-end'}}>
                                    <button onClick={()=>setDiagTicket(st)} style={{fontSize:8,padding:'2px 6px',borderRadius:3,background:'#00338D',color:'#fff',border:'none',cursor:'pointer',fontWeight:600}}>RCA</button>
                                    <button onClick={()=>setParamTicket(st)} style={{fontSize:8,padding:'2px 6px',borderRadius:3,background:'#f1f5f9',color:'#475569',border:'1px solid #cbd5e1',cursor:'pointer',fontWeight:600}}>Param</button>
                                    <button onClick={()=>downloadPdf(st)} style={{fontSize:8,padding:'2px 6px',borderRadius:3,background:'#f1f5f9',color:'#475569',border:'1px solid #cbd5e1',cursor:'pointer',fontWeight:600}}>PDF</button>
                                    {!resolved?<button onClick={async()=>{try{await apiCall(`/api/network-issues/overutilized/${t.id}/resolve-site`,{method:'POST',body:JSON.stringify({site_id:s.site_id})});fetchOverutilized();}catch(_){}}}
                                      style={{fontSize:8,padding:'2px 6px',borderRadius:3,background:'#16A34A',color:'#fff',border:'none',cursor:'pointer',fontWeight:600}}>Resolve</button>
                                    :<span style={{fontSize:8,color:'#16A34A',fontWeight:700}}>Done</span>}
                                  </div>
                                </td>
                              </tr>
                            );})}</tbody>
                        </table>
                        {t.status!=='resolved'&&<button onClick={async()=>{if(!window.confirm('Resolve all?'))return;try{await apiCall(`/api/network-issues/overutilized/${t.id}/status`,{method:'PUT',body:JSON.stringify({status:'resolved'})});fetchOverutilized();}catch(_){}}}
                          style={{marginTop:6,padding:'4px 12px',borderRadius:6,fontSize:10,fontWeight:600,background:'#16A34A',color:'#fff',border:'none',cursor:'pointer'}}>Resolve All</button>}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
           )}
        </div>
      )}

      {/* ── WORST CELL TAB ── */}
      {issueTab === 'worst' && <>
      {/* Filter tabs */}
      <div style={{display:'flex',gap:8,marginBottom:16}}>
        {['All','Pending','In Progress','Resolved'].map(f=>(
          <button key={f} onClick={()=>setFilter(f)} style={{padding:'6px 18px',borderRadius:4,fontSize:12,fontWeight:700,cursor:'pointer',borderBottom:filter===f?'3px solid #00338D':'3px solid transparent',background:'transparent',color:filter===f?'#00338D':'#64748b',border:'none'}}>
            {f}
          </button>
        ))}
      </div>

      {/* Priority legend */}
      <div style={{display:'flex',gap:14,marginBottom:14}}>
        {Object.entries(P_CFG).map(([k,v])=>(<span key={k} style={{display:'flex',alignItems:'center',gap:4,fontSize:11,color:'var(--text-muted)'}}><span style={{width:10,height:10,borderRadius:2,background:v.bar}}/>{v.label}</span>))}
      </div>

      {/* Ticket cards */}
      {filtered.length === 0 ? (
        <div style={{textAlign:'center',padding:60,color:'#94a3b8',fontSize:14}}>
          No network issue tickets. Click "Detect & Create Tickets" to scan for worst cells.
        </div>
      ) : filtered.map(t => {
        const pc = P_CFG[t.priority] || P_CFG.Low;

        return (
          <div key={t.id} style={{background:'var(--bg-card)',borderRadius:10,border:'1px solid var(--border)',marginBottom:12,borderLeft:`4px solid ${pc.bar}`,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
            {/* Main row */}
            <div style={{display:'grid',gridTemplateColumns:'180px 1fr 80px 90px 140px 140px',alignItems:'center',padding:'14px 18px',gap:12}}>
              {/* Site + Cells */}
              <div>
                <div style={{fontSize:10,fontWeight:700,color:'var(--primary)',textTransform:'uppercase',letterSpacing:'.5px'}}>Site</div>
                <div style={{fontSize:14,fontWeight:800,color:'var(--text)'}}>{t.site_id}</div>
                <div style={{fontSize:10,color:'var(--text-muted)',marginTop:2}}>
                  {t.cell_count} cell{t.cell_count!==1?'s':''} · {t.zone}
                </div>
              </div>

              {/* Category + KPIs */}
              <div>
                <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:4}}>
                  <span style={{padding:'2px 10px',borderRadius:12,fontSize:10,fontWeight:700,
                    background:t.category==='Severe Worst'?'#7C3AED18':'#dc262618',
                    color:t.category==='Severe Worst'?'#7C3AED':'#dc2626'}}>{t.category}</span>
                  <span style={{fontSize:10,color:'#94a3b8'}}>{t.location}</span>
                </div>
                <div style={{display:'flex',gap:10,fontSize:10,color:'var(--text-muted)'}}>
                  <span>Drop: <b style={{color:t.avg_drop_rate>1.5?'#dc2626':'#16a34a'}}>{f(t.avg_drop_rate,2)}%</b></span>
                  <span>CSSR: <b style={{color:t.avg_cssr<98.5?'#dc2626':'#16a34a'}}>{f(t.avg_cssr,1)}%</b></span>
                  <span>Tput: <b style={{color:t.avg_tput<8?'#dc2626':'#16a34a'}}>{f(t.avg_tput,1)}Mbps</b></span>
                  <span>Avg RRC: <b>{f(t.avg_rrc,0)}</b></span>
                  <span>Max RRC: <b>{f(t.max_rrc,0)}</b></span>
                  <span>Rev: <b>${f(t.revenue_total,0)}</b></span>
                </div>
              </div>

              {/* Priority */}
              <div style={{textAlign:'center'}}>
                <span style={{padding:'3px 12px',borderRadius:12,fontSize:10,fontWeight:700,background:pc.bar+'18',color:pc.bar}}>{t.priority}</span>
              </div>

              {/* Status */}
              <div style={{textAlign:'center'}}>
                <span style={{padding:'3px 12px',borderRadius:12,fontSize:10,fontWeight:700,
                  background:t.status==='open'?'rgba(37,99,235,0.1)':t.status==='in_progress'?'rgba(217,119,6,0.1)':t.status==='resolved'?'rgba(22,163,106,0.1)':'var(--bg)',
                  color:t.status==='open'?'#2563eb':t.status==='in_progress'?'#d97706':t.status==='resolved'?'#16a34a':'#64748b'}}>
                  {t.status==='in_progress'?'In Progress':t.status.charAt(0).toUpperCase()+t.status.slice(1)}
                </span>
              </div>

              {/* SLA */}
              <SlaTimer deadline={t.deadline_time} slaHours={t.sla_hours} status={t.status}/>

              {/* Agent */}
              <div style={{fontSize:11}}>
                {t.agent_name ? (
                  <div>
                    <div style={{fontWeight:700,color:'var(--text)'}}>{t.agent_name}</div>
                    <div style={{fontSize:9,color:'#94a3b8'}}>{t.agent_eid}</div>
                  </div>
                ) : <span style={{color:'#94a3b8'}}>Unassigned</span>}
              </div>
            </div>

            {/* Action buttons */}
            <div style={{display:'flex',alignItems:'center',gap:8,padding:'8px 18px 14px',flexWrap:'wrap'}}>
              <button onClick={()=>setDiagTicket(t)} style={{display:'flex',alignItems:'center',gap:5,padding:'6px 14px',borderRadius:8,fontSize:11,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:'pointer'}}>
                {IC.cpu} AI Diagnosis
              </button>
              <button onClick={()=>downloadPdf(t)} disabled={pdfLoading===t.id} style={{display:'flex',alignItems:'center',gap:5,padding:'6px 14px',borderRadius:8,fontSize:11,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:pdfLoading===t.id?'wait':'pointer',opacity:pdfLoading===t.id?0.6:1}}>
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                {pdfLoading===t.id ? 'Generating PDF...' : 'Download PDF'}
              </button>
              {t.status !== 'resolved' && (
                <button onClick={()=>setParamTicket(t)} style={{display:'flex',alignItems:'center',gap:5,padding:'6px 14px',borderRadius:8,fontSize:11,fontWeight:600,background:'var(--bg)',color:'var(--text-secondary)',border:'1px solid var(--border)',cursor:'pointer'}}>
                  {IC.tune} Parameter Change
                </button>
              )}
              {t.status !== 'resolved' && (
                <button onClick={()=>markResolved(t.id)} disabled={resolving===t.id} style={{display:'flex',alignItems:'center',gap:5,padding:'6px 14px',borderRadius:8,fontSize:11,fontWeight:600,background:'#16a34a',color:'#fff',border:'none',cursor:'pointer'}}>
                  {IC.check} {resolving===t.id?'Resolving...':'Mark Resolved'}
                </button>
              )}
              <span style={{fontSize:10,color:'#16a34a',fontWeight:600,padding:'3px 10px',borderRadius:12,background:'#dcfce7'}}>
                Assigned to you
              </span>
              <span style={{marginLeft:'auto',fontSize:10,color:'#94a3b8',textAlign:'right',lineHeight:'1.5'}}>
                Created {t.created_at ? new Date(t.created_at).toLocaleString() : '—'}
                {t.updated_at && t.created_at && t.updated_at !== t.created_at && (
                  <><br/>Updated {new Date(t.updated_at).toLocaleString()}</>
                )}
              </span>
            </div>
          </div>
        );
      })}

      </>}

      {/* AI Diagnosis Modal — works for both worst cell and overutilized tickets */}
      {diagTicket && <AIDiagnosisModal ticket={diagTicket} onClose={()=>setDiagTicket(null)}/>}
      {paramTicket && <CRFormModal open={!!paramTicket} networkIssue={paramTicket} onClose={()=>{setParamTicket(null);fetchAll();}}/>}

      {/* Today's Routing Modal */}
      {showRouting && (
        <div style={{position:'fixed',inset:0,background:'rgba(15,23,42,0.45)',display:'flex',alignItems:'center',justifyContent:'center',zIndex:1000}} onClick={()=>setShowRouting(false)}>
          <div onClick={e=>e.stopPropagation()} style={{background:'var(--bg-card)',borderRadius:12,width:880,maxWidth:'95vw',maxHeight:'85vh',overflowY:'auto',boxShadow:'0 20px 60px rgba(0,0,0,.15)'}}>
            <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'16px 24px',borderBottom:'1px solid var(--border)'}}>
              <div>
                <h3 style={{margin:0,fontSize:16,fontWeight:700,color:'var(--text)'}}>Today's Routing</h3>
                <p style={{margin:'2px 0 0',fontSize:11,color:'var(--text-muted)'}}>Tickets created or updated today — {routingData.length} ticket{routingData.length!==1?'s':''}</p>
              </div>
              <button onClick={()=>setShowRouting(false)} style={{border:'none',background:'var(--bg)',borderRadius:6,width:28,height:28,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center'}}>{IC.x}</button>
            </div>
            <div style={{padding:20}}>
              {routingLoading ? (
                <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Loading...</div>
              ) : routingData.length === 0 ? (
                <div style={{textAlign:'center',padding:40,color:'#94a3b8',fontSize:13}}>No tickets created or updated today.</div>
              ) : (
                <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
                  <thead>
                    <tr style={{borderBottom:'2px solid #e2e8f0'}}>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Ticket</th>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Category</th>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Type</th>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Site / Zone</th>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Sites/Cells</th>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Priority</th>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Status</th>
                      <th style={{textAlign:'left',padding:'8px 10px',fontWeight:700,color:'#475569',fontSize:11,textTransform:'uppercase'}}>Assigned Agent</th>
                    </tr>
                  </thead>
                  <tbody>
                    {routingData.map((r,i) => {
                      const prc = P_CFG[r.priority] || P_CFG.Low;
                      const isOU = r.category === 'Overutilized';
                      const catColor = isOU ? '#E65100' : '#00338D';
                      const catBg = isOU ? '#FFF7ED' : '#EFF6FF';
                      const ticketLabel = isOU ? `OU-${r.ticket_id}` : `#${r.ticket_id}`;
                      return (
                        <tr key={`${r.category}-${r.ticket_id}`||i} style={{borderBottom:'1px solid #f1f5f9',background:i%2===0?'#fff':'#f8fafc'}}>
                          <td style={{padding:'8px 10px',fontWeight:700,color:catColor,fontFamily:'monospace'}}>{ticketLabel}</td>
                          <td style={{padding:'8px 10px'}}><span style={{padding:'2px 8px',borderRadius:10,fontSize:9,fontWeight:700,background:catBg,color:catColor,border:`1px solid ${catColor}33`}}>
                            {isOU ? 'Overutilized' : 'Worst Cell'}</span></td>
                          <td style={{padding:'8px 10px'}}><span style={{padding:'2px 8px',borderRadius:10,fontSize:10,fontWeight:700,
                            background:r.type==='created'?'#16A34A18':r.type==='updated'?'#0091DA18':'#94a3b818',
                            color:r.type==='created'?'#16A34A':r.type==='updated'?'#0091DA':'#94a3b8'}}>
                            {r.type==='created'?'New':r.type==='updated'?'Updated':'Existing'}</span></td>
                          <td style={{padding:'8px 10px',fontWeight:600,color:'#0f172a'}}>{r.site_id}<br/><span style={{fontSize:10,color:'#64748b'}}>{r.zone}{r.location?' · '+r.location:''}</span></td>
                          <td style={{padding:'8px 10px',color:'#475569'}}>{r.cell_count} {isOU?'site':'cell'}{r.cell_count!==1?'s':''}</td>
                          <td style={{padding:'8px 10px'}}><span style={{padding:'2px 8px',borderRadius:10,fontSize:10,fontWeight:700,background:prc.bar+'18',color:prc.bar}}>{r.priority}</span></td>
                          <td style={{padding:'8px 10px'}}><span style={{padding:'2px 8px',borderRadius:10,fontSize:10,fontWeight:700,
                            background:r.status==='resolved'?'#16A34A18':r.status==='in_progress'?'#F59E0B18':'#00338D18',
                            color:r.status==='resolved'?'#16A34A':r.status==='in_progress'?'#F59E0B':'#00338D'}}>
                            {(r.status||'open').replace('_',' ')}</span></td>
                          <td style={{padding:'8px 10px'}}><span style={{fontWeight:600,color:'var(--text)'}}>{r.agent_name || 'Unassigned'}</span>{r.agent_email?<><br/><span style={{fontSize:10,color:'var(--text-muted)'}}>{r.agent_email}</span></>:null}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      )}

      {/* System Logs Modal */}
      {showLogs && (
        <div style={{position:'fixed',inset:0,background:'rgba(15,23,42,0.45)',display:'flex',alignItems:'center',justifyContent:'center',zIndex:1000}} onClick={()=>setShowLogs(false)}>
          <div onClick={e=>e.stopPropagation()} style={{background:'var(--bg-card)',borderRadius:12,width:780,maxWidth:'95vw',maxHeight:'85vh',overflowY:'auto',boxShadow:'0 20px 60px rgba(0,0,0,.15)'}}>
            <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'16px 24px',borderBottom:'1px solid var(--border)'}}>
              <div>
                <h3 style={{margin:0,fontSize:16,fontWeight:700,color:'var(--text)'}}>AI Ticket System Logs</h3>
                <p style={{margin:'2px 0 0',fontSize:11,color:'var(--text-muted)'}}>Data pipeline, scheduler status, and ticket history</p>
              </div>
              <button onClick={()=>setShowLogs(false)} style={{border:'none',background:'var(--bg)',borderRadius:6,width:28,height:28,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center'}}>{IC.x}</button>
            </div>
            <div style={{padding:'16px 24px',fontFamily:'monospace',fontSize:12,lineHeight:1.8}}>
              {logsLoading ? (
                <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Loading...</div>
              ) : logsData.length === 0 ? (
                <div style={{textAlign:'center',padding:40,color:'#94a3b8',fontSize:13}}>No logs available.</div>
              ) : logsData.map((log, i) => {
                if (log.type === 'header') return (
                  <div key={i} style={{margin:i>0?'16px 0 6px':'0 0 6px',padding:'6px 10px',background:'var(--primary)',color:'#fff',borderRadius:6,fontSize:11,fontWeight:700,letterSpacing:'0.05em',textTransform:'uppercase'}}>
                    {log.text}
                  </div>
                );
                const colors = {
                  info: {bg:'rgba(3,105,161,0.08)',color:'#0369a1',icon:'i'},
                  warn: {bg:'rgba(180,83,9,0.08)',color:'#b45309',icon:'!'},
                  error: {bg:'rgba(220,38,38,0.08)',color:'#dc2626',icon:'x'},
                  detail: {bg:'var(--bg)',color:'var(--text-secondary)',icon:'>'},
                  ticket: {bg:'rgba(22,101,52,0.08)',color:'#166534',icon:'#'},
                };
                const c = colors[log.type] || colors.info;
                return (
                  <div key={i} style={{padding:'5px 10px',marginBottom:2,background:c.bg,borderRadius:4,color:c.color,borderLeft:`3px solid ${c.color}`,display:'flex',gap:8,alignItems:'flex-start'}}>
                    <span style={{opacity:0.6,flexShrink:0,width:14,textAlign:'center',fontWeight:700}}>{c.icon}</span>
                    <span style={{whiteSpace:'pre-wrap',wordBreak:'break-word'}}>{log.text}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
