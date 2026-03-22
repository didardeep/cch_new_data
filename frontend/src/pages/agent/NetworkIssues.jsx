import { useState, useEffect, useCallback, useRef } from 'react';
import { apiGet, apiCall } from '../../api';

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
      <div style={{background:'#e2e8f0',borderRadius:4,height:4,overflow:'hidden',width:120}}>
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
      <div style={{background:'#fff',borderRadius:12,width:900,maxWidth:'95vw',maxHeight:'90vh',overflowY:'auto',boxShadow:'0 20px 60px rgba(0,0,0,.15)'}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'16px 24px',borderBottom:'1px solid #e2e8f0'}}>
          <h3 style={{margin:0,fontSize:16,fontWeight:700,color:'#0f172a'}}>{title}</h3>
          <button onClick={onClose} style={{border:'none',background:'#f1f5f9',borderRadius:6,width:28,height:28,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center'}}>{IC.x}</button>
        </div>
        <div style={{padding:24}}>{children}</div>
      </div>
    </div>
  );
}

/* ── RCA Points Renderer ─────────────────────────────────────────────────────── */
function RcaPoints({ text }) {
  if (!text) return <div style={{color:'#94a3b8',fontSize:12,padding:16}}>Click "Run Analysis" to generate</div>;
  const lines = text.split('\n').filter(l => l.trim());
  return (
    <div style={{display:'flex',flexDirection:'column',gap:8}}>
      {lines.map((line, i) => {
        const cleaned = line.replace(/^\d+[\.\)]\s*/, '');
        const bm = cleaned.match(/^\*\*(.+?)\*\*[:\s]*(.*)/);
        return (
          <div key={i} style={{display:'flex',gap:10,padding:'10px 14px',background:'#f8fafc',borderLeft:'3px solid #00338D',borderRadius:6,border:'1px solid #e2e8f0'}}>
            <span style={{width:24,height:24,borderRadius:'50%',background:'#00338D',color:'#fff',display:'flex',alignItems:'center',justifyContent:'center',fontSize:11,fontWeight:800,flexShrink:0}}>{i+1}</span>
            <div style={{fontSize:12,lineHeight:1.6,color:'#334155'}}>
              {bm ? <><b style={{color:'#0f172a'}}>{bm[1]}:</b> {bm[2]}</> : cleaned}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── Trend Chart (lazy loads recharts) ────────────────────────────────────────── */
function TrendChart({ kpiName, data, color = '#00338D' }) {
  const [RC, setRC] = useState(null);
  useEffect(() => { import('recharts').then(m => setRC(m)); }, []);
  if (!RC || !data?.length) return <div style={{background:'#f8fafc',border:'1px solid #e2e8f0',borderRadius:8,padding:12,height:180}}>
    <div style={{fontSize:11,fontWeight:700,color:'#475569',marginBottom:6}}>{kpiName}</div>
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
    <div style={{background:'#fff',border:'1px solid #e2e8f0',borderRadius:10,padding:'10px 12px',height:200,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:4}}>
        <span style={{fontSize:11,fontWeight:700,color:'#1e293b'}}>{kpiName}</span>
        {dropIdx.size>0&&<span style={{fontSize:8,padding:'1px 6px',borderRadius:8,background:'#fef2f2',color:'#dc2626',fontWeight:700}}>{dropIdx.size} drop{dropIdx.size>1?'s':''}</span>}
      </div>
      <div style={{display:'flex',gap:10,marginBottom:3,fontSize:9,color:'#64748b'}}>
        <span>Avg: <b style={{color:'#1e293b'}}>{avg.toFixed(1)}</b></span>
        <span>Min: <b style={{color:'#dc2626'}}>{minV.toFixed(1)}</b></span>
        <span>Max: <b style={{color:'#16a34a'}}>{maxV.toFixed(1)}</b></span>
      </div>
      <ResponsiveContainer width="100%" height={140}>
        <ComposedChart data={data} margin={{top:5,right:5,bottom:0,left:-5}}>
          <defs><linearGradient id={gid} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={color} stopOpacity={.25}/><stop offset="100%" stopColor={color} stopOpacity={.02}/></linearGradient></defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false}/>
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

/* ── AI Diagnosis Modal ──────────────────────────────────────────────────────── */
function AIDiagnosisModal({ ticket, onClose }) {
  const [tab, setTab] = useState('trends');
  const [target, setTarget] = useState('site');
  const [period, setPeriod] = useState('day');
  const [trends, setTrends] = useState(null);
  const [trendsLoading, setTrendsLoading] = useState(false);
  const [rca, setRca] = useState({});
  const [rcaLoading, setRcaLoading] = useState(false);
  const [rec, setRec] = useState({});
  const [recLoading, setRecLoading] = useState(false);

  const cells = ticket.cells ? ticket.cells : (ticket.cells_affected||'').split(',').filter(Boolean);
  const cellSiteIds = ticket.cell_site_id_list ? ticket.cell_site_id_list : (ticket.cell_site_ids||'').split(',').filter(Boolean);

  const fetchTrends = useCallback(async (t, p) => {
    setTrendsLoading(true);
    try {
      const d = await apiGet(`/api/network-issues/${ticket.id}/trends?target=${t}&period=${p}`);
      setTrends(d.trends || {});
    } catch (_) { setTrends({}); }
    setTrendsLoading(false);
  }, [ticket.id]);

  useEffect(() => { fetchTrends(target, period); }, [target, period, fetchTrends]);

  const runRCA = async (t) => {
    setRcaLoading(true);
    try {
      const r = await apiCall(`/api/network-issues/${ticket.id}/rca`, { method: 'POST', body: JSON.stringify({ target: t }) });
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
        const rcaRes = await apiCall(`/api/network-issues/${ticket.id}/rca`, { method: 'POST', body: JSON.stringify({ target: t }) });
        rcaText = rcaRes.root_cause || '';
        setRca(prev => ({ ...prev, [t]: rcaText }));
      }
      const r = await apiCall(`/api/network-issues/${ticket.id}/recommendations`, { method: 'POST', body: JSON.stringify({ target: t, root_cause: rcaText }) });
      setRec(prev => ({ ...prev, [t]: r.recommendation }));
    } catch (_) {}
    setRecLoading(false);
  };

  const TargetButtons = ({ onClick, current }) => (
    <div style={{display:'flex',gap:5,marginBottom:12,flexWrap:'wrap'}}>
      <button onClick={()=>onClick('site')} style={{padding:'5px 14px',borderRadius:16,fontSize:10,fontWeight:700,cursor:'pointer',
        background:current==='site'?'#00338D':'#f1f5f9',color:current==='site'?'#fff':'#475569',border:current==='site'?'none':'1px solid #e2e8f0'}}>
         {ticket.site_id}
      </button>
      {cells.map((c, i) => (
        <button key={c} onClick={()=>onClick(c)} style={{padding:'5px 14px',borderRadius:16,fontSize:10,fontWeight:700,cursor:'pointer',
          background:current===c?'#7C3AED':'#f1f5f9',color:current===c?'#fff':'#475569',border:current===c?'none':'1px solid #e2e8f0'}}>
           {cellSiteIds[i] || c}
        </button>
      ))}
    </div>
  );

  return (
    <Modal title={`AI Network Diagnosis — ${ticket.site_id}`} onClose={onClose}>
      {/* Tabs */}
      <div style={{display:'flex',gap:4,marginBottom:16,borderBottom:'2px solid #e2e8f0',paddingBottom:8}}>
        {[['trends',' Trend Analysis'],['rca',' Root Cause Analysis'],['rec',' Final Recommendations']].map(([k,l])=>(
          <button key={k} onClick={()=>setTab(k)} style={{padding:'7px 18px',borderRadius:8,fontSize:12,fontWeight:700,cursor:'pointer',
            background:tab===k?'#00338D':'transparent',color:tab===k?'#fff':'#475569',border:'none'}}>{l}</button>
        ))}
      </div>

      {/* Trend Analysis */}
      {tab==='trends'&&(
        <div>
          <TargetButtons onClick={t=>{setTarget(t);}} current={target}/>
          <div style={{display:'flex',gap:5,marginBottom:12}}>
            {['month','week','day','hour'].map(p=>(
              <button key={p} onClick={()=>setPeriod(p)} style={{padding:'4px 12px',borderRadius:14,fontSize:10,fontWeight:700,cursor:'pointer',
                background:period===p?'#0f172a':'#f1f5f9',color:period===p?'#fff':'#64748b',border:period===p?'none':'1px solid #e2e8f0'}}>
                {p==='day'?'Daily':p==='hour'?'Hourly':p==='week'?'Weekly':'Monthly'}
              </button>
            ))}
          </div>
          {trendsLoading ? <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Loading trends...</div> : (
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:10}}>
              {Object.entries(trends||{}).map(([kpi,data])=>(
                <TrendChart key={kpi} kpiName={kpi} data={data} color={target==='site'?'#00338D':'#7C3AED'}/>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Root Cause Analysis */}
      {tab==='rca'&&(
        <div>
          <TargetButtons onClick={t=>{setTarget(t);if(!rca[t])runRCA(t);}} current={target}/>
          {rcaLoading ? <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Analyzing...</div> : (
            <div>
              {!rca[target] && <button onClick={()=>runRCA(target)} style={{padding:'8px 20px',borderRadius:8,fontSize:12,fontWeight:700,background:'#00338D',color:'#fff',border:'none',cursor:'pointer',marginBottom:12}}> Run Root Cause Analysis</button>}
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
            background:i<=stage?(isFail&&i===stage?'#dc2626':'#00338D'):'#e2e8f0',color:i<=stage?'#fff':'#94a3b8'}}>{i+1}</div>
          {i<5&&<div style={{flex:1,height:2,background:i<stage?'#00338D':'#e2e8f0'}}/>}
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
          <h3 style={{margin:'0 0 6px',fontSize:17,fontWeight:700,color:'#0f172a'}}>Change Request Raised!</h3>
          <p style={{margin:'0 0 8px',fontSize:13,color:'#475569'}}>
            Routed to <b>{managerInfo ? `Manager ${managerInfo.name}` : 'manager'}</b> for approval.
          </p>
          {managerInfo && <p style={{margin:'0 0 4px',fontSize:11,color:'#64748b'}}>Email: {managerInfo.email}</p>}
          {cr && <p style={{fontSize:12,color:'#00338D',fontFamily:'monospace',fontWeight:700,margin:'8px 0'}}>{cr.cr_number}</p>}
          {cr && <MiniPipeline status={cr.status}/>}
          <p style={{fontSize:11,color:'#94a3b8',margin:'12px 0'}}>Approval deadline: 30% of remaining SLA time</p>
          <button onClick={onClose} style={{padding:'8px 24px',borderRadius:8,fontSize:12,fontWeight:700,background:'#00338D',color:'#fff',border:'none',cursor:'pointer',marginTop:8}}>Done</button>
        </div>
      ) : loading ? (
        <div style={{textAlign:'center',padding:40,color:'#94a3b8'}}>Loading...</div>
      ) : cr ? (
        /* Show existing CR status */
        <div>
          <div style={{background:'#f8fafc',border:'1px solid #e2e8f0',borderRadius:10,padding:'14px 16px',marginBottom:16}}>
            <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:6}}>
              <span style={{fontFamily:'monospace',fontSize:12,fontWeight:800,color:'#00338D'}}>{cr.cr_number}</span>
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
              <div style={{fontSize:10,color:'#d97706',marginTop:6,padding:'4px 8px',background:'#fffbeb',borderRadius:4}}>
                Approval deadline: {new Date(change.approval_deadline).toLocaleString()}
              </div>
            )}
            {cr.approval_remark && <div style={{fontSize:11,color:'#374151',background:'#f0fdf4',border:'1px solid #bbf7d0',borderRadius:6,padding:'6px 10px',marginTop:8}}><b>Manager Note:</b> {cr.approval_remark}</div>}
          </div>
          {change && (
            <div style={{background:'#f8fafc',borderRadius:8,padding:'10px 14px',border:'1px solid #e2e8f0'}}>
              <div style={{fontSize:10,color:'#94a3b8',fontWeight:700,textTransform:'uppercase',marginBottom:4}}>Proposed Change</div>
              <div style={{fontSize:12,color:'#334155',lineHeight:1.5}}>{change.proposed_change}</div>
            </div>
          )}
          <button onClick={onClose} style={{marginTop:16,padding:'8px 24px',borderRadius:8,fontSize:12,fontWeight:600,background:'#f1f5f9',color:'#475569',border:'1px solid #e2e8f0',cursor:'pointer'}}>Close</button>
        </div>
      ) : (
        /* Submit form */
        <div>
          <div style={{fontSize:12,fontWeight:700,color:'#0f172a',marginBottom:6}}>Propose Parameter Change</div>
          <div style={{fontSize:10,color:'#64748b',marginBottom:10}}>
            Site: <b>{ticket.site_id}</b> · Cells: {ticket.cell_count} · SLA: {ticket.sla_hours}h · Priority: {ticket.priority}
          </div>
          <label style={{fontSize:11,fontWeight:600,color:'#334155',display:'block',marginBottom:3}}>Proposed Change *</label>
          <textarea value={proposed} onChange={e=>setProposed(e.target.value)} rows={3}
            placeholder="e.g., Increase E-tilt from 3° to 5° for GUR_LTE_0900 to reduce overshooting..."
            style={{width:'100%',padding:'8px 10px',borderRadius:8,border:'1px solid #e2e8f0',fontSize:12,fontFamily:'inherit',resize:'vertical',outline:'none',boxSizing:'border-box',marginBottom:10}}/>
          <label style={{fontSize:11,fontWeight:600,color:'#334155',display:'block',marginBottom:3}}>Impact Assessment</label>
          <textarea value={impact} onChange={e=>setImpact(e.target.value)} rows={2}
            placeholder="Expected impact on network KPIs..."
            style={{width:'100%',padding:'8px 10px',borderRadius:8,border:'1px solid #e2e8f0',fontSize:12,fontFamily:'inherit',resize:'vertical',outline:'none',boxSizing:'border-box',marginBottom:10}}/>
          <label style={{fontSize:11,fontWeight:600,color:'#334155',display:'block',marginBottom:3}}>Rollback Plan</label>
          <textarea value={rollback} onChange={e=>setRollback(e.target.value)} rows={2}
            placeholder="Steps to revert if change fails..."
            style={{width:'100%',padding:'8px 10px',borderRadius:8,border:'1px solid #e2e8f0',fontSize:12,fontFamily:'inherit',resize:'vertical',outline:'none',boxSizing:'border-box',marginBottom:10}}/>
          {errMsg && <div style={{color:'#dc2626',fontSize:11,marginBottom:8}}>{errMsg}</div>}
          <div style={{display:'flex',gap:8}}>
            <button onClick={submit} disabled={submitting||!proposed.trim()}
              style={{padding:'8px 20px',borderRadius:8,fontSize:12,fontWeight:700,background:'#00338D',color:'#fff',border:'none',cursor:'pointer'}}>
              {submitting?' Submitting...':'Submit & Route to Manager'}
            </button>
            <button onClick={onClose} style={{padding:'8px 20px',borderRadius:8,fontSize:12,fontWeight:600,background:'#f1f5f9',color:'#475569',border:'1px solid #e2e8f0',cursor:'pointer'}}>Cancel</button>
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
  const [tickets, setTickets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('All');
  const [stats, setStats] = useState({});
  const [diagTicket, setDiagTicket] = useState(null);
  const [paramTicket, setParamTicket] = useState(null);
  const [triggering, setTriggering] = useState(false);
  const [resolving, setResolving] = useState(null);

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

  useEffect(() => { fetchAll(); }, [fetchAll]);
  // Refresh every 30s for SLA timer
  useEffect(() => { const iv = setInterval(fetchAll, 30000); return () => clearInterval(iv); }, [fetchAll]);

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

  const filtered = filter === 'All' ? tickets
    : filter === 'Pending' ? tickets.filter(t => t.status === 'open')
    : filter === 'In Progress' ? tickets.filter(t => t.status === 'in_progress')
    : tickets.filter(t => t.status === 'resolved');

  const openCount = tickets.filter(t => t.status === 'open').length;
  const resolvedCount = tickets.filter(t => t.status === 'resolved').length;

  if (loading) return <div style={{display:'flex',alignItems:'center',justifyContent:'center',height:400}}><div className="spinner"/></div>;

  return (
    <div>
      {/* Header */}
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16}}>
        <div>
          <h2 style={{margin:0,fontSize:20,fontWeight:800,color:'#0f172a',display:'flex',alignItems:'center',gap:8}}>
            <span style={{width:4,height:28,background:'#00338D',borderRadius:2,display:'inline-block'}}/>
            Network Issues — Worst Cell Offenders
          </h2>
          <p style={{margin:'4px 0 0',fontSize:12,color:'#64748b'}}>{openCount} open · {resolvedCount} resolved · {tickets.length} total</p>
        </div>
        <div style={{display:'flex',gap:8}}>
          <button onClick={triggerJob} disabled={triggering} style={{display:'flex',alignItems:'center',gap:5,padding:'7px 16px',borderRadius:8,fontSize:12,fontWeight:600,background:'#00338D',color:'#fff',border:'none',cursor:'pointer'}}>
            {triggering?' Running...':' Detect & Create Tickets'}
          </button>
          <button onClick={fetchAll} style={{display:'flex',alignItems:'center',gap:5,padding:'7px 12px',borderRadius:8,fontSize:12,fontWeight:600,background:'#f8fafc',color:'#475569',border:'1px solid #e2e8f0',cursor:'pointer'}}>
            {IC.refresh} Refresh
          </button>
        </div>
      </div>

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
        {Object.entries(P_CFG).map(([k,v])=>(<span key={k} style={{display:'flex',alignItems:'center',gap:4,fontSize:11,color:'#64748b'}}><span style={{width:10,height:10,borderRadius:2,background:v.bar}}/>{v.label}</span>))}
      </div>

      {/* Ticket cards */}
      {filtered.length === 0 ? (
        <div style={{textAlign:'center',padding:60,color:'#94a3b8',fontSize:14}}>
          No network issue tickets. Click "Detect & Create Tickets" to scan for worst cells.
        </div>
      ) : filtered.map(t => {
        const pc = P_CFG[t.priority] || P_CFG.Low;
        const isMine = t.is_mine;
        const isUnassigned = !t.assigned_agent;

        // For tickets assigned to OTHER agents — show compact note only
        if (!isMine && !isUnassigned) {
          return (
            <div key={t.id} style={{background:'#f8fafc',borderRadius:10,border:'1px solid #e2e8f0',marginBottom:8,borderLeft:`4px solid ${pc.bar}`,padding:'12px 18px',display:'flex',alignItems:'center',justifyContent:'space-between'}}>
              <div style={{display:'flex',alignItems:'center',gap:12}}>
                <span style={{padding:'3px 10px',borderRadius:12,fontSize:10,fontWeight:700,background:pc.bar+'18',color:pc.bar}}>{t.priority}</span>
                <span style={{fontWeight:700,color:'#0f172a',fontSize:13}}>{t.site_id}</span>
                <span style={{fontSize:10,color:'#64748b'}}>{t.cell_count} cells · {t.category}</span>
              </div>
              <div style={{display:'flex',alignItems:'center',gap:10}}>
                <SlaTimer deadline={t.deadline_time} slaHours={t.sla_hours} status={t.status}/>
                <span style={{fontSize:11,color:'#475569',fontWeight:600,padding:'4px 12px',borderRadius:8,background:'#f1f5f9',border:'1px solid #e2e8f0'}}>
                  Assigned to <b style={{color:'#00338D'}}>{t.agent_name}</b> ({t.agent_eid})
                </span>
              </div>
            </div>
          );
        }

        return (
          <div key={t.id} style={{background:'#fff',borderRadius:10,border:'1px solid #e2e8f0',marginBottom:12,borderLeft:`4px solid ${pc.bar}`,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
            {/* Main row */}
            <div style={{display:'grid',gridTemplateColumns:'180px 1fr 80px 90px 140px 140px',alignItems:'center',padding:'14px 18px',gap:12}}>
              {/* Site + Cells */}
              <div>
                <div style={{fontSize:10,fontWeight:700,color:'#00338D',textTransform:'uppercase',letterSpacing:'.5px'}}>Site</div>
                <div style={{fontSize:14,fontWeight:800,color:'#0f172a'}}>{t.site_id}</div>
                <div style={{fontSize:10,color:'#64748b',marginTop:2}}>
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
                <div style={{display:'flex',gap:10,fontSize:10,color:'#64748b'}}>
                  <span>Drop: <b style={{color:t.avg_drop_rate>1.5?'#dc2626':'#16a34a'}}>{f(t.avg_drop_rate,2)}%</b></span>
                  <span>CSSR: <b style={{color:t.avg_cssr<98.5?'#dc2626':'#16a34a'}}>{f(t.avg_cssr,1)}%</b></span>
                  <span>Tput: <b style={{color:t.avg_tput<8?'#dc2626':'#16a34a'}}>{f(t.avg_tput,1)}Mbps</b></span>
                  <span>Avg RRC: <b>{f(t.avg_rrc,0)}</b></span>
                  <span>Max RRC: <b>{f(t.max_rrc,0)}</b></span>
                  <span>Rev: <b>₹{f(t.revenue_total,0)}L</b></span>
                </div>
              </div>

              {/* Priority */}
              <div style={{textAlign:'center'}}>
                <span style={{padding:'3px 12px',borderRadius:12,fontSize:10,fontWeight:700,background:pc.bar+'18',color:pc.bar}}>{t.priority}</span>
              </div>

              {/* Status */}
              <div style={{textAlign:'center'}}>
                <span style={{padding:'3px 12px',borderRadius:12,fontSize:10,fontWeight:700,
                  background:t.status==='open'?'#dbeafe':t.status==='in_progress'?'#fef3c7':t.status==='resolved'?'#dcfce7':'#f1f5f9',
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
                    <div style={{fontWeight:700,color:'#0f172a'}}>{t.agent_name}</div>
                    <div style={{fontSize:9,color:'#94a3b8'}}>{t.agent_eid}</div>
                  </div>
                ) : <span style={{color:'#94a3b8'}}>Unassigned</span>}
              </div>
            </div>

            {/* Action buttons */}
            <div style={{display:'flex',alignItems:'center',gap:8,padding:'8px 18px 14px',flexWrap:'wrap'}}>
              <button onClick={()=>setDiagTicket(t)} style={{display:'flex',alignItems:'center',gap:5,padding:'6px 14px',borderRadius:8,fontSize:11,fontWeight:600,background:'#f8fafc',color:'#475569',border:'1px solid #e2e8f0',cursor:'pointer'}}>
                {IC.cpu} AI Diagnosis
              </button>
              {t.status !== 'resolved' && (
                <button onClick={()=>setParamTicket(t)} style={{display:'flex',alignItems:'center',gap:5,padding:'6px 14px',borderRadius:8,fontSize:11,fontWeight:600,background:'#f8fafc',color:'#475569',border:'1px solid #e2e8f0',cursor:'pointer'}}>
                  {IC.tune} Parameter Change
                </button>
              )}
              {t.status !== 'resolved' && (
                <button onClick={()=>markResolved(t.id)} disabled={resolving===t.id} style={{display:'flex',alignItems:'center',gap:5,padding:'6px 14px',borderRadius:8,fontSize:11,fontWeight:600,background:'#16a34a',color:'#fff',border:'none',cursor:'pointer'}}>
                  {IC.check} {resolving===t.id?'Resolving...':'Mark Resolved'}
                </button>
              )}
              {t.agent_name ? (
                <span style={{fontSize:10,color:isMine?'#16a34a':'#64748b',fontWeight:600,padding:'3px 10px',borderRadius:12,background:isMine?'#dcfce7':'#f1f5f9'}}>
                  {isMine ? ' Assigned to you' : `Assigned to ${t.agent_name} (${t.agent_eid})`}
                </span>
              ) : (
                <span style={{fontSize:10,color:'#dc2626',fontWeight:600,padding:'3px 10px',borderRadius:12,background:'#fef2f2'}}>Unassigned</span>
              )}
              <span style={{marginLeft:'auto',fontSize:10,color:'#94a3b8'}}>Created {t.created_at ? new Date(t.created_at).toLocaleString() : '—'}</span>
            </div>
          </div>
        );
      })}

      {/* AI Diagnosis Modal */}
      {diagTicket && <AIDiagnosisModal ticket={diagTicket} onClose={()=>setDiagTicket(null)}/>}
      {paramTicket && <ParamChangeModal ticket={paramTicket} onClose={()=>{setParamTicket(null);fetchAll();}}/>}
    </div>
  );
}
