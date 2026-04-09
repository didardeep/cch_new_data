import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  LineChart, Line, BarChart, Bar, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  Cell, PieChart, Pie, Legend, ComposedChart, ReferenceLine,
  RadarChart, Radar, PolarGrid, PolarAngleAxis,
  ScatterChart, Scatter, ZAxis,
} from 'recharts';
import { apiGet } from '../../api';
import { useTheme } from '../../ThemeContext';

// ── Themes ─────────────────────────────────────────────────────────────────
const T_LIGHT = {
  bg:'#EEF2F7', surface:'#FFFFFF', surface2:'#F7FAFC',
  border:'#E2E8F0', text:'#0F172A', textSub:'#475569', muted:'#94A3B8',
  kpmgBlue:'#00338D', blue2:'#005EB8', blue3:'#0091DA', teal:'#009A93',
  green:'#16A34A', amber:'#D97706', red:'#DC2626', purple:'#7C3AED',
  headerBg:'#FFFFFF', cardShadow:'0 2px 10px rgba(0,51,141,0.09)',
};
const T_DARK = {
  bg:'#06101E', surface:'#0E1B30', surface2:'#152238',
  border:'#1C2E48', text:'#E2E8F0', textSub:'#94A3B8', muted:'#4A5568',
  kpmgBlue:'#4DA3FF', blue2:'#60A5FA', blue3:'#38BDF8', teal:'#2DD4BF',
  green:'#34D399', amber:'#FBBF24', red:'#F87171', purple:'#A78BFA',
  headerBg:'#0E1B30', cardShadow:'0 4px 24px rgba(0,0,0,0.5)',
};

// 5 brand colors (from pentagon palette) + shades for charts. Maps keep red/orange/green.
const PAL = [
  '#2B4DCC','#0A2463','#E91E8C','#00BCD4','#7B1FA2',  // 5 primary
  '#4A6FE5','#1A3CA6','#F06EB0','#26D9E8','#9C42C4',  // 5 lighter shades
];
const f  = (v,d=1) => (v==null||v===''||isNaN(+v)) ? '—' : d===0 ? String(Math.round(+v)) : (+v).toFixed(d).replace(/\.?0+$/, '') || '0';
const fn = (v,d=1) => (v==null||isNaN(+v)) ? 0 : +((+v).toFixed(d));
const card = T => ({ background:T.surface, border:`1px solid ${T.border}`, borderRadius:12, boxShadow:T.cardShadow });
const sel  = T => ({ padding:'5px 9px', borderRadius:7, border:`1px solid ${T.border}`, fontSize:11, color:T.text, background:T.surface, cursor:'pointer', outline:'none', fontFamily:'inherit' });

const KPI_FILTERS = [
  { key:'low_access',    label:'Low Accessibility Sites',      icon:'', desc:'RRC/E-RAB SR < 90%' },
  { key:'high_latency',  label:'High Latency Cells',           icon:'', desc:'DL Latency > 60ms' },
  { key:'volte_fail',    label:'VoLTE Failure Cells',          icon:'', desc:'Low VoLTE & high drop' },
  { key:'interference',  label:'Interference Zones',           icon:'', desc:'SINR < 5dB' },
  { key:'overloaded',    label:'Overloaded Sites',             icon:'', desc:'PRB > 85%' },
  { key:'underutilized', label:'Underutilized Sites',          icon:'', desc:'PRB < 20%' },
  { key:'rev_leakage',   label:'Revenue Leakage Sites',        icon:'', desc:'High util, low revenue' },
  { key:'low_margin',    label:'Low Margin Sites',             icon:'', desc:'EBITDA < 25%' },
  { key:'high_rev_util', label:'High Revenue High Util Sites', icon:'', desc:'Top performers' },
  { key:'low_tput',      label:'< 5 Mbps Cells',              icon:'', desc:'DL tput < 5 Mbps' },
  { key:'worst_drop',    label:'Worst Call Drop Offenders',    icon:'', desc:'E-RAB drop > 2%' },
  { key:'worst_ho',      label:'Worst Handover Offenders',     icon:'', desc:'HO SR < 90%' },
  { key:'worst_tput',    label:'Worst Throughput Offenders',   icon:'', desc:'Bottom 10% DL' },
  { key:'critical_avail',label:'Low Availability Sites',       icon:'', desc:'Avail < 95%' },
];

// ── CSS ────────────────────────────────────────────────────────────────────
const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0;}
::-webkit-scrollbar{width:4px;height:4px;}
::-webkit-scrollbar-thumb{background:#CBD5E1;border-radius:2px;}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.45}}
@keyframes bounce{0%,80%,100%{transform:translateY(0)}40%{transform:translateY(-5px)}}
.kc:hover{transform:translateY(-2px);box-shadow:0 8px 24px rgba(0,51,141,.15)!important;}
.nav-btn:hover{opacity:.85;}
.dd-item:hover{background:rgba(0,51,141,.07);}

`;

// ── Multi-select dropdown (checkbox style) ────────────────────────────────
function MultiSel({T,label,options=[],value,onChange}) {
  // value is comma-separated string e.g. "CBD,Edge"
  const selected = value ? value.split(',').filter(Boolean) : [];
  const [open,setOpen] = useState(false);
  const ref = useRef(null);
  useEffect(()=>{
    const h=e=>{if(ref.current&&!ref.current.contains(e.target))setOpen(false);};
    document.addEventListener('mousedown',h);return()=>document.removeEventListener('mousedown',h);
  },[]);
  const toggle=(v)=>{
    const s=new Set(selected);
    if(s.has(v)) s.delete(v); else s.add(v);
    onChange([...s].join(','));
  };
  const display = selected.length===0 ? label : selected.length<=2 ? selected.join(', ') : `${selected.length} selected`;
  return (
    <div ref={ref} style={{position:'relative',display:'inline-block'}}>
      <button onClick={()=>setOpen(o=>!o)} style={{padding:'5px 9px',borderRadius:7,border:`1px solid ${T.border}`,fontSize:11,color:selected.length?T.kpmgBlue:T.text,background:T.surface,cursor:'pointer',fontFamily:'inherit',display:'flex',alignItems:'center',gap:4,minWidth:70}}>
        {display} <span style={{fontSize:8,opacity:.5}}>▼</span>
      </button>
      {open&&(
        <div style={{position:'absolute',top:'100%',left:0,marginTop:4,background:T.surface,border:`1px solid ${T.border}`,borderRadius:8,boxShadow:'0 8px 24px rgba(0,0,0,.15)',zIndex:50,minWidth:140,maxHeight:220,overflowY:'auto',padding:'4px 0'}}>
          {selected.length>0&&(
            <div onClick={()=>{onChange('');setOpen(false);}} style={{padding:'5px 10px',fontSize:10,color:T.red,cursor:'pointer',borderBottom:`1px solid ${T.border}`,fontWeight:600}}> Clear all</div>
          )}
          {options.map(v=>(
            <label key={v} style={{display:'flex',alignItems:'center',gap:6,padding:'4px 10px',cursor:'pointer',fontSize:10.5,color:T.text}}
              onMouseEnter={e=>e.currentTarget.style.background=T.surface2} onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
              <input type="checkbox" checked={selected.includes(v)} onChange={()=>toggle(v)} style={{accentColor:T.kpmgBlue,margin:0}}/>
              {v}
            </label>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Micro helpers ──────────────────────────────────────────────────────────
function Bdg({color,children,sm}) {
  return <span style={{display:'inline-flex',alignItems:'center',padding:sm?'1px 5px':'2px 7px',borderRadius:20,fontSize:sm?8.5:9.5,fontWeight:700,background:color+'1a',color}}>{children}</span>;
}
function SL({T,children}) { return <div style={{fontSize:9.5,fontWeight:700,color:T.muted,textTransform:'uppercase',letterSpacing:'0.1em',marginBottom:8}}>{children}</div>; }
function CT({T,children,mb=10}) { return <div style={{fontSize:11,fontWeight:700,color:T.kpmgBlue,textTransform:'uppercase',letterSpacing:'0.05em',marginBottom:mb}}>{children}</div>; }
function Empty({T,h=100}) { return <div style={{height:h,display:'flex',alignItems:'center',justifyContent:'center',fontSize:12,color:T.muted,flexDirection:'column',gap:6}}><span style={{fontSize:28}}></span>No data</div>; }
function Spin({T}) { return <div style={{height:80,display:'flex',alignItems:'center',justifyContent:'center'}}><div style={{width:24,height:24,border:`3px solid ${T.border}`,borderTopColor:T.kpmgBlue,borderRadius:'50%',animation:'spin .7s linear infinite'}}/></div>; }

function CC({T,title,children,col,action,p='14px 16px'}) {
  return (
    <div style={{...card(T),padding:p,gridColumn:col||'auto'}}>
      {title&&<div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:10}}>
        <span style={{fontSize:11,fontWeight:700,color:T.kpmgBlue,textTransform:'uppercase',letterSpacing:'0.05em'}}>{title}</span>
        {action}
      </div>}
      {children}
    </div>
  );
}

function KpiCard({T,label,value,unit,icon,color,sub,badge}) {
  return (
    <div className="kc" style={{...card(T),padding:'11px 13px',borderTop:`3px solid ${color}`,transition:'all .2s',cursor:'default'}}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:4}}>
        <span style={{fontSize:9,fontWeight:700,color:T.muted,textTransform:'uppercase',letterSpacing:'0.06em',lineHeight:1.3}}>{label}</span>
        <span style={{fontSize:14}}>{icon}</span>
      </div>
      <div style={{display:'flex',alignItems:'baseline',gap:3,marginBottom:3}}>
        <span style={{fontSize:20,fontWeight:800,color:T.text,lineHeight:1,fontFamily:"'IBM Plex Mono',monospace"}}>{value}</span>
        {unit&&<span style={{fontSize:10,color:T.muted,fontWeight:500}}>{unit}</span>}
      </div>
      {badge&&<Bdg color={badge.color}>{badge.text}</Bdg>}
      {sub&&!badge&&<div style={{fontSize:9.5,color:T.muted}}>{sub}</div>}
    </div>
  );
}

function Gauge({score,label,T}) {
  const color=score>=80?T.green:score>=60?T.amber:T.red;
  const r=37,cx=46,cy=46,circ=2*Math.PI*r,arc=circ*0.75,fill=(Math.min(score,100)/100)*arc;
  return (
    <div style={{display:'flex',flexDirection:'column',alignItems:'center'}}>
      <svg width={92} height={64} style={{overflow:'visible'}}>
        <circle cx={cx} cy={cy} r={r} fill="none" stroke={T.border} strokeWidth={7} strokeDasharray={`${arc} ${circ-arc}`} strokeLinecap="round" transform="rotate(-225 46 46)"/>
        <circle cx={cx} cy={cy} r={r} fill="none" stroke={color} strokeWidth={7} strokeDasharray={`${fill} ${circ-fill}`} strokeLinecap="round" transform="rotate(-225 46 46)" style={{transition:'stroke-dasharray 1s ease'}}/>
        <text x={cx} y={cy+5} textAnchor="middle" fontSize={16} fontWeight={800} fill={T.text} fontFamily="'IBM Plex Mono',monospace">{score}</text>
      </svg>
      <span style={{fontSize:9,fontWeight:700,color,textTransform:'uppercase',marginTop:-3}}>{label}</span>
    </div>
  );
}

function Tip({T,active,payload,label}) {
  if(!active||!payload?.length) return null;
  return (
    <div style={{background:T.surface,border:`1px solid ${T.kpmgBlue}33`,borderRadius:10,padding:'9px 13px',boxShadow:'0 8px 24px rgba(0,51,141,.15)',fontSize:11,minWidth:140}}>
      <div style={{fontWeight:700,color:T.kpmgBlue,marginBottom:5,fontSize:11.5}}>{typeof label==='string'?label.replace(/_/g,' '):label}</div>
      {payload.map((p,i)=>(
        <div key={i} style={{display:'flex',alignItems:'center',gap:6,marginBottom:2}}>
          <span style={{width:7,height:7,borderRadius:'50%',background:p.color,flexShrink:0}}/>
          <span style={{color:T.textSub,flex:1,fontSize:10.5}}>{String(p.name).replace(/_/g,' ')}:</span>
          <strong style={{color:T.text,fontFamily:"'IBM Plex Mono',monospace",fontSize:10.5}}>{typeof p.value==='number'?p.value.toFixed(2):p.value}</strong>
        </div>
      ))}
    </div>
  );
}

// ── Leaflet Map ─────────────────────────────────────────────────────────────
function LeafletMap({sites=[],highlight=[],T,height=300}) {
  const mapRef=useRef(null), leafRef=useRef(null), markersRef=useRef([]);
  useEffect(()=>{
    if(!document.getElementById('lf-css')){
      const l=document.createElement('link');
      l.id='lf-css';l.rel='stylesheet';l.href='https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
      document.head.appendChild(l);
    }
    const load=()=>new Promise(res=>{
      if(window.L){res();return;}
      const s=document.createElement('script');s.src='https://unpkg.com/leaflet@1.9.4/dist/leaflet.js';
      s.onload=res;document.head.appendChild(s);
    });
    load().then(()=>{
      if(!mapRef.current||leafRef.current)return;
      const L=window.L;
      const map=L.map(mapRef.current,{zoomControl:true,attributionControl:true}).setView([28.47,77.03],11);
      leafRef.current=map;
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OSM'}).addTo(map);
    });
    return()=>{if(leafRef.current){leafRef.current.remove();leafRef.current=null;}};
  },[]);

  useEffect(()=>{
    let tries=0;
    const go=()=>{
      if(!window.L||!leafRef.current){if(tries++<20)setTimeout(go,300);return;}
      const L=window.L;
      markersRef.current.forEach(m=>m.remove());markersRef.current=[];
      const list=highlight.length?highlight:sites;
      if(!list.length)return;
      list.forEach(site=>{
        const lat=parseFloat(site.lat||site.latitude);
        const lng=parseFloat(site.lng||site.longitude);
        if(!lat||!lng||isNaN(lat)||isNaN(lng))return;
        // 4-factor health: use pre-computed status/color from backend, fallback to PRB-only
        const status=site.status||'healthy';
        const color=site.color||(status==='critical'?'#DC2626':status==='degraded'?'#F97316':status==='warning'?'#EAB308':'#22c55e');
        const sz=highlight.length?16:12;
        const html=`<div style="width:${sz}px;height:${sz}px;border-radius:50%;background:${color};border:2px solid white;box-shadow:0 0 6px ${color}88,0 2px 4px rgba(0,0,0,.3);cursor:pointer;transition:transform .2s" onmouseover="this.style.transform='scale(1.4)'" onmouseout="this.style.transform='scale(1)'"></div>`;
        const icon=L.divIcon({className:'',html,iconSize:[sz,sz],iconAnchor:[sz/2,sz/2],popupAnchor:[0,-sz/2-2]});
        const sid=site.site_id||site.cell_id||'—';
        const prb=parseFloat(site.prb_utilization||site.dl_prb_util||site.avg_prb||0);
        const drop=parseFloat(site.call_drop_rate||site.erab_drop_rate||0);
        const cssr=parseFloat(site.lte_cssr||100);
        const usrTput=parseFloat(site.dl_usr_tput||site.dl_tput||site.throughput||site.dl_cell_tput||0);
        const healthScore=site.health_score!=null?site.health_score:'—';
        const popup=L.popup({maxWidth:220,className:'lf-pop'}).setContent(
          `<div style="font-family:system-ui;padding:2px;min-width:190px">
            <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px">
              <span style="width:10px;height:10px;border-radius:50%;background:${color};display:inline-block"></span>
              <span style="font-weight:800;color:#00338D;font-size:12px"> ${sid}</span>
              <span style="margin-left:auto;font-size:9px;padding:1px 6px;border-radius:10px;background:${color}22;color:${color};font-weight:700">${status.toUpperCase()}</span>
            </div>
            <div style="font-size:9px;color:#64748B;margin-bottom:6px;font-weight:600">Health Score: ${typeof healthScore==='number'?healthScore.toFixed(1):healthScore}/100</div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px">
              ${[
                ['PRB Util',`${prb.toFixed(1)}%`,prb>70?'#DC2626':'#16A34A'],
                ['Drop Rate',`${drop.toFixed(2)}%`,drop>1.5?'#DC2626':'#16A34A'],
                ['CSSR',`${cssr.toFixed(1)}%`,cssr<98.5?'#DC2626':'#16A34A'],
                ['DL Tput',`${usrTput.toFixed(1)} Mbps`,usrTput<8?'#DC2626':'#16A34A'],
                ['Zone',site.cluster||site.zone||'—','#475569']
              ].map(([l,v,vc])=>`
              <div style="background:#F8FAFC;border-radius:5px;padding:4px 7px">
                <div style="font-size:8px;color:#94A3B8;font-weight:600">${l}</div>
                <div style="font-size:11px;font-weight:800;color:${vc};font-family:monospace">${v}</div>
              </div>`).join('')}
            </div>
          </div>`
        );
        const mk=L.marker([lat,lng],{icon}).bindPopup(popup);
        mk.on('mouseover',function(){this.openPopup();});
        mk.addTo(leafRef.current);
        markersRef.current.push(mk);
      });
      if(markersRef.current.length){
        try{leafRef.current.fitBounds(L.featureGroup(markersRef.current).getBounds().pad(.1));}catch(_){}
      }
    };
    go();
  },[sites,highlight]);

  return (
    <>
      <style>{`.lf-pop .leaflet-popup-content-wrapper{border-radius:10px!important;box-shadow:0 8px 30px rgba(0,51,141,.15)!important;border:1px solid #E2E8F0!important;}.lf-pop .leaflet-popup-content{margin:8px 10px!important;}`}</style>
      <div ref={mapRef} style={{width:'100%',height,borderRadius:10,overflow:'hidden',border:`1px solid ${T.border}`}}/>
    </>
  );
}

// ── Site Search Input ────────────────────────────────────────────────────────
function SiteSearch({T,layer,onSelect,placeholder='Search site ID…',filters:searchFilters}) {
  const [q,setQ]=useState('');
  const [results,setResults]=useState([]);
  const [loading,setLoading]=useState(false);
  const [open,setOpen]=useState(false);
  const debRef=useRef(null);

  const search=useCallback((val)=>{
    clearTimeout(debRef.current);
    if(!val||val.length<2){setResults([]);setOpen(false);return;}
    debRef.current=setTimeout(async()=>{
      setLoading(true);
      try{
        const fq=searchFilters?Object.entries(searchFilters).filter(([,v])=>v).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&'):'';
        const d=await apiGet(`/api/network/site-search?q=${encodeURIComponent(val)}&layer=${layer||'ran'}${fq?'&'+fq:''}`);
        setResults(d.sites||[]);
        setOpen(true);
      }catch(_){setResults([]);}
      setLoading(false);
    },300);
  },[layer,searchFilters]);

  return (
    <div style={{position:'relative',minWidth:220}}>
      <div style={{position:'relative'}}>
        <input value={q} onChange={e=>{setQ(e.target.value);search(e.target.value);}}
          onFocus={()=>results.length&&setOpen(true)}
          placeholder={placeholder}
          style={{...sel(T),width:'100%',padding:'7px 32px 7px 10px',fontSize:12,borderRadius:8}}/>
        {loading
          ? <span style={{position:'absolute',right:9,top:'50%',transform:'translateY(-50%)',fontSize:10,color:T.muted,animation:'spin .7s linear infinite',display:'inline-block'}}></span>
          : q&&<span onClick={()=>{setQ('');setResults([]);setOpen(false);}} style={{position:'absolute',right:9,top:'50%',transform:'translateY(-50%)',cursor:'pointer',fontSize:12,color:T.muted}}></span>
        }
      </div>
      {open&&results.length>0&&(
        <div style={{position:'absolute',top:'100%',left:0,right:0,background:T.surface,border:`1px solid ${T.border}`,borderRadius:8,boxShadow:T.cardShadow,zIndex:200,maxHeight:220,overflowY:'auto',marginTop:2}}>
          {results.map(sid=>(
            <div key={sid} className="dd-item" onClick={()=>{setQ(sid);setOpen(false);onSelect(sid);}}
              style={{padding:'7px 12px',cursor:'pointer',fontSize:11.5,color:T.text,borderBottom:`1px solid ${T.border}40`,fontFamily:"'IBM Plex Mono',monospace"}}>
               {sid}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── AI View (kept for reference — charts now rendered in NetworkAiChat page) ─
function AIView({result,T,onClose}) {
  const [showTable,setShowTable]=useState(false);
  if(!result)return null;

  const {title,data=[],columns=[],x_axis,y_axes,response,row_count,chart_type,query_type,chart_config,provider,sql}=result;
  const xKey=x_axis||columns[0]||'';
  const SKIP_COLS=new Set(['lat','lng','latitude','longitude','site_id','cell_id','cluster','region','zone','technology','color','status']);
  const yKeys=(y_axes&&y_axes.length)?y_axes:columns.filter(c=>c!==xKey&&!SKIP_COLS.has(c)).slice(0,5);
  const ctype=chart_type||query_type||'bar';
  const cfg=chart_config||{};
  const threshold=cfg.threshold!=null?parseFloat(cfg.threshold):null;
  const TipC=(p)=><Tip T={T} {...p}/>;
  const isTimeSeries=xKey.includes('hour')||xKey.includes('time')||xKey.includes('date');
  const shortLbl=v=>{if(typeof v!=='string')return String(v??'');const m=v.match(/_(\d{4,})$/);return m?`#${m[1]}`:v.length>14?'…'+v.slice(-11):v;};

  // Stat cards per metric
  const stats=yKeys.slice(0,5).map((k,i)=>{
    const vals=data.map(r=>parseFloat(r[k])).filter(v=>!isNaN(v));
    const avg=vals.length?vals.reduce((a,b)=>a+b,0)/vals.length:0;
    return{k,avg,max:vals.length?Math.max(...vals):0,min:vals.length?Math.min(...vals):0,color:PAL[i%PAL.length]};
  });

  const hasGeo=data.some(r=>r.lat||r.latitude);

  // ── Smart chart render ─────────────────────────────────────────────────────
  const renderChart=()=>{
    const h=280;

    // PIE — distribution queries (zone/technology breakdown, 1 metric)
    if(ctype==='pie'&&yKeys.length>=1){
      const pieData=data.slice(0,12).map(r=>({name:shortLbl(r[xKey]),value:parseFloat(r[yKeys[0]])||0}));
      return(
        <ResponsiveContainer width="100%" height={h}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} innerRadius={45} paddingAngle={3} label={({name,percent})=>`${name} ${(percent*100).toFixed(0)}%`} labelLine={false} animationDuration={1000} animationEasing="ease-in-out">
              {pieData.map((_,i)=><Cell key={i} fill={PAL[i%PAL.length]}/>)}
            </Pie>
            <Tooltip formatter={(v)=>f(v,1)}/>
            <Legend iconType="circle" iconSize={8} wrapperStyle={{fontSize:10}}/>
          </PieChart>
        </ResponsiveContainer>
      );
    }

    // RADAR — multi-KPI profile for few sites
    if(ctype==='radar'&&yKeys.length>=3){
      const radarData=yKeys.map(k=>{
        const obj={metric:k.replace(/_/g,' ')};
        data.slice(0,5).forEach((r,i)=>{obj[`site${i}`]=parseFloat(r[k])||0;});
        return obj;
      });
      const radarKeys=data.slice(0,5).map((_,i)=>`site${i}`);
      return(
        <ResponsiveContainer width="100%" height={h}>
          <RadarChart data={radarData}>
            <PolarGrid stroke={T.border}/>
            <PolarAngleAxis dataKey="metric" tick={{fontSize:9,fill:T.muted}}/>
            {radarKeys.map((k,i)=><Radar key={k} name={data[i]?.[xKey]||k} dataKey={k} stroke={PAL[i%PAL.length]} fill={PAL[i%PAL.length]} fillOpacity={0.15} animationDuration={1000} animationEasing="ease-in-out"/>)}
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
            <Tooltip/>
          </RadarChart>
        </ResponsiveContainer>
      );
    }

    // SCATTER — correlation between two metrics
    if(ctype==='scatter'&&yKeys.length>=2){
      const scatterData=data.map(r=>({x:parseFloat(r[yKeys[0]])||0,y:parseFloat(r[yKeys[1]])||0,z:1,name:r[xKey]}));
      return(
        <ResponsiveContainer width="100%" height={h}>
          <ScatterChart margin={{top:10,right:20,left:0,bottom:10}}>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border}/>
            <XAxis type="number" dataKey="x" name={yKeys[0].replace(/_/g,' ')} tick={{fontSize:9,fill:T.muted}} axisLine={false}/>
            <YAxis type="number" dataKey="y" name={yKeys[1].replace(/_/g,' ')} tick={{fontSize:9,fill:T.muted}} axisLine={false} width={38}/>
            <ZAxis dataKey="z" range={[30,30]}/>
            <Tooltip cursor={{strokeDasharray:'3 3'}} content={({active,payload})=>{
              if(!active||!payload?.length)return null;
              const d=payload[0]?.payload;
              return<div style={{...card(T),padding:'7px 10px',fontSize:10}}><b>{d?.name}</b><br/>{yKeys[0]}: {f(d?.x,2)}<br/>{yKeys[1]}: {f(d?.y,2)}</div>;
            }}/>
            {threshold!=null&&<ReferenceLine y={threshold} stroke={T.amber} strokeDasharray="4 2" label={{value:`Threshold ${threshold}`,fontSize:9,fill:T.amber}}/>}
            <Scatter name="Sites" data={scatterData} fill={T.kpmgBlue} opacity={0.75} animationDuration={1000} animationEasing="ease-in-out"/>
          </ScatterChart>
        </ResponsiveContainer>
      );
    }

    // COMPOSED — bar + line overlay (e.g. PRB bar + tput line)
    if(ctype==='composed'&&yKeys.length>=2){
      const barKey=yKeys[0], lineKey=yKeys[1];
      const cTickInterval=data.length>15?Math.ceil(data.length/10):0;
      return(
        <ResponsiveContainer width="100%" height={h+30}>
          <ComposedChart data={data} margin={{top:5,right:20,left:0,bottom:35}}>
            <defs>
              <linearGradient id="cga" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={PAL[0]} stopOpacity={.45}/><stop offset="100%" stopColor={PAL[0]} stopOpacity={.03}/></linearGradient>
              <linearGradient id="cgl" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={PAL[3]} stopOpacity={.3}/><stop offset="100%" stopColor={PAL[3]} stopOpacity={.02}/></linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
            <XAxis dataKey={xKey} tick={{fontSize:8,fill:T.muted}} axisLine={false} tickLine={false}
              interval={cTickInterval} angle={-35} textAnchor="end" height={45}
              tickFormatter={v=>{if(typeof v!=='string')return v;if(v.length>10)return v.slice(5,10);return v;}}/>
            <YAxis yAxisId="l" tick={{fontSize:9,fill:T.muted}} axisLine={false} width={35}/>
            <YAxis yAxisId="r" orientation="right" tick={{fontSize:9,fill:T.muted}} axisLine={false} width={35}/>
            <Tooltip content={<TipC/>} cursor={{stroke:T.kpmgBlue,strokeWidth:1,strokeDasharray:'4 2'}}/>
            {threshold!=null&&<ReferenceLine yAxisId="l" y={threshold} stroke={T.amber} strokeDasharray="4 2"/>}
            <Area yAxisId="l" type="natural" dataKey={barKey} name={barKey.replace(/_/g,' ')} fill="url(#cga)" stroke={PAL[0]} strokeWidth={2.5} activeDot={{r:5,strokeWidth:2,stroke:'#fff'}} animationDuration={1000} animationEasing="ease-in-out"/>
            <Line yAxisId="r" type="natural" dataKey={lineKey} name={lineKey.replace(/_/g,' ')} stroke={PAL[3]} strokeWidth={2.5} dot={data.length<=20?{r:2.5,strokeWidth:2,stroke:'#fff'}:false} activeDot={{r:6,strokeWidth:2,stroke:'#fff'}} animationDuration={1200} animationEasing="ease-in-out"/>
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
          </ComposedChart>
        </ResponsiveContainer>
      );
    }

    // LINE / AREA — time series
    if(ctype==='line'||(ctype==='area')||isTimeSeries){
      const tickInterval=data.length>20?Math.ceil(data.length/8):data.length>10?2:0;
      return(
        <ResponsiveContainer width="100%" height={h+40}>
          <AreaChart data={data} margin={{top:5,right:30,left:5,bottom:45}}>
            <defs>{yKeys.map((k,i)=><linearGradient key={k} id={`aig${i}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={PAL[i%10]} stopOpacity={.45}/><stop offset="100%" stopColor={PAL[i%10]} stopOpacity={.03}/></linearGradient>)}</defs>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
            <XAxis dataKey={xKey} tick={{fontSize:8,fill:T.muted}} axisLine={false} tickLine={false}
              interval={tickInterval} angle={-40} textAnchor="end" height={55}
              tickFormatter={v=>{if(typeof v!=='string')return v;return v.replace(/^20\d{2}-/,'').slice(0,5);}}/>
            <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={42}/>
            <Tooltip content={<TipC/>} cursor={{stroke:T.kpmgBlue,strokeWidth:1,strokeDasharray:'4 2'}}/>
            {threshold!=null&&<ReferenceLine y={threshold} stroke={T.amber} strokeDasharray="4 2" label={{value:`${threshold}`,fontSize:9,fill:T.amber}}/>}
            {yKeys.map((k,i)=><Area key={k} type="natural" dataKey={k} stroke={PAL[i%10]} fill={`url(#aig${i})`} strokeWidth={2.5} dot={data.length<=30?{r:3,strokeWidth:2,stroke:'#fff'}:false} activeDot={{r:6,strokeWidth:2,stroke:'#fff'}} name={k.replace(/_/g,' ')} animationDuration={1000} animationEasing="ease-in-out"/>)}
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
          </AreaChart>
        </ResponsiveContainer>
      );
    }

    // BAR — vertical bars for small datasets, horizontal for rankings
    const isHorizontal=data.length>8||typeof data[0]?.[xKey]==='string';
    if(isHorizontal){
      return(
        <ResponsiveContainer width="100%" height={Math.max(h,data.length*18+40)}>
          <BarChart data={data} layout="vertical" margin={{top:5,right:25,left:110,bottom:5}}>
            <defs>{yKeys.slice(0,3).map((k,i)=><linearGradient key={k} id={`hbg${i}`} x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stopColor={PAL[i%10]} stopOpacity={.85}/><stop offset="100%" stopColor={PAL[(i+1)%10]} stopOpacity={.95}/></linearGradient>)}</defs>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} horizontal={false}/>
            <XAxis type="number" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
            <YAxis type="category" dataKey={xKey} width={105} tick={{fontSize:8.5,fill:T.muted}} tickFormatter={shortLbl} axisLine={false} tickLine={false}/>
            <Tooltip content={<TipC/>} cursor={{fill:T.kpmgBlue+'0a'}}/>
            {threshold!=null&&<ReferenceLine x={threshold} stroke={T.amber} strokeDasharray="4 2"/>}
            {yKeys.slice(0,3).map((k,i)=>(
              <Bar key={k} dataKey={k} name={k.replace(/_/g,' ')} radius={[0,6,6,0]} barSize={yKeys.length>1?8:14} animationDuration={1200} animationEasing="ease-in-out">
                {yKeys.length===1?data.map((d,di)=>{
                  const v=parseFloat(d[k]);
                  const bad=threshold!=null&&(cfg.threshold_dir==='below'?v<threshold:v>threshold);
                  return<Cell key={di} fill={bad?T.red:`url(#hbg${i})`}/>;
                }):<Cell fill={`url(#hbg${i})`}/>}
              </Bar>
            ))}
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
          </BarChart>
        </ResponsiveContainer>
      );
    }
    return(
      <ResponsiveContainer width="100%" height={h}>
        <BarChart data={data} margin={{top:5,right:10,left:0,bottom:30}}>
          <defs>{yKeys.slice(0,3).map((k,i)=><linearGradient key={k} id={`vbg${i}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={PAL[i%10]} stopOpacity={.95}/><stop offset="100%" stopColor={PAL[i%10]} stopOpacity={.6}/></linearGradient>)}</defs>
          <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
          <XAxis dataKey={xKey} tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} angle={-35} textAnchor="end" tickFormatter={shortLbl}/>
          <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
          <Tooltip content={<TipC/>} cursor={{fill:T.kpmgBlue+'0a'}}/>
          {threshold!=null&&<ReferenceLine y={threshold} stroke={T.amber} strokeDasharray="4 2"/>}
          {yKeys.slice(0,3).map((k,i)=><Bar key={k} dataKey={k} name={k.replace(/_/g,' ')} radius={[6,6,0,0]} fill={`url(#vbg${i})`} barSize={20} animationDuration={1200} animationEasing="ease-in-out"/>)}
          <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
        </BarChart>
      </ResponsiveContainer>
    );
  };

  const chartIcon={'bar':'','line':'','area':'','pie':'','scatter':'⬡','composed':'','radar':'','heatmap':''}[ctype]||'';

  return(
    <div style={{animation:'fadeIn .3s ease'}}>
      {/* Header */}
      <div style={{display:'flex',alignItems:'flex-start',justifyContent:'space-between',marginBottom:14}}>
        <div>
          <div style={{fontSize:17,fontWeight:800,color:T.text,marginBottom:3}}> {title}</div>
          <div style={{fontSize:11.5,color:T.muted,lineHeight:1.6}}>{response}</div>
          <div style={{display:'flex',gap:6,marginTop:5,flexWrap:'wrap'}}>
            <Bdg color={T.kpmgBlue}>{row_count} records</Bdg>
            <Bdg color={T.purple}>{chartIcon} {ctype}</Bdg>
            {provider&&<Bdg color={T.teal}> {provider}</Bdg>}
            {threshold!=null&&<Bdg color={T.amber}>SLA: {threshold}</Bdg>}
          </div>
        </div>
        <div style={{display:'flex',gap:8,alignItems:'center'}}>
          <button onClick={()=>setShowTable(t=>!t)} style={{padding:'5px 12px',borderRadius:16,fontSize:10.5,fontWeight:700,background:showTable?T.kpmgBlue:'transparent',color:showTable?'#fff':T.textSub,border:`1.5px solid ${showTable?T.kpmgBlue:T.border}`,cursor:'pointer'}}>
            {showTable?' Chart':' Table'}
          </button>
          <button onClick={onClose} style={{padding:'5px 12px',borderRadius:16,fontSize:10.5,fontWeight:700,background:'transparent',border:`1.5px solid ${T.border}`,color:T.textSub,cursor:'pointer'}}>← Back</button>
        </div>
      </div>

      {/* KPI Stat Cards */}
      {stats.length>0&&(
        <div style={{display:'grid',gridTemplateColumns:`repeat(${Math.min(stats.length,5)},1fr)`,gap:9,marginBottom:13}}>
          {stats.map(({k,avg,max,min,color})=>(
            <div key={k} style={{...card(T),padding:'11px 13px',borderTop:`3px solid ${color}`}}>
              <div style={{fontSize:8.5,fontWeight:700,color:T.muted,textTransform:'uppercase',marginBottom:4,lineHeight:1.3}}>{k.replace(/_/g,' ')}</div>
              <div style={{fontSize:20,fontWeight:800,color:T.text,fontFamily:"'IBM Plex Mono',monospace"}}>{f(avg,1)}</div>
              <div style={{display:'flex',gap:10,marginTop:4}}>
                <span style={{fontSize:9,color:T.green}}>↑ {f(max,1)}</span>
                <span style={{fontSize:9,color:T.red}}>↓ {f(min,1)}</span>
              </div>
            </div>
          ))}
        </div>
      )}

      {showTable?(
        /* Data Table */
        <CC T={T} title=" Query Results">
          <div style={{overflowX:'auto',maxHeight:420,overflowY:'auto'}}>
            <table style={{width:'100%',borderCollapse:'collapse',fontSize:10.5}}>
              <thead style={{position:'sticky',top:0,zIndex:1}}>
                <tr style={{background:T.surface2}}>
                  {columns.map(h=><th key={h} style={{padding:'6px 9px',textAlign:'center',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase',whiteSpace:'nowrap'}}>{h}</th>)}
                </tr>
              </thead>
              <tbody>
                {data.slice(0,200).map((row,i)=>(
                  <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent'}}>
                    {columns.map(c=><td key={c} style={{padding:'4px 8px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace",fontSize:10}}>{row[c]==null?'—':typeof row[c]==='number'?f(row[c],2):String(row[c])}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CC>
      ):(
        /* Chart + Map */
        <div style={{display:'grid',gridTemplateColumns:hasGeo?'1.4fr 1fr':'1fr',gap:12}}>
          <div style={{boxShadow:'0 4px 20px rgba(0,51,141,0.08)',borderRadius:12}}>
            <CC T={T} title={`${chartIcon} ${cfg.x_label||xKey.replace(/_/g,' ')} → ${(yKeys[0]||'').replace(/_/g,' ')}`}>
              {data.length>0?renderChart():<Empty T={T}/>}
            </CC>
          </div>
          {hasGeo&&(
            <CC T={T} title=" Geographic View">
              <LeafletMap sites={data} highlight={data} T={T} height={280}/>
            </CC>
          )}
        </div>
      )}

      {/* SQL disclosure */}
      {sql&&(
        <details style={{marginTop:10}}>
          <summary style={{fontSize:9.5,color:T.muted,cursor:'pointer',fontWeight:600}}> View generated SQL</summary>
          <pre style={{background:T.surface2,border:`1px solid ${T.border}`,borderRadius:8,padding:'10px 12px',fontSize:9.5,color:T.textSub,overflow:'auto',marginTop:6,lineHeight:1.6}}>{sql}</pre>
        </details>
      )}
    </div>
  );
}

// ── Overview Page ─────────────────────────────────────────────────────────────
function OverviewPage({T,data,mapSites,filters}) {
  const d=data||{};
  const TipC=(p)=><Tip T={T} {...p}/>;
  const hs=Math.round(d.network_health_score||0);

  // Pre-scan worst cells (updated every 30 min by backend) — unfiltered, always shows all
  const [preScanCells,setPreScanCells]=useState([]);
  const [preScanTime,setPreScanTime]=useState(null);
  useEffect(()=>{
    apiGet('/api/network-issues/worst-cells')
      .then(r=>{setPreScanCells(r.sites||[]);setPreScanTime(r.scan_time);})
      .catch(()=>{});
  },[]);

  const kpis=[
    {label:'Total Sites',    value:f(d.total_sites,0),    icon:'', color:T.kpmgBlue},
    {label:'Active Cells',   value:f(d.total_cells,0),    icon:'', color:T.blue3},
    {label:'Congested Sites',value:f(d.congested_sites,0),icon:'', color:d.congested_sites>0?T.red:T.green, badge:d.congested_sites>0?{color:T.red,text:'PRB>85%'}:null},
    {label:'Avg DL Tput',    value:f(d.avg_dl_tput),      unit:'Mbps',icon:'', color:T.teal},
    {label:'Call Drop Rate', value:f(d.avg_drop_rate,2),  unit:'%',  icon:'', color:d.avg_drop_rate>2?T.red:T.green, badge:d.avg_drop_rate>2?{color:T.red,text:'High'}:null},
    {label:'Avg PRB Util',   value:f(d.avg_prb),          unit:'%',  icon:'', color:d.avg_prb>80?T.red:d.avg_prb>60?T.amber:T.green},
    {label:'Avg DL Volume',  value:f(d.avg_dl_vol,1),     unit:'GB', icon:'', color:T.blue2},
    {label:'Avg RRC Users',  value:f(d.avg_rrc_ue,0),     icon:'', color:T.purple},
    {label:'Packet Loss',    value:f(d.avg_packet_loss,2), unit:'%', icon:'', color:T.blue2, badge:d.avg_packet_loss>3?{color:T.red,text:'Alert'}:null},
  ];

  const coreStat=[
    {label:'Auth Success Rate',  value:f(d.avg_auth_sr),  unit:'%', color:T.green},
    {label:'Attach Success Rate',value:f(d.avg_attach_sr),unit:'%', color:T.blue3},
    {label:'CPU Utilization',    value:f(d.avg_cpu_util), unit:'%', color:d.avg_cpu_util>80?T.red:T.amber},
    {label:'PDP Bearer SR',      value:f(d.avg_pdp_sr),   unit:'%', color:T.teal},
  ];
  const trStat=[
    {label:'Link Utilization',   value:f(d.avg_link_util), unit:'%', color:T.kpmgBlue},
    {label:'Transport Latency',  value:f(d.avg_tr_latency),unit:'ms',color:T.amber},
    {label:'Transport Pkt Loss', value:f(d.avg_tr_pkt_loss,3),unit:'%',color:T.red},
    {label:'Link Availability',  value:f(d.avg_tr_avail),  unit:'%', color:T.green},
  ];

  const zone=d.zone_performance||[];
  const worst=d.worst_sites||[];
  const worstCells=d.worst_cells||[];
  const best=d.best_sites||[];
  const lowMargin=d.low_margin_sites||[];
  const trend=d.tput_trend||[];

  return (
    <div style={{animation:'fadeIn .3s ease'}}>
      {/* KPI row */}
      <div style={{marginBottom:13}}>
        <SL T={T}>Network Health Overview</SL>
        <div style={{display:'grid',gridTemplateColumns:'auto repeat(9,1fr)',gap:8}}>
          <div style={{...card(T),padding:'12px 14px',display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',minWidth:110}}>
            <div style={{fontSize:9,fontWeight:700,color:T.muted,textTransform:'uppercase',marginBottom:3}}>Health Score</div>
            <Gauge score={hs} label={d.health_label||'Fair'} T={T}/>
          </div>
          {kpis.map((k,i)=><KpiCard key={i} T={T} {...k}/>)}
        </div>
      </div>

      {/* Map + problems */}
      <div style={{display:'grid',gridTemplateColumns:'1fr 280px',gap:11,marginBottom:11}}>
        <CC T={T} title=" Interactive Network Map — Site Health">
          <div style={{display:'flex',gap:10,marginBottom:8,flexWrap:'wrap'}}>
            {[['#22c55e','Healthy (0 issues)'],['#EAB308','Warning (1 issue)'],['#F97316','Degraded (2 issues)'],['#DC2626','Critical (3+ issues)']].map(([c,l])=>(
              <span key={l} style={{display:'flex',alignItems:'center',gap:4,fontSize:9.5,color:T.muted}}>
                <span style={{width:8,height:8,borderRadius:'50%',background:c,border:'1.5px solid white',boxShadow:`0 0 3px ${c}66`}}/>{l}
              </span>
            ))}
            <span style={{fontSize:8.5,color:T.muted,marginLeft:'auto',fontStyle:'italic'}}>PRB &gt;70% · Drop &gt;1.5% · CSSR &lt;98.5% · Tput &lt;8Mbps</span>
          </div>
          <LeafletMap sites={mapSites} T={T} height={260}/>
        </CC>
        <div style={{display:'flex',flexDirection:'column',gap:9}}>
          {/* Network problems */}
          <CC T={T} title=" Network Issues">
            {[
              {icon:'',text:`${d.congested_sites||0} sites congested (PRB > 85%)`,color:T.red},
              {icon:'',text:`Call drop rate ${f(d.avg_drop_rate,2)}%`,color:d.avg_drop_rate>2?T.red:T.green},
              {icon:'',text:`Avg DL throughput ${f(d.avg_dl_tput)} Mbps`,color:T.teal},
              {icon:'',text:`Core Auth SR ${f(d.avg_auth_sr)}%`,color:d.avg_auth_sr>95?T.green:T.amber},
            ].map((p,i)=>(
              <div key={i} style={{display:'flex',gap:7,padding:'5px 7px',borderRadius:7,background:p.color+'10',marginBottom:5}}>
                <span style={{fontSize:11}}>{p.icon}</span>
                <span style={{fontSize:10.5,color:T.text,lineHeight:1.4}}>{p.text}</span>
              </div>
            ))}
          </CC>
          {/* Revenue */}
          <CC T={T} title=" Revenue Q1">
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:7}}>
              {[
                {label:'Revenue',value:`₹${f(d.total_q1_revenue)} L`,color:T.green},
                {label:'OpEx',   value:`₹${f(d.total_q1_opex)} L`, color:T.amber},
                {label:'EBITDA', value:`₹${f(d.ebitda)} L`,        color:d.ebitda>0?T.teal:T.red},
              ].map((s,i)=>(
                <div key={i} style={{background:T.surface2,borderRadius:7,padding:'7px 9px',border:`1px solid ${T.border}`}}>
                  <div style={{fontSize:9,color:T.muted,fontWeight:700,textTransform:'uppercase',marginBottom:2}}>{s.label}</div>
                  <div style={{fontSize:14,fontWeight:800,color:s.color,fontFamily:"'IBM Plex Mono',monospace"}}>{s.value}</div>
                </div>
              ))}
            </div>
          </CC>
        </div>
      </div>

      {/* Charts row */}
      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:10,marginBottom:10}}>
        <CC T={T} title=" Zone PRB Performance">
          {zone.length>0?(
            <ResponsiveContainer width="100%" height={190}>
              <BarChart data={zone}>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                <XAxis dataKey="zone" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={30}/>
                <Tooltip content={<TipC/>}/>
                <Bar dataKey="avg_prb" name="Avg PRB %" radius={[4,4,0,0]} barSize={20}>
                  {zone.map((d,i)=><Cell key={i} fill={d.avg_prb>75?T.red:d.avg_prb>55?T.amber:T.green}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ):<Empty T={T}/>}
        </CC>
        <CC T={T} title=" DL Throughput Trend">
          {trend.length>0?(
            <ResponsiveContainer width="100%" height={190}>
              <AreaChart data={trend}>
                <defs><linearGradient id="tg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#00BCD4" stopOpacity={.4}/><stop offset="100%" stopColor="#00BCD4" stopOpacity={.02}/></linearGradient></defs>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                <XAxis dataKey="time" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} tickFormatter={v=>v?.slice(11,16)||v?.slice(0,10)}/>
                <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
                <Tooltip content={<TipC/>}/>
                <Area type="monotone" dataKey="avg_tput" stroke="#00BCD4" fill="url(#tg)" strokeWidth={2} dot={false} name="DL Tput (Mbps)"/>
                <Line type="monotone" dataKey="avg_prb" stroke="#0A2463" strokeWidth={1.5} dot={false} name="PRB %" strokeDasharray="4 2"/>
                <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
              </AreaChart>
            </ResponsiveContainer>
          ):<Empty T={T}/>}
        </CC>
        <CC T={T} title=" Cross-Layer Health">
          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:6}}>
            {[...coreStat,...trStat].map((s,i)=>(
              <div key={i} style={{background:T.surface2,borderRadius:7,padding:'7px 9px',border:`1px solid ${T.border}`}}>
                <div style={{fontSize:8.5,color:T.muted,fontWeight:700,textTransform:'uppercase',marginBottom:2,lineHeight:1.3}}>{s.label}</div>
                <div style={{fontSize:15,fontWeight:800,color:s.color,fontFamily:"'IBM Plex Mono',monospace"}}>{s.value}<span style={{fontSize:9,color:T.muted,marginLeft:2}}>{s.unit}</span></div>
              </div>
            ))}
          </div>
        </CC>
      </div>

      {/* Worst Cells (Last 7 Days) — populated from daily pre-scan */}
      <div style={{marginBottom:10}}>
        <CC T={T} title=" Worst Cells (Last 7 Days)" action={
          preScanTime&&<span style={{fontSize:9,color:T.muted}}>Updated: {new Date(preScanTime).toLocaleString()}</span>
        }>
          {(()=>{
            // Flatten pre-scan site data into cell-level rows (current date scan only)
            const rows = preScanCells.flatMap(s => (s.cells||[]).map(c => ({
                  cell_id: c, site_id: s.site_id, zone: s.zone || '—',
                  call_drop_rate: f(s.avg_drop,2), lte_cssr: f(s.avg_cssr,2),
                  dl_usr_tput: f(s.avg_tput,2), violations: 3,
                })));
            return rows.length>0?(
              <div style={{maxHeight:220,overflowY:'auto'}}>
                <table style={{width:'100%',borderCollapse:'collapse',fontSize:10.5}}>
                  <thead>
                    <tr>{['Cell','Site','Zone','Drop%','CSSR%','Tput(Mbps)','Flags'].map(h=><th key={h} style={{padding:'4px 6px',textAlign:'left',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase'}}>{h}</th>)}</tr>
                  </thead>
                  <tbody>
                    {rows.map((s,i)=>(
                      <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent'}}>
                        <td style={{padding:'4px 6px',fontWeight:700,color:T.kpmgBlue,fontSize:9.5,fontFamily:"'IBM Plex Mono',monospace"}}>{s.cell_id}</td>
                        <td style={{padding:'4px 6px',color:T.textSub,fontSize:9.5}}>{s.site_id}</td>
                        <td style={{padding:'4px 6px',color:T.textSub,fontSize:9.5}}>{s.zone||s.cluster||'—'}</td>
                        <td style={{padding:'4px 6px',fontFamily:"'IBM Plex Mono',monospace",fontSize:9.5,color:parseFloat(s.call_drop_rate||0)>1.5?T.red:T.green,fontWeight:700}}>{s.call_drop_rate??'—'}</td>
                        <td style={{padding:'4px 6px',fontFamily:"'IBM Plex Mono',monospace",fontSize:9.5,color:parseFloat(s.lte_cssr||100)<98.5?T.red:T.green,fontWeight:700}}>{s.lte_cssr??'—'}</td>
                        <td style={{padding:'4px 6px',fontFamily:"'IBM Plex Mono',monospace",fontSize:9.5,color:parseFloat(s.dl_usr_tput||999)<8?T.red:T.green,fontWeight:700}}>{s.dl_usr_tput??'—'}</td>
                        <td style={{padding:'4px 6px',fontSize:9.5,fontWeight:700,color:T.red}}>{s.violations}/3</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ):<Empty T={T}/>;
          })()}
        </CC>
      </div>

      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10,alignItems:'start'}}>
        <CC T={T} title=" Best Sites (Throughput)">
          {best.length>0?(
            <ResponsiveContainer width="100%" height={Math.max(170, best.length*22+40)}>
              <BarChart data={best} layout="vertical" margin={{left:4,right:12,top:4,bottom:4}}>
                <defs>
                  {best.map((_,i)=>{
                    const colors=[
                      ['#0B3D91','#1E88E5'],['#0D47A1','#2196F3'],['#1565C0','#42A5F5'],
                      ['#1976D2','#64B5F6'],['#1E88E5','#90CAF9'],['#2196F3','#BBDEFB'],
                      ['#42A5F5','#E3F2FD'],['#1A237E','#3F51B5'],['#283593','#5C6BC0'],
                      ['#303F9F','#7986CB']
                    ];
                    const [c1,c2]=colors[i%10];
                    return <linearGradient key={`bg${i}`} id={`bestGrad${i}`} x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stopColor={c1}/><stop offset="100%" stopColor={c2}/></linearGradient>;
                  })}
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} horizontal={false} vertical={true}/>
                <XAxis type="number" unit=" Mbps" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                <YAxis type="category" dataKey="site_id" width={92} tick={{fontSize:9,fill:T.muted,fontFamily:"'IBM Plex Mono',monospace"}} axisLine={false} tickLine={false}/>
                <Tooltip content={<TipC/>}/>
                <Bar dataKey="dl_tput" name="DL Throughput" radius={[0,6,6,0]} barSize={16} label={{position:'right',fontSize:9,fill:T.textSub,fontWeight:700,formatter:v=>`${v} Mbps`}}>
                  {best.map((_,i)=><Cell key={i} fill={`url(#bestGrad${i})`}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ):<Empty T={T}/>}
        </CC>
        <CC T={T} title=" Low Margin Sites">
          {lowMargin.length>0?(
            <div style={{maxHeight:Math.max(170, best.length*22+40),overflowY:'auto'}}>
              <table style={{width:'100%',borderCollapse:'collapse',fontSize:10.5}}>
                <thead>
                  <tr>{['Site','Revenue (₹L)','OPEX (₹L)','Rev − OPEX (₹L)'].map(h=><th key={h} style={{padding:'4px 6px',textAlign:'left',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase'}}>{h}</th>)}</tr>
                </thead>
                <tbody>
                  {lowMargin.map((s,i)=>{const diff=parseFloat(s.q1_rev||0)-parseFloat(s.q1_opex||0);return(
                    <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent'}}>
                      <td style={{padding:'4px 6px',fontWeight:700,color:T.kpmgBlue,fontSize:9.5,fontFamily:"'IBM Plex Mono',monospace"}}>{s.site_id}</td>
                      <td style={{padding:'4px 6px',fontFamily:"'IBM Plex Mono',monospace",fontSize:9.5}}>{f(s.q1_rev)}</td>
                      <td style={{padding:'4px 6px',fontFamily:"'IBM Plex Mono',monospace",fontSize:9.5}}>{f(s.q1_opex)}</td>
                      <td style={{padding:'4px 6px',fontFamily:"'IBM Plex Mono',monospace",fontSize:9.5,color:diff<0?T.red:T.green,fontWeight:700}}>{diff<0?`(${f(Math.abs(diff))})`:`${f(diff)}`}</td>
                    </tr>
                  )})}
                </tbody>
              </table>
            </div>
          ):<Empty T={T}/>}
        </CC>
      </div>
    </div>
  );
}

// ── RAN Page ──────────────────────────────────────────────────────────────────
function RANPage({T,data,mapSites,filters,opts}) {
  const [viewMode,setViewMode]=useState('network');
  const [selectedSite,setSelectedSite]=useState(null);
  const [siteData,setSiteData]=useState(null);
  const [siteLoading,setSiteLoading]=useState(false);
  const d=data||{};
  const TipC=(p)=><Tip T={T} {...p}/>;

  const callDrop=d.call_drop_trend||[];
  const prbDist=d.prb_distribution||[];
  const hourlyDL=d.hourly_dl_traffic||[];
  const zonePerf=d.zone_performance||[];
  const topIssues=d.top_issues||[];
  const sites=d.sites||mapSites||[];

  const fetchSite=useCallback(async(sid)=>{
    setSelectedSite(sid);setSiteData(null);setSiteLoading(true);
    try{
      const fq=Object.entries(filters||{}).filter(([,v])=>v).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&');
      const res=await apiGet(`/api/network/site-ran-detail?site_id=${encodeURIComponent(sid)}&${fq}`);
      setSiteData(res);
    }catch(_){setSiteData(null);}
    setSiteLoading(false);
  },[filters]);

  // Re-fetch selected site when filters change
  useEffect(()=>{if(selectedSite)fetchSite(selectedSite);},[filters]);// eslint-disable-line

  // 6 key RAN KPI cards
  const summary6=[
    {label:'Call Drop Rate',   value:f(d.erab_drop_rate,2),unit:'%',  icon:'', color:fn(d.erab_drop_rate)>2?T.red:T.green,   badge:fn(d.erab_drop_rate)>2?{color:T.red,text:'High'}:null},
    {label:'Call Failure Rate',value:f(100-fn(d.lte_call_setup_sr),2),unit:'%',icon:'',color:T.red},
    {label:'DL Throughput',    value:f(d.dl_cell_tput),    unit:'Mbps',icon:'', color:T.teal},
    {label:'RRC Attached UEs', value:f(d.avg_rrc_ue,0),    icon:'',  color:T.purple},
    {label:'DL PRB Util',      value:f(d.dl_prb_util),     unit:'%',   icon:'', color:fn(d.dl_prb_util)>80?T.red:T.amber},
    {label:'DL Traffic Volume',value:f(d.dl_data_vol),     unit:'GB',  icon:'', color:T.blue3},
  ];

  return (
    <div style={{animation:'fadeIn .3s ease'}}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:13}}>
        <div>
          <div style={{fontSize:17,fontWeight:800,color:T.text,display:'flex',alignItems:'center',gap:8}}> RAN Network Performance</div>
          <div style={{fontSize:11,color:T.muted,marginTop:2}}>Radio Access Network · Call Drop · Throughput · PRB · RRC Users · DL Traffic</div>
        </div>
        <div style={{display:'flex',gap:7,alignItems:'center'}}>
          {['network','sitewise'].map(m=>(
            <button key={m} onClick={()=>{setViewMode(m);if(m==='network')setSelectedSite(null);}}
              style={{padding:'5px 13px',borderRadius:18,fontSize:11,fontWeight:700,background:viewMode===m?T.kpmgBlue:'transparent',color:viewMode===m?'#fff':T.textSub,border:`1.5px solid ${viewMode===m?T.kpmgBlue:T.border}`,cursor:'pointer',transition:'all .2s'}}>
              {m==='network'?' Network':' Site-wise'}
            </button>
          ))}
        </div>
      </div>

      {/* 6 KPI summary cards */}
      <div style={{display:'grid',gridTemplateColumns:'repeat(6,1fr)',gap:8,marginBottom:12}}>
        {summary6.map((k,i)=><KpiCard key={i} T={T} {...k}/>)}
      </div>

      {viewMode==='network'?(
        <>
          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:10,marginBottom:10}}>
            {/* Call Drop Trend */}
            <CC T={T} title=" Call Drop Rate Trend">
              {callDrop.length>0?(
                <ResponsiveContainer width="100%" height={185}>
                  <AreaChart data={callDrop}>
                    <defs><linearGradient id="cdg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#E91E8C" stopOpacity={.4}/><stop offset="100%" stopColor="#E91E8C" stopOpacity={.02}/></linearGradient></defs>
                    <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                    <XAxis dataKey="date" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} tickFormatter={v=>v?.slice(0,10)}/>
                    <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
                    <Tooltip content={<TipC/>}/>
                    <ReferenceLine y={2} stroke="#0A2463" strokeDasharray="4 2" label={{value:'SLA 2%',fill:'#0A2463',fontSize:9}}/>
                    <Area type="monotone" dataKey="drop_rate" stroke="#E91E8C" fill="url(#cdg)" strokeWidth={2} dot={false} name="Drop Rate %"/>
                  </AreaChart>
                </ResponsiveContainer>
              ):<Empty T={T}/>}
            </CC>
            {/* PRB Distribution */}
            <CC T={T} title=" PRB Utilization Distribution">
              {prbDist.length>0?(
                <ResponsiveContainer width="100%" height={185}>
                  <BarChart data={prbDist}>
                    <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                    <XAxis dataKey="range" tick={{fontSize:8.5,fill:T.muted}} axisLine={false} tickLine={false}/>
                    <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={30}/>
                    <Tooltip content={<TipC/>}/>
                    <Bar dataKey="count" name="Sites" radius={[4,4,0,0]}>
                      {prbDist.map((_,i)=><Cell key={i} fill={i<2?'#00BCD4':i<4?'#2B4DCC':'#E91E8C'}/>)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              ):<Empty T={T}/>}
            </CC>
            {/* Hourly DL Traffic */}
            <CC T={T} title=" Hourly DL Traffic Volume">
              {hourlyDL.length>0?(
                <ResponsiveContainer width="100%" height={185}>
                  <ComposedChart data={hourlyDL}>
                    <defs><linearGradient id="dlg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#2B4DCC" stopOpacity={.4}/><stop offset="100%" stopColor="#2B4DCC" stopOpacity={.02}/></linearGradient></defs>
                    <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                    <XAxis dataKey="hour" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} tickFormatter={v=>v?.slice(11,16)||v?.slice(0,10)}/>
                    <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
                    <Tooltip content={<TipC/>}/>
                    <Area type="monotone" dataKey="dl_volume" stroke="#2B4DCC" fill="url(#dlg)" strokeWidth={2} dot={false} name="DL Vol (GB)"/>
                    <Line type="monotone" dataKey="ul_volume" stroke="#7B1FA2" strokeWidth={1.5} dot={false} name="UL Vol (GB)" strokeDasharray="4 2"/>
                    <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
                  </ComposedChart>
                </ResponsiveContainer>
              ):<Empty T={T}/>}
            </CC>
          </div>
          <div style={{display:'grid',gridTemplateColumns:'1fr 1.4fr 1fr',gap:10}}>
            {/* Zone Radar */}
            <CC T={T} title=" Zone Performance Radar">
              <ResponsiveContainer width="100%" height={210}>
                <RadarChart data={zonePerf.length>0?zonePerf:[{zone:'No data',avg_prb:0,avg_tput:0}]} cx="50%" cy="50%" outerRadius={75}>
                  <PolarGrid stroke={T.border}/>
                  <PolarAngleAxis dataKey="zone" tick={{fontSize:9.5,fill:T.muted}}/>
                  <Radar name="PRB %" dataKey="avg_prb" stroke="#2B4DCC" fill="#2B4DCC" fillOpacity={.25}/>
                  <Radar name="Throughput" dataKey="avg_tput" stroke="#00BCD4" fill="#00BCD4" fillOpacity={.2}/>
                  <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
                  <Tooltip content={<TipC/>}/>
                </RadarChart>
              </ResponsiveContainer>
            </CC>
            {/* Map */}
            <CC T={T} title=" Network Site Map — 4-Factor Health">
              <div style={{display:'flex',gap:8,marginBottom:6,flexWrap:'wrap'}}>
                {[['#22c55e','Healthy'],['#EAB308','Warning'],['#F97316','Degraded'],['#DC2626','Critical']].map(([c,l])=>(
                  <span key={l} style={{display:'flex',alignItems:'center',gap:3,fontSize:9,color:T.muted}}>
                    <span style={{width:7,height:7,borderRadius:'50%',background:c,border:'1.5px solid white',boxShadow:`0 0 3px ${c}66`}}/>{l}
                  </span>
                ))}
              </div>
              <LeafletMap sites={sites} T={T} height={200}/>
            </CC>
            {/* Top issues */}
            <CC T={T} title=" Top Issue Sites">
              <div style={{maxHeight:210,overflowY:'auto'}}>
                {topIssues.length===0?<Empty T={T}/>:topIssues.slice(0,10).map((s,i)=>(
                  <div key={i} style={{display:'flex',alignItems:'center',gap:7,padding:'5px 0',borderBottom:`1px solid ${T.border}`}}>
                    <span style={{width:18,height:18,borderRadius:'50%',background:i<3?T.red:i<6?T.amber:T.green,display:'flex',alignItems:'center',justifyContent:'center',fontSize:9,fontWeight:800,color:'#fff',flexShrink:0}}>{i+1}</span>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{fontSize:10,fontWeight:700,color:T.text,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{s.site_id}</div>
                      <div style={{fontSize:9,color:T.muted}}>PRB {f(s.avg_prb)}% · Drop {f(s.drop_rate,2)}%</div>
                    </div>
                    <Bdg color={i<3?T.red:i<6?T.amber:T.green} sm>{i<3?'Crit':i<6?'Warn':'Watch'}</Bdg>
                  </div>
                ))}
              </div>
            </CC>
          </div>
        </>
      ):(
        /* Site-wise view */
        <div>
          <div style={{display:'flex',alignItems:'center',gap:10,marginBottom:12}}>
            <SiteSearch T={T} layer="ran" onSelect={fetchSite} placeholder="Type site ID to see detailed KPIs…" filters={filters}/>
            {selectedSite&&<span style={{fontSize:11,color:T.muted}}>Showing: <strong style={{color:T.kpmgBlue}}>{selectedSite}</strong></span>}
            {selectedSite&&<button onClick={()=>{setSelectedSite(null);setSiteData(null);}} style={{padding:'4px 10px',borderRadius:8,fontSize:10,border:`1px solid ${T.border}`,background:T.surface2,color:T.textSub,cursor:'pointer'}}> Clear</button>}
          </div>

          {selectedSite&&(
            siteLoading?<Spin T={T}/>:siteData?(
              <div style={{animation:'fadeIn .3s ease'}}>
                {/* Site location map + KPI cards */}
                <div style={{display:'grid',gridTemplateColumns:'1fr 280px',gap:10,marginBottom:11}}>
                  <div style={{display:'grid',gridTemplateColumns:'repeat(3,1fr)',gap:8,alignContent:'start'}}>
                    {[
                      {label:'Call Drop Rate',  value:f(siteData.summary?.call_drop_rate,2),unit:'%',icon:'',color:fn(siteData.summary?.call_drop_rate)>2?T.red:T.green},
                      {label:'Call Failure',    value:f(siteData.summary?.call_failure_rate,2),unit:'%',icon:'',color:T.red},
                      {label:'DL Throughput',   value:f(siteData.summary?.dl_throughput),unit:'Mbps',icon:'',color:T.teal},
                      {label:'RRC Users (Avg)', value:f(siteData.summary?.rrc_users,0),icon:'',color:T.purple},
                      {label:'DL PRB Util',     value:f(siteData.summary?.dl_prb_util),unit:'%',icon:'',color:fn(siteData.summary?.dl_prb_util)>80?T.red:T.amber},
                      {label:'DL Traffic Vol',  value:f(siteData.summary?.dl_traffic_vol),unit:'GB',icon:'',color:T.blue3},
                    ].map((k,i)=><KpiCard key={i} T={T} {...k}/>)}
                  </div>
                  <CC T={T} title={` ${selectedSite} Location`}>
                    {siteData.meta?.latitude&&siteData.meta?.longitude?(
                      <LeafletMap sites={[{site_id:selectedSite,lat:siteData.meta.latitude,lng:siteData.meta.longitude,cluster:siteData.meta.zone||'',dl_prb_util:siteData.summary?.dl_prb_util||0,call_drop_rate:siteData.summary?.call_drop_rate||0,dl_tput:siteData.summary?.dl_throughput||0}]} highlight={[{site_id:selectedSite,lat:siteData.meta.latitude,lng:siteData.meta.longitude,cluster:siteData.meta.zone||'',dl_prb_util:siteData.summary?.dl_prb_util||0,call_drop_rate:siteData.summary?.call_drop_rate||0,dl_tput:siteData.summary?.dl_throughput||0}]} T={T} height={180}/>
                    ):<div style={{padding:20,textAlign:'center',color:T.muted,fontSize:11}}>No location data</div>}
                  </CC>
                </div>
                {/* Trend charts */}
                <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10,marginBottom:10}}>
                  <CC T={T} title={` Call Drop & Failure Trend — ${selectedSite}`}>
                    {siteData.daily_trend?.length>0?(
                      <ResponsiveContainer width="100%" height={195}>
                        <ComposedChart data={siteData.daily_trend}>
                          <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                          <XAxis dataKey="date" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                          <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
                          <Tooltip content={<TipC/>}/>
                          <ReferenceLine y={2} stroke={T.amber} strokeDasharray="4 2" label={{value:'SLA',fill:T.amber,fontSize:9}}/>
                          <Bar dataKey="call_drop_rate" name="Call Drop %" fill="#E91E8C" opacity={.85} barSize={8} radius={[2,2,0,0]}/>
                          <Line type="monotone" dataKey="call_failure_rate" stroke="#0A2463" strokeWidth={2.5} dot={false} name="Call Failure %"/>
                          <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
                        </ComposedChart>
                      </ResponsiveContainer>
                    ):<Empty T={T}/>}
                  </CC>
                  <CC T={T} title={` Throughput & PRB — ${selectedSite}`}>
                    {siteData.daily_trend?.length>0?(
                      <ResponsiveContainer width="100%" height={195}>
                        <ComposedChart data={siteData.daily_trend}>
                          <defs><linearGradient id="stg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#00BCD4" stopOpacity={.4}/><stop offset="100%" stopColor="#00BCD4" stopOpacity={.02}/></linearGradient></defs>
                          <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                          <XAxis dataKey="date" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                          <YAxis yAxisId="l" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
                          <YAxis yAxisId="r" orientation="right" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={32}/>
                          <Tooltip content={<TipC/>}/>
                          <Area yAxisId="l" type="monotone" dataKey="dl_throughput" stroke="#00BCD4" fill="url(#stg)" strokeWidth={2} dot={false} name="DL Tput (Mbps)"/>
                          <Line yAxisId="r" type="monotone" dataKey="dl_prb_util" stroke="#0A2463" strokeWidth={2} dot={false} name="PRB %" strokeDasharray="4 2"/>
                          <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
                        </ComposedChart>
                      </ResponsiveContainer>
                    ):<Empty T={T}/>}
                  </CC>
                  <CC T={T} title={` RRC Attached Users — ${selectedSite}`}>
                    {siteData.daily_trend?.length>0?(
                      <ResponsiveContainer width="100%" height={185}>
                        <BarChart data={siteData.daily_trend}>
                          <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                          <XAxis dataKey="date" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                          <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={38} domain={[0,'auto']}/>
                          <Tooltip content={<TipC/>}/>
                          <Bar dataKey="rrc_users" name="RRC Users" fill="#7B1FA2" opacity={.85} barSize={10} radius={[3,3,0,0]}/>
                        </BarChart>
                      </ResponsiveContainer>
                    ):<Empty T={T}/>}
                  </CC>
                  <CC T={T} title={` DL Traffic Volume — ${selectedSite}`}>
                    {siteData.daily_trend?.length>0?(
                      <ResponsiveContainer width="100%" height={185}>
                        <BarChart data={siteData.daily_trend}>
                          <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                          <XAxis dataKey="date" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                          <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
                          <Tooltip content={<TipC/>}/>
                          <Bar dataKey="dl_traffic_vol" name="DL Traffic (GB)" radius={[3,3,0,0]}>
                            {(siteData.daily_trend||[]).map((_,i)=><Cell key={i} fill={PAL[i%10]}/>)}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    ):<Empty T={T}/>}
                  </CC>
                </div>
                {/* Cell table */}
                {siteData.cells?.length>0&&(
                  <CC T={T} title={` Cell-level Breakdown — ${selectedSite}`}>
                    <table style={{width:'100%',borderCollapse:'collapse',fontSize:10.5}}>
                      <thead>
                        <tr style={{background:T.surface2}}>{['Cell ID','PRB %','DL Tput (Mbps)','Call Drop %','RRC Users'].map(h=><th key={h} style={{padding:'6px 9px',textAlign:'center',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase'}}>{h}</th>)}</tr>
                      </thead>
                      <tbody>
                        {siteData.cells.map((c,i)=>(
                          <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent'}}>
                            <td style={{padding:'5px 9px',fontWeight:700,color:T.kpmgBlue,fontFamily:"'IBM Plex Mono',monospace"}}>{c.cell_id}</td>
                            <td style={{padding:'5px 9px',textAlign:'center',color:c.dl_prb_util>80?T.red:T.amber,fontWeight:700,fontFamily:"'IBM Plex Mono',monospace"}}>{f(c.dl_prb_util)}%</td>
                            <td style={{padding:'5px 9px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace"}}>{f(c.dl_cell_tput||c.dl_throughput)}</td>
                            <td style={{padding:'5px 9px',textAlign:'center',color:(c.erab_drop_rate||c.call_drop_rate)>2?T.red:T.green,fontWeight:700,fontFamily:"'IBM Plex Mono',monospace"}}>{f(c.erab_drop_rate||c.call_drop_rate,2)}%</td>
                            <td style={{padding:'5px 9px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace"}}>{f(c.avg_rrc_ue||c.rrc_users,0)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </CC>
                )}
              </div>
            ):<div style={{padding:20,textAlign:'center',color:T.muted,fontSize:12}}>No data found for site "{selectedSite}"</div>
          )}

          {!selectedSite&&(
            <CC T={T} title=" Site-wise RAN KPI Table (All Sites)">
              <div style={{overflowX:'auto'}}>
                <table style={{width:'100%',borderCollapse:'collapse',fontSize:10}}>
                  <thead>
                    <tr style={{background:T.surface2}}>
                      {['Site ID','Zone','Call Drop %','DL Tput (Mbps)','RRC Users','PRB %','DL Traffic (GB)','Status'].map(h=>(
                        <th key={h} style={{padding:'6px 8px',textAlign:'center',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase',whiteSpace:'nowrap'}}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sites.slice(0,80).map((s,i)=>{
                      const prb=fn(s.dl_prb_util||s.prb_utilization);
                      const st=prb>85?'Critical':prb>60?'Warning':'Healthy';
                      const sc=st==='Critical'?T.red:st==='Warning'?T.amber:T.green;
                      return (
                        <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent',cursor:'pointer'}}
                          onClick={()=>fetchSite(s.site_id)}>
                          <td style={{padding:'5px 8px',fontWeight:700,color:T.kpmgBlue,fontSize:10}}>{s.site_id}</td>
                          <td style={{padding:'5px 8px',textAlign:'center',color:T.textSub}}>{s.zone||s.cluster||'—'}</td>
                          <td style={{padding:'5px 8px',textAlign:'center',color:fn(s.erab_drop_rate||s.call_drop_rate)>2?T.red:T.green,fontWeight:700,fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.erab_drop_rate||s.call_drop_rate,2)}%</td>
                          <td style={{padding:'5px 8px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.dl_cell_tput||s.throughput)}</td>
                          <td style={{padding:'5px 8px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.avg_rrc_ue,0)}</td>
                          <td style={{padding:'5px 8px',textAlign:'center',color:prb>80?T.red:T.amber,fontWeight:700,fontFamily:"'IBM Plex Mono',monospace"}}>{f(prb)}%</td>
                          <td style={{padding:'5px 8px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.dl_data_vol)}</td>
                          <td style={{padding:'5px 8px',textAlign:'center'}}><Bdg color={sc} sm>{st}</Bdg></td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </CC>
          )}
        </div>
      )}
    </div>
  );
}

// ── Core Page ────────────────────────────────────────────────────────────────
// Reusable ComposedChart for a single Core KPI with stats strip
function CoreKpiChart({T,data,dataKey,color,sla,slaLabel,height=190}) {
  const TipC=(p)=><Tip T={T} {...p}/>;
  if(!data||data.length===0) return <Empty T={T}/>;
  const vals=data.map(d=>parseFloat(d[dataKey]??0)).filter(v=>!isNaN(v));
  const avg=vals.length?(vals.reduce((a,b)=>a+b,0)/vals.length).toFixed(1):'—';
  const min=vals.length?Math.min(...vals).toFixed(1):'—';
  const max=vals.length?Math.max(...vals).toFixed(1):'—';
  return (
    <div>
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} margin={{top:4,right:8,bottom:0,left:0}}>
          <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
          <XAxis dataKey="date" tick={{fontSize:8.5,fill:T.muted}} axisLine={false} tickLine={false}
                 tickFormatter={v=>v?.slice(5)||v}/>
          <YAxis tick={{fontSize:8.5,fill:T.muted}} axisLine={false} tickLine={false} width={32} domain={['auto','auto']}/>
          <Tooltip content={<TipC/>}/>
          {sla&&<ReferenceLine y={sla} stroke={T.amber} strokeDasharray="5 3"
                               label={{value:slaLabel||`SLA ${sla}%`,fill:T.amber,fontSize:8.5,position:'insideTopRight'}}/>}
          <Bar dataKey={dataKey} fill={color} opacity={.85} barSize={10} radius={[3,3,0,0]}
               name={dataKey.replace(/_/g,' ')}/>
        </BarChart>
      </ResponsiveContainer>
      <div style={{display:'flex',gap:20,justifyContent:'center',padding:'5px 0 2px',borderTop:`1px solid ${T.border}`,marginTop:3}}>
        {[['Avg',avg],['Min',min],['Max',max]].map(([lbl,val])=>(
          <div key={lbl} style={{textAlign:'center'}}>
            <div style={{fontSize:8,color:T.muted,fontWeight:700,letterSpacing:'0.04em'}}>{lbl}</div>
            <div style={{fontSize:11,fontWeight:800,color,fontFamily:"'IBM Plex Mono',monospace"}}>{val}%</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function CorePage({T,data,filters}) {
  const [selectedSite,setSelectedSite]=useState(null);
  const [siteData,setSiteData]=useState(null);
  const [siteLoading,setSiteLoading]=useState(false);
  const d=data||{};

  const fetchSite=useCallback(async(sid)=>{
    setSelectedSite(sid);setSiteData(null);setSiteLoading(true);
    try{
      const fq=filters?Object.entries(filters).filter(([,v])=>v).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&'):'';
      const r=await apiGet(`/api/network/site-core-detail?site_id=${encodeURIComponent(sid)}${fq?'&'+fq:''}`);
      setSiteData(r);
    }catch(_){setSiteData(null);}
    setSiteLoading(false);
  },[filters]);

  useEffect(()=>{if(selectedSite)fetchSite(selectedSite);},[filters]);// eslint-disable-line

  const kpis=[
    {label:'Auth Success Rate', value:f(d.avg_auth_sr),  unit:'%',icon:'',color:fn(d.avg_auth_sr)<95?T.red:T.green,  badge:fn(d.avg_auth_sr)<95?{color:T.red,text:'Below SLA'}:null},
    {label:'CPU Utilization',   value:f(d.avg_cpu),      unit:'%',icon:'', color:fn(d.avg_cpu)>80?T.red:T.amber,     badge:fn(d.avg_cpu)>80?{color:T.red,text:'High'}:null},
    {label:'Attach Success Rate',value:f(d.avg_attach_sr),unit:'%',icon:'',color:fn(d.avg_attach_sr)<95?T.red:T.green},
    {label:'PDP Bearer SR',     value:f(d.avg_pdp_sr),   unit:'%',icon:'',color:fn(d.avg_pdp_sr)<95?T.red:T.teal},
  ];

  const siteSummary=d.site_summary||[];

  // Shared chart config for network & site level
  const CORE_CHARTS=[
    {title:' Authentication SR Trend', key:'auth_sr',  color:'#2B4DCC', sla:95, slaLabel:'SLA 95%'},
    {title:' CPU Utilization Trend',   key:'cpu_util', color:'#E91E8C', sla:80, slaLabel:'Alert 80%'},
    {title:' Attach Success Rate',     key:'attach_sr',color:'#00BCD4', sla:95},
    {title:' PDP Bearer SR',           key:'pdp_sr',   color:'#7B1FA2', sla:95},
  ];

  // Map key → network trend array
  const netTrends={auth_sr:d.auth_trend,cpu_util:d.cpu_trend,attach_sr:d.attach_trend,pdp_sr:d.pdp_trend};
  // Map key → site trend array
  const siteTrends=siteData?{auth_sr:siteData.auth_trend,cpu_util:siteData.cpu_trend,attach_sr:siteData.attach_trend,pdp_sr:siteData.pdp_trend}:{};

  return (
    <div style={{animation:'fadeIn .3s ease'}}>
      <div style={{marginBottom:13}}>
        <div style={{fontSize:17,fontWeight:800,color:T.text,marginBottom:3}}> Core Network Performance</div>
        <div style={{fontSize:11,color:T.muted}}>EPC / 5GC · Authentication · Attach · PDP Bearer · CPU Utilization</div>
      </div>
      <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:9,marginBottom:12}}>
        {kpis.map((k,i)=><KpiCard key={i} T={T} {...k}/>)}
      </div>

      {/* Network-level trend charts — 2×2 grid with ComposedChart + stats */}
      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10,marginBottom:12}}>
        {CORE_CHARTS.map(({title,key,color,sla,slaLabel})=>(
          <CC key={key} T={T} title={title}>
            <CoreKpiChart T={T} data={netTrends[key]} dataKey={key} color={color} sla={sla} slaLabel={slaLabel}/>
          </CC>
        ))}
      </div>

      {/* Site search + site-level */}
      <CC T={T} title=" Site-level Core KPI Analysis">
        <div style={{display:'flex',gap:10,marginBottom:12,alignItems:'center'}}>
          <SiteSearch T={T} layer="core" onSelect={fetchSite} placeholder="Search core site ID…" filters={filters}/>
          {selectedSite&&<button onClick={()=>{setSelectedSite(null);setSiteData(null);}} style={{padding:'4px 10px',borderRadius:8,fontSize:10,border:`1px solid ${T.border}`,background:T.surface2,color:T.textSub,cursor:'pointer'}}> Clear</button>}
        </div>

        {selectedSite&&(
          siteLoading?<Spin T={T}/>:siteData?.trend?.length>0?(
            <div style={{animation:'fadeIn .3s ease',marginBottom:4}}>
              {/* Site location map + KPI summary cards */}
              <div style={{display:'grid',gridTemplateColumns:'1fr 280px',gap:10,marginBottom:12}}>
                <div style={{display:'grid',gridTemplateColumns:'repeat(2,1fr)',gap:8,alignContent:'start'}}>
                  {[
                    {label:'Auth SR',   value:f(siteData.summary?.auth_sr),  unit:'%',icon:'',color:fn(siteData.summary?.auth_sr)<95?T.red:T.green},
                    {label:'CPU Util',  value:f(siteData.summary?.cpu_util), unit:'%',icon:'', color:fn(siteData.summary?.cpu_util)>80?T.red:T.amber},
                    {label:'Attach SR', value:f(siteData.summary?.attach_sr),unit:'%',icon:'',color:T.blue3},
                    {label:'PDP SR',    value:f(siteData.summary?.pdp_sr),   unit:'%',icon:'',color:T.teal},
                  ].map((k,i)=><KpiCard key={i} T={T} {...k}/>)}
                </div>
                <CC T={T} title={` ${selectedSite} Location`}>
                  {siteData.meta?.latitude&&siteData.meta?.longitude?(
                    <LeafletMap sites={[{site_id:selectedSite,lat:siteData.meta.latitude,lng:siteData.meta.longitude,cluster:siteData.meta.zone||''}]} highlight={[{site_id:selectedSite,lat:siteData.meta.latitude,lng:siteData.meta.longitude,cluster:siteData.meta.zone||''}]} T={T} height={160}/>
                  ):<div style={{padding:20,textAlign:'center',color:T.muted,fontSize:11}}>No location data</div>}
                </CC>
              </div>
              {/* Site-level 2×2 chart grid — same style as network level */}
              <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10}}>
                {CORE_CHARTS.map(({title,key,color,sla,slaLabel})=>(
                  <CC key={key} T={T} title={`${title} — ${selectedSite}`}>
                    <CoreKpiChart T={T} data={siteTrends[key]||siteData.trend} dataKey={key} color={color} sla={sla} slaLabel={slaLabel}/>
                  </CC>
                ))}
              </div>
            </div>
          ):<div style={{padding:16,textAlign:'center',color:T.muted,fontSize:12}}>No core data for "{selectedSite}"</div>
        )}

        {/* Network-level site table */}
        {!selectedSite&&siteSummary.length>0&&(
          <div style={{overflowX:'auto'}}>
            <table style={{width:'100%',borderCollapse:'collapse',fontSize:10.5}}>
              <thead>
                <tr style={{background:T.surface2}}>{['Site ID','Auth SR %','CPU %','Attach SR %','PDP SR %','Status'].map(h=><th key={h} style={{padding:'6px 9px',textAlign:'center',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase'}}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {siteSummary.map((s,i)=>{
                  const ok=fn(s.auth_sr)>=95&&fn(s.attach_sr)>=95&&fn(s.pdp_sr)>=95&&fn(s.cpu_util)<80;
                  return (
                    <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent',cursor:'pointer'}} onClick={()=>fetchSite(s.site_id)}>
                      <td style={{padding:'5px 9px',fontWeight:700,color:T.kpmgBlue}}>{s.site_id}</td>
                      {['auth_sr','cpu_util','attach_sr','pdp_sr'].map(k=>(
                        <td key={k} style={{padding:'5px 9px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace",fontWeight:700,
                          color:k==='cpu_util'?(fn(s[k])>80?T.red:T.green):(fn(s[k])<95?T.red:T.green)}}>
                          {f(s[k])}
                        </td>
                      ))}
                      <td style={{padding:'5px 9px',textAlign:'center'}}><Bdg color={ok?T.green:T.red} sm>{ok?'Healthy':'Alert'}</Bdg></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {!selectedSite&&siteSummary.length===0&&<Empty T={T}/>}
      </CC>
    </div>
  );
}

// ── Transport Page ────────────────────────────────────────────────────────────
function TransportPage({T,data,filters}) {
  const [selectedSite,setSelectedSite]=useState(null);
  const [siteData,setSiteData]=useState(null);
  const [siteLoading,setSiteLoading]=useState(false);
  const d=data||{};
  const TipC=(p)=><Tip T={T} {...p}/>;

  const fetchSite=useCallback(async(sid)=>{
    setSelectedSite(sid);setSiteData(null);setSiteLoading(true);
    try{
      const fq=filters?Object.entries(filters).filter(([,v])=>v).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&'):'';
      const r=await apiGet(`/api/network/site-transport-detail?site_id=${encodeURIComponent(sid)}${fq?'&'+fq:''}`);
      setSiteData(r);
    }catch(_){setSiteData(null);}
    setSiteLoading(false);
  },[filters]);

  useEffect(()=>{if(selectedSite)fetchSite(selectedSite);},[filters]);// eslint-disable-line

  const kpis=[
    {label:'Avg Link Util',      value:f(d.avg_util),            unit:'%', icon:'',color:T.kpmgBlue},
    {label:'Avg Packet Loss',    value:f(d.avg_packet_loss,3),   unit:'%', icon:'',color:T.red},
    {label:'Avg Latency',        value:f(d.avg_latency),         unit:'ms',icon:'', color:T.amber},
    {label:'Avg Jitter',         value:f(d.avg_jitter),          unit:'ms',icon:'',color:T.purple},
    {label:'Link Availability',  value:f(d.avg_availability),    unit:'%', icon:'',color:T.green},
    {label:'Throughput Efficiency',value:f(d.avg_tput_efficiency),unit:'%',icon:'',color:T.teal},
  ];
  const sites=d.sites||[];
  const zoneUtil=d.zone_util||[];
  const backhaulMix=d.backhaul_mix||[{name:'Fiber',value:45},{name:'Microwave',value:30},{name:'Ethernet',value:15},{name:'MPLS',value:10}];

  return (
    <div style={{animation:'fadeIn .3s ease'}}>
      <div style={{marginBottom:13}}>
        <div style={{fontSize:17,fontWeight:800,color:T.text,marginBottom:3}}> Transport Network Performance</div>
        <div style={{fontSize:11,color:T.muted}}>Backhaul · Latency · Packet Loss · Link Utilization · Jitter</div>
      </div>
      <div style={{display:'grid',gridTemplateColumns:'repeat(6,1fr)',gap:9,marginBottom:12}}>
        {kpis.map((k,i)=><KpiCard key={i} T={T} {...k}/>)}
      </div>

      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:10,marginBottom:12}}>
        <CC T={T} title=" Link Utilization Trend">
          {(d.link_util_trend||[]).length>0?(
            <ResponsiveContainer width="100%" height={185}>
              <AreaChart data={d.link_util_trend}>
                <defs><linearGradient id="lug" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#2B4DCC" stopOpacity={.4}/><stop offset="100%" stopColor="#2B4DCC" stopOpacity={.02}/></linearGradient></defs>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                <XAxis dataKey="date" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={30}/>
                <Tooltip content={<TipC/>}/>
                <ReferenceLine y={80} stroke="#E91E8C" strokeDasharray="4 2" label={{value:'Alert',fill:'#E91E8C',fontSize:9}}/>
                <Area type="monotone" dataKey="utilization" stroke="#2B4DCC" fill="url(#lug)" strokeWidth={2} dot={false} name="Util %"/>
              </AreaChart>
            </ResponsiveContainer>
          ):<Empty T={T}/>}
        </CC>
        <CC T={T} title=" Latency & Jitter Trend">
          {(d.latency_trend||[]).length>0?(
            <ResponsiveContainer width="100%" height={185}>
              <LineChart data={d.latency_trend}>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                <XAxis dataKey="date" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={35}/>
                <Tooltip content={<TipC/>}/>
                <Line type="monotone" dataKey="latency" stroke="#E91E8C" strokeWidth={2} dot={false} name="Latency (ms)"/>
                <Line type="monotone" dataKey="jitter"  stroke="#7B1FA2" strokeWidth={1.5} dot={false} name="Jitter (ms)" strokeDasharray="4 2"/>
                <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
              </LineChart>
            </ResponsiveContainer>
          ):<Empty T={T}/>}
        </CC>
        <CC T={T} title=" Backhaul Type Mix">
          <ResponsiveContainer width="100%" height={185}>
            <PieChart>
              <Pie data={backhaulMix} cx="50%" cy="45%" outerRadius={62} innerRadius={36} paddingAngle={3} dataKey="value"
                label={false} labelLine={false}>
                {backhaulMix.map((_,i)=><Cell key={i} fill={PAL[i%10]}/>)}
              </Pie>
              <Tooltip formatter={(v,n)=>[v+' sites',n]} contentStyle={{fontSize:11}}/>
              <Legend iconType="circle" iconSize={8} formatter={(val,entry)=>`${val} (${((entry.payload.value/(backhaulMix.reduce((a,b)=>a+b.value,0)||1))*100).toFixed(0)}%)`} wrapperStyle={{fontSize:10,color:T.muted,paddingTop:4}}/>
            </PieChart>
          </ResponsiveContainer>
        </CC>
      </div>

      {/* Zone util chart */}
      {zoneUtil.length>0&&(
        <CC T={T} title=" Zone-wise Link Utilization" p="14px 16px">
          <div style={{marginBottom:10}}>
            <ResponsiveContainer width="100%" height={160}>
              <BarChart data={zoneUtil}>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
                <XAxis dataKey="zone" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
                <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={30}/>
                <Tooltip content={<TipC/>}/>
                <Bar dataKey="avg_util" name="Avg Util %" radius={[4,4,0,0]} barSize={20}>
                  {zoneUtil.map((d,i)=><Cell key={i} fill={d.avg_util>80?T.red:d.avg_util>60?T.amber:T.green}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </CC>
      )}

      {/* Site search + site-level */}
      <CC T={T} title=" Site-level Transport KPI Analysis" p="14px 16px">
        <div style={{display:'flex',gap:10,marginBottom:12,alignItems:'center'}}>
          <SiteSearch T={T} layer="transport" onSelect={fetchSite} placeholder="Search transport site ID…" filters={filters}/>
          {selectedSite&&<button onClick={()=>{setSelectedSite(null);setSiteData(null);}} style={{padding:'4px 10px',borderRadius:8,fontSize:10,border:`1px solid ${T.border}`,background:T.surface2,color:T.textSub,cursor:'pointer'}}> Clear</button>}
        </div>

        {selectedSite&&(
          siteLoading?<Spin T={T}/>:siteData&&!siteData.data===null?(
            <div style={{padding:16,textAlign:'center',color:T.muted}}>{siteData.message||`No transport data for "${selectedSite}"`}</div>
          ):siteData?(
            <div style={{animation:'fadeIn .3s ease',marginBottom:10}}>
              <div style={{display:'grid',gridTemplateColumns:'1fr 280px',gap:10,marginBottom:10}}>
                <div>
                  <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:8,marginBottom:8}}>
                    {[
                      {label:'Avg Util',    value:f(siteData.avg_util),      unit:'%', icon:'',color:fn(siteData.avg_util)>80?T.red:T.kpmgBlue},
                      {label:'Latency',     value:f(siteData.avg_latency),   unit:'ms',icon:'', color:T.amber},
                      {label:'Packet Loss', value:f(siteData.packet_loss,3), unit:'%', icon:'',color:T.red},
                      {label:'Availability',value:f(siteData.availability),  unit:'%', icon:'',color:fn(siteData.availability)<99?T.amber:T.green},
                    ].map((k,i)=><KpiCard key={i} T={T} {...k}/>)}
                  </div>
                  <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:8}}>
                    {[
                      {label:'Backhaul Type',   value:siteData.backhaul_type||'—', isStr:true, icon:''},
                      {label:'Link Capacity',   value:f(siteData.link_capacity), unit:'Mbps', icon:'',color:T.teal},
                      {label:'Peak Util',       value:f(siteData.peak_util),    unit:'%', icon:'',color:T.amber},
                      {label:'Jitter',          value:f(siteData.jitter,2),     unit:'ms',icon:'〰',color:T.purple},
                    ].map((k,i)=>(
                      <div key={i} style={{...card(T),padding:'10px 12px'}}>
                        <div style={{fontSize:9,color:T.muted,fontWeight:700,textTransform:'uppercase',marginBottom:3}}>{k.label}</div>
                        <div style={{fontSize:16,fontWeight:800,color:k.color||T.text,fontFamily:k.isStr?'inherit':"'IBM Plex Mono',monospace"}}>
                          {k.value}{k.unit&&<span style={{fontSize:9,color:T.muted,marginLeft:2}}>{k.unit}</span>}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
                <CC T={T} title={` ${selectedSite} Location`}>
                  {siteData.meta?.latitude&&siteData.meta?.longitude?(
                    <LeafletMap sites={[{site_id:selectedSite,lat:siteData.meta.latitude,lng:siteData.meta.longitude,cluster:siteData.meta.zone||siteData.zone||''}]} highlight={[{site_id:selectedSite,lat:siteData.meta.latitude,lng:siteData.meta.longitude,cluster:siteData.meta.zone||siteData.zone||''}]} T={T} height={180}/>
                  ):<div style={{padding:20,textAlign:'center',color:T.muted,fontSize:11}}>No location data</div>}
                </CC>
              </div>
              {siteData.alarms>0&&<div style={{marginTop:8,padding:'7px 10px',borderRadius:8,background:T.red+'18',border:`1px solid ${T.red}33`,color:T.red,fontSize:11,fontWeight:600}}> {siteData.alarms} active alarm{siteData.alarms>1?'s':''} on this site</div>}
            </div>
          ):null
        )}

        {/* Network-level sites table */}
        {!selectedSite&&sites.length>0&&(
          <div style={{overflowX:'auto'}}>
            <table style={{width:'100%',borderCollapse:'collapse',fontSize:10.5}}>
              <thead>
                <tr style={{background:T.surface2}}>{['Site ID','Zone','Backhaul','Util %','Latency (ms)','Pkt Loss %','Availability %','Alarms','Status'].map(h=><th key={h} style={{padding:'6px 8px',textAlign:'center',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase',whiteSpace:'nowrap'}}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {sites.slice(0,80).map((s,i)=>{
                  const ok=fn(s.avg_util)<80&&fn(s.avg_latency)<30&&fn(s.packet_loss)<1;
                  const st=!ok?'Warning':'Healthy';
                  return (
                    <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent',cursor:'pointer'}} onClick={()=>fetchSite(s.site_id)}>
                      <td style={{padding:'5px 8px',fontWeight:700,color:T.kpmgBlue}}>{s.site_id}</td>
                      <td style={{padding:'5px 8px',textAlign:'center',color:T.textSub}}>{s.zone||'—'}</td>
                      <td style={{padding:'5px 8px',textAlign:'center',color:T.textSub}}>{s.backhaul_type||'—'}</td>
                      <td style={{padding:'5px 8px',textAlign:'center',color:fn(s.avg_util)>80?T.red:T.green,fontWeight:700,fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.avg_util)}%</td>
                      <td style={{padding:'5px 8px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.avg_latency)}</td>
                      <td style={{padding:'5px 8px',textAlign:'center',color:fn(s.packet_loss)>1?T.red:T.green,fontWeight:700,fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.packet_loss,3)}%</td>
                      <td style={{padding:'5px 8px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace"}}>{f(s.availability)}%</td>
                      <td style={{padding:'5px 8px',textAlign:'center',color:s.alarms>0?T.red:T.muted,fontWeight:700}}>{s.alarms||0}</td>
                      <td style={{padding:'5px 8px',textAlign:'center'}}><Bdg color={ok?T.green:T.amber} sm>{st}</Bdg></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {!selectedSite&&sites.length===0&&<Empty T={T}/>}
      </CC>
    </div>
  );
}

// ── KPI Filter Page ───────────────────────────────────────────────────────────
function KPIFilterPage({T,kpiFilter,data,mapSites:_mapSites,filters}) {
  const TipC=(p)=><Tip T={T} {...p}/>;
  const kd=data||{};
  const sites=kd.sites||[];
  const meta=KPI_FILTERS.find(k=>k.key===kpiFilter)||{label:kpiFilter,icon:'',desc:''};
  const [focusSite,setFocusSite]=useState(null);
  const focusedSiteData=focusSite?sites.find(s=>s.site_id===focusSite):null;

  // Per-filter viz config — all use clean horizontal bar ranking
  const FVIZ = {
    low_access:    {pk:'lte_rrc_setup_sr', pl:'RRC Setup SR',    pu:'%',   pc:'#1565C0',sla:90,  asc:true,  ek:'erab_setup_sr',       el:'E-RAB SR'},
    high_latency:  {pk:'avg_latency_dl',   pl:'DL Latency',      pu:' ms', pc:'#E91E8C',sla:60,  asc:false},
    volte_fail:    {pk:'erab_drop_rate',   pl:'E-RAB Drop Rate', pu:'%',   pc:'#E91E8C',sla:2,   asc:false, ek:'volte_traffic_erl',   el:'VoLTE Trf (Erl)'},
    interference:  {pk:'avg_ni_carrier',   pl:'NI Carrier',      pu:' dBm',pc:'#7B1FA2',sla:null,asc:false},
    overloaded:    {pk:'prb_utilization',  pl:'PRB Utilization', pu:'%',   pc:'#E91E8C',sla:85,  asc:false, ek:'dl_cell_tput',        el:'DL Tput (Mbps)'},
    underutilized: {pk:'prb_utilization',  pl:'PRB Utilization', pu:'%',   pc:'#00BCD4',sla:20,  asc:true},
    rev_leakage:   {pk:'prb_utilization',  pl:'PRB Utilization', pu:'%',   pc:'#0A2463',sla:70,  asc:false, ek:'q1_rev',              el:'Q1 Revenue (K)'},
    low_margin:    {pk:'ebitda_margin',    pl:'EBITDA Margin',   pu:'%',   pc:'#E91E8C',sla:25,  asc:true,  ek:'q1_rev',              el:'Revenue (₹L)'},
    high_rev_util: {pk:'q1_rev',           pl:'Q1 Revenue',      pu:'K',   pc:'#00BCD4',sla:null,asc:false, ek:'prb_utilization',     el:'PRB Util %'},
    low_tput:      {pk:'dl_cell_tput',     pl:'DL Cell Tput',    pu:' Mbps',pc:'#7B1FA2',sla:5,   asc:true},
    worst_drop:    {pk:'erab_drop_rate',   pl:'E-RAB Drop Rate', pu:'%',   pc:'#DC2626',sla:2,   asc:false},
    worst_ho:      {pk:'intra_freq_ho_sr', pl:'HO Success Rate', pu:'%',   pc:'#0A2463',sla:90,  asc:true},
    worst_tput:    {pk:'dl_cell_tput',     pl:'DL Cell Tput',    pu:' Mbps',pc:'#7B1FA2',sla:null,asc:true},
    critical_avail:{pk:'availability',     pl:'Availability',    pu:'%',   pc:'#E91E8C',sla:95,  asc:true},
  };
  const cfg=FVIZ[kpiFilter]||{pk:'prb_utilization',pl:'PRB Util',pu:'%',pc:T.red,sla:85,asc:false};

  // Build chart data (top 10 for clean viz, sorted by primary metric)
  const hasGrouped=!!cfg.ek;
  const chartData=sites.slice(0,10).map(s=>({
    site:(s.site_id||'?').replace(/^GUR_LTE_/,''),
    full_site:s.site_id||'?',
    primary:fn(s[cfg.pk]||0),
    secondary:cfg.ek?fn(s[cfg.ek]||0):undefined,
    ...s,
  })).sort((a,b)=>cfg.asc?a.primary-b.primary:b.primary-a.primary);

  // Summary stats
  const vals=sites.map(s=>fn(s[cfg.pk]||0));
  const avg=vals.length?(vals.reduce((a,b)=>a+b,0)/vals.length):0;
  const worst=chartData[0];
  const CRIT_FN={
    low_access:    s=>fn(s.lte_rrc_setup_sr)<85,
    high_latency:  s=>fn(s.avg_latency_dl)>100,
    volte_fail:    s=>fn(s.erab_drop_rate)>5,
    overloaded:    s=>fn(s.prb_utilization)>95,
    underutilized: s=>fn(s.prb_utilization)<10,
    rev_leakage:   s=>fn(s.q1_rev)<10,
    low_margin:    s=>fn(s.ebitda_margin)<0,
    low_tput:      s=>fn(s.dl_cell_tput)<2,
    worst_drop:    s=>fn(s.erab_drop_rate)>5,
    worst_ho:      s=>fn(s.intra_freq_ho_sr)<70,
    worst_tput:    s=>fn(s.dl_cell_tput)<2,
    critical_avail:s=>fn(s.availability)<80,
  };
  const critFn=CRIT_FN[kpiFilter]||((s,idx)=>idx<Math.round(sites.length*.2));
  const critCount=sites.filter((s,idx)=>critFn(s,idx)).length;

  // Second card: avg of extra metric or avg of primary
  const extraVals=cfg.ek?sites.map(s=>fn(s[cfg.ek]||0)).filter(v=>v>0):null;
  const extraAvg=extraVals&&extraVals.length?(extraVals.reduce((a,b)=>a+b,0)/extraVals.length):null;

  const summaryCards=[
    {label:'Sites Affected', value:sites.length,                         icon:meta.icon, color:T.red},
    {label:`Avg ${cfg.pl}`,  value:(avg!=null&&sites.length>0)?f(avg,1)+cfg.pu.trim():'—', icon:'', color:cfg.pc, isStr:true},
    {label:'Critical',       value:critCount,                             icon:'',      color:T.red},
    extraAvg!=null
      ?{label:`Avg ${cfg.el}`,value:f(extraAvg,1),                       icon:'',      color:T.purple,isStr:true}
      :{label:'Worst Site',  value:worst?.site_id||'—',                   icon:'',      color:T.amber, isStr:true},
  ];

  // Filter-specific table columns: [header, renderFn(s)->string, colorFn(s)->color|null]
  const TC={
    low_access:    [['RRC SR',       s=>f(s.lte_rrc_setup_sr,1)+'%',      s=>fn(s.lte_rrc_setup_sr)<90?T.red:T.green],
                    ['E-RAB SR',     s=>f(s.erab_setup_sr,1)+'%',         s=>fn(s.erab_setup_sr)<90?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null]],
    high_latency:  [['DL Latency',   s=>f(s.avg_latency_dl,1)+' ms',      s=>fn(s.avg_latency_dl)>60?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
                    ['DL Tput',      s=>f(s.dl_cell_tput||s.throughput,1)+' Mbps', ()=>null]],
    volte_fail:    [['VoLTE Traffic',s=>f(s.volte_traffic_erl,2)+' Erl',  ()=>null],
                    ['Drop Rate',    s=>f(s.erab_drop_rate,2)+'%',        s=>fn(s.erab_drop_rate)>2?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null]],
    interference:  [['NI Carrier',   s=>f(s.avg_ni_carrier,1)+' dBm',    ()=>null],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
                    ['DL Tput',      s=>f(s.dl_cell_tput||s.throughput,1)+' Mbps', ()=>null]],
    overloaded:    [['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', s=>fn(s.prb_utilization||s.dl_prb_util)>85?T.red:T.amber],
                    ['DL Tput',      s=>f(s.dl_cell_tput||s.throughput,1)+' Mbps', ()=>null],
                    ['Drop Rate',    s=>f(s.erab_drop_rate,2)+'%',        s=>fn(s.erab_drop_rate)>2?T.red:T.green]],
    underutilized: [['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', s=>fn(s.prb_utilization||s.dl_prb_util)<10?T.blue3:T.amber],
                    ['DL Tput',      s=>f(s.dl_cell_tput||s.throughput,1)+' Mbps', ()=>null],
                    ['Availability', s=>f(s.availability,1)+'%',          ()=>null]],
    rev_leakage:   [['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', s=>fn(s.prb_utilization||s.dl_prb_util)>70?T.red:T.green],
                    ['Q1 Revenue',   s=>'K '+f(s.q1_rev,0),               ()=>null],
                    ['EBITDA %',     s=>f(s.ebitda_margin,1)+'%',         s=>fn(s.ebitda_margin)<25?T.red:T.green]],
    low_margin:    [['Revenue (₹L)', s=>f(s.q1_rev,1),                    ()=>null],
                    ['OPEX (₹L)',    s=>f(s.q1_opex,1),                   ()=>null],
                    ['Rev−OPEX',     s=>f(s.rev_minus_opex||((fn(s.q1_rev)-fn(s.q1_opex))||0),1), s=>(fn(s.q1_rev)-fn(s.q1_opex))<0?T.red:T.green]],
    high_rev_util: [['Q1 Revenue',   s=>'K '+f(s.q1_rev,0),               ()=>null],
                    ['PRB Util %',   s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', s=>fn(s.prb_utilization||s.dl_prb_util)>70?'#E91E8C':'#00BCD4'],
                    ['Score',        s=>f(s.composite_score,1),           ()=>null]],
    low_tput:      [['DL Tput',      s=>f(s.dl_cell_tput||s.throughput,1)+' Mbps', s=>fn(s.dl_cell_tput||s.throughput)<5?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
                    ['Drop Rate',    s=>f(s.erab_drop_rate,2)+'%',        s=>fn(s.erab_drop_rate)>2?T.red:T.green]],
    worst_drop:    [['Drop Rate',    s=>f(s.erab_drop_rate,2)+'%',        s=>fn(s.erab_drop_rate)>2?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
                    ['RRC SR',       s=>f(s.lte_rrc_setup_sr,1)+'%',      s=>fn(s.lte_rrc_setup_sr)<90?T.red:T.green]],
    worst_ho:      [['HO SR',        s=>f(s.intra_freq_ho_sr,1)+'%',      s=>fn(s.intra_freq_ho_sr)<90?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
                    ['Drop Rate',    s=>f(s.erab_drop_rate,2)+'%',        s=>fn(s.erab_drop_rate)>2?T.red:T.green]],
    worst_tput:    [['DL Tput',      s=>f(s.dl_cell_tput||s.throughput,1)+' Mbps', s=>fn(s.dl_cell_tput||s.throughput)<5?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
                    ['E-RAB SR',     s=>f(s.erab_setup_sr,1)+'%',         s=>fn(s.erab_setup_sr)<90?T.red:T.green]],
    critical_avail:[['Availability', s=>f(s.availability,1)+'%',          s=>fn(s.availability)<95?T.red:T.green],
                    ['PRB %',        s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
                    ['Drop Rate',    s=>f(s.erab_drop_rate,2)+'%',        s=>fn(s.erab_drop_rate)>2?T.red:T.green]],
  };
  const cols=TC[kpiFilter]||[
    ['PRB %',    s=>f(s.prb_utilization||s.dl_prb_util,1)+'%', ()=>null],
    ['DL Tput',  s=>f(s.dl_cell_tput||s.throughput,1)+' Mbps', ()=>null],
    ['Drop %',   s=>f(s.erab_drop_rate,2)+'%',                  ()=>null],
  ];

  // Row status label
  const rowStatus=s=>{
    const v=fn(s[cfg.pk]||0);
    if(cfg.sla==null) return ['Listed',T.blue3];
    if(cfg.asc) return v<cfg.sla*0.9?['Critical',T.red]:v<cfg.sla?['Warning',T.amber]:['OK',T.green];
    return v>cfg.sla*1.15?['Critical',T.red]:v>cfg.sla?['Warning',T.amber]:['Watch',T.amber];
  };

  return (
    <div style={{animation:'fadeIn .3s ease'}}>
      {/* Header */}
      <div style={{display:'flex',alignItems:'center',gap:12,marginBottom:14}}>
        <span style={{fontSize:26}}>{meta.icon}</span>
        <div>
          <div style={{fontSize:17,fontWeight:800,color:T.text}}>{meta.label}</div>
          <div style={{fontSize:11,color:T.muted}}>{meta.desc} · {sites.length} sites</div>
        </div>
      </div>

      {/* Summary cards */}
      <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:9,marginBottom:12}}>
        {summaryCards.map((k,i)=>(
          <div key={i} style={{...card(T),padding:'12px 14px',borderTop:`3px solid ${k.color}`}}>
            <div style={{display:'flex',justifyContent:'space-between',marginBottom:4}}>
              <span style={{fontSize:9,fontWeight:700,color:T.muted,textTransform:'uppercase'}}>{k.label}</span>
              <span>{k.icon}</span>
            </div>
            <div style={{fontSize:k.isStr?13:20,fontWeight:800,color:k.color,fontFamily:k.isStr?'inherit':"'IBM Plex Mono',monospace"}}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* Chart + Map */}
      <div style={{display:'grid',gridTemplateColumns:'1.2fr 1fr',gap:11,marginBottom:11}}>
        <CC T={T} title={`${meta.icon} ${cfg.pl} — Worst ${chartData.length} Sites`}>
          {chartData.length>0?(
            <ResponsiveContainer width="100%" height={Math.max(280,chartData.length*(hasGrouped?38:28)+60)}>
              <BarChart data={chartData} layout="vertical" margin={{top:5,right:hasGrouped?55:45,left:5,bottom:hasGrouped?25:5}}
                barGap={2} barCategoryGap={hasGrouped?'20%':'15%'}
                onClick={d=>d?.activePayload?.[0]&&setFocusSite(d.activePayload[0].payload.full_site)}>
                <defs>
                  {/* Primary: solid blue gradient */}
                  <linearGradient id="kfBlue" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#0D47A1"/><stop offset="100%" stopColor="#42A5F5"/>
                  </linearGradient>
                  {/* Secondary: solid purple gradient */}
                  <linearGradient id="kfPurple" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#7B1FA2"/><stop offset="100%" stopColor="#CE93D8"/>
                  </linearGradient>
                  {/* SLA violation: red */}
                  <linearGradient id="kfBarBad" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#C62828"/><stop offset="100%" stopColor="#EF5350"/>
                  </linearGradient>
                  <linearGradient id="kfBarWarn" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#E65100"/><stop offset="100%" stopColor="#FFA726"/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke={T.border} horizontal={false}/>
                <XAxis type="number" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}
                  label={{value:cfg.pl+' ('+cfg.pu.trim()+')',position:'insideBottom',offset:-2,fontSize:9,fill:T.muted}}/>
                <YAxis type="category" dataKey="site" width={70}
                  tick={{fontSize:9.5,fill:T.text,fontWeight:600,fontFamily:"'IBM Plex Mono',monospace"}}
                  axisLine={false} tickLine={false}/>
                <Tooltip cursor={{fill:T.surface2+'88'}} content={({active,payload})=>{
                  if(!active||!payload?.length)return null;
                  const d=payload[0]?.payload;if(!d)return null;
                  const bad=cfg.sla!=null&&(cfg.asc?d.primary<cfg.sla:d.primary>cfg.sla);
                  return(
                    <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:10,padding:'10px 14px',
                      boxShadow:'0 8px 30px rgba(0,0,0,.12)',fontSize:10,minWidth:160}}>
                      <div style={{fontWeight:800,color:T.kpmgBlue,fontSize:12,marginBottom:6}}>{d.full_site}</div>
                      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:'4px 12px'}}>
                        <span style={{color:T.muted}}>{cfg.pl}</span>
                        <span style={{fontWeight:800,color:bad?'#DC2626':T.text,textAlign:'right'}}>{f(d.primary,2)}{cfg.pu.trim()}</span>
                        {hasGrouped&&d.secondary!=null&&<><span style={{color:T.muted}}>{cfg.el}</span>
                        <span style={{fontWeight:700,color:cfg.pc,textAlign:'right'}}>{f(d.secondary,2)}</span></>}
                        {d.zone&&<><span style={{color:T.muted}}>Zone</span><span style={{textAlign:'right'}}>{d.zone||d.cluster}</span></>}
                      </div>
                      {cfg.sla!=null&&<div style={{marginTop:5,paddingTop:4,borderTop:`1px solid ${T.border}`,fontSize:9,
                        color:bad?'#DC2626':T.green}}>{bad?' Violates':' Within'} SLA ({cfg.sla}{cfg.pu.trim()})</div>}
                    </div>);
                }}/>
                {cfg.sla!=null&&<ReferenceLine x={cfg.sla} stroke={T.amber} strokeWidth={2} strokeDasharray="6 3"
                  label={{value:`SLA ${cfg.sla}`,position:'top',fontSize:8.5,fill:T.amber,fontWeight:700}}/>}
                <Bar dataKey="primary" name={cfg.pl} radius={[0,5,5,0]} barSize={hasGrouped?9:16} cursor="pointer"
                  minPointSize={4}
                  label={{position:'right',fontSize:9,fontWeight:700,fill:T.textSub,
                    formatter:v=>f(v,1)+cfg.pu.trim()}}>
                  {chartData.map((d,i)=><Cell key={i} fill="url(#kfBlue)"/>)}
                </Bar>
                {hasGrouped&&(
                  <Bar dataKey="secondary" name={cfg.el} radius={[0,5,5,0]} barSize={9} cursor="pointer"
                    fill="url(#kfPurple)"
                    label={{position:'right',fontSize:8.5,fontWeight:600,fill:'#7B1FA2',
                      formatter:v=>(v!=null&&v>0)?f(v,1):''}}>
                  </Bar>
                )}
                {hasGrouped&&<Legend iconType="circle" iconSize={8} wrapperStyle={{fontSize:10,paddingTop:4}}/>}
              </BarChart>
            </ResponsiveContainer>
          ):<Empty T={T}/>}
        </CC>
        <div style={{display:'flex',flexDirection:'column',gap:11}}>
          <CC T={T} title=" Affected Sites Map">
            <LeafletMap sites={sites} highlight={focusedSiteData?[focusedSiteData]:sites} T={T} height={220}/>
            {focusSite&&<div style={{padding:'5px 10px',borderTop:`1px solid ${T.border}`,fontSize:10,color:T.muted,display:'flex',alignItems:'center',gap:6}}>
              <span> <b style={{color:T.kpmgBlue}}>{focusSite}</b></span>
              <button onClick={()=>setFocusSite(null)} style={{padding:'2px 8px',borderRadius:6,fontSize:9,border:`1px solid ${T.border}`,background:T.surface2,color:T.textSub,cursor:'pointer'}}>Show All</button>
            </div>}
          </CC>
          {/* Quick stats panel */}
          <CC T={T} title=" Quick Stats">
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:6}}>
              {[
                {l:'Avg '+cfg.pl, v:f(avg,1)+cfg.pu.trim(), c:cfg.pc},
                {l:'Sites', v:sites.length, c:T.kpmgBlue},
                {l:'Critical', v:critCount, c:'#DC2626'},
                {l:'Worst', v:worst?.full_site||'—', c:T.amber, small:true},
              ].map((s,i)=>(
                <div key={i} style={{background:T.surface2,borderRadius:8,padding:'8px 10px',border:`1px solid ${T.border}`}}>
                  <div style={{fontSize:8,color:T.muted,fontWeight:700,textTransform:'uppercase',marginBottom:2}}>{s.l}</div>
                  <div style={{fontSize:s.small?10:15,fontWeight:800,color:s.c,fontFamily:s.small?'inherit':"'IBM Plex Mono',monospace"}}>{s.v}</div>
                </div>
              ))}
            </div>
          </CC>
        </div>
      </div>

      {/* Filter-specific table */}
      <CC T={T} title={` Site Detail Table — ${sites.length} sites`}>
        <div style={{overflowX:'auto',maxHeight:400,overflowY:'auto'}}>
          <table style={{width:'100%',borderCollapse:'collapse',fontSize:10.5}}>
            <thead>
              <tr style={{background:T.surface2,position:'sticky',top:0,zIndex:1}}>
                {['#','Site ID','Zone',...cols.map(c=>c[0]),'Status'].map(h=>(
                  <th key={h} style={{padding:'6px 9px',textAlign:'center',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:9,textTransform:'uppercase',whiteSpace:'nowrap',background:T.surface2}}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sites.map((s,i)=>{
                const [stLabel,stColor]=rowStatus(s);
                const isFocused=focusSite===s.site_id;
                return (
                  <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:isFocused?'#2B4DCC18':i%2===0?T.surface2:'transparent'}}>
                    <td style={{padding:'5px 8px',textAlign:'center',color:T.muted,fontSize:9}}>{i+1}</td>
                    <td style={{padding:'5px 8px',fontWeight:700,color:T.kpmgBlue,cursor:'pointer',textDecoration:'underline'}} onClick={()=>setFocusSite(isFocused?null:s.site_id)}>{s.site_id}</td>
                    <td style={{padding:'5px 8px',textAlign:'center',color:T.textSub}}>{s.cluster||s.zone||'—'}</td>
                    {cols.map(([,renderFn,colorFn],ci)=>{
                      const cv=colorFn(s);
                      return <td key={ci} style={{padding:'5px 8px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace",fontWeight:cv?700:400,color:cv||T.text}}>{renderFn(s)}</td>;
                    })}
                    <td style={{padding:'5px 8px',textAlign:'center'}}><Bdg color={stColor} sm>{stLabel}</Bdg></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </CC>
    </div>
  );
}

// ── MAIN COMPONENT ─────────────────────────────────────────────────────────────
export default function NetworkAnalyticsDashboard() {
  const { isDark: dark, toggleTheme } = useTheme();
  const T=dark?T_DARK:T_LIGHT;

  // Data
  const [overview,   setOverview]   = useState(null);
  const [mapData,    setMapData]    = useState(null);
  const [ran,        setRan]        = useState(null);
  const [core,       setCore]       = useState(null);
  const [transport,  setTransport]  = useState(null);
  const [kpiFilterData,setKpiFilterData] = useState(null);
  const [opts,       setOpts]       = useState({clusters:[],technologies:[],regions:[],sites:[]});
  const [loading,    setLoading]    = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpd,    setLastUpd]    = useState(null);

  // Navigation
  const [page,       setPage]       = useState('overview');
  const [layerDrop,  setLayerDrop]  = useState(false);
  const [kpiDrop,    setKpiDrop]    = useState(false);
  const [selKpi,     setSelKpi]     = useState(null);

  // Filters — applied globally to all pages
  // Filters — cluster/city support comma-separated multi-select (e.g. "CBD,Edge")
  const [filters,    setFilters]    = useState({time_range:'7d',cluster:'',technology:'',region:'',country:'',state:'',city:''});

  const navigate = useNavigate();

  const qs=useCallback(()=>
    Object.entries(filters).filter(([,v])=>v).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&'),
  [filters]);

  // Wrap apiGet with a timeout so one slow call never blocks the whole UI
  const fetchWithTimeout=useCallback((url,ms=15000)=>{
    const ctrl=new AbortController();
    const tid=setTimeout(()=>ctrl.abort(),ms);
    return fetch(url,{
      headers:{Authorization:`Bearer ${localStorage.getItem('token')}`},
      signal:ctrl.signal,
    }).then(r=>{clearTimeout(tid);return r.ok?r.json():Promise.reject(r.status);})
      .catch(e=>{clearTimeout(tid);console.warn('fetch failed',url,e);return null;});
  },[]);

  const fetchAll=useCallback(async(silent=false,fresh=false)=>{
    if(!silent)setRefreshing(true);
    const q=qs();
    const base=process.env.REACT_APP_API_URL||'';
    const fr=fresh?'&fresh=1':'';

    // ── Phase 1: overview + filters — fast, dismisses spinner ────────────────
    try{
      const [ov,fo]=await Promise.all([
        fetchWithTimeout(`${base}/api/network/overview-stats?${q}${fr}`,25000),
        fetchWithTimeout(`${base}/api/network/filters?${['country','state'].filter(k=>filters[k]).map(k=>`${k}=${encodeURIComponent(filters[k])}`).join('&')}`,15000),
      ]);
      if(ov)  setOverview(ov);
      if(fo)  setOpts(fo);
      setLastUpd(new Date());
    }catch(_){}
    setLoading(false);
    setRefreshing(false);

    // ── Phase 2: map + layer data — background, non-blocking ─────────────────
    Promise.allSettled([
      fetchWithTimeout(`${base}/api/network/map?${q}${fr}`,35000),
      fetchWithTimeout(`${base}/api/network/ran-analytics?${q}${fr}`,35000),
      fetchWithTimeout(`${base}/api/network/core-analytics?${q}${fr}`,35000),
      fetchWithTimeout(`${base}/api/network/transport-analytics?${q}${fr}`,35000),
    ]).then(([mp,rn,co,tr])=>{
      if(mp.status==='fulfilled'&&mp.value) setMapData(mp.value);
      if(rn.status==='fulfilled'&&rn.value) setRan(rn.value);
      if(co.status==='fulfilled'&&co.value) setCore(co.value);
      if(tr.status==='fulfilled'&&tr.value) setTransport(tr.value);
      setLastUpd(new Date());
    }).catch(()=>{});
  },[qs,fetchWithTimeout]);

  useEffect(()=>{fetchAll(false,false);},[]);// eslint-disable-line — use cache on initial load for speed
  const mounted=useRef(false);
  // Keep refs so filter-change effect can see current page/selKpi without stale closure
  const pageRef=useRef('overview');
  const selKpiRef=useRef(null);
  useEffect(()=>{pageRef.current=page;},[page]);
  useEffect(()=>{selKpiRef.current=selKpi;},[selKpi]);

  useEffect(()=>{
    if(!mounted.current){mounted.current=true;return;}
    fetchAll();
    // Also re-fetch KPI filter page if currently open
    if(pageRef.current==='kpi'&&selKpiRef.current) fetchKpiFilter(selKpiRef.current);
  },[filters]);// eslint-disable-line

  // Fetch KPI filter data when filter selected
  const fetchKpiFilter=useCallback(async(kf)=>{
    if(!kf)return;
    setKpiFilterData(null); // clear so page shows loading
    try{
      const q=qs();
      const d=await apiGet(`/api/network/kpi-filter?kpi_filter=${kf}&${q}`);
      setKpiFilterData(d);
    }catch(_){}
  },[qs]);

  const goKpi=(kf)=>{setSelKpi(kf);setPage('kpi');setKpiDrop(false);fetchKpiFilter(kf);};
  const goLayer=(l)=>{setPage(l);setLayerDrop(false);};

  // Close dropdowns on outside click
  useEffect(()=>{
    const h=()=>{setLayerDrop(false);setKpiDrop(false);};
    document.addEventListener('click',h);
    return()=>document.removeEventListener('click',h);
  },[]);

  if(loading) return (
    <div style={{minHeight:'100vh',display:'flex',alignItems:'center',justifyContent:'center',background:T.bg,fontFamily:"'Plus Jakarta Sans',system-ui,sans-serif"}}>
      <style>{CSS}</style>
      <div style={{textAlign:'center'}}>
        <div style={{width:38,height:38,borderRadius:'50%',border:`4px solid ${T.border}`,borderTopColor:T.kpmgBlue,animation:'spin .8s linear infinite',margin:'0 auto 12px'}}/>
        <div style={{fontSize:12,color:T.muted}}>Loading Network Dashboard…</div>
      </div>
    </div>
  );

  const layerActive=['ran','core','transport'].includes(page);
  const pageLabel=page==='ran'?'RAN Layer':page==='core'?'Core Layer':page==='transport'?'Transport Layer':page==='kpi'?KPI_FILTERS.find(k=>k.key===selKpi)?.label:'Overview';

  return (
    <div style={{minHeight:'100vh',background:T.bg,fontFamily:"'Plus Jakarta Sans',system-ui,sans-serif",color:T.text}}>
      <style>{CSS}</style>

      {/* ── HEADER ── */}
      <div style={{position:'sticky',top:0,zIndex:200,background:T.headerBg,borderBottom:`1px solid ${T.border}`,boxShadow:T.cardShadow}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'8px 20px'}}>
          <div style={{display:'flex',alignItems:'center',gap:9}}>
            <img src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg" alt="KPMG" style={{height:18,filter:dark?'brightness(0) invert(1)':'none'}}/>
            <div style={{width:1,height:18,background:T.border}}/>
            <div>
              <div style={{fontSize:12,fontWeight:800,color:T.kpmgBlue,lineHeight:1.1}}>Predictive Network Analysis</div>
              <div style={{fontSize:8.5,color:T.muted,letterSpacing:'0.06em'}}>TELECOM INTELLIGENCE · AGENT PORTAL</div>
            </div>
          </div>
          <div style={{display:'flex',alignItems:'center',gap:5,flexWrap:'wrap'}}>
            {/* Filter selects — Time, Zone, Tech, Country→State→City */}
            <select value={filters.time_range} onChange={e=>setFilters(p=>({...p,time_range:e.target.value}))} style={sel(T)}>
              {[['1h','1H'],['6h','6H'],['24h','24H'],['7d','7D'],['30d','30D'],['all','All']].map(([v,l])=><option key={v} value={v}>{l}</option>)}
            </select>
            <MultiSel T={T} label="All Zone" options={opts.clusters||[]} value={filters.cluster} onChange={v=>setFilters(p=>({...p,cluster:v}))}/>
            <MultiSel T={T} label="All Tech" options={opts.technologies||[]} value={filters.technology} onChange={v=>setFilters(p=>({...p,technology:v}))}/>
            <select value={filters.country} onChange={e=>setFilters(p=>({...p,country:e.target.value,state:'',city:''}))} style={sel(T)}>
              <option value="">All Country</option>
              {(opts.countries||[]).map(v=><option key={v} value={v}>{v}</option>)}
            </select>
            <select value={filters.state} onChange={e=>setFilters(p=>({...p,state:e.target.value,city:''}))} style={sel(T)}>
              <option value="">All State</option>
              {(opts.states||[]).map(v=><option key={v} value={v}>{v}</option>)}
            </select>
            <MultiSel T={T} label="All City" options={opts.cities||[]} value={filters.city} onChange={v=>setFilters(p=>({...p,city:v}))}/>
            <button onClick={toggleTheme} style={{padding:'5px 10px',borderRadius:18,fontSize:10.5,fontWeight:600,background:dark?'#F59E0B22':'#00338D10',border:`1px solid ${dark?'#F59E0B44':T.border}`,color:dark?'#F59E0B':T.kpmgBlue,cursor:'pointer',display:'flex',alignItems:'center',gap:4}}>
              {dark?(<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>):(<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>)}
              {dark?'Light':'Dark'}
            </button>
            <button onClick={()=>navigate('/agent/network-ai')} style={{padding:'5px 11px',borderRadius:18,fontSize:11,fontWeight:700,background:`linear-gradient(135deg,${T.kpmgBlue},${T.blue3})`,border:'none',color:'#fff',cursor:'pointer'}}>AI Chat</button>
            <button onClick={()=>fetchAll()} style={{padding:'5px 9px',borderRadius:6,fontSize:10.5,fontWeight:600,background:T.kpmgBlue,color:'#fff',border:'none',cursor:'pointer'}}>{refreshing?'':'↻'}</button>
          </div>
        </div>

        {/* Active filter chips */}
        {Object.entries(filters).some(([k,v])=>v&&k!=='time_range')&&(
          <div style={{display:'flex',alignItems:'center',gap:5,padding:'3px 20px 5px',flexWrap:'wrap',borderTop:`1px solid ${T.border}55`}}>
            <span style={{fontSize:9,color:T.muted,fontWeight:600,textTransform:'uppercase',letterSpacing:'0.05em'}}>Active filters:</span>
            {filters.cluster&&(
              <span style={{display:'inline-flex',alignItems:'center',gap:4,padding:'2px 8px',borderRadius:12,fontSize:9.5,fontWeight:700,background:`${T.kpmgBlue}18`,color:T.kpmgBlue,border:`1px solid ${T.kpmgBlue}33`}}>
                 {filters.cluster}
                <button onClick={()=>setFilters(p=>({...p,cluster:''}))} style={{background:'none',border:'none',color:T.kpmgBlue,cursor:'pointer',padding:0,fontSize:10,lineHeight:1}}>×</button>
              </span>
            )}
            {filters.technology&&(
              <span style={{display:'inline-flex',alignItems:'center',gap:4,padding:'2px 8px',borderRadius:12,fontSize:9.5,fontWeight:700,background:`${T.teal}18`,color:T.teal,border:`1px solid ${T.teal}33`}}>
                 {filters.technology}
                <button onClick={()=>setFilters(p=>({...p,technology:''}))} style={{background:'none',border:'none',color:T.teal,cursor:'pointer',padding:0,fontSize:10,lineHeight:1}}>×</button>
              </span>
            )}
            {filters.region&&(
              <span style={{display:'inline-flex',alignItems:'center',gap:4,padding:'2px 8px',borderRadius:12,fontSize:9.5,fontWeight:700,background:`${T.purple}18`,color:T.purple,border:`1px solid ${T.purple}33`}}>
                 {filters.region}
                <button onClick={()=>setFilters(p=>({...p,region:''}))} style={{background:'none',border:'none',color:T.purple,cursor:'pointer',padding:0,fontSize:10,lineHeight:1}}>×</button>
              </span>
            )}
            {filters.country&&(
              <span style={{display:'inline-flex',alignItems:'center',gap:4,padding:'2px 8px',borderRadius:12,fontSize:9.5,fontWeight:700,background:'#0E793418',color:'#0E7934',border:'1px solid #0E793433'}}>
                🌍 {filters.country}
                <button onClick={()=>setFilters(p=>({...p,country:'',state:'',city:''}))} style={{background:'none',border:'none',color:'#0E7934',cursor:'pointer',padding:0,fontSize:10,lineHeight:1}}>×</button>
              </span>
            )}
            {filters.state&&(
              <span style={{display:'inline-flex',alignItems:'center',gap:4,padding:'2px 8px',borderRadius:12,fontSize:9.5,fontWeight:700,background:'#B4540018',color:'#B45400',border:'1px solid #B4540033'}}>
                📍 {filters.state}
                <button onClick={()=>setFilters(p=>({...p,state:'',city:''}))} style={{background:'none',border:'none',color:'#B45400',cursor:'pointer',padding:0,fontSize:10,lineHeight:1}}>×</button>
              </span>
            )}
            {filters.city&&(
              <span style={{display:'inline-flex',alignItems:'center',gap:4,padding:'2px 8px',borderRadius:12,fontSize:9.5,fontWeight:700,background:'#6B21A818',color:'#6B21A8',border:'1px solid #6B21A833'}}>
                🏙 {filters.city}
                <button onClick={()=>setFilters(p=>({...p,city:''}))} style={{background:'none',border:'none',color:'#6B21A8',cursor:'pointer',padding:0,fontSize:10,lineHeight:1}}>×</button>
              </span>
            )}
            <button onClick={()=>setFilters({time_range:filters.time_range,cluster:'',technology:'',region:'',country:'',state:'',city:''})}
              style={{fontSize:9,color:T.red,background:'none',border:`1px solid ${T.red}44`,borderRadius:10,cursor:'pointer',padding:'1px 7px',fontWeight:600}}>
              Clear all
            </button>
          </div>
        )}

        {/* Nav bar */}
        <div style={{display:'flex',alignItems:'center',gap:4,padding:'0 20px 7px',flexWrap:'wrap'}}>
          <button className="nav-btn" onClick={()=>setPage('overview')}
            style={{padding:'4px 12px',borderRadius:16,fontSize:10.5,fontWeight:700,background:page==='overview'?T.kpmgBlue:'transparent',color:page==='overview'?'#fff':T.textSub,border:`1.5px solid ${page==='overview'?T.kpmgBlue:T.border}`,cursor:'pointer',transition:'all .2s'}}>
             Overview
          </button>

          {/* Layer dropdown */}
          <div style={{position:'relative'}} onClick={e=>e.stopPropagation()}>
            <button className="nav-btn" onClick={()=>{setLayerDrop(o=>!o);setKpiDrop(false);}}
              style={{padding:'4px 12px',borderRadius:16,fontSize:10.5,fontWeight:700,background:layerActive?T.kpmgBlue:'transparent',color:layerActive?'#fff':T.textSub,border:`1.5px solid ${layerActive?T.kpmgBlue:T.border}`,cursor:'pointer',display:'flex',alignItems:'center',gap:4,transition:'all .2s'}}>
               Layers {layerActive&&<Bdg color="rgba(255,255,255,0.4)" sm>{pageLabel}</Bdg>} ▾
            </button>
            {layerDrop&&(
              <div style={{position:'absolute',top:'100%',left:0,minWidth:260,background:T.surface,border:`1px solid ${T.border}`,borderRadius:10,boxShadow:'0 12px 36px rgba(0,0,0,.18)',zIndex:300,overflow:'hidden',marginTop:2}}>
                {[
                  {key:'ran',       icon:'',label:'RAN',      desc:'Call Drop · Throughput · PRB · RRC Users · DL Traffic'},
                  {key:'core',      icon:'', label:'Core',     desc:'Auth SR · CPU · Attach SR · PDP Bearer'},
                  {key:'transport', icon:'',label:'Transport', desc:'Link Util · Latency · Jitter · Packet Loss'},
                ].map(l=>(
                  <div key={l.key} className="dd-item" onClick={()=>goLayer(l.key)}
                    style={{padding:'10px 15px',cursor:'pointer',borderBottom:`1px solid ${T.border}`,background:page===l.key?`${T.kpmgBlue}14`:'transparent'}}>
                    <div style={{fontWeight:700,fontSize:11.5,color:T.text}}>{l.icon} {l.label}</div>
                    <div style={{fontSize:9.5,color:T.muted,marginTop:2}}>{l.desc}</div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* KPI filter dropdown */}
          <div style={{position:'relative'}} onClick={e=>e.stopPropagation()}>
            <button className="nav-btn" onClick={()=>{setKpiDrop(o=>!o);setLayerDrop(false);}}
              style={{padding:'4px 12px',borderRadius:16,fontSize:10.5,fontWeight:700,background:page==='kpi'?T.kpmgBlue:'transparent',color:page==='kpi'?'#fff':T.textSub,border:`1.5px solid ${page==='kpi'?T.kpmgBlue:T.border}`,cursor:'pointer',display:'flex',alignItems:'center',gap:4,transition:'all .2s'}}>
               KPI Filters {page==='kpi'&&selKpi&&<Bdg color="rgba(255,255,255,0.4)" sm>{KPI_FILTERS.find(k=>k.key===selKpi)?.icon}</Bdg>} ▾
            </button>
            {kpiDrop&&(
              <div style={{position:'absolute',top:'100%',left:0,minWidth:280,maxHeight:380,overflowY:'auto',background:T.surface,border:`1px solid ${T.border}`,borderRadius:10,boxShadow:'0 12px 36px rgba(0,0,0,.18)',zIndex:300,overflow:'auto',marginTop:2}}>
                {KPI_FILTERS.map(kf=>(
                  <div key={kf.key} className="dd-item" onClick={()=>goKpi(kf.key)}
                    style={{padding:'9px 14px',cursor:'pointer',borderBottom:`1px solid ${T.border}`,background:selKpi===kf.key?`${T.kpmgBlue}14`:'transparent'}}>
                    <div style={{fontWeight:700,fontSize:11,color:T.text,display:'flex',alignItems:'center',gap:6}}><span style={{fontSize:14}}>{kf.icon}</span>{kf.label}</div>
                    <div style={{fontSize:9.5,color:T.muted,marginTop:1,paddingLeft:20}}>{kf.desc}</div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Breadcrumb */}
          {page!=='overview'&&(
            <div style={{marginLeft:'auto',display:'flex',alignItems:'center',gap:5,padding:'3px 9px',borderRadius:8,background:`${T.kpmgBlue}10`,border:`1px solid ${T.kpmgBlue}28`}}>
              <button onClick={()=>setPage('overview')} style={{fontSize:9,color:T.muted,background:'none',border:'none',cursor:'pointer'}}>Overview</button>
              <span style={{fontSize:9,color:T.muted}}>›</span>
              <span style={{fontSize:9,fontWeight:700,color:T.kpmgBlue}}>{pageLabel}</span>
            </div>
          )}
        </div>
      </div>

      {/* ── MAIN CONTENT ── */}
      <div style={{padding:'13px 18px 32px',position:'relative'}}>
        {/* Filter context banner — shown whenever non-default filters are active */}
        {(filters.cluster||filters.technology||filters.region||filters.country||filters.state||filters.city||filters.time_range!=='24h')&&(
          <div style={{display:'flex',alignItems:'center',gap:8,padding:'6px 12px',marginBottom:12,borderRadius:9,background:`${T.kpmgBlue}0c`,border:`1px solid ${T.kpmgBlue}22`,flexWrap:'wrap'}}>
            <span style={{fontSize:9.5,fontWeight:700,color:T.kpmgBlue,textTransform:'uppercase',letterSpacing:'0.05em'}}> Filtered View</span>
            {filters.time_range!=='24h'&&<Bdg color={T.kpmgBlue}> {filters.time_range.toUpperCase()}</Bdg>}
            {filters.cluster&&<Bdg color={T.kpmgBlue}> Zone: {filters.cluster}</Bdg>}
            {filters.technology&&<Bdg color={T.teal}> Tech: {filters.technology}</Bdg>}
            {filters.country&&<Bdg color={T.green}> {filters.country}</Bdg>}
            {filters.state&&<Bdg color={T.purple}> {filters.state}</Bdg>}
            {filters.city&&<Bdg color={T.amber}> {filters.city}</Bdg>}
            {filters.region&&<Bdg color={T.blue2}> Region: {filters.region}</Bdg>}
            <span style={{fontSize:9,color:T.muted,marginLeft:'auto'}}>All charts and metrics reflect these filters</span>
            {refreshing&&<span style={{fontSize:9,color:T.amber,animation:'pulse 1s ease infinite'}}>↻ Refreshing…</span>}
          </div>
        )}
        {/* Refreshing spinner overlay */}
        {refreshing&&(
          <div style={{position:'absolute',top:0,right:18,display:'flex',alignItems:'center',gap:5,padding:'4px 10px',borderRadius:8,background:T.surface,border:`1px solid ${T.border}`,fontSize:10,color:T.muted,zIndex:10}}>
            <div style={{width:10,height:10,border:`2px solid ${T.border}`,borderTopColor:T.kpmgBlue,borderRadius:'50%',animation:'spin .7s linear infinite'}}/>
            Updating…
          </div>
        )}
        {page==='ran' ? (
          <RANPage T={T} data={ran} mapSites={mapData?.sites||[]} filters={filters} opts={opts}/>
        ) : page==='core' ? (
          <CorePage T={T} data={core} filters={filters}/>
        ) : page==='transport' ? (
          <TransportPage T={T} data={transport} filters={filters}/>
        ) : page==='kpi' ? (
          kpiFilterData
            ? <KPIFilterPage T={T} kpiFilter={selKpi} data={kpiFilterData} mapSites={mapData?.sites||[]} filters={filters}/>
            : <div style={{display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',height:300,gap:12}}>
                <div style={{width:28,height:28,border:`3px solid ${T.border}`,borderTopColor:T.kpmgBlue,borderRadius:'50%',animation:'spin .7s linear infinite'}}/>
                <div style={{fontSize:12,color:T.muted}}>Loading filter data…</div>
              </div>
        ) : (
          <OverviewPage T={T} data={overview} mapSites={mapData?.sites||[]} filters={filters}/>
        )}
      </div>

      {/* Last updated */}
      {lastUpd&&(
        <div style={{position:'fixed',bottom:8,left:18,fontSize:9,color:T.muted,opacity:.7}}>
          Updated: {lastUpd.toLocaleTimeString()}
        </div>
      )}

    </div>
  );
}