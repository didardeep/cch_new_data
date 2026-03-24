import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  LineChart, Line, BarChart, Bar, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  Cell, PieChart, Pie, Legend, ComposedChart, ReferenceLine,
  RadarChart, Radar, PolarGrid, PolarAngleAxis,
  ScatterChart, Scatter, ZAxis,
} from 'recharts';
import { apiGet, apiPost, apiDelete } from '../../api';
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
const PAL = [
  '#2563EB','#E91E8C','#00BCD4','#F59E0B','#8B5CF6',
  '#10B981','#EF4444','#3B82F6','#EC4899','#06B6D4',
];
const f = (v,d=1) => (v==null||v===''||isNaN(+v)) ? '—' : d===0 ? String(Math.round(+v)) : (+v).toFixed(d).replace(/\.?0+$/, '') || '0';
const card = T => ({ background:T.surface, border:`1px solid ${T.border}`, borderRadius:12, boxShadow:T.cardShadow });

const SUGGESTED = [
  { label:'Worst 5 cells', prompt:'Show 5 worst sites where E-RAB Call Drop Rate_1 is greater than 1.5% or LTE Call Setup Success Rate is less than 98.5% or LTE DL - Usr Ave Throughput is less than 8 Mbps. Show all three KPIs for each site.' },
  { label:'Call drops',    prompt:'Show top 10 sites with highest E-RAB Call Drop Rate_1 along with their LTE Call Setup Success Rate and DL PRB Utilization' },
  { label:'Throughput',    prompt:'Show bottom 10 sites by LTE DL - Usr Ave Throughput along with their DL PRB Utilization and E-RAB Call Drop Rate_1' },
  { label:'Congestion',    prompt:'Show top 10 sites where DL PRB Utilization (1BH) is highest along with their LTE DL - Usr Ave Throughput and Ave RRC Connected Ue' },
  { label:'Availability',  prompt:'Show sites with lowest Availability along with their E-RAB Call Drop Rate_1 and LTE Call Setup Success Rate' },
  { label:'Compare zones', prompt:'Compare all zones by average E-RAB Call Drop Rate_1, LTE DL - Usr Ave Throughput, DL PRB Utilization (1BH), and LTE Call Setup Success Rate' },
];

const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0;}
::-webkit-scrollbar{width:4px;height:4px;}
::-webkit-scrollbar-thumb{background:#CBD5E1;border-radius:2px;}
@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.45}}
@keyframes bounce{0%,80%,100%{transform:translateY(0)}40%{transform:translateY(-5px)}}
.ai-session-item:hover{background:rgba(0,51,141,.07)!important;}
.ai-session-item.active{background:rgba(0,51,141,.12)!important;border-right:3px solid #00338D!important;}
`;

// ── Tooltip ────────────────────────────────────────────────────────────────
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

// ── Pivot helper for UNION ALL multi-series data ──────────────────────────
function pivotMultiSeries(data, columns, xKey) {
  // Detect if data has a 'kpi_name' or series differentiator column + single 'value' column
  const hasKpiName = columns.includes('kpi_name');
  const hasSiteId = columns.includes('site_id');
  const hasValue = columns.includes('value');
  if (!hasValue || !xKey) return null;

  // Build series key from kpi_name + site_id (whichever are present)
  const distinctKpis = hasKpiName ? [...new Set(data.map(r => r.kpi_name).filter(Boolean))] : [];
  const distinctSites = hasSiteId ? [...new Set(data.map(r => r.site_id).filter(Boolean))] : [];

  // Only pivot if there are multiple distinct series (kpi or site)
  const multiKpi = distinctKpis.length > 1;
  const multiSite = distinctSites.length > 1;
  if (!multiKpi && !multiSite) return null;

  // Build series label for each row
  const seriesKey = (r) => {
    const parts = [];
    if (multiKpi && r.kpi_name) {
      // Shorten KPI name for column key
      const short = String(r.kpi_name).replace(/[^a-zA-Z0-9]/g, '_').replace(/_+/g, '_').slice(0, 30);
      parts.push(short);
    }
    if (multiSite && r.site_id) {
      const s = String(r.site_id);
      const m = s.match(/_(\d{4,})$/);
      parts.push(m ? m[1] : s.slice(-10));
    }
    return parts.join('_') || 'value';
  };

  // Build series label (human readable) for legend
  const seriesLabel = (r) => {
    const parts = [];
    if (multiKpi && r.kpi_name) parts.push(String(r.kpi_name));
    if (multiSite && r.site_id) parts.push(String(r.site_id));
    return parts.join(' — ') || 'value';
  };

  // Collect all unique series keys + labels
  const seriesMap = new Map(); // key → label
  data.forEach(r => {
    const k = seriesKey(r);
    if (!seriesMap.has(k)) seriesMap.set(k, seriesLabel(r));
  });
  const seriesKeys = [...seriesMap.keys()];
  const seriesLabels = Object.fromEntries(seriesMap);

  // Pivot: group by xKey, spread value into series columns
  const grouped = new Map();
  data.forEach(r => {
    const x = r[xKey];
    if (!grouped.has(x)) grouped.set(x, { [xKey]: x });
    const row = grouped.get(x);
    const sk = seriesKey(r);
    const v = parseFloat(r.value);
    if (!isNaN(v)) row[sk] = v;
  });

  const pivoted = [...grouped.values()].sort((a, b) => {
    const av = a[xKey], bv = b[xKey];
    return typeof av === 'string' ? av.localeCompare(bv) : (av - bv);
  });

  return { data: pivoted, yKeys: seriesKeys, labels: seriesLabels };
}

// ── Inline Chart Renderer ──────────────────────────────────────────────────
function InlineChart({result,T}) {
  const [showTable,setShowTable]=useState(false);
  if(!result||!result.data?.length) return null;

  const {title,data:rawData=[],columns=[],x_axis,y_axes,response,row_count,chart_type,query_type,chart_config,provider,sql}=result;
  const xKey=x_axis||columns[0]||'';
  const SKIP_COLS=new Set(['lat','lng','latitude','longitude','site_id','cell_id','cluster','region','zone','technology','color','status','kpi_name']);

  // Try to detect and pivot UNION ALL multi-series data
  const pivoted = pivotMultiSeries(rawData, columns, xKey);

  const data = pivoted ? pivoted.data : rawData;
  const yKeys = pivoted ? pivoted.yKeys
    : (y_axes&&y_axes.length) ? y_axes
    : columns.filter(c=>c!==xKey&&!SKIP_COLS.has(c)).slice(0,5);
  const seriesLabels = pivoted ? pivoted.labels : null;

  const ctype=chart_type||query_type||'bar';
  const cfg=chart_config||{};
  const threshold=cfg.threshold!=null?parseFloat(cfg.threshold):null;
  const TipC=(p)=><Tip T={T} {...p}/>;
  const isTimeSeries=xKey.includes('hour')||xKey.includes('time')||xKey.includes('date');
  const shortLbl=v=>{if(typeof v!=='string')return String(v??'');const m=v.match(/_(\d{4,})$/);return m?`#${m[1]}`:v.length>14?'…'+v.slice(-11):v;};
  const keyLabel=k=> seriesLabels?.[k] || k.replace(/_/g,' ');

  const stats=yKeys.slice(0,5).map((k,i)=>{
    const vals=data.map(r=>parseFloat(r[k])).filter(v=>!isNaN(v));
    const avg=vals.length?vals.reduce((a,b)=>a+b,0)/vals.length:0;
    return{k,label:keyLabel(k),avg,max:vals.length?Math.max(...vals):0,min:vals.length?Math.min(...vals):0,color:PAL[i%PAL.length]};
  });

  const renderChart=()=>{
    const h=340;

    if(ctype==='pie'&&yKeys.length>=1){
      const pieData=data.slice(0,12).map(r=>({name:shortLbl(r[xKey]),value:parseFloat(r[yKeys[0]])||0}));
      return(
        <ResponsiveContainer width="100%" height={h}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={90} innerRadius={40} paddingAngle={3} label={({name,percent})=>`${name} ${(percent*100).toFixed(0)}%`} labelLine={false} animationDuration={1000} animationEasing="ease-in-out">
              {pieData.map((_,i)=><Cell key={i} fill={PAL[i%PAL.length]}/>)}
            </Pie>
            <Tooltip formatter={(v)=>f(v,1)}/><Legend iconType="circle" iconSize={8} wrapperStyle={{fontSize:10}}/>
          </PieChart>
        </ResponsiveContainer>);
    }

    if(ctype==='radar'&&yKeys.length>=3){
      const radarData=yKeys.map(k=>{const obj={metric:k.replace(/_/g,' ')};data.slice(0,5).forEach((r,i)=>{obj[`site${i}`]=parseFloat(r[k])||0;});return obj;});
      const radarKeys=data.slice(0,5).map((_,i)=>`site${i}`);
      return(
        <ResponsiveContainer width="100%" height={h}>
          <RadarChart data={radarData}>
            <PolarGrid stroke={T.border}/><PolarAngleAxis dataKey="metric" tick={{fontSize:9,fill:T.muted}}/>
            {radarKeys.map((k,i)=><Radar key={k} name={data[i]?.[xKey]||k} dataKey={k} stroke={PAL[i%PAL.length]} fill={PAL[i%PAL.length]} fillOpacity={0.15} animationDuration={1000} animationEasing="ease-in-out"/>)}
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/><Tooltip/>
          </RadarChart>
        </ResponsiveContainer>);
    }

    if(ctype==='scatter'&&yKeys.length>=2){
      const scatterData=data.map(r=>({x:parseFloat(r[yKeys[0]])||0,y:parseFloat(r[yKeys[1]])||0,z:1,name:r[xKey]}));
      return(
        <ResponsiveContainer width="100%" height={h}>
          <ScatterChart margin={{top:10,right:20,left:0,bottom:10}}>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border}/>
            <XAxis type="number" dataKey="x" name={yKeys[0].replace(/_/g,' ')} tick={{fontSize:9,fill:T.muted}} axisLine={false}/>
            <YAxis type="number" dataKey="y" name={yKeys[1].replace(/_/g,' ')} tick={{fontSize:9,fill:T.muted}} axisLine={false} width={38}/>
            <ZAxis dataKey="z" range={[30,30]}/>
            <Tooltip cursor={{strokeDasharray:'3 3'}} content={({active,payload})=>{if(!active||!payload?.length)return null;const d=payload[0]?.payload;return<div style={{...card(T),padding:'7px 10px',fontSize:10}}><b>{d?.name}</b><br/>{yKeys[0]}: {f(d?.x,2)}<br/>{yKeys[1]}: {f(d?.y,2)}</div>;}}/>
            {threshold!=null&&<ReferenceLine y={threshold} stroke={T.amber} strokeDasharray="4 2"/>}
            <Scatter name="Sites" data={scatterData} fill={T.kpmgBlue} opacity={0.75} animationDuration={1000} animationEasing="ease-in-out"/>
          </ScatterChart>
        </ResponsiveContainer>);
    }

    if(ctype==='composed'&&yKeys.length>=2){
      const cTickInterval=data.length>15?Math.ceil(data.length/10):0;
      const chartColors=[PAL[0],PAL[1],PAL[2],PAL[3],PAL[4]];
      return(
        <ResponsiveContainer width="100%" height={h+30}>
          <ComposedChart data={data} margin={{top:5,right:25,left:5,bottom:35}}>
            <defs>
              {yKeys.map((k,i)=>(
                <linearGradient key={k} id={`cg${i}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={chartColors[i%chartColors.length]} stopOpacity={.4}/><stop offset="100%" stopColor={chartColors[i%chartColors.length]} stopOpacity={.03}/></linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
            <XAxis dataKey={xKey} tick={{fontSize:8,fill:T.muted}} axisLine={false} tickLine={false} interval={cTickInterval} angle={-35} textAnchor="end" height={45} tickFormatter={v=>{if(typeof v!=='string')return v;if(v.length>10)return v.slice(5,10);return v;}}/>
            <YAxis yAxisId="l" tick={{fontSize:9,fill:T.muted}} axisLine={false} width={45} label={yKeys[0]?{value:keyLabel(yKeys[0]).slice(0,20),angle:-90,position:'insideLeft',fontSize:8,fill:T.muted}:undefined}/>
            {yKeys.length>1&&<YAxis yAxisId="r" orientation="right" tick={{fontSize:9,fill:T.muted}} axisLine={false} width={45} label={yKeys[1]?{value:keyLabel(yKeys[1]).slice(0,20),angle:90,position:'insideRight',fontSize:8,fill:T.muted}:undefined}/>}
            <Tooltip content={<TipC/>} cursor={{stroke:T.kpmgBlue,strokeWidth:1,strokeDasharray:'4 2'}}/>
            {threshold!=null&&<ReferenceLine yAxisId="l" y={threshold} stroke={T.amber} strokeDasharray="4 2"/>}
            {yKeys.map((k,i)=>{
              const yId=i===0?'l':(i===1?'r':'l');
              const color=chartColors[i%chartColors.length];
              return i%2===0?(
                <Area key={k} yAxisId={yId} type="natural" dataKey={k} name={keyLabel(k)} fill={`url(#cg${i})`} stroke={color} strokeWidth={2.5} activeDot={{r:5,strokeWidth:2,stroke:'#fff'}} animationDuration={1000} animationEasing="ease-in-out"/>
              ):(
                <Line key={k} yAxisId={yId} type="natural" dataKey={k} name={keyLabel(k)} stroke={color} strokeWidth={2.5} dot={data.length<=20?{r:3,strokeWidth:2,stroke:'#fff'}:false} activeDot={{r:6,strokeWidth:2,stroke:'#fff'}} animationDuration={1200} animationEasing="ease-in-out"/>
              );
            })}
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
          </ComposedChart>
        </ResponsiveContainer>);
    }

    if(ctype==='line'||ctype==='area'||isTimeSeries){
      const tickInterval=data.length>20?Math.ceil(data.length/8):data.length>10?2:0;
      return(
        <ResponsiveContainer width="100%" height={h+40}>
          <AreaChart data={data} margin={{top:5,right:30,left:5,bottom:45}}>
            <defs>{yKeys.map((k,i)=><linearGradient key={k} id={`aig${i}`} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={PAL[i%10]} stopOpacity={.45}/><stop offset="100%" stopColor={PAL[i%10]} stopOpacity={.03}/></linearGradient>)}</defs>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false}/>
            <XAxis dataKey={xKey} tick={{fontSize:8,fill:T.muted}} axisLine={false} tickLine={false} interval={tickInterval} angle={-40} textAnchor="end" height={55} tickFormatter={v=>{if(typeof v!=='string')return v;return v.replace(/^20\d{2}-/,'').slice(0,5);}}/>
            <YAxis tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} width={42}/>
            <Tooltip content={<TipC/>} cursor={{stroke:T.kpmgBlue,strokeWidth:1,strokeDasharray:'4 2'}}/>
            {threshold!=null&&<ReferenceLine y={threshold} stroke={T.amber} strokeDasharray="4 2" label={{value:`${threshold}`,fontSize:9,fill:T.amber}}/>}
            {yKeys.map((k,i)=><Area key={k} type="natural" dataKey={k} stroke={PAL[i%10]} fill={`url(#aig${i})`} strokeWidth={2.5} dot={data.length<=30?{r:3,strokeWidth:2,stroke:'#fff'}:false} activeDot={{r:6,strokeWidth:2,stroke:'#fff'}} name={keyLabel(k)} animationDuration={1000} animationEasing="ease-in-out"/>)}
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
          </AreaChart>
        </ResponsiveContainer>);
    }

    // BAR
    const isHorizontal=data.length>8||typeof data[0]?.[xKey]==='string';
    if(isHorizontal){
      return(
        <ResponsiveContainer width="100%" height={Math.max(h,data.length*36+60)}>
          <BarChart data={data} layout="vertical" margin={{top:5,right:25,left:110,bottom:5}}>
            <defs>{yKeys.slice(0,3).map((k,i)=><linearGradient key={k} id={`hbg${i}`} x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stopColor={PAL[i%10]} stopOpacity={.85}/><stop offset="100%" stopColor={PAL[(i+1)%10]} stopOpacity={.95}/></linearGradient>)}</defs>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} horizontal={false}/>
            <XAxis type="number" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false}/>
            <YAxis type="category" dataKey={xKey} width={105} tick={{fontSize:8.5,fill:T.muted}} tickFormatter={shortLbl} axisLine={false} tickLine={false}/>
            <Tooltip content={<TipC/>} cursor={{fill:T.kpmgBlue+'0a'}}/>
            {threshold!=null&&<ReferenceLine x={threshold} stroke={T.amber} strokeDasharray="4 2"/>}
            {yKeys.slice(0,3).map((k,i)=>(
              <Bar key={k} dataKey={k} name={keyLabel(k)} radius={[0,6,6,0]} barSize={yKeys.length>1?14:22} animationDuration={1200} animationEasing="ease-in-out">
                {yKeys.length===1?data.map((d,di)=>{const v=parseFloat(d[k]);const bad=threshold!=null&&(cfg.threshold_dir==='below'?v<threshold:v>threshold);return<Cell key={di} fill={bad?T.red:`url(#hbg${i})`}/>;}):<Cell fill={`url(#hbg${i})`}/>}
              </Bar>))}
            <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
          </BarChart>
        </ResponsiveContainer>);
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
          {yKeys.slice(0,3).map((k,i)=><Bar key={k} dataKey={k} name={keyLabel(k)} radius={[6,6,0,0]} fill={`url(#vbg${i})`} barSize={20} animationDuration={1200} animationEasing="ease-in-out"/>)}
          <Legend iconType="circle" iconSize={7} wrapperStyle={{fontSize:10}}/>
        </BarChart>
      </ResponsiveContainer>);
  };

  return(
    <div style={{marginTop:8}}>
      {/* Title bar */}
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:6}}>
        <div style={{fontSize:12,fontWeight:700,color:T.text}}>{title}</div>
        <div style={{display:'flex',gap:5}}>
          <span style={{fontSize:9,padding:'2px 7px',borderRadius:8,background:T.kpmgBlue+'18',color:T.kpmgBlue,fontWeight:600}}>{row_count} records</span>
          <span style={{fontSize:9,padding:'2px 7px',borderRadius:8,background:T.purple+'18',color:T.purple,fontWeight:600}}>{ctype}</span>
          {provider&&<span style={{fontSize:9,padding:'2px 7px',borderRadius:8,background:T.teal+'18',color:T.teal,fontWeight:600}}>{provider}</span>}
        </div>
      </div>

      {/* KPI stat cards */}
      {stats.length>0&&(
        <div style={{display:'grid',gridTemplateColumns:`repeat(${Math.min(stats.length,5)},1fr)`,gap:6,marginBottom:8}}>
          {stats.map(({k,label,avg,max,min,color})=>(
            <div key={k} style={{...card(T),padding:'8px 10px',borderTop:`3px solid ${color}`}}>
              <div style={{fontSize:7.5,fontWeight:700,color:T.muted,textTransform:'uppercase',marginBottom:3,lineHeight:1.3}}>{label}</div>
              <div style={{fontSize:16,fontWeight:800,color:T.text,fontFamily:"'IBM Plex Mono',monospace"}}>{f(avg,1)}</div>
              <div style={{display:'flex',gap:8,marginTop:3}}>
                <span style={{fontSize:8,color:T.green}}>↑ {f(max,1)}</span>
                <span style={{fontSize:8,color:T.red}}>↓ {f(min,1)}</span>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Chart / Table toggle */}
      <div style={{display:'flex',justifyContent:'flex-end',marginBottom:4}}>
        <button onClick={()=>setShowTable(t=>!t)} style={{padding:'3px 10px',borderRadius:12,fontSize:9.5,fontWeight:600,background:showTable?T.kpmgBlue:'transparent',color:showTable?'#fff':T.textSub,border:`1px solid ${showTable?T.kpmgBlue:T.border}`,cursor:'pointer'}}>
          {showTable?'Chart':'Table'}
        </button>
      </div>

      {showTable?(
        <div style={{overflowX:'auto',maxHeight:300,overflowY:'auto'}}>
          <table style={{width:'100%',borderCollapse:'collapse',fontSize:10}}>
            <thead style={{position:'sticky',top:0,zIndex:1}}>
              <tr style={{background:T.surface2}}>
                {columns.map(h=><th key={h} style={{padding:'5px 7px',textAlign:'center',borderBottom:`2px solid ${T.border}`,color:T.muted,fontWeight:700,fontSize:8.5,textTransform:'uppercase',whiteSpace:'nowrap'}}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {data.slice(0,100).map((row,i)=>(
                <tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?T.surface2:'transparent'}}>
                  {columns.map(c=><td key={c} style={{padding:'3px 7px',textAlign:'center',fontFamily:"'IBM Plex Mono',monospace",fontSize:9.5}}>{row[c]==null?'—':typeof row[c]==='number'?f(row[c],2):String(row[c])}</td>)}
                </tr>))}
            </tbody>
          </table>
        </div>
      ):(
        <div style={{boxShadow:'0 4px 20px rgba(0,51,141,0.08)',borderRadius:10,overflow:'hidden'}}>
          {renderChart()}
        </div>
      )}

      {sql&&(
        <details style={{marginTop:6}}>
          <summary style={{fontSize:9,color:T.muted,cursor:'pointer',fontWeight:600}}>View SQL</summary>
          <pre style={{background:T.surface2,border:`1px solid ${T.border}`,borderRadius:6,padding:'8px 10px',fontSize:9,color:T.textSub,overflow:'auto',marginTop:4,lineHeight:1.5}}>{sql}</pre>
        </details>
      )}
    </div>
  );
}

// ── Main Page Component ────────────────────────────────────────────────────
export default function NetworkAiChat() {
  const { isDark: dark, toggleTheme } = useTheme();
  const T = dark ? T_DARK : T_LIGHT;
  const navigate = useNavigate();

  // Session management
  const [sessions, setSessions]         = useState([]);
  const [activeSessionId, setActiveId]  = useState(null);
  const [messages, setMessages]         = useState([]);
  const [input, setInput]               = useState('');
  const [loading, setLoading]           = useState(false);
  const [sessionsLoading, setSessLoad]  = useState(true);
  const [sidebarOpen, setSidebarOpen]   = useState(true);

  const endRef  = useRef(null);
  const inputRef = useRef(null);

  // Auto-scroll
  useEffect(()=>{ endRef.current?.scrollIntoView({behavior:'smooth'}); },[messages]);

  // Load sessions on mount
  useEffect(()=>{
    apiGet('/api/network/ai-sessions')
      .then(r=>{ setSessions(r.sessions||[]); setSessLoad(false); })
      .catch(()=>setSessLoad(false));
  },[]);

  // Load messages when session changes
  const loadSession = useCallback(async(id)=>{
    setActiveId(id);
    if(!id){ setMessages([]); return; }
    try{
      const r = await apiGet(`/api/network/ai-sessions/${id}/messages`);
      setMessages(r.messages||[]);
    }catch{ setMessages([]); }
  },[]);

  const handleNewChat = useCallback(async()=>{
    try{
      const r = await apiPost('/api/network/ai-sessions',{});
      const s = r.session;
      setSessions(prev=>[s,...prev]);
      setActiveId(s.id);
      setMessages([]);
      inputRef.current?.focus();
    }catch{}
  },[]);

  const handleDelete = useCallback(async(id,e)=>{
    e?.stopPropagation();
    try{
      await apiDelete(`/api/network/ai-sessions/${id}`);
      setSessions(prev=>prev.filter(s=>s.id!==id));
      if(activeSessionId===id){ setActiveId(null); setMessages([]); }
    }catch{}
  },[activeSessionId]);

  const handleSend = useCallback(async(text)=>{
    if(!text?.trim()||loading) return;

    let sid = activeSessionId;
    // Auto-create session if none
    if(!sid){
      try{
        const r = await apiPost('/api/network/ai-sessions',{});
        sid = r.session.id;
        setSessions(prev=>[r.session,...prev]);
        setActiveId(sid);
      }catch{ return; }
    }

    // Optimistic UI
    const userMsg = { id:Date.now(), role:'user', content:text, created_at:new Date().toISOString() };
    setMessages(prev=>[...prev,userMsg]);
    setInput('');
    setLoading(true);

    try{
      const result = await apiPost('/api/network/ai-query',{
        prompt:text,
        session_id:sid,
        context:{ filters:{} },
      });

      const assistantMsg = {
        id:Date.now()+1,
        role:'assistant',
        content:result.response||'Here are the results.',
        payload:result,
        created_at:new Date().toISOString(),
      };
      setMessages(prev=>[...prev,assistantMsg]);

      // Update session title
      if(result.title){
        setSessions(prev=>prev.map(s=>
          s.id===sid&&s.title==='New Chat' ? {...s,title:result.title} : s
        ));
      }
    }catch{
      setMessages(prev=>[...prev,{
        id:Date.now()+1, role:'assistant',
        content:'Could not reach the server. Please try again.',
        created_at:new Date().toISOString(),
      }]);
    }
    setLoading(false);
  },[activeSessionId,loading]);

  const formatTime = (iso)=>{
    if(!iso) return '';
    const d=new Date(iso);
    const now=new Date();
    const diff=now-d;
    if(diff<86400000) return d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
    if(diff<604800000) return d.toLocaleDateString([],{weekday:'short'});
    return d.toLocaleDateString([],{month:'short',day:'numeric'});
  };

  return(
    <div style={{fontFamily:"'Plus Jakarta Sans',sans-serif",background:T.bg,color:T.text,height:'100vh',display:'flex',flexDirection:'column',overflow:'hidden'}}>
      <style>{CSS}</style>

      {/* ── Top Bar ── */}
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'10px 18px',background:T.headerBg,borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
        <div style={{display:'flex',alignItems:'center',gap:10}}>
          <button onClick={()=>setSidebarOpen(o=>!o)} style={{background:'none',border:'none',color:T.textSub,cursor:'pointer',fontSize:18,padding:4}}>☰</button>
          <div style={{fontSize:15,fontWeight:800,color:T.kpmgBlue}}>Network AI</div>
          <span style={{fontSize:10,color:T.muted,fontWeight:500}}>TELECOM INTELLIGENCE</span>
        </div>
        <div style={{display:'flex',gap:8,alignItems:'center'}}>
          <button onClick={toggleTheme} style={{padding:'4px 10px',borderRadius:14,fontSize:10.5,fontWeight:600,background:'transparent',border:`1px solid ${T.border}`,color:T.textSub,cursor:'pointer'}}>
            {dark?'Light':'Dark'}
          </button>
          <button onClick={()=>navigate('/agent/network')} style={{padding:'4px 10px',borderRadius:14,fontSize:10.5,fontWeight:600,background:'transparent',border:`1px solid ${T.border}`,color:T.textSub,cursor:'pointer'}}>
            ← Dashboard
          </button>
        </div>
      </div>

      {/* ── Main Layout ── */}
      <div style={{display:'flex',flex:1,overflow:'hidden'}}>

        {/* ── Session Sidebar ── */}
        {sidebarOpen&&(
          <div style={{width:260,borderRight:`1px solid ${T.border}`,background:T.surface,display:'flex',flexDirection:'column',flexShrink:0}}>
            {/* New Chat */}
            <div style={{padding:12}}>
              <button onClick={handleNewChat}
                style={{width:'100%',padding:'9px 14px',borderRadius:10,border:`1.5px dashed ${T.border}`,background:'transparent',color:T.kpmgBlue,fontWeight:700,fontSize:12,cursor:'pointer',fontFamily:'inherit',display:'flex',alignItems:'center',justifyContent:'center',gap:6}}>
                + New Chat
              </button>
            </div>

            {/* Sessions list */}
            <div style={{flex:1,overflowY:'auto',padding:'0 8px 12px'}}>
              {sessionsLoading?(
                <div style={{textAlign:'center',padding:20,color:T.muted,fontSize:11}}>Loading...</div>
              ):sessions.length===0?(
                <div style={{textAlign:'center',padding:20,color:T.muted,fontSize:11}}>No sessions yet</div>
              ):sessions.map(s=>(
                <div key={s.id}
                  className={`ai-session-item${s.id===activeSessionId?' active':''}`}
                  onClick={()=>loadSession(s.id)}
                  style={{padding:'9px 12px',borderRadius:8,cursor:'pointer',marginBottom:3,display:'flex',alignItems:'center',justifyContent:'space-between',transition:'background .15s',background:s.id===activeSessionId?T.kpmgBlue+'18':'transparent',borderRight:s.id===activeSessionId?`3px solid ${T.kpmgBlue}`:'3px solid transparent'}}>
                  <div style={{flex:1,minWidth:0}}>
                    <div style={{fontSize:11.5,fontWeight:600,color:T.text,whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{s.title||'New Chat'}</div>
                    <div style={{fontSize:9.5,color:T.muted,marginTop:2}}>{formatTime(s.last_message_at)} · {s.message_count||0} msgs</div>
                  </div>
                  <button onClick={(e)=>handleDelete(s.id,e)}
                    style={{background:'none',border:'none',color:T.muted,cursor:'pointer',fontSize:14,padding:'2px 4px',opacity:.5,flexShrink:0}}
                    title="Delete session">×</button>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── Chat Area ── */}
        <div style={{flex:1,display:'flex',flexDirection:'column',overflow:'hidden'}}>

          {/* Messages */}
          <div style={{flex:1,overflowY:'auto',padding:'20px 24px'}}>
            {messages.length===0?(
              /* Welcome Screen */
              <div style={{display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',height:'100%',gap:16}}>
                <div style={{fontSize:40,opacity:.3}}>🤖</div>
                <div style={{fontSize:20,fontWeight:800,color:T.text,opacity:.7}}>Network AI Assistant</div>
                <div style={{fontSize:12,color:T.muted,maxWidth:400,textAlign:'center',lineHeight:1.6}}>
                  Ask questions about network performance, site KPIs, and trends. I can generate charts and data analysis.
                </div>
                <div style={{display:'flex',flexWrap:'wrap',gap:8,justifyContent:'center',maxWidth:600,marginTop:8}}>
                  {SUGGESTED.map(s=>(
                    <button key={s.label} onClick={()=>handleSend(s.prompt)}
                      style={{padding:'8px 16px',borderRadius:20,fontSize:11.5,fontWeight:600,background:T.surface,border:`1.5px solid ${T.border}`,color:T.textSub,cursor:'pointer',fontFamily:'inherit',transition:'all .15s'}}>
                      {s.label}
                    </button>
                  ))}
                </div>
              </div>
            ):(
              /* Message Thread */
              <div style={{maxWidth:1100,margin:'0 auto',width:'100%'}}>
                {messages.map(m=>(
                  <div key={m.id} style={{display:'flex',justifyContent:m.role==='user'?'flex-end':'flex-start',marginBottom:16,animation:'fadeIn .3s ease'}}>
                    <div style={{
                      maxWidth:m.role==='user'?'55%':'92%',
                      minWidth:m.role==='assistant'&&m.payload?.data?.length>0?'min(680px,100%)':undefined,
                      padding:m.role==='user'?'10px 16px':'14px 18px',
                      borderRadius:m.role==='user'?'18px 18px 4px 18px':'18px 18px 18px 4px',
                      background:m.role==='user'?`linear-gradient(135deg,${T.kpmgBlue},${T.blue2})`:T.surface,
                      color:m.role==='user'?'#fff':T.text,
                      boxShadow:m.role==='user'?'0 2px 12px rgba(0,51,141,.2)':T.cardShadow,
                      border:m.role==='user'?'none':`1px solid ${T.border}`,
                    }}>
                      {/* Role label */}
                      <div style={{fontSize:9,fontWeight:700,marginBottom:4,opacity:.6,textTransform:'uppercase'}}>
                        {m.role==='user'?'You':'AI Assistant'}
                      </div>

                      {/* Text content */}
                      <div style={{fontSize:12.5,lineHeight:1.65,whiteSpace:'pre-wrap'}}>{m.content}</div>

                      {/* Inline chart for assistant messages */}
                      {m.role==='assistant'&&m.payload&&m.payload.data?.length>0&&(
                        <InlineChart result={m.payload} T={T}/>
                      )}

                      {/* Timestamp */}
                      <div style={{fontSize:8.5,opacity:.4,marginTop:6,textAlign:m.role==='user'?'right':'left'}}>
                        {m.created_at?new Date(m.created_at).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}):''}
                      </div>
                    </div>
                  </div>
                ))}

                {/* Loading indicator */}
                {loading&&(
                  <div style={{display:'flex',justifyContent:'flex-start',marginBottom:16}}>
                    <div style={{padding:'14px 20px',borderRadius:'18px 18px 18px 4px',background:T.surface,border:`1px solid ${T.border}`,boxShadow:T.cardShadow}}>
                      <div style={{display:'flex',gap:5}}>
                        {[0,1,2].map(i=>(
                          <span key={i} style={{width:7,height:7,borderRadius:'50%',background:T.kpmgBlue,animation:`bounce .6s ${i*0.15}s infinite`}}/>
                        ))}
                      </div>
                    </div>
                  </div>
                )}

                <div ref={endRef}/>
              </div>
            )}
          </div>

          {/* ── Input Bar ── */}
          <div style={{padding:'12px 24px 16px',borderTop:`1px solid ${T.border}`,background:T.surface,flexShrink:0}}>
            <div style={{maxWidth:1100,margin:'0 auto',display:'flex',gap:10,alignItems:'center'}}>
              <input ref={inputRef}
                value={input}
                onChange={e=>setInput(e.target.value)}
                onKeyDown={e=>e.key==='Enter'&&!e.shiftKey&&(e.preventDefault(),handleSend(input))}
                placeholder="Ask about network KPIs, site performance, trends..."
                disabled={loading}
                style={{flex:1,padding:'12px 18px',borderRadius:24,border:`2px solid ${loading?T.muted:T.border}`,background:T.surface2,color:T.text,fontSize:13,fontFamily:'inherit',outline:'none',transition:'border .2s'}}
              />
              <button onClick={()=>handleSend(input)} disabled={loading||!input.trim()}
                style={{width:44,height:44,borderRadius:'50%',border:'none',background:loading||!input.trim()?T.muted:`linear-gradient(135deg,${T.kpmgBlue},${T.blue3})`,color:'#fff',cursor:loading?'not-allowed':'pointer',fontSize:18,display:'flex',alignItems:'center',justifyContent:'center',flexShrink:0,transition:'background .2s'}}>
                {loading?<span style={{animation:'pulse 1s infinite',fontSize:14}}>···</span>:'→'}
              </button>
            </div>
            <div style={{maxWidth:1100,margin:'4px auto 0',display:'flex',gap:6,flexWrap:'wrap'}}>
              {SUGGESTED.slice(0,4).map(s=>(
                <button key={s.label} onClick={()=>handleSend(s.prompt)} disabled={loading}
                  style={{padding:'2px 8px',borderRadius:10,fontSize:9,fontWeight:500,background:'transparent',border:`1px solid ${T.border}`,color:T.muted,cursor:loading?'not-allowed':'pointer',fontFamily:'inherit'}}>
                  {s.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
