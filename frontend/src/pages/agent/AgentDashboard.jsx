import { useState, useEffect, useCallback, useMemo } from 'react';
import {
  AreaChart, Area, BarChart, Bar, PieChart, Pie, Cell, Legend,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  RadialBarChart, RadialBar, ComposedChart, Line,
  FunnelChart, Funnel, LabelList, Treemap,
} from 'recharts';
import { ComposableMap, Geographies, Geography, ZoomableGroup, Marker } from 'react-simple-maps';
import { apiGet } from '../../api';
import { useAuth } from '../../AuthContext';
import { useTheme } from '../../ThemeContext';

const GEO_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json';

/* ═══════════════════════════════════════════════════════════════════════════
   THEME  (KPMG primary #00338D + shades)
   ═══════════════════════════════════════════════════════════════════════════ */
const K = { navy:'#00338D', royal:'#005EB8', sky:'#0091DA', indigo:'#483698', violet:'#7C3AED' };
const TL = {
  bg:'#F0F2F5', surface:'#FFFFFF', surface2:'#F8FAFC', surface3:'#EFF6FF',
  border:'#E2E8F0', text:'#0F172A', textSub:'#475569', muted:'#94A3B8',
  blue:K.navy, blue2:K.royal, green:'#16A34A', amber:'#D97706',
  red:'#DC2626', purple:K.indigo, teal:'#0891B2', cyan:'#06B6D4', pink:'#EC4899',
  cardShadow:'0 1px 6px rgba(0,51,141,0.07)',
  kpiCardBg:'#fff', progressTrack:'#e2e8f0', gridStroke:'#f1f5f9',
  glow:'rgba(0,51,141,0.06)',
};
const TD = {
  bg:'#060D19', surface:'#0C1829', surface2:'#12213A', surface3:'#1A2D4D',
  border:'#1C2E48', text:'#E2E8F0', textSub:'#94A3B8', muted:'#4A5568',
  blue:'#4DA3FF', blue2:'#60A5FA', green:'#34D399', amber:'#FBBF24',
  red:'#F87171', purple:'#A78BFA', teal:'#22D3EE', cyan:'#22D3EE', pink:'#F472B6',
  cardShadow:'0 4px 20px rgba(0,0,0,0.5)',
  kpiCardBg:'#0C1829', progressTrack:'#1C2E48', gridStroke:'#1C2E48',
  glow:'rgba(77,163,255,0.06)',
};

const SENTIMENT_COLORS = ['#16A34A','#22D3EE','#94A3B8','#F59E0B','#DC2626'];
const TIER_COLORS = { Platinum:'#7C3AED', Gold:'#F59E0B', Silver:'#94A3B8', Bronze:'#D97706' };
const TIER_ORDER = ['Platinum','Gold','Silver','Bronze'];
const RISK_COLORS = { safe:'#16A34A', warning:'#F59E0B', critical:'#F97316', breached:'#DC2626' };
const ZONE_PALETTE = ['#00338D','#005EB8','#0091DA','#483698','#7C3AED','#0891B2','#16A34A'];
const DAYS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
const BADGE_COLORS = {
  'SLA Champion':K.navy, 'Customer Expert':'#F59E0B', 'Speed Resolver':K.violet,
  'Zero Reopen':'#16A34A', 'First Touch Pro':'#0891B2', 'Volume Leader':'#EC4899',
  'Crisis Handler':'#DC2626',
};

/* ── SVG Icon Set (professional) ─────────────────────────────────────────── */
const I = (d, s=18) => <svg width={s} height={s} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">{d}</svg>;
const IC = {
  clock:  I(<><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></>),
  check:  I(<><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></>),
  target: I(<><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></>),
  star:   I(<><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></>),
  repeat: I(<><polyline points="17 1 21 5 17 9"/><path d="M3 11V9a4 4 0 0 1 4-4h14"/><polyline points="7 23 3 19 7 15"/><path d="M21 13v2a4 4 0 0 1-4 4H3"/></>),
  alert:  I(<><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></>),
  zap:    I(<><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></>),
  aging:  I(<><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></>),
  shield: I(<><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></>, 14),
  trend:  I(<><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></>, 14),
  refresh:I(<><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></>, 14),
  grid:   I(<><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></>, 16),
  bulb:   I(<><line x1="9" y1="18" x2="15" y2="18"/><line x1="10" y1="22" x2="14" y2="22"/><path d="M15.09 14c.18-.98.65-1.74 1.41-2.5A4.65 4.65 0 0 0 18 8 6 6 0 0 0 6 8c0 1 .23 2.23 1.5 3.5A4.61 4.61 0 0 1 8.91 14"/></>, 14),
  map:    I(<><polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6"/><line x1="8" y1="2" x2="8" y2="18"/><line x1="16" y1="6" x2="16" y2="22"/></>, 14),
  users:  I(<><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></>, 14),
  activity:I(<><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></>, 14),
};

const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;600;700&display=swap');
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
@keyframes countUp{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
@keyframes pulseRing{0%{box-shadow:0 0 0 0 rgba(0,51,141,0.4)}70%{box-shadow:0 0 0 8px rgba(0,51,141,0)}100%{box-shadow:0 0 0 0 rgba(0,51,141,0)}}
@keyframes shimmer{0%{background-position:-200% 0}100%{background-position:200% 0}}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:0.5;transform:scale(1.3)}}
.dk{transition:all .22s cubic-bezier(.4,0,.2,1);cursor:default;}
.dk:hover{transform:translateY(-2px);box-shadow:0 8px 28px rgba(0,51,141,.12)!important;}
.hm-c{transition:all .12s ease;border-radius:2px;}
.hm-c:hover{transform:scale(1.5);z-index:10;position:relative;box-shadow:0 0 8px rgba(0,51,141,.3);}
.recharts-funnel-trapezoid{transition:opacity .2s ease;cursor:default;}
.recharts-funnel-trapezoid:hover{opacity:.85;}
`;

/* ── Performance Needle Meter ─────────────────────────────────────────── */
function Speedometer({ score, T }) {
  const v = Math.min(Math.max(Math.round(score * 10) / 10, 0), 100);
  const pR = p => Math.PI * (1 - p / 100);
  const cx = 120, cy = 90, R = 55, sw = 12;

  const arcD = (f, t) => {
    const a1 = pR(f), a2 = pR(t);
    return `M${cx+R*Math.cos(a1)},${cy-R*Math.sin(a1)} A${R},${R} 0 ${t-f>50?1:0} 1 ${cx+R*Math.cos(a2)},${cy-R*Math.sin(a2)}`;
  };

  const nA = pR(v), nL = R - 14;
  const tx = cx + nL * Math.cos(nA), ty = cy - nL * Math.sin(nA);
  const bp = 3;
  const b1x = cx + bp * Math.sin(nA), b1y = cy + bp * Math.cos(nA);
  const b2x = cx - bp * Math.sin(nA), b2y = cy - bp * Math.cos(nA);

  return (
    <svg viewBox="30 10 180 120" width="100%" style={{ display: 'block' }}>
      <defs>
        {/* Shiny glossy filter */}
        <filter id="shiny" x="-10%" y="-10%" width="120%" height="120%">
          <feGaussianBlur in="SourceGraphic" stdDeviation="0.8" result="blur"/>
          <feSpecularLighting in="blur" surfaceScale="3" specularConstant="0.8" specularExponent="25" result="spec">
            <fePointLight x={cx} y={cy - R - 20} z="60"/>
          </feSpecularLighting>
          <feComposite in="spec" in2="SourceGraphic" operator="in" result="specClip"/>
          <feComposite in="SourceGraphic" in2="specClip" operator="arithmetic" k1="0" k2="1" k3="0.4" k4="0"/>
        </filter>
      </defs>

      {/* Grey track only */}
      <path d={arcD(0, 100)} fill="none" stroke={T.progressTrack} strokeWidth={sw} strokeLinecap="round"/>

      {/* Vibrant active arc with shiny filter - 3 color segments */}
      <g filter="url(#shiny)">
        {v > 0 && v <= 40 && <path d={arcD(0, v)} fill="none" stroke="#EF4444" strokeWidth={sw} strokeLinecap="round"
          style={{transition:'all 1.2s cubic-bezier(.4,0,.2,1)'}}/>}
        {v > 40 && <>
          <path d={arcD(0, 40)} fill="none" stroke="#EF4444" strokeWidth={sw} strokeLinecap="round"/>
          {v <= 70 && <path d={arcD(40, v)} fill="none" stroke="#F59E0B" strokeWidth={sw} strokeLinecap="butt"
            style={{transition:'all 1.2s cubic-bezier(.4,0,.2,1)'}}/>}
        </>}
        {v > 70 && <>
          <path d={arcD(0, 40)} fill="none" stroke="#EF4444" strokeWidth={sw} strokeLinecap="round"/>
          <path d={arcD(40, 70)} fill="none" stroke="#F59E0B" strokeWidth={sw} strokeLinecap="butt"/>
          <path d={arcD(70, v)} fill="none" stroke="#22C55E" strokeWidth={sw} strokeLinecap="round"
            style={{transition:'all 1.2s cubic-bezier(.4,0,.2,1)'}}/>
        </>}
      </g>

      {/* Tick labels inside */}
      {[0, 25, 50, 75, 100].map(t => {
        const a = pR(t);
        const r1 = R - sw/2 - 3, r2 = r1 - 4, rL = r2 - 9;
        return (
          <g key={t}>
            <line x1={cx+r1*Math.cos(a)} y1={cy-r1*Math.sin(a)}
                  x2={cx+r2*Math.cos(a)} y2={cy-r2*Math.sin(a)}
                  stroke={T.muted} strokeWidth="1" opacity=".4"/>
            <text x={cx+rL*Math.cos(a)} y={cy-rL*Math.sin(a)}
                  textAnchor="middle" dominantBaseline="central"
                  fontSize="8" fontWeight="700" fill={T.muted}
                  fontFamily="'IBM Plex Mono',monospace">{t}</text>
          </g>
        );
      })}

      {/* Needle */}
      <polygon points={`${tx},${ty} ${b1x},${b1y} ${b2x},${b2y}`}
        fill={T.text} opacity=".7" style={{transition:'all 1.2s cubic-bezier(.4,0,.2,1)'}}/>

      {/* Hub */}
      <circle cx={cx} cy={cy} r="5" fill={T.surface} stroke={T.border} strokeWidth="1.5"/>
      <circle cx={cx} cy={cy} r="2" fill={T.text} opacity=".5"/>

      {/* Score */}
      <text x={cx} y={cy+19} textAnchor="middle" fontSize="20" fontWeight="800"
        fill={T.text} fontFamily="'IBM Plex Mono',monospace">{v}</text>
      <text x={cx} y={cy+30} textAnchor="middle" fontSize="7" fontWeight="600" fill={T.muted}>out of 100</text>
    </svg>
  );
}

/* ── KPI Card ──────────────────────────────────────────────────────────── */
function KpiCard({ label, value, unit, icon, sub, alert:isAlert, T, color, trend }) {
  const accent = color || (isAlert ? T.red : K.navy);
  const darkAccent = isAlert ? T.red : T.blue;
  const finalAccent = T === TD ? darkAccent : accent;
  return (
    <div className="dk" style={{
      background: T.kpiCardBg, borderRadius:12,
      border:`1px solid ${T.border}`, padding:'14px 14px 12px',
      display:'flex', flexDirection:'column', gap:5,
      boxShadow:T.cardShadow, borderTop:`3px solid ${finalAccent}`,
      animation:'fadeUp .4s ease',
    }}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between'}}>
        <span style={{fontSize:9,fontWeight:700,color:T.muted,textTransform:'uppercase',letterSpacing:'.06em',lineHeight:1.3}}>{label}</span>
        <span style={{color:finalAccent,display:'flex',opacity:.65}}>{icon}</span>
      </div>
      <div style={{fontSize:22,fontWeight:800,color:T.text,lineHeight:1,fontFamily:"'IBM Plex Mono',monospace",animation:'countUp .5s ease'}}>
        {value}<span style={{fontSize:10,fontWeight:500,color:T.muted,marginLeft:3}}>{unit}</span>
      </div>
      {sub && <div style={{fontSize:9,color:T.muted,lineHeight:1.4}}>{sub}</div>}
    </div>
  );
}

/* ── Card wrapper ────────────────────────────────────────────────────────── */
function Card({ T, title, subtitle, icon, children, style:sx }) {
  return (
    <div className="dk" style={{
      background:T.surface, borderRadius:12, border:`1px solid ${T.border}`,
      boxShadow:T.cardShadow, padding:'14px 18px', animation:'fadeUp .45s ease',
      display:'flex', flexDirection:'column', ...sx,
    }}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:10}}>
        <div style={{display:'flex',alignItems:'center',gap:8}}>
          {icon && <span style={{color:T.blue,display:'flex'}}>{icon}</span>}
          <div>
            <div style={{fontSize:11,fontWeight:700,color:T.blue,textTransform:'uppercase',letterSpacing:'.04em'}}>{title}</div>
            {subtitle && <div style={{fontSize:9,color:T.muted,marginTop:1}}>{subtitle}</div>}
          </div>
        </div>
      </div>
      <div style={{flex:1,display:'flex',flexDirection:'column',justifyContent:'center'}}>{children}</div>
    </div>
  );
}

/* ── Tooltip ─────────────────────────────────────────────────────────────── */
function DTip({ active, payload, label, T }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:10,padding:'10px 14px',boxShadow:'0 8px 32px rgba(0,0,0,.18)',fontSize:11,minWidth:120,backdropFilter:'blur(8px)'}}>
      <div style={{fontWeight:700,color:T.blue,marginBottom:5,fontSize:11.5,borderBottom:`1px solid ${T.border}`,paddingBottom:4}}>{label}</div>
      {payload.map((p,i) => (
        <div key={i} style={{display:'flex',alignItems:'center',gap:6,marginBottom:2}}>
          <span style={{width:7,height:7,borderRadius:2,background:p.color,flexShrink:0}}/>
          <span style={{color:T.textSub,flex:1,fontSize:10}}>{p.name}</span>
          <strong style={{color:T.text,fontFamily:"'IBM Plex Mono',monospace",fontSize:10.5}}>{typeof p.value==='number'?p.value.toFixed(1):p.value}</strong>
        </div>
      ))}
    </div>
  );
}

/* ── Weekly Heatmap ──────────────────────────────────────────────────────── */
function Heatmap({ data, T }) {
  const flat = data.flat();
  const mx = Math.max(...flat, 1);
  const hrs = Array.from({length:24},(_,i)=>i);
  const gc = v => {
    if(!v) return T.progressTrack;
    const r = v/mx;
    return r>.75?K.navy:r>.5?K.royal:r>.25?K.sky:K.sky+'60';
  };
  return (
    <div style={{overflowX:'auto'}}>
      <div style={{display:'flex',marginLeft:36,marginBottom:3}}>
        {hrs.filter(h=>h%3===0).map(h=>(
          <div key={h} style={{width:`${100/8}%`,fontSize:7.5,color:T.muted,fontFamily:"'IBM Plex Mono',monospace",fontWeight:600}}>{String(h).padStart(2,'0')}h</div>
        ))}
      </div>
      {DAYS.map((day,di)=>(
        <div key={day} style={{display:'flex',alignItems:'center',gap:2,marginBottom:2}}>
          <span style={{width:32,fontSize:8.5,fontWeight:600,color:T.muted,textAlign:'right',paddingRight:4}}>{day}</span>
          {hrs.map(h=>(
            <div key={h} className="hm-c" title={`${day} ${String(h).padStart(2,'0')}:00 -- ${data[di][h]} ticket(s)`}
              style={{flex:1,height:14,minWidth:11,background:gc(data[di][h]),opacity:data[di][h]===0?.25:1}}/>
          ))}
        </div>
      ))}
      <div style={{display:'flex',alignItems:'center',gap:6,marginTop:6,marginLeft:36}}>
        <span style={{fontSize:8,color:T.muted}}>Less</span>
        {[T.progressTrack,K.sky+'60',K.sky,K.royal,K.navy].map((c,i)=>(
          <div key={i} style={{width:11,height:11,borderRadius:2,background:c,opacity:i===0?.25:1}}/>
        ))}
        <span style={{fontSize:8,color:T.muted}}>More</span>
      </div>
    </div>
  );
}

/* ── Dynamic Zone Map (any country) ───────────────────────────────────── */
// GeoJSON sources per country — add more as needed
// Local GeoJSON files (no external network dependency)
const GEO_SOURCES = {
  'India': '/india_states.geojson',
};
const GEO_NAME_KEYS = { 'India': 'ST_NM' };
const GEO_PROJECTIONS = {
  'India': { center: [82, 24], scale: 700 },
};
const WORLD_GEO = '/countries-110m.json';

function WorldZoneMap({ zones, states, T, country }) {
  const [hovered, setHovered] = useState(null);
  const stateList = states || [];
  const maxTotal = Math.max(...stateList.map(s => s.total), 1);
  const stateMap = {};
  stateList.forEach(s => { stateMap[s.state] = s; });
  const palette = [K.navy, K.royal, K.sky, K.indigo, K.violet, '#0891B2', '#16A34A', '#F59E0B'];

  // Dynamic GeoJSON + projection based on detected country
  const detectedCountry = country || 'India';
  const geoUrl = GEO_SOURCES[detectedCountry] || WORLD_GEO;
  const projConfig = GEO_PROJECTIONS[detectedCountry] || { center: [0, 20], scale: 120 };
  const nameKey = GEO_NAME_KEYS[detectedCountry] || 'name';

  // Fuzzy match: geo property name → state_data name
  const matchState = (geoName) => {
    if (stateMap[geoName]) return geoName;
    // Case-insensitive match
    const lower = geoName.toLowerCase();
    for (const s of stateList) {
      if (s.state.toLowerCase() === lower) return s.state;
      if (lower.includes(s.state.toLowerCase()) || s.state.toLowerCase().includes(lower)) return s.state;
    }
    return null;
  };

  return (
    <div>
      <ComposableMap
        projection="geoMercator"
        projectionConfig={projConfig}
        style={{ width: '100%', height: 260 }}
      >
        <Geographies geography={geoUrl}>
          {({ geographies }) =>
            geographies.map(geo => {
              const geoName = geo.properties[nameKey] || geo.properties.NAME_1 || geo.properties.name || '';
              const matched = matchState(geoName);
              const sd = matched ? stateMap[matched] : null;
              const stateIdx = sd ? stateList.findIndex(s=>s.state===matched) : -1;
              const stateColor = stateIdx >= 0 ? palette[stateIdx % palette.length] : null;
              const intensity = sd ? sd.total / maxTotal : 0;
              const isHov = hovered === matched;
              return (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  fill={sd ? stateColor + (isHov ? 'EE' : 'BB') : (T === TD ? '#1C2E48' : '#E8ECF0')}
                  stroke={T === TD ? '#253550' : '#CBD5E1'}
                  strokeWidth={0.5}
                  onMouseEnter={() => sd && setHovered(matched)}
                  onMouseLeave={() => setHovered(null)}
                  style={{
                    default: { outline: 'none' },
                    hover: { outline: 'none', cursor: sd ? 'pointer' : 'default' },
                    pressed: { outline: 'none' },
                  }}
                />
              );
            })
          }
        </Geographies>
      </ComposableMap>

      {/* Tooltip — always rendered, fixed height to prevent layout shift */}
      <div style={{textAlign:'center',padding:'4px 0',height:20,marginTop:-4}}>
        {hovered && stateMap[hovered] && (()=>{
          const sd = stateMap[hovered];
          const idx = stateList.findIndex(s=>s.state===hovered);
          const c = palette[Math.max(idx,0) % palette.length];
          return <>
            <span style={{fontSize:10,fontWeight:700,color:c,marginRight:6}}>{hovered}</span>
            <span style={{fontSize:9,color:T.textSub}}>
              {sd.total} tickets · {sd.resolved} resolved · <strong>{sd.rate}%</strong>
            </span>
          </>;
        })()}
      </div>

      {/* State legend grid */}
      <div style={{display:'grid',gridTemplateColumns:'repeat(3,1fr)',gap:4,marginTop:6}}>
        {stateList.map((s,i) => {
          const c = palette[i % palette.length];
          return (
            <div key={s.state} style={{display:'flex',alignItems:'center',gap:4,padding:'3px 6px',borderRadius:5,
              background:hovered===s.state ? c+'18' : T.surface2,border:`1px solid ${hovered===s.state ? c+'40' : T.border}`,
              cursor:'pointer',transition:'all .2s ease'}}
              onMouseEnter={()=>setHovered(s.state)} onMouseLeave={()=>setHovered(null)}>
              <div style={{width:8,height:8,borderRadius:2,background:c,flexShrink:0}}/>
              <div style={{flex:1,minWidth:0}}>
                <div style={{fontSize:7.5,fontWeight:700,color:T.textSub,whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{s.state}</div>
              </div>
              <span style={{fontSize:8,fontWeight:800,color:c,fontFamily:"'IBM Plex Mono',monospace"}}>{s.total}</span>
              <span style={{fontSize:7,color:T.muted,fontWeight:600}}>{s.rate}%</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ── Treemap Custom Content ──────────────────────────────────────────────── */
function TreemapContent({ x, y, width, height, name, size, rate, depth, index }) {
  if (width < 30 || height < 22) return null;
  const colors = [K.navy, K.royal, K.sky, K.indigo, K.violet];
  const c = colors[index % colors.length];
  const rx = Math.round(x), ry = Math.round(y), rw = Math.round(width), rh = Math.round(height);
  const maxChars = Math.max(Math.floor((rw - 16) / 7), 4);
  const displayName = (name||'').length > maxChars ? (name||'').slice(0, maxChars - 1) + '..' : name;
  return (
    <foreignObject x={rx+1} y={ry+1} width={rw-2} height={rh-2}>
      <div style={{width:'100%',height:'100%',background:c,borderRadius:5,border:'2px solid #fff',
        padding:'8px 10px',boxSizing:'border-box',overflow:'hidden',display:'flex',flexDirection:'column',justifyContent:'center'}}>
        {rw > 45 && <div style={{color:'#fff',fontSize:Math.min(13,rw/7),fontWeight:700,
          fontFamily:"'Plus Jakarta Sans',sans-serif",lineHeight:1.2,marginBottom:2}}>{displayName}</div>}
        {rw > 40 && rh > 40 && <div style={{color:'rgba(255,255,255,0.9)',fontSize:Math.min(11,rw/9),fontWeight:700,
          fontFamily:"'IBM Plex Mono',monospace",lineHeight:1.3}}>{size} tickets</div>}
        {rw > 55 && rh > 55 && <div style={{color:'rgba(255,255,255,0.7)',fontSize:9,fontWeight:600,
          fontFamily:"'IBM Plex Mono',monospace",lineHeight:1.3}}>{rate}% resolved</div>}
      </div>
    </foreignObject>
  );
}

/* ── Empty State ─────────────────────────────────────────────────────────── */
function Empty({ T }) {
  return <div style={{height:160,display:'flex',alignItems:'center',justifyContent:'center',fontSize:11,color:T.muted}}>No data available</div>;
}

/* ═══════════════════════════════════════════════════════════════════════════
   MAIN DASHBOARD
   ═══════════════════════════════════════════════════════════════════════════ */
export default function AgentDashboard() {
  const { user } = useAuth();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);
  const { isDark: dark } = useTheme();
  const T = dark ? TD : TL;

  const fetchDashboard = useCallback(async (silent = false) => {
    if (!silent) setRefreshing(true);
    try { const d = await apiGet('/api/agent/dashboard'); setData(d); setLastUpdated(new Date()); }
    catch { /* keep prev */ }
    finally { setLoading(false); if (!silent) setRefreshing(false); }
  }, []);

  useEffect(() => { fetchDashboard(false); const iv = setInterval(()=>fetchDashboard(true),30000); return ()=>clearInterval(iv); }, [fetchDashboard]);

  // Memoized data extraction
  const D = useMemo(() => {
    if (!data) return null;
    const kpis = data.kpis||{};
    const summary = data.summary||{};
    return {
      kpis, summary,
      monthly: data.monthly_trend||[],
      sentiment: data.sentiment||[],
      catRes: data.category_resolution||[],
      efficiency: data.efficiency_metrics||{},
      tiers: (data.customer_tiers||[]).sort((a,b)=>TIER_ORDER.indexOf(a.tier)-TIER_ORDER.indexOf(b.tier)),
      aiVsAgent: data.ai_vs_agent||{},
      heatmap: data.heatmap||Array.from({length:7},()=>Array(24).fill(0)),
      perfScore: data.performance_score||0,
      badges: data.badges||[],
      perfRadar: data.perf_radar||[],
      hotspots: data.issue_hotspots||[],
      zones: data.zone_data||[],
      states: data.state_data||[],
      detectedCountry: data.detected_country||'Unknown',
      slaRisk: data.sla_risk||[],
      slaRiskSum: data.sla_risk_summary||{safe:0,warning:0,critical:0,breached:0},
      slaHealthPct: data.sla_health_pct||0,
      slaPriorityDist: data.sla_priority_dist||[],
      slaTotalOpen: data.sla_total_open||0,
      treemap: data.category_treemap||[],
      hourly: data.hourly_today||[],
      insights: data.ai_insights||[],
      totalSentiment: (data.sentiment||[]).reduce((a,s)=>a+(s.value||0),0),
    };
  }, [data]);

  if (loading) return <div className="page-loader"><div className="spinner"/></div>;
  if (!D) return <div style={{padding:40,textAlign:'center',color:'#94a3b8'}}>Failed to load dashboard data.</div>;

  const TipC = p => <DTip T={T} {...p}/>;

  // KPI cards
  const kpiCards = [
    { label:'MTTR',                     value:D.kpis.mttr??0,                          unit:'hrs', icon:IC.clock,  sub:'Mean Time To Resolve',              color:K.navy },
    { label:'SLA Compliance',           value:`${D.kpis.sla_compliance_rate??0}`,       unit:'%',   icon:IC.shield, sub:'Resolved within SLA',               color:K.royal },
    { label:'First Contact Resolution', value:`${D.kpis.first_contact_resolution??0}`,  unit:'%',   icon:IC.target, sub:'Resolved without re-contact',       color:K.indigo },
    { label:'CSAT Score',               value:D.kpis.csat??0,                           unit:'/ 5', icon:IC.star,   sub:`${D.kpis.csat_pct??0}% rated 4+`,   color:K.indigo },
    { label:'Reopen Rate',              value:`${D.kpis.reopen_rate??0}`,               unit:'%',   icon:IC.repeat, sub:'Tickets reopened',                  alert:(D.kpis.reopen_rate??0)>10 },
    { label:'H/S Incident Resolution',  value:D.kpis.hs_incident_resolution_time??0,    unit:'hrs', icon:IC.alert,  sub:'Critical & High resolution',        alert:true },
    { label:'H/S Incident Response',    value:D.kpis.hs_incident_response_time??0,      unit:'hrs', icon:IC.zap,    sub:'Avg first response time',           color:K.sky },
    { label:'Avg Open Ticket Age',      value:D.kpis.avg_aging_hours??0,                unit:'hrs', icon:IC.aging,  sub:'Unresolved ticket aging',           alert:(D.kpis.avg_aging_hours??0)>48 },
  ];

  return (
    <div style={{background:T.bg,minHeight:'100vh',padding:'0 0 40px',fontFamily:"'Plus Jakarta Sans',system-ui,sans-serif"}}>
      <style>{CSS}</style>

      {/* ═══ HEADER ═══════════════════════════════════════════════════════ */}
      <div style={{
        background:dark?'linear-gradient(135deg,#080F1E 0%,#0D1A33 40%,#151535 100%)':'linear-gradient(135deg,#00338D 0%,#005EB8 50%,#0091DA 100%)',
        padding:'18px 28px 20px',borderBottom:`1px solid ${T.border}`,
      }}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',flexWrap:'wrap',gap:12}}>
          <div>
            <div style={{display:'flex',alignItems:'center',gap:10,flexWrap:'wrap'}}>
              <h1 style={{margin:0,fontSize:20,fontWeight:800,color:'#fff',letterSpacing:'-.01em'}}>
                Performance Command Center
              </h1>
              {D.badges.length>0 && <div style={{display:'flex',gap:5,flexWrap:'wrap'}}>
                {D.badges.map(b=>(
                  <span key={b.tag} style={{
                    display:'inline-flex',alignItems:'center',gap:3,padding:'2px 9px',borderRadius:16,
                    fontSize:9,fontWeight:700,background:`${BADGE_COLORS[b.tag]||K.navy}35`,
                    color:'#fff',border:`1px solid ${BADGE_COLORS[b.tag]||K.navy}55`,
                  }}>{b.tag}</span>
                ))}
              </div>}
            </div>
            <p style={{margin:'3px 0 0',fontSize:11.5,color:'rgba(255,255,255,.6)'}}>
              {user?.name} {data?.agent_domain?`/ ${data.agent_domain}`:''} {data?.agent_location?`/ ${data.agent_location}`:''}
            </p>
          </div>
          <div style={{display:'flex',alignItems:'center',gap:8}}>
            {lastUpdated && <span style={{fontSize:9.5,color:'rgba(255,255,255,.4)'}}>{lastUpdated.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}</span>}
            <button onClick={()=>fetchDashboard(false)} disabled={refreshing}
              style={{display:'flex',alignItems:'center',gap:4,padding:'4px 13px',borderRadius:16,fontSize:10,fontWeight:600,
                background:'rgba(255,255,255,.12)',border:'1px solid rgba(255,255,255,.22)',color:'#fff',cursor:'pointer',opacity:refreshing?.5:1}}>
              <span style={{display:'inline-block',animation:refreshing?'spin 1s linear infinite':'none'}}>{IC.refresh}</span>
              {refreshing?'Refreshing...':'Refresh'}
            </button>
          </div>
        </div>
      </div>

      <div style={{padding:'20px 28px 0'}}>

        {/* ═══ SUMMARY RIBBON ═════════════════════════════════════════════ */}
        <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:14,marginBottom:16}}>
          {[
            {l:'Total Tickets',      v:D.summary.total_tickets??0,  ic:IC.grid,  d:'All assigned',       g:`linear-gradient(135deg,${K.navy},${K.royal})`},
            {l:'Resolved',           v:D.summary.resolved??0,       ic:IC.check, d:'Closed successfully', g:'linear-gradient(135deg,#059669,#10b981)'},
            {l:'Open / In Progress', v:D.summary.open??0,           ic:IC.clock, d:'Awaiting resolution', g:'linear-gradient(135deg,#d97706,#f59e0b)'},
            {l:'Customer Feedbacks', v:D.summary.total_feedback??0,  ic:IC.star,  d:'Ratings received',    g:`linear-gradient(135deg,${K.indigo},${K.violet})`},
          ].map(({l,v,ic,d,g})=>(
            <div key={l} style={{
              background:T.kpiCardBg,borderRadius:12,border:`1px solid ${T.border}`,padding:'15px 18px',
              boxShadow:T.cardShadow,display:'flex',alignItems:'center',gap:14,animation:'fadeUp .4s ease',
            }}>
              <div style={{width:42,height:42,borderRadius:11,background:g,display:'flex',alignItems:'center',justifyContent:'center',color:'#fff',flexShrink:0,boxShadow:'0 4px 12px rgba(0,0,0,.2)'}}>{ic}</div>
              <div>
                <div style={{fontSize:10,color:T.muted,fontWeight:500,marginBottom:1}}>{l}</div>
                <div style={{fontSize:26,fontWeight:800,color:T.text,lineHeight:1,fontFamily:"'IBM Plex Mono',monospace"}}>{v}</div>
                <div style={{fontSize:9.5,color:T.muted,marginTop:2}}>{d}</div>
              </div>
            </div>
          ))}
        </div>

        {/* ═══ KPI CARDS (8, 4x2) ═════════════════════════════════════════ */}
        <Sec T={T}>Key Performance Indicators</Sec>
        <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:12,marginBottom:16}}>
          {kpiCards.map(k=><KpiCard key={k.label} {...k} T={T}/>)}
        </div>

        {/* ═══ ROW 1: Gauge + AI vs Human + SLA Risk ═════════════════════ */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1.5fr 1.2fr',gap:16,marginBottom:16}}>

          {/* Speedometer */}
          <Card T={T} title="Performance Index" icon={IC.activity}>
            <div style={{display:'flex',flexDirection:'column',alignItems:'center',gap:6}}>
              <Speedometer score={D.perfScore} T={T}/>
              <div style={{padding:'3px 14px',borderRadius:16,fontSize:10,fontWeight:700,
                background:D.perfScore>=80?T.green+'1A':D.perfScore>=60?T.amber+'1A':T.red+'1A',
                color:D.perfScore>=80?T.green:D.perfScore>=60?T.amber:T.red,
                border:`1px solid ${D.perfScore>=80?T.green:D.perfScore>=60?T.amber:T.red}30`,
              }}>
                {D.perfScore>=90?'Outstanding':D.perfScore>=80?'Excellent':D.perfScore>=70?'Good':D.perfScore>=60?'Average':'Needs Improvement'}
              </div>
              <div style={{display:'flex',gap:10,flexWrap:'wrap',justifyContent:'center'}}>
                {[{range:'0 - 40',color:'#EF4444'},{range:'40 - 70',color:'#F59E0B'},{range:'70 - 100',color:'#22C55E'}].map(z=>(
                  <div key={z.range} style={{display:'flex',alignItems:'center',gap:4}}>
                    <div style={{width:8,height:8,borderRadius:2,background:z.color}}/>
                    <span style={{fontSize:7.5,color:T.muted,fontWeight:600}}>{z.range}</span>
                  </div>
                ))}
              </div>
            </div>
          </Card>

          {/* AI Chatbot vs Human Agent */}
          <Card T={T} title="AI Chatbot vs Human Agent" subtitle="Customer -> Chatbot -> Resolved OR -> Escalate -> Agent" icon={IC.users}>
            <div style={{display:'flex',flexDirection:'column',gap:12}}>
              {/* Flow visualization */}
              <div style={{display:'flex',alignItems:'center',gap:8}}>
                <div style={{textAlign:'center',flex:'0 0 auto'}}>
                  <div style={{fontSize:22,fontWeight:800,color:T.text,fontFamily:"'IBM Plex Mono',monospace"}}>{D.aiVsAgent.total_conversations||0}</div>
                  <div style={{fontSize:7.5,fontWeight:600,color:T.muted,textTransform:'uppercase',marginTop:1}}>Conversations</div>
                </div>
                <svg width="20" height="20" viewBox="0 0 20 20" style={{flexShrink:0,opacity:.3}}><path d="M4 10h12M12 6l4 4-4 4" fill="none" stroke={T.muted} strokeWidth="1.5"/></svg>
                <div style={{flex:1,display:'flex',flexDirection:'column',gap:5}}>
                  <div style={{display:'flex',alignItems:'center',gap:8,background:K.sky+'0D',borderRadius:8,padding:'6px 12px',border:`1px solid ${K.sky}20`}}>
                    <div style={{width:8,height:8,borderRadius:'50%',background:K.sky,flexShrink:0}}/>
                    <span style={{fontSize:8,fontWeight:600,color:K.sky,textTransform:'uppercase',flex:1}}>AI Resolved</span>
                    <span style={{fontSize:16,fontWeight:800,color:K.sky,fontFamily:"'IBM Plex Mono',monospace"}}>{D.aiVsAgent.ai_resolved||0}</span>
                  </div>
                  <div style={{display:'flex',alignItems:'center',gap:8,background:K.navy+'0D',borderRadius:8,padding:'6px 12px',border:`1px solid ${K.navy}20`}}>
                    <div style={{width:8,height:8,borderRadius:'50%',background:K.navy,flexShrink:0}}/>
                    <span style={{fontSize:8,fontWeight:600,color:K.navy,textTransform:'uppercase',flex:1}}>Escalated</span>
                    <span style={{fontSize:16,fontWeight:800,color:K.navy,fontFamily:"'IBM Plex Mono',monospace"}}>{D.aiVsAgent.escalated_to_agent||0}</span>
                  </div>
                </div>
              </div>

              {/* Rate bars */}
              <div style={{display:'flex',flexDirection:'column',gap:8}}>
                {[
                  {label:'AI Resolution',rate:D.aiVsAgent.ai_resolution_rate||0,color:K.sky},
                  {label:'Agent Resolution',rate:D.aiVsAgent.agent_resolution_rate||0,color:K.navy},
                  {label:'Escalation',rate:D.aiVsAgent.escalation_rate||0,color:K.indigo},
                ].map(b=>(
                  <div key={b.label}>
                    <div style={{display:'flex',justifyContent:'space-between',alignItems:'baseline',marginBottom:3}}>
                      <span style={{fontSize:9.5,fontWeight:600,color:T.textSub}}>{b.label}</span>
                      <span style={{fontSize:11,fontWeight:800,color:b.color,fontFamily:"'IBM Plex Mono',monospace"}}>{b.rate}%</span>
                    </div>
                    <div style={{background:T.progressTrack,borderRadius:4,height:6,overflow:'hidden'}}>
                      <div style={{height:'100%',width:`${b.rate}%`,background:b.color,borderRadius:4,transition:'width 1s ease'}}/>
                    </div>
                  </div>
                ))}
              </div>

              {/* Avg times */}
              <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10}}>
                <div style={{textAlign:'center',padding:'8px 6px',background:T.surface2,borderRadius:8,borderTop:`2px solid ${K.sky}`}}>
                  <div style={{fontSize:7.5,color:T.muted,fontWeight:600,textTransform:'uppercase'}}>Avg AI Time</div>
                  <div style={{fontSize:15,fontWeight:800,color:T.text,fontFamily:"'IBM Plex Mono',monospace",marginTop:3}}>{D.aiVsAgent.ai_avg_time||0}<span style={{fontSize:9,color:T.muted,fontWeight:500}}>h</span></div>
                </div>
                <div style={{textAlign:'center',padding:'8px 6px',background:T.surface2,borderRadius:8,borderTop:`2px solid ${K.navy}`}}>
                  <div style={{fontSize:7.5,color:T.muted,fontWeight:600,textTransform:'uppercase'}}>Avg Agent Time</div>
                  <div style={{fontSize:15,fontWeight:800,color:T.text,fontFamily:"'IBM Plex Mono',monospace",marginTop:3}}>{D.aiVsAgent.agent_avg_time||0}<span style={{fontSize:9,color:T.muted,fontWeight:500}}>h</span></div>
                </div>
              </div>
            </div>
          </Card>

          {/* SLA Risk Predictor — Pure Analytics */}
          <Card T={T} title="SLA Risk Predictor" icon={IC.alert}>
            {D.slaTotalOpen === 0 ? (
              <div style={{textAlign:'center',padding:30,color:T.muted,fontSize:11}}>No open tickets to analyze</div>
            ) : (<>
              {/* Row 1: Health Gauge + Risk Donut side by side */}
              <div style={{display:'flex',gap:8,marginBottom:14,alignItems:'center'}}>
                {/* Radial Health Gauge */}
                <div style={{width:110,height:110,flexShrink:0,position:'relative'}}>
                  <ResponsiveContainer width="100%" height="100%">
                    <RadialBarChart cx="50%" cy="50%" innerRadius="68%" outerRadius="100%" startAngle={210} endAngle={-30}
                      data={[{value:D.slaHealthPct,fill:D.slaHealthPct>=80?'#16A34A':D.slaHealthPct>=50?'#F59E0B':'#DC2626'}]}>
                      <RadialBar background={{fill:T.progressTrack}} dataKey="value" cornerRadius={6} max={100}/>
                    </RadialBarChart>
                  </ResponsiveContainer>
                  <div style={{position:'absolute',top:'50%',left:'50%',transform:'translate(-50%,-50%)',textAlign:'center'}}>
                    <div style={{fontSize:20,fontWeight:900,color:D.slaHealthPct>=80?'#16A34A':D.slaHealthPct>=50?'#F59E0B':'#DC2626',fontFamily:"'IBM Plex Mono',monospace",lineHeight:1}}>{D.slaHealthPct}%</div>
                    <div style={{fontSize:7,color:T.muted,fontWeight:600,marginTop:2}}>SLA HEALTH</div>
                  </div>
                </div>
                {/* Risk Distribution Donut */}
                <div style={{flex:1,height:110}}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie data={[
                        {name:'Safe',value:D.slaRiskSum.safe||0,fill:RISK_COLORS.safe},
                        {name:'Warning',value:D.slaRiskSum.warning||0,fill:RISK_COLORS.warning},
                        {name:'Critical',value:D.slaRiskSum.critical||0,fill:RISK_COLORS.critical},
                        {name:'Breached',value:D.slaRiskSum.breached||0,fill:RISK_COLORS.breached},
                      ].filter(d=>d.value>0)} cx="50%" cy="50%" innerRadius={28} outerRadius={44} paddingAngle={3} dataKey="value">
                        {[
                          {fill:RISK_COLORS.safe},{fill:RISK_COLORS.warning},{fill:RISK_COLORS.critical},{fill:RISK_COLORS.breached},
                        ].filter((_,i)=>[D.slaRiskSum.safe,D.slaRiskSum.warning,D.slaRiskSum.critical,D.slaRiskSum.breached][i]>0)
                         .map((d,i)=><Cell key={i} fill={d.fill} stroke={d.fill}/>)}
                      </Pie>
                      <Tooltip content={p=><DTip T={T} {...p}/>}/>
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Row 2: Risk Counters */}
              <div style={{display:'flex',gap:6,marginBottom:14}}>
                {['breached','critical','warning','safe'].map(k=>{
                  const v = D.slaRiskSum[k] || 0;
                  const isUrgent = v > 0 && (k === 'breached' || k === 'critical');
                  return (
                    <div key={k} style={{flex:1,textAlign:'center',padding:'8px 4px',borderRadius:10,
                      background:v>0 ? RISK_COLORS[k]+'18' : T.surface2,
                      border:`1.5px solid ${v>0 ? RISK_COLORS[k]+'40' : T.border}`,
                      boxShadow:isUrgent ? `0 0 12px ${RISK_COLORS[k]}30` : 'none',
                      transition:'all 0.3s ease'}}>
                      <div style={{fontSize:20,fontWeight:900,color:v>0?RISK_COLORS[k]:T.muted,fontFamily:"'IBM Plex Mono',monospace",lineHeight:1}}>{v}</div>
                      <div style={{fontSize:7.5,fontWeight:700,color:v>0?RISK_COLORS[k]:T.muted,textTransform:'uppercase',marginTop:3,letterSpacing:0.5}}>{k}</div>
                      {isUrgent && <div style={{width:6,height:6,borderRadius:'50%',background:RISK_COLORS[k],margin:'4px auto 0',
                        animation:'pulse 1.5s ease-in-out infinite',boxShadow:`0 0 8px ${RISK_COLORS[k]}`}}/>}
                    </div>
                  );
                })}
              </div>

              {/* Row 3: Priority-wise SLA Breakdown (stacked bars) */}
              {D.slaPriorityDist.length > 0 && (
                <div style={{marginBottom:14}}>
                  <div style={{fontSize:8.5,fontWeight:700,color:T.textSub,textTransform:'uppercase',marginBottom:6,letterSpacing:0.5}}>Risk by Priority</div>
                  {D.slaPriorityDist.map(p=>{
                    const total = p.total || 1;
                    return (
                      <div key={p.priority} style={{display:'flex',alignItems:'center',gap:6,marginBottom:5}}>
                        <span style={{fontSize:8.5,fontWeight:700,color:T.textSub,width:50,textTransform:'capitalize'}}>{p.priority}</span>
                        <div style={{flex:1,height:10,borderRadius:5,background:T.progressTrack,overflow:'hidden',display:'flex'}}>
                          {p.breached>0 && <div style={{width:`${(p.breached/total)*100}%`,height:'100%',background:RISK_COLORS.breached}}/>}
                          {p.critical>0 && <div style={{width:`${(p.critical/total)*100}%`,height:'100%',background:RISK_COLORS.critical}}/>}
                          {p.warning>0 && <div style={{width:`${(p.warning/total)*100}%`,height:'100%',background:RISK_COLORS.warning}}/>}
                          {p.safe>0 && <div style={{width:`${(p.safe/total)*100}%`,height:'100%',background:RISK_COLORS.safe}}/>}
                        </div>
                        <span style={{fontSize:8.5,fontWeight:700,color:T.muted,width:18,textAlign:'right'}}>{p.total}</span>
                      </div>
                    );
                  })}
                  {/* Legend */}
                  <div style={{display:'flex',gap:10,marginTop:6,justifyContent:'center'}}>
                    {['safe','warning','critical','breached'].map(k=>(
                      <div key={k} style={{display:'flex',alignItems:'center',gap:3}}>
                        <div style={{width:6,height:6,borderRadius:2,background:RISK_COLORS[k]}}/>
                        <span style={{fontSize:7,color:T.muted,textTransform:'capitalize',fontWeight:600}}>{k}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

            </>)}
          </Card>
        </div>

        {/* ═══ ROW 2: Zone Map + Complaint Funnel + Radar ════════════════ */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:16,marginBottom:16}}>

          {/* Zone Map */}
          <Card T={T} title="Zone-wise Performance" subtitle="Regional ticket distribution on map" icon={IC.map}>
            {D.zones.length>0 ? <WorldZoneMap zones={D.zones} states={D.states} T={T} country={D.detectedCountry}/> : <Empty T={T}/>}
          </Card>

          {/* Complaint Funnel */}
          <Card T={T} title="Issue Hotspots" subtitle="Top complaint types — open vs resolved" icon={IC.alert}>
            {D.hotspots.length>0 ? (
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={D.hotspots} layout="vertical" margin={{top:5,right:15,bottom:5,left:5}} barGap={2}>
                  <CartesianGrid strokeDasharray="3 3" stroke={T.gridStroke} horizontal={false}/>
                  <XAxis type="number" tick={{fontSize:9,fill:T.muted}} axisLine={false} tickLine={false} allowDecimals={false}/>
                  <YAxis type="category" dataKey="name" width={110} tick={{fontSize:8.5,fontWeight:600,fill:T.textSub}} axisLine={false} tickLine={false}/>
                  <Tooltip content={({payload,label})=>{
                    if(!payload||!payload.length) return null;
                    const d=payload[0]?.payload;
                    if(!d) return null;
                    return (
                      <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:8,padding:'10px 14px',boxShadow:'0 4px 12px rgba(0,0,0,.15)'}}>
                        <div style={{fontSize:10,fontWeight:700,color:T.text,marginBottom:4}}>{label}</div>
                        <div style={{fontSize:10,color:K.navy}}>Resolved: <strong>{d.resolved}</strong></div>
                        <div style={{fontSize:10,color:K.sky}}>Open: <strong>{d.open}</strong></div>
                        <div style={{fontSize:10,color:T.muted}}>Total: <strong>{d.total}</strong></div>
                      </div>
                    );
                  }}/>
                  <Bar dataKey="resolved" stackId="s" fill={K.navy} barSize={16} radius={[0,0,0,0]} name="Resolved"/>
                  <Bar dataKey="open" stackId="s" fill={K.sky} barSize={16} radius={[0,4,4,0]} name="Open"/>
                </BarChart>
              </ResponsiveContainer>
            ) : <Empty T={T}/>}
          </Card>

          {/* Performance DNA Radar */}
          <Card T={T} title="Performance DNA" subtitle="Multi-dimensional strengths" icon={IC.target}>
            {D.perfRadar.length>0 ? (
              <div>
                <ResponsiveContainer width="100%" height={250}>
                  <RadarChart data={D.perfRadar} cx="50%" cy="50%" outerRadius="68%">
                    <defs>
                      <linearGradient id="radarFill" x1="0" y1="0" x2="1" y2="1">
                        <stop offset="0%" stopColor={K.navy} stopOpacity=".4"/>
                        <stop offset="50%" stopColor={K.sky} stopOpacity=".25"/>
                        <stop offset="100%" stopColor={K.violet} stopOpacity=".15"/>
                      </linearGradient>
                    </defs>
                    <PolarGrid stroke={T.gridStroke} gridType="polygon"/>
                    <PolarAngleAxis dataKey="axis" tick={{fontSize:9.5,fill:T.textSub,fontWeight:700}}/>
                    <PolarRadiusAxis angle={90} domain={[0,100]} tick={{fontSize:7,fill:T.muted}} tickCount={5} axisLine={false}/>
                    <Tooltip content={({payload})=>{
                      if(!payload||!payload.length) return null;
                      const d=payload.find(p=>p.name==='Score');
                      if(!d) return null;
                      const item=d.payload;
                      return (
                        <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:8,padding:'8px 12px',boxShadow:'0 4px 12px rgba(0,0,0,.15)'}}>
                          <div style={{fontSize:11,fontWeight:800,color:K.navy}}>{item.axis}</div>
                          <div style={{fontSize:14,fontWeight:900,color:item.value>=75?K.navy:item.value>=50?K.royal:K.violet,fontFamily:"'IBM Plex Mono',monospace"}}>{item.value}/100</div>
                          {item.detail && <div style={{fontSize:9,color:T.muted,marginTop:2}}>{item.detail}</div>}
                        </div>
                      );
                    }}/>
                    {/* Target ring at 75% */}
                    <Radar name="Target" dataKey={() => 75} stroke={T.green} strokeWidth={1.5} strokeDasharray="4 3"
                      fill="none" dot={false}/>
                    <Radar name="Score" dataKey="value" stroke={K.navy} fill="url(#radarFill)" strokeWidth={2.5}
                      dot={{r:4,fill:K.navy,stroke:T.surface,strokeWidth:2}} activeDot={{r:6,fill:K.navy,stroke:'#fff',strokeWidth:2}}/>
                  </RadarChart>
                </ResponsiveContainer>
                {/* Dimension scores */}
                <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:4,marginTop:4}}>
                  {D.perfRadar.map((d,i)=>(
                    <div key={i} style={{textAlign:'center',padding:'3px 4px',borderRadius:6,background:T.surface2}}>
                      <div style={{fontSize:7.5,fontWeight:600,color:T.muted,textTransform:'uppercase'}}>{d.axis}</div>
                      <div style={{fontSize:12,fontWeight:900,color:d.value>=75?K.navy:d.value>=50?K.royal:K.violet,fontFamily:"'IBM Plex Mono',monospace"}}>{d.value}</div>
                    </div>
                  ))}
                </div>
                <div style={{display:'flex',justifyContent:'center',gap:14,marginTop:6}}>
                  <div style={{display:'flex',alignItems:'center',gap:4}}>
                    <div style={{width:14,height:3,borderRadius:1,background:K.navy}}/>
                    <span style={{fontSize:8.5,color:T.muted,fontWeight:600}}>Your Score</span>
                  </div>
                  <div style={{display:'flex',alignItems:'center',gap:4}}>
                    <div style={{width:14,height:0,borderTop:`2px dashed ${T.green}`}}/>
                    <span style={{fontSize:8.5,color:T.muted,fontWeight:600}}>Target (75)</span>
                  </div>
                </div>
              </div>
            ) : <Empty T={T}/>}
          </Card>
        </div>

        {/* ═══ ROW 3: Sentiment + Category Treemap ═══════════════════════ */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1.3fr',gap:16,marginBottom:16}}>

          {/* Sentiment Analysis — Clean 2D Donut */}
          <Card T={T} title="Customer Sentiment Analysis" subtitle="Feedback rating distribution" icon={IC.star}>
            {D.totalSentiment>0 ? (
              <div style={{display:'flex',alignItems:'center',gap:16}}>
                <ResponsiveContainer width="48%" height={200}>
                  <PieChart>
                    <Pie data={D.sentiment.filter(s=>s.value>0)} dataKey="value" nameKey="name"
                      cx="50%" cy="50%" outerRadius={80} innerRadius={48}
                      paddingAngle={3} strokeWidth={2} stroke="#fff">
                      {D.sentiment.filter(s=>s.value>0).map((_,i)=>(
                        <Cell key={i} fill={[K.navy,K.royal,K.sky,K.indigo,K.violet][i]}/>
                      ))}
                    </Pie>
                    <Tooltip content={({payload})=>{
                      if(!payload||!payload[0]) return null;
                      const d=payload[0];
                      return (
                        <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:8,padding:'8px 12px',boxShadow:'0 4px 12px rgba(0,0,0,.15)'}}>
                          <div style={{fontSize:11,fontWeight:700,color:T.text}}>{d.name}: {d.value} ({Math.round(d.value/D.totalSentiment*100)}%)</div>
                        </div>
                      );
                    }}/>
                    <text x="50%" y="46%" textAnchor="middle" fontSize={18} fontWeight={900}
                      fill={T.text} fontFamily="'IBM Plex Mono',monospace" dominantBaseline="central">{D.totalSentiment}</text>
                    <text x="50%" y="58%" textAnchor="middle" fontSize={7.5} fontWeight={600}
                      fill={T.muted} dominantBaseline="central">TOTAL</text>
                  </PieChart>
                </ResponsiveContainer>
                <div style={{display:'flex',flexDirection:'column',gap:7,flex:1}}>
                  {D.sentiment.map((s,i)=>{
                    const pct = D.totalSentiment>0?Math.round(s.value/D.totalSentiment*100):0;
                    return (
                      <div key={s.name} style={{display:'flex',alignItems:'center',gap:6}}>
                        <div style={{width:10,height:10,borderRadius:2,background:[K.navy,K.royal,K.sky,K.indigo,K.violet][i],flexShrink:0}}/>
                        <span style={{fontSize:10,color:T.textSub,flex:1,fontWeight:600}}>{s.name}</span>
                        <span style={{fontSize:12,fontWeight:800,color:T.text,fontFamily:"'IBM Plex Mono',monospace"}}>{s.value}</span>
                        <span style={{fontSize:9,color:T.muted,width:30,textAlign:'right',fontWeight:600}}>{pct}%</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : <Empty T={T}/>}
          </Card>

          {/* Category Treemap */}
          <Card T={T} title="Complaint Categories" subtitle="Proportional view by volume" icon={IC.grid}>
            {D.treemap.length>0 ? (
              <ResponsiveContainer width="100%" height={210}>
                <Treemap data={D.treemap} dataKey="size" nameKey="name"
                  aspectRatio={4/3} stroke={T.surface} strokeWidth={2}
                  content={<TreemapContent/>}/>
              </ResponsiveContainer>
            ) : <Empty T={T}/>}
          </Card>
        </div>

        {/* ═══ ROW 4: Customer Tiers (RadialBar) + Issue Hotspots ═════════ */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16,marginBottom:16}}>

          {/* Customer Tier Distribution — RadialBar with tier colors */}
          <Card T={T} title="Customer Tier Distribution" subtitle="Tickets & resolution by segment" icon={IC.users}>
            {D.tiers.length>0 ? (
              <div>
                <div style={{display:'flex',alignItems:'center',gap:14}}>
                  <ResponsiveContainer width="45%" height={200}>
                    <RadialBarChart cx="50%" cy="50%" innerRadius="25%" outerRadius="95%" barSize={12}
                      data={D.tiers.map(t=>({name:t.tier,value:t.rate,fill:TIER_COLORS[t.tier]||T.blue}))}
                      startAngle={180} endAngle={-180}>
                      <RadialBar background={{fill:T.progressTrack}} dataKey="value" cornerRadius={6}/>
                      <Tooltip content={({payload})=>{
                        if(!payload||!payload[0]) return null;
                        const d=payload[0].payload;
                        return (
                          <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:8,padding:'8px 12px',boxShadow:'0 4px 12px rgba(0,0,0,.15)'}}>
                            <div style={{fontSize:11,fontWeight:800,color:d.fill}}>{d.name}: {d.value}%</div>
                          </div>
                        );
                      }}/>
                    </RadialBarChart>
                  </ResponsiveContainer>
                  <div style={{display:'flex',flexDirection:'column',gap:8,flex:1}}>
                    {D.tiers.map(t=>{
                      const tc = TIER_COLORS[t.tier]||T.blue;
                      return (
                        <div key={t.tier} style={{display:'flex',alignItems:'center',gap:8}}>
                          <div style={{width:10,height:10,borderRadius:2,background:tc,flexShrink:0}}/>
                          <div style={{flex:1}}>
                            <div style={{fontSize:10.5,fontWeight:700,color:T.text}}>{t.tier}</div>
                            <div style={{fontSize:8.5,color:T.muted}}>{t.total} tickets / {t.avg_hours}h avg</div>
                          </div>
                          <span style={{fontSize:11,fontWeight:800,color:tc,fontFamily:"'IBM Plex Mono',monospace"}}>{t.rate}%</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
                {/* Legend */}
                <div style={{display:'flex',gap:14,justifyContent:'center',marginTop:8}}>
                  {TIER_ORDER.map(t=>(
                    <div key={t} style={{display:'flex',alignItems:'center',gap:4}}>
                      <div style={{width:10,height:10,borderRadius:2,background:TIER_COLORS[t]}}/>
                      <span style={{fontSize:8.5,fontWeight:600,color:T.muted}}>{t}</span>
                    </div>
                  ))}
                </div>
              </div>
            ) : <Empty T={T}/>}
          </Card>

          {/* Agent Efficiency Metrics */}
          <Card T={T} title="Agent Efficiency" subtitle="Conversation & response metrics" icon={IC.zap}>
            {D.efficiency.avg_msgs_per_ticket !== undefined ? (
              <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10}}>
                {[
                  {label:'Avg Messages/Ticket',value:D.efficiency.avg_msgs_per_ticket,unit:'',icon:IC.grid,color:K.navy},
                  {label:'Resolution Rate',value:D.efficiency.resolution_rate,unit:'%',icon:IC.check,color:K.royal},
                  {label:'Avg First Response',value:D.efficiency.avg_first_response_hrs,unit:'h',icon:IC.clock,color:K.royal},
                  {label:'Fastest Response',value:D.efficiency.fastest_response_hrs,unit:'h',icon:IC.zap,color:K.violet},
                ].map(m=>(
                  <div key={m.label} style={{padding:'12px 10px',borderRadius:10,background:T.surface2,
                    border:`1px solid ${T.border}`,textAlign:'center'}}>
                    <div style={{color:m.color,marginBottom:4,display:'flex',justifyContent:'center'}}>{m.icon}</div>
                    <div style={{fontSize:22,fontWeight:900,color:m.color,fontFamily:"'IBM Plex Mono',monospace",lineHeight:1}}>
                      {m.value}{m.unit}
                    </div>
                    <div style={{fontSize:8,fontWeight:600,color:T.muted,marginTop:4,textTransform:'uppercase',letterSpacing:0.5}}>{m.label}</div>
                  </div>
                ))}
                {/* AI vs Agent message split */}
                <div style={{gridColumn:'1 / -1',padding:'8px 12px',borderRadius:10,background:T.surface2,border:`1px solid ${T.border}`}}>
                  <div style={{fontSize:8.5,fontWeight:700,color:T.textSub,textTransform:'uppercase',marginBottom:6,letterSpacing:0.5}}>Message Split</div>
                  <div style={{display:'flex',height:10,borderRadius:5,overflow:'hidden'}}>
                    <div style={{width:`${D.efficiency.ai_msg_pct}%`,background:K.sky,transition:'width .5s ease'}}/>
                    <div style={{width:`${D.efficiency.agent_msg_pct}%`,background:K.navy,transition:'width .5s ease'}}/>
                  </div>
                  <div style={{display:'flex',justifyContent:'space-between',marginTop:4}}>
                    <span style={{fontSize:8,color:K.sky,fontWeight:700}}>AI {D.efficiency.ai_msg_pct}%</span>
                    <span style={{fontSize:8,color:K.navy,fontWeight:700}}>Agent {D.efficiency.agent_msg_pct}%</span>
                  </div>
                </div>
              </div>
            ) : <Empty T={T}/>}
          </Card>
        </div>

        {/* ═══ ROW 5: Monthly + Resolution Funnel + Hourly ═══════════════ */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:16,marginBottom:16}}>

          {/* Monthly Ticket Flow */}
          <Card T={T} title="Monthly Ticket Flow" subtitle="Created vs resolved per month">
            {D.monthly.length>0 ? (
              <div>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={D.monthly} margin={{top:10,right:5,bottom:0,left:0}} barGap={2} barCategoryGap="20%">
                    <CartesianGrid strokeDasharray="3 3" stroke={T.gridStroke} vertical={false}/>
                    <XAxis dataKey="month" tick={{fontSize:9,fill:T.muted,fontWeight:600}} axisLine={false} tickLine={false}/>
                    <YAxis tick={{fontSize:8,fill:T.muted}} axisLine={false} tickLine={false} width={25} allowDecimals={false}/>
                    <Tooltip content={({payload,label})=>{
                      if(!payload||!payload.length) return null;
                      const d=payload[0]?.payload;
                      return (
                        <div style={{background:T.surface,border:`1px solid ${T.border}`,borderRadius:8,padding:'8px 12px',boxShadow:'0 4px 12px rgba(0,0,0,.15)'}}>
                          <div style={{fontSize:10,fontWeight:700,color:T.muted,marginBottom:3}}>{label}</div>
                          <div style={{fontSize:10}}>Created: <strong style={{color:K.sky}}>{d?.created||0}</strong></div>
                          <div style={{fontSize:10}}>Resolved: <strong style={{color:K.navy}}>{d?.resolved||0}</strong></div>
                        </div>
                      );
                    }}/>
                    <Bar dataKey="created" name="Created" fill={K.sky} radius={[4,4,0,0]} barSize={20}/>
                    <Bar dataKey="resolved" name="Resolved" fill={K.navy} radius={[4,4,0,0]} barSize={20}/>
                  </BarChart>
                </ResponsiveContainer>
                <div style={{display:'flex',gap:12,justifyContent:'center',marginTop:4}}>
                  {[{label:'Created',color:K.sky},{label:'Resolved',color:K.navy}].map(l=>(
                    <div key={l.label} style={{display:'flex',alignItems:'center',gap:4}}>
                      <div style={{width:8,height:8,borderRadius:2,background:l.color}}/>
                      <span style={{fontSize:8,fontWeight:600,color:T.muted}}>{l.label}</span>
                    </div>
                  ))}
                </div>
              </div>
            ) : <Empty T={T}/>}
          </Card>

          {/* Resolution Funnel — SVG trapezoids */}
          <Card T={T} title="Resolution Funnel" subtitle="Conversation to resolution flow" icon={IC.trend}>
            {(()=>{
              const a = D.aiVsAgent;
              const stages = [
                {name:'Conversations', value:a.total_conversations||0, color:K.navy},
                {name:'AI Resolved', value:a.ai_resolved||0, color:K.royal},
                {name:'Escalated', value:a.escalated_to_agent||0, color:K.sky},
                {name:'Resolved', value:a.agent_resolved||0, color:K.indigo},
              ].filter(s=>s.value>0);
              if(stages.length===0) return <Empty T={T}/>;
              const maxV = stages[0].value || 1;
              const W = 260, segH = 42, gap = 2, cx = W/2;
              const totalH = stages.length * (segH + gap) + 20;
              // Width for each stage: proportional but min 20%
              const widths = stages.map(s => Math.max((s.value / maxV), 0.50) * W * 0.9);

              return (
                <div style={{display:'flex',flexDirection:'column',alignItems:'center'}}>
                  <svg viewBox={`0 0 ${W} ${totalH}`} width="100%" style={{display:'block',maxWidth:260}}>
                    {stages.map((s,i)=>{
                      const y = i * (segH + gap);
                      const topHalf = widths[i] / 2;
                      const botHalf = i < stages.length - 1 ? widths[i+1] / 2 : topHalf * 0.7;
                      return (
                        <g key={i}>
                          <polygon
                            points={`${cx-topHalf},${y} ${cx+topHalf},${y} ${cx+botHalf},${y+segH} ${cx-botHalf},${y+segH}`}
                            fill={s.color} stroke="#fff" strokeWidth="1.5"/>
                          <text x={cx} y={y+segH/2-6} textAnchor="middle" dominantBaseline="central"
                            fontSize="10" fontWeight="700" fill="#fff" fontFamily="'Plus Jakarta Sans',sans-serif">{s.name}</text>
                          <text x={cx} y={y+segH/2+8} textAnchor="middle" dominantBaseline="central"
                            fontSize="14" fontWeight="900" fill="#fff" fontFamily="'IBM Plex Mono',monospace">{s.value}</text>
                        </g>
                      );
                    })}
                  </svg>
                  <div style={{display:'flex',gap:10,marginTop:6,flexWrap:'wrap',justifyContent:'center'}}>
                    {stages.map(s=>(
                      <div key={s.name} style={{display:'flex',alignItems:'center',gap:3}}>
                        <div style={{width:8,height:8,borderRadius:2,background:s.color}}/>
                        <span style={{fontSize:7.5,fontWeight:600,color:T.muted}}>{s.name}</span>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })()}
          </Card>

          {/* Hourly Activity */}
          <Card T={T} title="Today's Hourly Activity" subtitle="Ticket inflow pattern today">
            {D.hourly.length>0 ? (
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={D.hourly} margin={{top:10,right:10,bottom:0,left:5}}>
                  <defs>
                    <linearGradient id="hArea" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={K.sky} stopOpacity=".4"/>
                      <stop offset="100%" stopColor={K.sky} stopOpacity=".02"/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={T.gridStroke} vertical={false}/>
                  <XAxis dataKey="hour" tick={{fontSize:8,fill:T.muted}} axisLine={false} tickLine={false} interval={2}/>
                  <YAxis tick={{fontSize:8.5,fill:T.muted}} axisLine={false} tickLine={false} width={20} allowDecimals={false}/>
                  <Tooltip content={TipC}/>
                  <Area type="monotone" dataKey="tickets" name="Tickets" fill="url(#hArea)" stroke={K.sky} strokeWidth={2.5}
                    dot={{r:3,fill:T.surface,stroke:K.sky,strokeWidth:2}}
                    activeDot={{r:5,fill:K.sky,stroke:T.surface,strokeWidth:2}}/>
                </AreaChart>
              </ResponsiveContainer>
            ) : <Empty T={T}/>}
          </Card>
        </div>

        {/* ═══ ROW 6: PREDICTIVE WORKLOAD FORECAST (UNIQUE) + Heatmap ═══ */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1.2fr',gap:16,marginBottom:16}}>

          {/* UNIQUE FEATURE: Predictive Workload Forecast */}
          <Card T={T} title="Predictive Workload Forecast" subtitle="AI-predicted ticket volume for next 7 days" icon={IC.trend}
            style={{background:dark?'linear-gradient(135deg,#0C1829,#151535)':'linear-gradient(135deg,#FFFFFF,#EFF6FF)',border:`1.5px solid ${T.blue}30`}}>
            {(data?.forecast||[]).length>0 ? (
              <div>
                <ResponsiveContainer width="100%" height={160}>
                  <ComposedChart data={data.forecast} margin={{top:10,right:10,bottom:0,left:-10}}>
                    <defs>
                      <linearGradient id="fArea" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor={K.violet} stopOpacity=".3"/>
                        <stop offset="100%" stopColor={K.violet} stopOpacity=".02"/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke={T.gridStroke} vertical={false}/>
                    <XAxis dataKey="day" tick={{fontSize:8.5,fill:T.muted,fontWeight:600}} axisLine={false} tickLine={false}/>
                    <YAxis tick={{fontSize:8,fill:T.muted}} axisLine={false} tickLine={false} width={25} allowDecimals={false}/>
                    <Tooltip content={TipC}/>
                    <Area type="monotone" dataKey="predicted" name="Predicted" fill="url(#fArea)" stroke={K.violet} strokeWidth={2.5}
                      dot={{r:4,fill:T.surface,stroke:K.violet,strokeWidth:2}}/>
                    <Line type="monotone" dataKey="capacity" name="Capacity" stroke={K.royal} strokeWidth={1.5} strokeDasharray="5 3" dot={false}/>
                  </ComposedChart>
                </ResponsiveContainer>
                {/* Burndown stats */}
                {data?.burndown && (
                  <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:8,marginTop:8}}>
                    <div style={{textAlign:'center',padding:'6px 4px',borderRadius:8,background:T.surface2}}>
                      <div style={{fontSize:7.5,color:T.muted,fontWeight:600,textTransform:'uppercase'}}>Current Rate</div>
                      <div style={{fontSize:14,fontWeight:800,color:T.blue,fontFamily:"'IBM Plex Mono',monospace"}}>{data.burndown.current_rate}%</div>
                    </div>
                    <div style={{textAlign:'center',padding:'6px 4px',borderRadius:8,background:T.surface2}}>
                      <div style={{fontSize:7.5,color:T.muted,fontWeight:600,textTransform:'uppercase'}}>Target</div>
                      <div style={{fontSize:14,fontWeight:800,color:K.royal,fontFamily:"'IBM Plex Mono',monospace"}}>{data.burndown.target_rate}%</div>
                    </div>
                    <div style={{textAlign:'center',padding:'6px 4px',borderRadius:8,background:data.burndown.needed>0?(K.indigo+'18'):K.royal+'18'}}>
                      <div style={{fontSize:7.5,color:T.muted,fontWeight:600,textTransform:'uppercase'}}>To Target</div>
                      <div style={{fontSize:14,fontWeight:800,color:data.burndown.needed>0?K.indigo:K.royal,fontFamily:"'IBM Plex Mono',monospace"}}>
                        {data.burndown.needed>0?`+${data.burndown.needed}`:'\u2713'}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ) : <Empty T={T}/>}
          </Card>

          {/* Weekly Heatmap */}
          <Card T={T} title="Weekly Activity Heatmap" subtitle="Your productivity pattern by day and hour">
            <Heatmap data={D.heatmap} T={T}/>
          </Card>
        </div>

        {/* ═══ AGING ALERT ════════════════════════════════════════════════ */}
        {(D.kpis.avg_aging_hours??0)>48 && (
          <div style={{
            display:'flex',alignItems:'center',gap:14,padding:'14px 20px',borderRadius:10,
            background:dark?'#7f1d1d22':'#fef2f2',border:`1px solid ${dark?'#991b1b44':'#fecaca'}`,
            animation:'fadeUp .4s ease',
          }}>
            <span style={{color:T.red,flexShrink:0}}>{IC.alert}</span>
            <div>
              <div style={{fontSize:13,fontWeight:700,color:T.red,marginBottom:2}}>High Aging Alert</div>
              <div style={{fontSize:12,color:dark?'#fca5a5':'#b91c1c'}}>
                Average open ticket age is <strong>{D.kpis.avg_aging_hours} hrs</strong>. Prioritize older tickets to maintain SLA compliance.
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Section Label ───────────────────────────────────────────────────────── */
function Sec({ T, children }) {
  return (
    <div style={{fontSize:9.5,fontWeight:700,color:T.muted,textTransform:'uppercase',letterSpacing:1.2,marginBottom:10,paddingLeft:2,
      display:'flex',alignItems:'center',gap:8}}>
      <div style={{width:3,height:14,borderRadius:2,background:K.navy}}/>
      {children}
    </div>
  );
}
