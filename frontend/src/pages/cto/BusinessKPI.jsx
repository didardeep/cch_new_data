import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import {
  Activity, AlertTriangle, DollarSign,
  MapPin, RefreshCw, Scissors, TrendingDown, TrendingUp,
  UserMinus, Users, Zap,
} from 'lucide-react';
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid,
  Cell, ReferenceLine, ReferenceArea, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiGet } from '../../api';

/* ─── Currency & number formatters ─────────────────────────── */
const fmtUsd = (v) => {
  const n = Number(v) || 0;
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000)     return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};
const fmtUsdFull = (v) =>
  `$${Number(v || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
const fmtInt = (v) => Math.round(Number(v) || 0).toLocaleString();
const fmtUsdPrecise = (v) => {
  const n = Number(v) || 0;
  const a = Math.abs(n);
  if (a >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (a >= 1_000)     return `$${(n / 1_000).toFixed(2)}K`;
  if (a >= 1)         return `$${n.toFixed(2)}`;
  if (a >= 0.01)      return `$${n.toFixed(3)}`;
  if (a >= 0.0001)    return `$${n.toFixed(5)}`;
  return `$${n.toFixed(6)}`;
};

/* ═══════════════════════════════════════════════════════════
   DATA COMPUTATION — all KPI logic lives here so it is
   easy to update when more months land in the API response.

   Expected API shape  (/api/cto/business-kpi):
   {
     // Revenue data (one row per site sector / cell)
     revenue_rows: [
       {
         site_id,        // e.g. "KSP0311TPO"
         site_abs_id,    // numeric id
         province,
         commune,
         utilization,    // e.g. 98  (integer %)
         opex,           // total OPEX for this site row
         monthly_revenue: {
           // keys are canonical month labels like "Feb" / "Mar" / "Apr" ...
           "Feb": 1071.3375,
           "Mar": 563.7125,
           // more months added here dynamically in future
         },
       },
       ...
     ],

     // Users data (one row per site sector / cell)
     users_rows: [
       {
         site_id,
         monthly_total_users: {
           // keys match revenue month labels
           "Feb": 811,
           "Mar": 831,
         },
       },
       ...
     ],
   }

   The function below derives every KPI card value and every
   chart series from these two arrays, regardless of how many
   months are present, by:
     1. Discovering the ordered list of months from the union
        of keys across all rows.
     2. Aggregating per-site (de-duplicating repeated sector rows
        by summing monthly values per unique site_id).
     3. Using the LAST month as "current" and the SECOND-TO-LAST
        as "previous" for growth / churn / risk calculations.

   ═══════════════════════════════════════════════════════════ */

/**
 * Canonical month ordering used to sort month keys
 * regardless of what the backend sends.
 */
const MONTH_ORDER = [
  'Jan','Feb','Mar','Apr','May','Jun',
  'Jul','Aug','Sep','Oct','Nov','Dec',
];

function sortMonths(keys) {
  return [...new Set(keys)].sort(
    (a, b) => MONTH_ORDER.indexOf(a) - MONTH_ORDER.indexOf(b),
  );
}

/**
 * Aggregate raw revenue_rows → Map<site_id, { monthly_revenue, utilization, opex }>
 * Multiple sector rows for the same site are SUMMED.
 */
function aggregateRevenueBySite(revenueRows) {
  const map = new Map();
  for (const row of revenueRows) {
    const sid = row.site_id;
    if (!map.has(sid)) {
      map.set(sid, {
        site_id: sid,
        utilization: Number(row.utilization) || 0,
        opex: Number(row.opex) || 0,
        total_revenue: Number(row.total_revenue) || 0,
        monthly_revenue: {},
      });
    }
    const entry = map.get(sid);
    // Site-level values: MAX (they are the same across sectors)
    entry.utilization = Math.max(entry.utilization, Number(row.utilization) || 0);
    entry.opex = Math.max(entry.opex, Number(row.opex) || 0);
    entry.total_revenue = Math.max(entry.total_revenue, Number(row.total_revenue) || 0);
    // Monthly revenue: MAX (site-level totals repeated per sector)
    for (const [month, val] of Object.entries(row.monthly_revenue || {})) {
      entry.monthly_revenue[month] = Math.max(
        entry.monthly_revenue[month] || 0,
        Number(val || 0)
      );
    }
  }
  return map; // Map<site_id, SiteRevenue>
}

/**
 * Aggregate raw users_rows → Map<site_id, { monthly_total_users }>
 * Multiple sector rows for the same site are SUMMED.
 */
function aggregateUsersBySite(usersRows) {
  const map = new Map();
  for (const row of usersRows) {
    const sid = row.site_id;
    if (!map.has(sid)) {
      map.set(sid, { site_id: sid, monthly_total_users: {} });
    }
    const entry = map.get(sid);
    // Monthly total users: MAX (site-level totals repeated per sector)
    for (const [month, val] of Object.entries(row.monthly_total_users || {})) {
      entry.monthly_total_users[month] = Math.max(
        entry.monthly_total_users[month] || 0,
        Number(val || 0)
      );
    }
  }
  return map;
}

/**
 * Main derivation function — returns everything the UI needs.
 */
function deriveKpis(apiData) {
  const revenueRows = apiData?.revenue_rows || [];
  const usersRows   = apiData?.users_rows   || [];

  // ── 1. Discover all months (union of both datasets) ──────
  const allMonthKeys = new Set();
  for (const row of revenueRows) {
    Object.keys(row.monthly_revenue || {}).forEach(k => allMonthKeys.add(k));
  }
  for (const row of usersRows) {
    Object.keys(row.monthly_total_users || {}).forEach(k => allMonthKeys.add(k));
  }
  const months = sortMonths([...allMonthKeys]); // e.g. ['Feb','Mar']

  if (months.length === 0) {
    // No data at all — return safe zeroes
    return {
      months,
      summary: {},
      trend: [],
      site_health: [],
      top_sites: [],
      low_margin_sites: [],
      overloaded_sites: [],
    };
  }

  const currentMonth  = months[months.length - 1];
  const previousMonth = months.length >= 2 ? months[months.length - 2] : null;

  // ── 2. Aggregate per unique site ─────────────────────────
  const revMap   = aggregateRevenueBySite(revenueRows);
  const usersMap = aggregateUsersBySite(usersRows);

  // Build combined site list (union of both maps)
  const allSiteIds = new Set([...revMap.keys(), ...usersMap.keys()]);

  // ── 3. KPI Card computations ─────────────────────────────

  // Total Users (current month)
  // Formula: Σ monthly_total_users[currentMonth] across all sites
  let totalUsersCurrent  = 0;
  let totalUsersPrevious = 0;
  for (const [, u] of usersMap) {
    totalUsersCurrent  += u.monthly_total_users[currentMonth]  || 0;
    totalUsersPrevious += previousMonth
      ? (u.monthly_total_users[previousMonth] || 0)
      : 0;
  }

  // Total Revenue (current month) = Σ monthly_revenue[currentMonth]
  let totalRevenueCurrent  = 0;
  let totalRevenuePrevious = 0;
  let totalOpex            = 0;
  for (const [, r] of revMap) {
    totalRevenueCurrent  += r.monthly_revenue[currentMonth]  || 0;
    totalRevenuePrevious += previousMonth
      ? (r.monthly_revenue[previousMonth] || 0)
      : 0;
    totalOpex += r.opex || 0;
  }

  // Number of unique sites (sites that appear in revenue data)
  const numSites = revMap.size || 1; // avoid /0

  // Avg Users per Site = Total Users / Number of Sites
  const avgUsers = totalUsersCurrent / numSites;

  // Growth % — computed on Revenue (consistent choice; labelled "Revenue Growth")
  // Growth = ((current - previous) / previous) * 100
  const revenueGrowth = previousMonth && totalRevenuePrevious > 0
    ? ((totalRevenueCurrent - totalRevenuePrevious) / totalRevenuePrevious) * 100
    : 0;

  // User Growth %
  const userGrowth = previousMonth && totalUsersPrevious > 0
    ? ((totalUsersCurrent - totalUsersPrevious) / totalUsersPrevious) * 100
    : 0;

  // ARPU = Total Revenue / Total Users (current month)
  const arpu = totalUsersCurrent > 0 ? totalRevenueCurrent / totalUsersCurrent : 0;

  // Revenue at Risk = Σ(prev - current) for sites where current < previous
  let revenueAtRisk = 0;
  if (previousMonth) {
    for (const [, r] of revMap) {
      const curr = r.monthly_revenue[currentMonth]  || 0;
      const prev = r.monthly_revenue[previousMonth] || 0;
      if (curr < prev) revenueAtRisk += (prev - curr);
    }
  }

  // Churn Rate (user-based) = (prev - current) / prev * 100   [if positive = churn]
  const churnRate = previousMonth && totalUsersPrevious > 0
    ? Math.max(0, ((totalUsersPrevious - totalUsersCurrent) / totalUsersPrevious) * 100)
    : 0;

  // Network ROI = (Total Revenue column - OPEX) / OPEX * 100
  // Uses the "Total Revenue" column from revenue file (overall total, not monthly sums)
  let overallTotalRev = 0;
  for (const [, r] of revMap) {
    overallTotalRev += r.total_revenue || 0;
  }
  const networkRoi = totalOpex > 0
    ? ((overallTotalRev - totalOpex) / totalOpex) * 100
    : 0;

  // ── 4. Per-site health scores ─────────────────────────────
  // STRICT: only include sites where ALL THREE are present and non-zero:
  //   • utilization  (from revenue file)
  //   • users        (from business export)
  //   • revenue      (from revenue file, current month)
  //
  // Formula: Site Health Score = 0.6 × RevScore + 0.2 × UsrScore + 0.2 × UtilScore
  //   where each component is mapped [-100%,+100%] growth → [0,100] score.
  //   Utilization is already a %, used directly as UtilScore [0–100].
  const W_REV  = 0.6;
  const W_USR  = 0.2;
  const W_UTIL = 0.2;

  const siteHealthList = [];

  for (const sid of allSiteIds) {
    const r = revMap.get(sid);
    const u = usersMap.get(sid);

    const revCurr  = r ? (r.monthly_revenue[currentMonth]  || 0) : 0;
    const revPrev  = r ? (r.monthly_revenue[previousMonth] || 0) : 0;
    const usrCurr  = u ? (u.monthly_total_users[currentMonth]  || 0) : 0;
    const usrPrev  = u ? (u.monthly_total_users[previousMonth] || 0) : 0;
    const util     = r ? (r.utilization || 0) : 0;

    // STRICT completeness gate — skip site if any dimension is zero/missing
    if (util <= 0 || usrCurr <= 0 || revCurr <= 0) continue;

    // Growth components (clamped to [-100, +100] to prevent outlier dominance)
    const siteRevGrowth = previousMonth && revPrev > 0
      ? Math.max(-100, Math.min(100, ((revCurr - revPrev) / revPrev) * 100))
      : 0;
    const siteUsrGrowth = previousMonth && usrPrev > 0
      ? Math.max(-100, Math.min(100, ((usrCurr - usrPrev) / usrPrev) * 100))
      : 0;

    // Map growth [-100, +100] → [0, 100]
    const revScore  = (siteRevGrowth + 100) / 2;
    const usrScore  = (siteUsrGrowth + 100) / 2;
    const utilScore = Math.min(100, Math.max(0, util));

    const healthScore = Math.round(
      W_REV * revScore + W_USR * usrScore + W_UTIL * utilScore
    );

    siteHealthList.push({
      site_id:      sid,
      health_score: healthScore,
      utilization:  util,
      users:        usrCurr,
      revenue:      revCurr,
    });
  }

  // Avg Site Health = Σ healthScore / eligible sites (not total sites)
  const avgHealthScore = siteHealthList.length > 0
    ? siteHealthList.reduce((s, x) => s + x.health_score, 0) / siteHealthList.length
    : 0;

  // Worst 10 sites by health score
  const worst10Health = [...siteHealthList]
    .sort((a, b) => a.health_score - b.health_score)
    .slice(0, 10);

  // ── 5. Top 10 sites by current revenue ───────────────────
  const top10Revenue = [...revMap.values()]
    .map(r => ({
      site_id: r.site_id,
      revenue: r.monthly_revenue[currentMonth] || 0,
    }))
    .filter(r => r.revenue > 0)
    .sort((a, b) => b.revenue - a.revenue)
    .slice(0, 10);

  // ── 6. Low Margin Sites ───────────────────────────────────
  // Pre-computed server-side (backend enforces all-three-present rule).
  // Just pass through directly from API — no re-computation needed.
  const lowMarginSites = apiData?.low_margin_sites || [];

  // ── 7. Overloaded Sites ───────────────────────────────────
  // Pre-computed server-side (backend enforces util+revenue-present rule).
  const overloadedSites = apiData?.overloaded_sites || [];

  // ── 8. Monthly trend series ───────────────────────────────
  // trend[]: { date: "Feb", users: Σ, revenue: Σ, arpu: revenue/users }
  const trend = months.map(month => {
    let usersSum   = 0;
    let revenueSum = 0;
    for (const [, u] of usersMap) {
      usersSum += u.monthly_total_users[month] || 0;
    }
    for (const [, r] of revMap) {
      revenueSum += r.monthly_revenue[month] || 0;
    }
    return {
      date:    month,
      users:   Math.round(usersSum),
      revenue: revenueSum,
      arpu:    usersSum > 0 ? revenueSum / usersSum : 0,
    };
  });

  return {
    months,
    currentMonth,
    previousMonth,
    summary: {
      total_users:      Math.round(totalUsersCurrent),
      avg_users:        avgUsers,
      revenue_growth:   revenueGrowth,
      user_growth:      userGrowth,
      arpu,
      revenue_at_risk:  revenueAtRisk,
      churn_rate:       churnRate,
      network_roi:      networkRoi,
      avg_health_score: avgHealthScore,
      total_revenue:    totalRevenueCurrent,
      total_opex:       totalOpex,
    },
    trend,                        // Users Trend, Revenue Trend, ARPU Trend charts
    top_sites:       top10Revenue,
    site_health:     worst10Health,
    low_margin_sites: lowMarginSites,   // replaces declining_sites
    overloaded_sites: overloadedSites,
  };
}

/* ═══════════════════════════════════════════════════════════
   DESIGN TOKENS
   ═══════════════════════════════════════════════════════════ */
const G = {
  indigo:  ['#6366f1', '#8b5cf6'],
  violet:  ['#7c3aed', '#9333ea'],
  emerald: ['#10b981', '#06b6d4'],
  amber:   ['#f59e0b', '#f97316'],
  rose:    ['#ef4444', '#ec4899'],
  teal:    ['#0d9488', '#06b6d4'],
  health:  (v) => v < 40 ? ['#ef4444', '#ec4899'] : v < 70 ? ['#f59e0b', '#f97316'] : ['#10b981', '#06b6d4'],
  glow:    (from, opacity = 0.22) =>
    `0 8px 32px ${from}${Math.round(opacity * 255).toString(16).padStart(2, '0')}`,
};

const linear = (colors) => `linear-gradient(135deg, ${colors[0]}, ${colors[1]})`;

const glassCard = {
  background: 'var(--bg-card)',
  border:      '1px solid var(--border)',
  borderRadius: 16,
  boxShadow:   'var(--shadow-md), inset 0 1px 0 rgba(255,255,255,0.5)',
};

const FADE_UP = (delay = 0) => ({
  initial:    { opacity: 0, y: 24 },
  animate:    { opacity: 1, y: 0 },
  transition: { duration: 0.45, delay, ease: [0.22, 1, 0.36, 1] },
});

/* ─── Custom chart tooltip ────────────────────────────────── */
function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: 'var(--bg-card)',
      border: '1px solid var(--border)',
      borderRadius: 12,
      boxShadow: '0 16px 40px rgba(0,0,0,0.12), inset 0 1px 0 rgba(255,255,255,0.5)',
      padding: '10px 14px',
      fontSize: 12,
      minWidth: 140,
    }}>
      <p style={{ color: 'var(--text-muted)', marginBottom: 8, fontWeight: 700, fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label}</p>
      {payload.map((p) => {
        const key = (p.name || p.dataKey || '').toLowerCase();
        const isArpu   = /arpu/.test(key);
        const isMoney  = /revenue|opex/.test(key) || isArpu;
        const display  = typeof p.value === 'number'
          ? (isArpu ? fmtUsdPrecise(p.value) : isMoney ? fmtUsdFull(p.value) : p.value.toLocaleString())
          : p.value;
        return (
          <div key={p.name} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 4 }}>
            <span style={{ width: 8, height: 8, borderRadius: '50%', background: p.color, flexShrink: 0, boxShadow: `0 0 6px ${p.color}60` }} />
            <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{p.name}:</span>
            <span style={{ color: 'var(--text)', fontWeight: 800, marginLeft: 'auto', paddingLeft: 8 }}>{display}</span>
          </div>
        );
      })}
    </div>
  );
}

/* ─── Gradient Pill Badge ─────────────────────────────────── */
function Pill({ value, suffix = '', positive, negative, neutral }) {
  let bg, color;
  if (positive)      { bg = 'rgba(16,185,129,0.13)';  color = '#10b981'; }
  else if (negative) { bg = 'rgba(239,68,68,0.13)';   color = '#ef4444'; }
  else if (neutral)  { bg = 'rgba(245,158,11,0.13)';  color = '#f59e0b'; }
  else               { bg = 'rgba(148,163,184,0.12)'; color = 'var(--text-muted)'; }
  return (
    <span style={{
      background: bg, color,
      borderRadius: 999, padding: '3px 9px',
      fontSize: 11, fontWeight: 800, whiteSpace: 'nowrap',
      border: `1px solid ${color}28`,
    }}>
      {value}{suffix}
    </span>
  );
}

/* ─── Gradient Progress Bar ───────────────────────────────── */
function ProgressBar({ value, max = 100, colors }) {
  const pct  = Math.min(100, Math.max(0, (value / max) * 100));
  const grad = colors ? linear(colors) : linear(G.emerald);
  return (
    <div style={{ background: 'var(--border)', borderRadius: 6, height: 6, width: 84, overflow: 'hidden' }}>
      <div style={{
        width: `${pct}%`, height: '100%',
        background: grad, borderRadius: 6,
        transition: 'width 0.7s cubic-bezier(0.22,1,0.36,1)',
        boxShadow: `0 0 8px ${(colors || G.emerald)[0]}60`,
      }} />
    </div>
  );
}

/* ─── KPI Card ────────────────────────────────────────────── */
function KpiCard({ label, value, sub, colors, icon: Icon, delay = 0 }) {
  const [from] = colors;
  return (
    <motion.div
      {...FADE_UP(delay)}
      whileHover={{ scale: 1.035, transition: { duration: 0.2 } }}
      style={{ ...glassCard, overflow: 'hidden', cursor: 'default' }}
    >
      <div style={{ height: 3, background: linear(colors), borderRadius: '16px 16px 0 0' }} />
      <div style={{ padding: '18px 20px 20px' }}>
        <div style={{ position: 'relative', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 14 }}>
          <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-muted)' }}>{label}</span>
          <div style={{ background: `${from}18`, borderRadius: 10, padding: 9, flexShrink: 0, boxShadow: `0 0 12px ${from}30` }}>
            <Icon size={18} color={from} strokeWidth={2.2} />
          </div>
        </div>
        <div style={{
          fontSize: 32, fontWeight: 900, lineHeight: 1,
          background: linear(colors),
          WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text',
          marginBottom: 6,
        }}>{value}</div>
        <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>{sub}</div>
      </div>
    </motion.div>
  );
}

/* ─── Section wrapper ─────────────────────────────────────── */
function Section({ title, children, delay = 0, style, accentColor }) {
  return (
    <motion.div {...FADE_UP(delay)} style={{ ...glassCard, overflow: 'hidden', ...style }}>
      <div style={{
        padding: '15px 20px 13px',
        borderBottom: '1px solid var(--border)',
        display: 'flex', alignItems: 'center', gap: 10,
        background: 'var(--bg-card)',
      }}>
        {accentColor && (
          <div style={{ width: 3, height: 16, borderRadius: 2, background: linear(accentColor), flexShrink: 0 }} />
        )}
        <h3 style={{ margin: 0, fontSize: 13, fontWeight: 700, color: 'var(--text)', letterSpacing: '-0.01em' }}>{title}</h3>
      </div>
      <div style={{ padding: '16px 20px' }}>{children}</div>
    </motion.div>
  );
}

/* ─── Premium Table ───────────────────────────────────────── */
function PremiumTable({ cols, rows, emptyText = 'No data' }) {
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
      <thead>
        <tr style={{ background: 'var(--bg)' }}>
          {cols.map((c) => (
            <th key={c.key} style={{
              padding: '8px 8px 8px 0', textAlign: 'left',
              fontSize: 9.5, fontWeight: 700, textTransform: 'uppercase',
              letterSpacing: '0.07em', color: 'var(--text-muted)', whiteSpace: 'nowrap',
            }}>{c.label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 && (
          <tr>
            <td colSpan={cols.length} style={{ padding: '28px 0', textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>{emptyText}</td>
          </tr>
        )}
        {rows.map((row, i) => (
          <tr
            key={i}
            style={{ borderTop: '1px solid var(--border)', transition: 'background 0.15s' }}
            onMouseEnter={e => { e.currentTarget.style.background = 'rgba(0,51,141,0.05)'; }}
            onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}
          >
            {cols.map((c) => (
              <td key={c.key} style={{ padding: '10px 8px 10px 0', color: 'var(--text)', ...c.cellStyle }}>
                {c.render ? c.render(row[c.key], row) : row[c.key]}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/* ─── Gradient defs (static — defined once outside render) ── */
function GradientDefs() {
  return (
    <defs>
      <linearGradient id="usersGrad"   x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stopColor="#6366f1" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#6366f1" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="revenueGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stopColor="#10b981" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#10b981" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="arpuGrad"    x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stopColor="#f59e0b" stopOpacity={0.35} />
        <stop offset="100%" stopColor="#f59e0b" stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="barGrad"     x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"   stopColor="#6366f1" />
        <stop offset="100%" stopColor="#8b5cf6" />
      </linearGradient>
    </defs>
  );
}

/* ═══════════════════════════════════════════════════════════
   MAIN COMPONENT
   ═══════════════════════════════════════════════════════════ */
export default function BusinessKPI() {
  const [rawData,    setRawData]    = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = () => {
    setRefreshing(true);
    apiGet('/api/cto/business-kpi')
      .then((resp) => { setRawData(resp); setRefreshing(false); })
      .catch(()    => { setRefreshing(false); });
  };

  useEffect(() => { load(); }, []);

  if (!rawData) return <div className="page-loader"><div className="spinner" /></div>;

  // ── Derive all KPIs from raw API data ────────────────────
  // deriveKpis() computes trend charts, site-health, declining / overloaded
  // tables from the raw revenue_rows + users_rows arrays.
  // For the KPI card header values (total_revenue, total_users, arpu,
  // current_month, period_label) we prefer the backend-pre-computed
  // summary_kpis because it uses the same column-detection logic that
  // built the rows — preventing "Feb Total" vs "Total Revenue" confusion.
  const kpi      = deriveKpis(rawData);
  const summary  = kpi.summary;
  const trend    = kpi.trend;           // [{date, users, revenue, arpu}, ...]

  // Backend authoritative summary (falls back to frontend derivation if absent)
  const beSummary = rawData?.summary_kpis || {};

  // KPI card values — backend wins when present, frontend derivation as fallback
  const currentMonth  = beSummary.current_month  || kpi.currentMonth  || '';
  const previousMonth = beSummary.previous_month || kpi.previousMonth || null;
  const periodLabel   = beSummary.period_label   ||
    (previousMonth && currentMonth ? `${previousMonth} → ${currentMonth}` : currentMonth);

  // Revenue / users / ARPU — backend authoritative values
  const totalUsers     = beSummary.total_users     ?? summary.total_users     ?? 0;
  const avgUsersPerSite = beSummary.avg_users_per_site ?? summary.avg_users    ?? 0;
  const arpuValue      = beSummary.arpu            ?? summary.arpu            ?? 0;

  const revGrowth      = beSummary.revenue_growth  ?? summary.revenue_growth  ?? 0;
  const revenueAtRisk  = beSummary.revenue_at_risk ?? summary.revenue_at_risk ?? 0;
  const churn          = beSummary.churn_rate       ?? summary.churn_rate      ?? 0;
  // ROI uses "Total Revenue" column vs OPEX (not monthly sums)
  const roi            = beSummary.network_roi     ?? summary.network_roi     ?? 0;
  const health         = summary.avg_health_score  || 0;
  const overallTotalRev  = beSummary.overall_total_revenue ?? 0;
  const overallTotalOpex = beSummary.overall_total_opex    ?? 0;

  /* ─── Business KPI Thresholds & Insights ─────────────────── */
  // Revenue: flag if drops below previous month average by >10%
  const revValues = trend.map(t => t.revenue).filter(v => v > 0);
  const revAvg = revValues.length ? revValues.reduce((a, b) => a + b, 0) / revValues.length : 0;
  const revThreshold = revAvg > 0 ? revAvg * 0.9 : null; // 10% below average

  // Users: flag if drops below previous month
  const userValues = trend.map(t => t.users).filter(v => v > 0);
  const userAvg = userValues.length ? userValues.reduce((a, b) => a + b, 0) / userValues.length : 0;
  const userThreshold = userAvg > 0 ? userAvg * 0.9 : null;

  // ARPU threshold
  const arpuValues = trend.map(t => t.arpu).filter(v => v > 0);
  const arpuAvg = arpuValues.length ? arpuValues.reduce((a, b) => a + b, 0) / arpuValues.length : 0;
  const arpuThreshold = arpuAvg > 0 ? arpuAvg * 0.85 : null;

  function bizInsight(series, key, threshold, label) {
    if (!series.length || !threshold) return null;
    const vals = series.map(d => d[key]).filter(v => v > 0);
    if (!vals.length) return null;
    const latest = vals[vals.length - 1];
    const breachCount = vals.filter(v => v < threshold).length;
    if (breachCount === 0) return { text: `${label}: Consistently above threshold. Performance is healthy.`, severity: 'good' };
    const pct = Math.round((breachCount / vals.length) * 100);
    if (latest < threshold) return { text: `${label}: Current value below threshold. ${pct}% of periods underperforming. Review needed for decision-making.`, severity: 'critical' };
    return { text: `${label}: ${breachCount} period(s) below threshold (${pct}%). Currently recovered. Monitor trend for sustainability.`, severity: 'warning' };
  }

  // Find breach regions for business charts
  function bizBreachRegions(series, key, threshold) {
    if (!threshold) return [];
    const regions = [];
    let start = null;
    for (let i = 0; i < series.length; i++) {
      const v = series[i][key];
      if (v > 0 && v < threshold && start === null) start = i;
      else if ((v >= threshold || v <= 0) && start !== null) {
        regions.push({ x1: series[start].date, x2: series[i - 1].date });
        start = null;
      }
    }
    if (start !== null) regions.push({ x1: series[start].date, x2: series[series.length - 1].date });
    return regions;
  }

  const revInsight = bizInsight(trend, 'revenue', revThreshold, 'Revenue');
  const userInsight = bizInsight(trend, 'users', userThreshold, 'Users');
  const arpuInsight = bizInsight(trend, 'arpu', arpuThreshold, 'ARPU');
  const revBreaches = bizBreachRegions(trend, 'revenue', revThreshold);
  const userBreaches = bizBreachRegions(trend, 'users', userThreshold);
  const arpuBreaches = bizBreachRegions(trend, 'arpu', arpuThreshold);

  const InsightBox = ({ insight }) => {
    if (!insight) return null;
    const bg = insight.severity === 'critical' ? 'rgba(239,68,68,0.08)' : insight.severity === 'warning' ? 'rgba(245,158,11,0.08)' : 'rgba(16,185,129,0.08)';
    const color = insight.severity === 'critical' ? '#dc2626' : insight.severity === 'warning' ? '#b45309' : '#059669';
    const border = insight.severity === 'critical' ? '#fecaca' : insight.severity === 'warning' ? '#fed7aa' : '#bbf7d0';
    const icon = insight.severity === 'critical' ? '⚠' : insight.severity === 'warning' ? '⚡' : '✓';
    return (
      <div style={{ margin: '8px 0 0', padding: '8px 14px', borderRadius: 8, fontSize: 11, fontWeight: 600, lineHeight: 1.5, background: bg, color, border: `1px solid ${border}` }}>
        {icon} {insight.text}
      </div>
    );
  };

  return (
    <div style={{ padding: '0 0 40px' }}>

      {/* ── Page Header ──────────────────────────────────────── */}
      <motion.div {...FADE_UP(0)} style={{
        display: 'flex', alignItems: 'flex-start',
        justifyContent: 'space-between', marginBottom: 28,
        flexWrap: 'wrap', gap: 12,
      }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 26, fontWeight: 900, letterSpacing: '-0.02em', color: 'var(--text)' }}>
            Business KPI
          </h1>
          <p style={{ margin: '5px 0 0', fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.5 }}>
            Commercial impact — users from Business Export, revenue from Revenue KPI &nbsp;|&nbsp;
            Period: <strong style={{ color: 'var(--text)' }}>{periodLabel}</strong>
          </p>
        </div>
        <button onClick={load} disabled={refreshing} style={{
          display: 'flex', alignItems: 'center', gap: 6,
          padding: '8px 16px', background: 'var(--bg-card)',
          border: '1px solid var(--border)', borderRadius: 10,
          cursor: 'pointer', fontSize: 13, fontWeight: 600,
          color: 'var(--text)', boxShadow: 'var(--shadow-sm)',
          transition: 'all 0.2s',
        }}>
          <RefreshCw size={14} style={{ animation: refreshing ? 'spin 1s linear infinite' : 'none' }} />
          Refresh
        </button>
      </motion.div>

      {/* ── KPI Cards (4-column) ─────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 16, marginBottom: 24 }}>

        {/* 1. Total Users — backend total_users for currentMonth */}
        <KpiCard
          label="Total Users"
          value={fmtInt(totalUsers)}
          sub={`${currentMonth} total — across all sites`}
          colors={G.indigo} icon={Users} delay={0.05}
        />

        {/* 2. Avg Users per Site — backend avg_users_per_site */}
        <KpiCard
          label="Avg Users / Site"
          value={fmtInt(avgUsersPerSite)}
          sub={`${currentMonth} total users ÷ site count`}

          colors={G.violet} icon={MapPin} delay={0.10}
        />

        {/* 3. Revenue Growth % — backend revenue_growth (MoM) */}
        <KpiCard
          label="Revenue Growth %"
          value={`${Number(revGrowth).toFixed(2)}%`}
          sub={`${previousMonth || '—'} → ${currentMonth} month-over-month`}
          colors={revGrowth >= 0 ? G.emerald : G.rose}
          icon={revGrowth >= 0 ? TrendingUp : TrendingDown}
          delay={0.15}
        />

        {/* 4. ARPU — backend arpu = total_revenue / total_users (same month column) */}
        <KpiCard
          label="ARPU"
          value={fmtUsdPrecise(arpuValue)}
          sub={`${currentMonth} total revenue ÷ ${currentMonth} total users`}
          colors={G.amber} icon={DollarSign} delay={0.20}
        />

        {/* 5. Revenue at Risk = Σ(prev - current) where current < prev */}
        <KpiCard
          label="Revenue At Risk"
          value={fmtUsd(revenueAtRisk)}
          sub={`Σ(${previousMonth || 'prev'} − ${currentMonth}) where ${currentMonth} < ${previousMonth || 'prev'}`}
          colors={G.amber} icon={AlertTriangle} delay={0.25}
        />

        {/* 6. Churn Rate = (Users(prev) - Users(current)) / Users(prev) × 100 */}
        <KpiCard
          label="Churn Rate"
          value={`${Number(churn).toFixed(1)}%`}
          sub={`(${previousMonth || 'prev'} users − ${currentMonth} users) / ${previousMonth || 'prev'} users × 100`}
          colors={churn > 20 ? G.rose : G.amber}
          icon={UserMinus} delay={0.30}
        />

        {/* 7. Network ROI = (Total Revenue − OPEX) / OPEX × 100 */}
        <KpiCard
          label="Network ROI"
          value={`${Number(roi).toFixed(1)}%`}
          sub={`(Total Rev ${fmtUsd(overallTotalRev)} − OPEX ${fmtUsd(overallTotalOpex)}) / OPEX × 100`}
          colors={roi >= 0 ? G.emerald : G.rose}
          icon={Zap} delay={0.35}
        />

        {/* 8. Avg Site Health */}
        <KpiCard
          label="Avg Site Health"
          value={Number(health).toFixed(1)}
          sub="Composite score /100 (Rev 60%, Users 20%, Util 20%)"
          colors={G.health(health)} icon={Activity} delay={0.40}
        />
      </div>

      {/* ── Users Trend & Revenue Trend ──────────────────────── */}
      {/* X-axis: months (e.g. Feb, Mar, Apr...)                  */}
      {/* Y-axis: Σ total users / Σ total revenue for that month  */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 20 }}>

        <Section title="Users Trend — Monthly Total Users" delay={0.45} accentColor={G.indigo}>
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={trend} margin={{ top: 6, right: 16, left: 8, bottom: 0 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis
                tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                domain={[
                  (dataMin) => Math.max(0, Math.floor(dataMin * 0.9)),
                  (dataMax) => Math.ceil(dataMax * 1.1),
                ]}
                tickFormatter={fmtInt}
                allowDecimals={false} width={72}
              />
              <Tooltip content={<ChartTooltip />} />
              {userBreaches.map((b, i) => (
                <ReferenceArea key={`ub-${i}`} x1={b.x1} x2={b.x2} fill="#ef4444" fillOpacity={0.08} />
              ))}
              {userThreshold > 0 && (
                <ReferenceLine y={userThreshold} stroke="#ef4444" strokeDasharray="6 4" strokeWidth={1.5}
                  label={{ value: `Min ${fmtInt(userThreshold)}`, position: 'right', fill: '#ef4444', fontSize: 9, fontWeight: 700 }} />
              )}
              <Area
                type="monotone" dataKey="users" stroke="#6366f1" strokeWidth={3}
                fill="url(#usersGrad)" name="Users"
                dot={{ r: 4, fill: '#6366f1' }}
                activeDot={{ r: 6, fill: '#6366f1', strokeWidth: 0 }}
              />
            </AreaChart>
          </ResponsiveContainer>
          <InsightBox insight={userInsight} />
        </Section>

        <Section title="Revenue Trend — Monthly Total Revenue (USD)" delay={0.47} accentColor={G.emerald}>
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={trend} margin={{ top: 6, right: 16, left: 8, bottom: 0 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis
                tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                domain={[
                  (dataMin) => Math.max(0, Math.floor(dataMin * 0.9)),
                  (dataMax) => Math.ceil(dataMax * 1.1),
                ]}
                tickFormatter={fmtUsd} width={72}
              />
              <Tooltip content={<ChartTooltip />} />
              {revBreaches.map((b, i) => (
                <ReferenceArea key={`rb-${i}`} x1={b.x1} x2={b.x2} fill="#ef4444" fillOpacity={0.08} />
              ))}
              {revThreshold > 0 && (
                <ReferenceLine y={revThreshold} stroke="#ef4444" strokeDasharray="6 4" strokeWidth={1.5}
                  label={{ value: `Min ${fmtUsd(revThreshold)}`, position: 'right', fill: '#ef4444', fontSize: 9, fontWeight: 700 }} />
              )}
              <Area
                type="monotone" dataKey="revenue" stroke="#10b981" strokeWidth={3}
                fill="url(#revenueGrad)" name="Revenue"
                dot={{ r: 4, fill: '#10b981' }}
                activeDot={{ r: 6, fill: '#10b981', strokeWidth: 0 }}
              />
            </AreaChart>
          </ResponsiveContainer>
          <InsightBox insight={revInsight} />
        </Section>
      </div>

      {/* ── ARPU Trend & Top 10 Sites ────────────────────────── */}
      {/* ARPU Trend: X = months, Y = Σ revenue[month] / Σ users[month] */}
      <div style={{ display: 'grid', gridTemplateColumns: '1.1fr 1fr', gap: 20, marginBottom: 20 }}>

        <Section title="ARPU Trend — Total Revenue / Total Users per Month (USD)" delay={0.50} accentColor={G.amber}>
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={trend} margin={{ top: 6, right: 16, left: 12, bottom: 0 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis
                tick={{ fontSize: 10, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false}
                domain={[
                  (dataMin) => Math.max(0, dataMin * 0.8),
                  (dataMax) => dataMax * 1.2,
                ]}
                tickFormatter={fmtUsdPrecise} width={92} allowDecimals
              />
              <Tooltip content={<ChartTooltip />} />
              {arpuBreaches.map((b, i) => (
                <ReferenceArea key={`apb-${i}`} x1={b.x1} x2={b.x2} fill="#ef4444" fillOpacity={0.08} />
              ))}
              {arpuThreshold > 0 && (
                <ReferenceLine y={arpuThreshold} stroke="#ef4444" strokeDasharray="6 4" strokeWidth={1.5}
                  label={{ value: `Min ${fmtUsdPrecise(arpuThreshold)}`, position: 'right', fill: '#ef4444', fontSize: 9, fontWeight: 700 }} />
              )}
              <Area
                type="monotone" dataKey="arpu" stroke="#f59e0b" strokeWidth={3}
                fill="url(#arpuGrad)" name="ARPU"
                dot={{ r: 4, fill: '#f59e0b' }}
                activeDot={{ r: 6, fill: '#f59e0b', strokeWidth: 0 }}
              />
            </AreaChart>
          </ResponsiveContainer>
          <InsightBox insight={arpuInsight} />
        </Section>

        <Section title={`Top 10 Sites by Revenue — ${currentMonth} (USD)`} delay={0.55} accentColor={G.indigo}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={kpi.top_sites} layout="vertical" margin={{ left: 10, right: 16 }}>
              <GradientDefs />
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.6} horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} tickFormatter={fmtUsd} />
              <YAxis type="category" dataKey="site_id" width={82} tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="revenue" radius={[0, 6, 6, 0]} name="Revenue">
                {(kpi.top_sites || []).map((_, idx) => (
                  <Cell key={idx} fill="url(#barGrad)" />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Section>
      </div>

      {/* ── Site Health Score — Worst 10 ─────────────────────── */}
      {/* Health = 0.6 × RevGrowth(mapped 0-100) + 0.2 × UserGrowth(mapped 0-100) + 0.2 × Utilization% */}
      <Section
        title="Site Health Score — Worst 10 Sites (Rev 60% · Users 20% · Utilization 20%)"
        delay={0.60} accentColor={G.rose}
        style={{ marginBottom: 20 }}
      >
        <PremiumTable
          emptyText="No site health data available"
          cols={[
            { key: 'site_id',      label: 'Site',         cellStyle: { fontWeight: 700 } },
            { key: 'health_score', label: 'Health Score', render: (v) => (
              <Pill value={v} positive={v >= 70} neutral={v >= 40 && v < 70} negative={v < 40} />
            )},
            { key: 'utilization',  label: 'Utilization',  render: (v) => (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <ProgressBar value={v} colors={v > 80 ? G.rose : v > 60 ? G.amber : G.emerald} />
                <span style={{ fontSize: 12, color: 'var(--text-muted)', minWidth: 30 }}>{v}%</span>
              </div>
            )},
            { key: 'users',   label: 'Users',          render: fmtInt    },
            { key: 'revenue', label: 'Revenue (USD)',   render: fmtUsdFull },
          ]}
          rows={kpi.site_health}
        />
      </Section>

      {/* ── Low Margin Sites & Overloaded Sites ─────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>

        {/* Low Margin Sites
            Eligibility: util + users + revenue ALL present and non-zero
            Definition:  Margin % = (Revenue − OPEX) / Revenue × 100  < 30%
            Sorted:      worst margin first (ascending)
            Backend enforces the all-three-present gate. */}
        <Section title="Low Margin Sites  (Margin < 30%)" delay={0.65} accentColor={G.rose}>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 10, lineHeight: 1.5 }}>
            Only sites where <strong>utilization · users · revenue</strong> are all present.
            Margin = (Revenue − OPEX) / Revenue × 100.
          </div>
          <PremiumTable
            emptyText={rawData?.low_margin_sites?.length === 0
              ? 'No low-margin sites — all sites above 30% margin'
              : 'Upload revenue data with OPEX column to see margin analysis'}
            cols={[
              { key: 'site_id',    label: 'Site',         cellStyle: { fontWeight: 700 } },
              { key: 'revenue',    label: 'Revenue (USD)', render: fmtUsdFull },
              { key: 'opex',       label: 'OPEX (USD)',    render: fmtUsdFull },
              { key: 'margin_pct', label: 'Margin %',      render: (v) => (
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <Scissors size={12} color={v < 0 ? '#ef4444' : '#f59e0b'} />
                  <Pill
                    value={`${v > 0 ? '' : ''}${Number(v).toFixed(1)}`}
                    suffix="%"
                    negative={v < 10}
                    neutral={v >= 10 && v < 30}
                  />
                </div>
              )},
              { key: 'users',      label: 'Users',         render: fmtInt },
            ]}
            rows={kpi.low_margin_sites}
          />
        </Section>

        {/* Overloaded Sites
            Eligibility: utilization AND revenue BOTH present and non-zero
            (users not required — this is a network capacity signal)
            Definition:  utilization ≥ 80%
            Sorted:      highest utilization first, then highest revenue */}
        <Section title="Overloaded Sites  (Utilization ≥ 80%)" delay={0.70} accentColor={G.amber}>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 10, lineHeight: 1.5 }}>
            Only sites where <strong>utilization · revenue</strong> are both present.
            Sorted by load, then revenue impact.
          </div>
          <PremiumTable
            emptyText="No overloaded sites — all sites below 80% utilization"
            cols={[
              { key: 'site_id',     label: 'Site',        cellStyle: { fontWeight: 700 } },
              { key: 'utilization', label: 'Utilization', render: (v) => (
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <ProgressBar value={v} colors={v >= 95 ? G.rose : v >= 90 ? ['#f97316','#ef4444'] : G.amber} />
                  <Pill
                    value={`${Number(v).toFixed(1)}`}
                    suffix="%"
                    negative={v >= 95}
                    neutral={v >= 90 && v < 95}
                  />
                </div>
              )},
              { key: 'revenue', label: 'Revenue (USD)', render: fmtUsdFull },
            ]}
            rows={kpi.overloaded_sites}
          />
        </Section>
      </div>

      <style>{`
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
    </div>
  );
}