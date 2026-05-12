/**
 * networkDashboardCache.js
 *
 * Persistent, per-layer cache for NetworkAnalyticsDashboard.
 *
 * Storage strategy:
 *   • Module-level object (DC)  — survives React unmount / route changes
 *   • localStorage (meta + opts only) — survives hard-refresh, HMR WebSocket
 *     reconnections, and tab-close/browser-restart.
 *
 * Heavy layers (ran, core, transport, map) are kept ONLY in the module-level
 * DC object.  They are never written to localStorage because:
 *   1. They can easily exceed 5 MB, hitting storage quota errors.
 *   2. Writing them on every filter change causes noticeable jank.
 *   3. They are re-fetched from the API on the next page load anyway
 *      (the API response is fast once the DB has the MATERIALIZED VIEW).
 *
 * Each data layer (overview, map, ran, core, transport) is kept as its own
 * DC property so that writing one layer never invalidates the others.
 */

// ─── Constants ────────────────────────────────────────────────────────────────

/** Cache version — bump this string whenever the stored shape changes. */
const VERSION = 'v5';

/** How long a stored entry is considered fresh (10 minutes). */
export const DC_TTL = 10 * 60 * 1000;

/** Maps an API path to its cache-key name inside DC. */
export const DC_PATH = {
  '/api/network/overview-stats' : 'overview',
  '/api/network/map'            : 'map',
  '/api/network/ran-analytics'  : 'ran',
  '/api/network/core-analytics' : 'core',
  '/api/network/transport-analytics': 'transport',
};

/** Layers persisted to localStorage (lightweight only). */
const PERSISTED_LAYERS = new Set(['overview']);

/** localStorage keys for the two lightweight entries. */
const KEYS = {
  meta    : `nad_${VERSION}_meta`,
  overview: `nad_${VERSION}_overview`,
  opts    : `nad_${VERSION}_opts`,
};

// ─── Low-level storage helpers ────────────────────────────────────────────────

/**
 * Try to read `key` from localStorage.
 * Returns the parsed `data` field when the entry exists AND is fresh;
 * removes the stale entry and returns null otherwise.
 */
function storeRead(key) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed && parsed.ts && (Date.now() - parsed.ts) < DC_TTL) {
      return parsed.data;
    }
    // Stale — evict.
    localStorage.removeItem(key);
  } catch (_) { /* ignore parse / security errors */ }
  return null;
}

/**
 * Write `data` under `key` to localStorage.
 * Handles QuotaExceededError by evicting the old entry and retrying once.
 */
function storeWrite(key, data) {
  let payload;
  try {
    payload = JSON.stringify({ data, ts: Date.now() });
  } catch (_) {
    return; // un-serialisable data — skip silently
  }
  try {
    localStorage.setItem(key, payload);
  } catch (e) {
    try { localStorage.removeItem(key); localStorage.setItem(key, payload); } catch (_) {}
  }
}

/** Remove a single key from localStorage (used when invalidating). */
function storeRemove(key) {
  try { localStorage.removeItem(key); } catch (_) {}
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Save one data layer.
 *
 * Lightweight layers (overview): persisted to localStorage + DC.
 * Heavy layers (ran, core, transport, map): stored in DC only.
 *
 * @param {string} layerName  – one of the values in DC_PATH ('overview', 'ran', …)
 * @param {*}      data       – the API response to store
 * @param {object} dc         – the live DC object so ts is kept in sync
 */
export function saveLayer(layerName, data, dc) {
  dc[layerName] = data;
  dc.ts = Date.now();
  if (PERSISTED_LAYERS.has(layerName) && KEYS[layerName]) {
    storeWrite(KEYS[layerName], data);
  }
  saveMeta(dc);
}

/**
 * Persist only the lightweight meta record (fetchedLayers, ts, opts).
 * Called whenever fetchedLayers or opts changes without a full layer write.
 */
export function saveMeta(dc) {
  storeWrite(KEYS.meta, {
    ts           : dc.ts,
    fetchedLayers: dc.fetchedLayers,
  });
  if (dc.opts) storeWrite(KEYS.opts, dc.opts);
}

/**
 * Load a single layer from storage on demand (lazy restore).
 * Only overview is persisted; heavy layers always return null here
 * (they must be re-fetched from the API on a fresh page load).
 */
export function loadLayerFromStore(layerName, dc) {
  if (dc[layerName]) return dc[layerName]; // already in memory
  if (!PERSISTED_LAYERS.has(layerName)) return null;
  const data = storeRead(KEYS[layerName]);
  if (data) dc[layerName] = data;
  return data;
}

/**
 * Wipe all stored entries for this dashboard (e.g. on logout / forced refresh).
 */
export function clearCache(dc) {
  Object.keys(KEYS).forEach(k => storeRemove(KEYS[k]));
  Object.assign(dc, {
    overview: null, map: null, ran: null,
    core: null, transport: null, opts: null,
    fetchedLayers: { ran: false, core: false, transport: false },
    ts: 0,
  });
}

// ─── Module-level DC object ───────────────────────────────────────────────────
// Initialised once when the module is first imported.  On a hard refresh the
// module re-evaluates and this code runs again, re-reading overview + opts
// from localStorage so the Overview tab is instant.

function buildDC() {
  const meta = storeRead(KEYS.meta) || { ts: 0, fetchedLayers: { ran: false, core: false, transport: false } };
  return {
    overview     : storeRead(KEYS.overview),
    map          : null,  // heavy — not persisted
    ran          : null,  // heavy — not persisted
    core         : null,  // heavy — not persisted
    transport    : null,  // heavy — not persisted
    opts         : storeRead(KEYS.opts),
    fetchedLayers: { ...meta.fetchedLayers },
    ts           : meta.ts,
  };
}

/**
 * The single shared cache instance.
 * Import this object in any component that needs access to the cached data.
 */
export const DC = buildDC();
