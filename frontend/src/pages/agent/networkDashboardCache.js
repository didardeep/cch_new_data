/**
 * networkDashboardCache.js
 *
 * Persistent, per-layer cache for NetworkAnalyticsDashboard.
 *
 * Persistence hierarchy (fastest → most durable):
 *   1. Module-level object (DC)  — survives React unmount / route changes
 *   2. sessionStorage             — survives hard-refresh (F5) and HMR
 *                                   WebSocket reconnections within the tab
 *   3. localStorage               — survives tab-close and browser restart
 *
 * Each data layer (overview, map, ran, core, transport) is stored as its own
 * key so that:
 *   • Writing one layer never rewrites the others (no "one big blob").
 *   • A storage-quota failure on a large layer (e.g. RAN) does not corrupt
 *     the others.
 *   • Module initialisation only reads the two small keys it needs for the
 *     Overview page (meta + overview); the heavy layers are lazy-loaded from
 *     storage only when the user first visits that tab.
 */

// ─── Constants ────────────────────────────────────────────────────────────────

/** Cache version — bump this string whenever the stored shape changes. */
const VERSION = 'v4';

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

/** Per-layer storage keys (one entry in sessionStorage / localStorage each). */
const KEYS = {
  meta     : `nad_${VERSION}_meta`,
  overview : `nad_${VERSION}_overview`,
  map      : `nad_${VERSION}_map`,
  ran      : `nad_${VERSION}_ran`,
  core     : `nad_${VERSION}_core`,
  transport: `nad_${VERSION}_transport`,
  opts     : `nad_${VERSION}_opts`,
};

// ─── Low-level storage helpers ────────────────────────────────────────────────

/**
 * Try to read `key` from sessionStorage then localStorage.
 * Returns the parsed `data` field when the entry exists AND is fresh;
 * removes the stale entry and returns null otherwise.
 */
function storeRead(key) {
  for (const store of [sessionStorage, localStorage]) {
    try {
      const raw = store.getItem(key);
      if (!raw) continue;
      const parsed = JSON.parse(raw);
      if (parsed && parsed.ts && (Date.now() - parsed.ts) < DC_TTL) {
        return parsed.data;
      }
      // Stale — remove from this store and keep checking the next.
      store.removeItem(key);
    } catch (_) { /* ignore parse / security errors */ }
  }
  return null;
}

/**
 * Write `data` under `key` to both sessionStorage and localStorage.
 * Handles QuotaExceededError by evicting the old entry and retrying once.
 */
function storeWrite(key, data) {
  const payload = JSON.stringify({ data, ts: Date.now() });
  for (const store of [sessionStorage, localStorage]) {
    try {
      store.setItem(key, payload);
    } catch (e) {
      try { store.removeItem(key); store.setItem(key, payload); } catch (_) {}
    }
  }
}

/** Remove a single layer from both stores (used when invalidating). */
function storeRemove(key) {
  for (const store of [sessionStorage, localStorage]) {
    try { store.removeItem(key); } catch (_) {}
  }
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Save one data layer and update the shared meta timestamp.
 *
 * @param {string} layerName  – one of the values in DC_PATH ('overview', 'ran', …)
 * @param {*}      data       – the API response to store
 * @param {object} dc         – the live DC object so ts is kept in sync
 */
export function saveLayer(layerName, data, dc) {
  if (!KEYS[layerName]) return;
  storeWrite(KEYS[layerName], data);
  dc[layerName] = data;
  dc.ts = Date.now();
  saveMeta(dc); // keep meta in sync with the new timestamp
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
 * Populates DC in place and returns the data (or null if not in storage).
 */
export function loadLayerFromStore(layerName, dc) {
  if (dc[layerName]) return dc[layerName]; // already in memory
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
// module re-evaluates and this code runs again, re-reading from storage so the
// user never sees a blank screen.

function buildDC() {
  // Always read meta first — it's tiny and tells us what's available.
  const meta = storeRead(KEYS.meta) || { ts: 0, fetchedLayers: { ran: false, core: false, transport: false } };

  // Only pre-load overview + opts at startup (needed for Overview page).
  // The heavier layers (ran, core, transport, map) are pulled from storage
  // lazily via loadLayerFromStore() when the user first visits that tab.
  return {
    overview     : storeRead(KEYS.overview),
    map          : null,   // lazy
    ran          : null,   // lazy
    core         : null,   // lazy
    transport    : null,   // lazy
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
