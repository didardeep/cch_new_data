import { memo, useEffect, useMemo, useState } from 'react';
import { CircleMarker, MapContainer, Marker, Popup, TileLayer, Tooltip, useMap } from 'react-leaflet';
import MarkerClusterGroup from 'react-leaflet-cluster';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { apiGet } from '../../api';

const DEFAULT_MAP_CENTER = [12.5657, 104.9910];

function statusColor(status) {
  const normalized = String(status || 'active').toLowerCase();
  if (normalized === 'down' || normalized === 'off_air') return '#dc2626';
  if (normalized === 'alarm' || normalized === 'warning') return '#f59e0b';
  return '#16a34a';
}

function statusLabel(status) {
  const normalized = String(status || 'active').toLowerCase();
  if (normalized === 'on_air') return 'Active';
  if (normalized === 'off_air') return 'Down';
  if (normalized === 'alarm') return 'Alarm';
  if (normalized === 'warning') return 'Warning';
  return normalized.replace(/_/g, ' ');
}

function createSiteIcon(status) {
  const color = statusColor(status);
  const normalized = String(status || '').toLowerCase();
  const isAlert = ['down', 'off_air', 'alarm', 'warning'].includes(normalized);
  return L.divIcon({
    className: 'cto-site-marker',
    html: `<div class="${isAlert ? 'cto-marker-alert' : ''}" style="width:18px;height:18px;border-radius:999px;background:${color};border:2.5px solid #fff;box-shadow:0 2px 12px ${color}99,0 0 0 4px ${color}33"></div>`,
    iconSize: [18, 18],
    iconAnchor: [9, 9],
  });
}

function MapBounds({ sites }) {
  const map = useMap();
  useEffect(() => {
    if (!map) return;
    const valid = (sites || []).filter(s => {
      const la = Number(s.lat), lo = Number(s.lng);
      return Number.isFinite(la) && Number.isFinite(lo) && la !== 0 && lo !== 0;
    });
    if (!valid.length) { map.setView(DEFAULT_MAP_CENTER, 6); return; }
    try {
      const bounds = L.latLngBounds(valid.map(s => [Number(s.lat), Number(s.lng)]));
      if (bounds.isValid()) map.fitBounds(bounds, { padding: [28, 28], maxZoom: 12 });
    } catch (_) { /* ignore */ }
  }, [map, sites]);
  return null;
}

function FlyToSite({ target }) {
  const map = useMap();
  useEffect(() => {
    if (!target) return;
    map.flyTo([target.lat, target.lng], 18, { duration: 1.2 });
    setTimeout(() => {
      map.eachLayer(layer => {
        if (layer instanceof L.Marker) {
          const pos = layer.getLatLng();
          if (Math.abs(pos.lat - target.lat) < 0.0001 && Math.abs(pos.lng - target.lng) < 0.0001) {
            layer.openPopup();
          }
        }
      });
    }, 1400);
  }, [target, map]);
  return null;
}

const SiteMarkers = memo(function SiteMarkers({ sites }) {
  const markers = useMemo(
    () =>
      sites.map((site) => (
        <Marker
          key={`${site.site_id}-${site.lat}-${site.lng}`}
          position={[site.lat, site.lng]}
          icon={createSiteIcon(site.status)}
        >
          <Tooltip direction="top" offset={[0, -10]}>{site.site_id}</Tooltip>
          <Popup>
            <div style={{ minWidth: 220 }}>
              <div style={{ fontWeight: 700, color: '#00338D', marginBottom: 6 }}>{site.site_id}</div>
              <div style={{ fontSize: 12, color: '#475569', marginBottom: 2 }}>Zone: {site.zone || 'N/A'}</div>
              <div style={{ fontSize: 12, color: '#475569', marginBottom: 2 }}>Status: {statusLabel(site.status) || 'Active'}</div>
              <div style={{ fontSize: 12, color: '#475569', marginBottom: 2 }}>Lat: {Number(site.lat).toFixed(6)}</div>
              <div style={{ fontSize: 12, color: '#475569', marginBottom: 2 }}>Lng: {Number(site.lng).toFixed(6)}</div>
              {site.alarm ? <div style={{ fontSize: 12, color: '#b45309', marginTop: 6 }}><strong>Alarm:</strong> {site.alarm}</div> : null}
              {site.solution ? <div style={{ fontSize: 12, color: '#475569', marginTop: 6 }}><strong>Solution:</strong> {site.solution}</div> : null}
            </div>
          </Popup>
        </Marker>
      )),
    [sites]
  );

  return (
    <MarkerClusterGroup
      chunkedLoading
      disableClusteringAtZoom={18}
      iconCreateFunction={(cluster) => {
        const count = cluster.getChildCount();
        const size = count >= 100 ? 28 : count >= 10 ? 24 : 20;
        return L.divIcon({
          html: `<div style="width:${size}px;height:${size}px;border-radius:50%;background:#002266;color:#fff;border:2px solid #fff;box-shadow:0 2px 8px rgba(0,34,102,0.45);display:flex;align-items:center;justify-content:center;font-size:${count >= 100 ? 9 : 10}px;font-weight:700;font-family:sans-serif;">${count}</div>`,
          className: 'cto-cluster-icon',
          iconSize: [size, size],
          iconAnchor: [size / 2, size / 2],
        });
      }}
    >
      {markers}
    </MarkerClusterGroup>
  );
});

/* ── Ticket density color (green → yellow → red) ─────────── */
function ticketBubbleColor(count, max) {
  const ratio = Math.min(1, count / Math.max(max, 1));
  if (ratio < 0.33) return '#10b981';
  if (ratio < 0.66) return '#f59e0b';
  return '#ef4444';
}

export default function CTOMap() {
  const [sites, setSites]         = useState([]);
  const [loading, setLoading]     = useState(true);
  const [query, setQuery]         = useState('');
  const [flyTo, setFlyTo]         = useState(null);
  const [ticketData, setTicketData] = useState(null);
  const [viewMode, setViewMode]   = useState('sites');

  useEffect(() => {
    let mounted = true;
    apiGet('/api/cto/map-data')
      .then((data) => {
        if (!mounted) return;
        const nextSites = Array.isArray(data?.sites) ? data.sites : [];
        setSites(nextSites.filter((site) => {
          const lat = Number(site.lat), lng = Number(site.lng);
          return Number.isFinite(lat) && Number.isFinite(lng) && lat !== 0 && lng !== 0;
        }));
      })
      .catch(() => { if (mounted) setSites([]); })
      .finally(() => { if (mounted) setLoading(false); });
    return () => { mounted = false; };
  }, []);

  useEffect(() => {
    apiGet('/api/cto/ticket-heatmap')
      .then(setTicketData)
      .catch(() => setTicketData(null));
  }, []);

  const averageCenter = useMemo(() => {
    const valid = (sites || []).filter(s => {
      const la = Number(s.lat), lo = Number(s.lng);
      return Number.isFinite(la) && Number.isFinite(lo) && la !== 0 && lo !== 0;
    });
    if (!valid.length) return DEFAULT_MAP_CENTER;
    const sum = valid.reduce((acc, s) => ({ lat: acc.lat + Number(s.lat), lng: acc.lng + Number(s.lng) }), { lat: 0, lng: 0 });
    const c = [sum.lat / valid.length, sum.lng / valid.length];
    if (!Number.isFinite(c[0]) || !Number.isFinite(c[1])) return DEFAULT_MAP_CENTER;
    return c;
  }, [sites]);

  const maxTickets = useMemo(() => {
    if (!ticketData?.state_data?.length) return 1;
    return Math.max(...ticketData.state_data.map(s => s.total), 1);
  }, [ticketData]);

  const ticketStates = useMemo(() => {
    if (!ticketData?.state_data) return [];
    return ticketData.state_data.filter(s => s.lat && s.lng && s.total > 0);
  }, [ticketData]);

  const results = query.length >= 2
    ? sites.filter(s => s.site_id.toLowerCase().includes(query.toLowerCase())).slice(0, 8)
    : [];

  if (loading) {
    return <div className="page-loader" style={{ minHeight: '84vh' }}><div className="spinner" /></div>;
  }

  return (
    <>
      <style>{`
        @keyframes ctoAlarmPulse {
          0%   { transform: scale(1);   opacity: 1;   }
          50%  { transform: scale(1.4); opacity: 0.75; }
          100% { transform: scale(1);   opacity: 1;   }
        }
        .cto-marker-alert { animation: ctoAlarmPulse 1.6s ease-in-out infinite; }
      `}</style>

      <div style={{ position: 'relative', height: '84vh', width: '100%', background: '#e2e8f0' }}>

        {/* View mode toggle */}
        <div style={{ position: 'absolute', top: 12, left: 12, zIndex: 1000, display: 'flex', gap: 4, background: '#fff', borderRadius: 10, padding: 4, boxShadow: '0 2px 12px rgba(0,0,0,0.12)', border: '1px solid #e2e8f0' }}>
          {[
            { key: 'sites', label: 'Site Status' },
            { key: 'heatmap', label: 'Zone Performance' },
          ].map(m => (
            <button key={m.key} onClick={() => setViewMode(m.key)} style={{
              padding: '7px 16px', borderRadius: 8, border: 'none', cursor: 'pointer',
              fontSize: 12, fontWeight: 700, transition: 'all 0.2s',
              background: viewMode === m.key ? '#002266' : 'transparent',
              color: viewMode === m.key ? '#fff' : '#64748b',
            }}>{m.label}</button>
          ))}
        </div>

        {/* Search bar */}
        <div style={{ position: 'absolute', top: 12, right: 12, zIndex: 1000, width: 280 }}>
          <input
            placeholder="Search site ID..."
            value={query}
            onChange={e => { setQuery(e.target.value); setFlyTo(null); }}
            onKeyDown={e => {
              if (e.key === 'Enter' && results.length > 0) {
                setFlyTo(results[0]);
                setQuery(results[0].site_id);
              }
            }}
            style={{
              width: '100%', padding: '9px 14px', borderRadius: 10,
              border: '1px solid #cbd5e1', fontSize: 13, fontWeight: 500,
              boxShadow: '0 2px 12px rgba(0,0,0,0.12)',
              outline: 'none', background: '#fff', boxSizing: 'border-box',
            }}
          />
          {results.length > 0 && (
            <div style={{
              background: '#fff', borderRadius: 10, marginTop: 4,
              boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
              border: '1px solid #e2e8f0', overflow: 'hidden',
            }}>
              {results.map(s => (
                <div
                  key={s.site_id}
                  onClick={() => { setFlyTo(s); setQuery(s.site_id); }}
                  style={{
                    padding: '8px 14px', cursor: 'pointer', fontSize: 13,
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    borderBottom: '1px solid #f1f5f9', background: '#fff',
                  }}
                  onMouseEnter={e => e.currentTarget.style.background = '#f8fafc'}
                  onMouseLeave={e => e.currentTarget.style.background = '#fff'}
                >
                  <span style={{ fontWeight: 600, color: '#002266' }}>{s.site_id}</span>
                  <span style={{
                    fontSize: 11, padding: '2px 7px', borderRadius: 999, fontWeight: 600,
                    background: ['down','off_air'].includes(String(s.status).toLowerCase()) ? '#fee2e2' : '#dcfce7',
                    color: ['down','off_air'].includes(String(s.status).toLowerCase()) ? '#dc2626' : '#16a34a',
                  }}>
                    {statusLabel(s.status)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Legend for heatmap */}
        {viewMode === 'heatmap' && ticketStates.length > 0 && (
          <div style={{
            position: 'absolute', bottom: 24, left: 12, zIndex: 1000,
            background: '#fff', borderRadius: 12, padding: '14px 18px',
            boxShadow: '0 4px 20px rgba(0,0,0,0.12)', border: '1px solid #e2e8f0',
            minWidth: 180,
          }}>
            <div style={{ fontSize: 11, fontWeight: 800, color: '#1e293b', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Ticket Density by State
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {[
                { color: '#10b981', label: 'Low' },
                { color: '#f59e0b', label: 'Medium' },
                { color: '#ef4444', label: 'High' },
              ].map(({ color, label }) => (
                <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{ width: 14, height: 14, borderRadius: '50%', background: color, opacity: 0.7, border: `2px solid ${color}` }} />
                  <span style={{ fontSize: 11, color: '#475569', fontWeight: 600 }}>{label}</span>
                </div>
              ))}
            </div>
            {ticketData?.detected_country && (
              <div style={{ marginTop: 8, fontSize: 10, color: '#94a3b8', fontWeight: 600 }}>
                Region: {ticketData.detected_country}
              </div>
            )}
          </div>
        )}

        <MapContainer center={averageCenter} zoom={5} style={{ height: '100%', width: '100%' }} scrollWheelZoom>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          />
          <MapBounds sites={sites} />
          <FlyToSite target={flyTo} />

          {/* Ticket density circles */}
          {viewMode === 'heatmap' && ticketStates.map(s => {
            const color = ticketBubbleColor(s.total, maxTickets);
            const radius = Math.max(18, Math.min(55, 18 + (s.total / maxTickets) * 40));
            return (
              <CircleMarker
                key={s.state}
                center={[s.lat, s.lng]}
                radius={radius}
                pathOptions={{
                  fillColor: color,
                  fillOpacity: 0.55,
                  color: color,
                  weight: 2.5,
                  opacity: 0.85,
                }}
              >
                <Tooltip direction="top" offset={[0, -radius]} sticky>
                  <div style={{ minWidth: 160 }}>
                    <div style={{ fontWeight: 800, fontSize: 13, color: '#1e293b', marginBottom: 6, borderBottom: '2px solid #e2e8f0', paddingBottom: 4 }}>{s.state}</div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                      <span style={{ color: '#64748b', fontSize: 11 }}>Total Tickets:</span>
                      <span style={{ fontWeight: 800, fontSize: 12, color: '#dc2626' }}>{s.total}</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                      <span style={{ color: '#64748b', fontSize: 11 }}>Resolved:</span>
                      <span style={{ fontWeight: 700, fontSize: 12, color: '#10b981' }}>{s.resolved}</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: '#64748b', fontSize: 11 }}>Pending:</span>
                      <span style={{ fontWeight: 700, fontSize: 12, color: '#f59e0b' }}>{s.pending}</span>
                    </div>
                  </div>
                </Tooltip>
                <Popup>
                  <div style={{ minWidth: 180 }}>
                    <div style={{ fontWeight: 800, fontSize: 14, color: '#002266', marginBottom: 8 }}>{s.state}</div>
                    <div style={{ fontSize: 12, marginBottom: 4 }}><strong>Total Tickets:</strong> {s.total}</div>
                    <div style={{ fontSize: 12, marginBottom: 4, color: '#10b981' }}><strong>Resolved:</strong> {s.resolved}</div>
                    <div style={{ fontSize: 12, color: '#f59e0b' }}><strong>Pending:</strong> {s.pending}</div>
                  </div>
                </Popup>
              </CircleMarker>
            );
          })}

          {/* Site markers on sites view */}
          {viewMode === 'sites' && <SiteMarkers sites={sites} />}
        </MapContainer>
      </div>
    </>
  );
}
