import { memo, useEffect, useMemo, useState } from 'react';
import { MapContainer, Marker, Popup, TileLayer, Tooltip, useMap } from 'react-leaflet';
import MarkerClusterGroup from 'react-leaflet-cluster';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { apiGet } from '../../api';

const INDIA_CENTER = [22.5937, 78.9629];

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
    if (!sites.length) { map.setView(INDIA_CENTER, 5); return; }
    const bounds = L.latLngBounds(sites.map((site) => [site.lat, site.lng]));
    map.fitBounds(bounds, { padding: [28, 28], maxZoom: 12 });
  }, [map, sites]);
  return null;
}

function FlyToSite({ target }) {
  const map = useMap();
  useEffect(() => {
    if (!target) return;
    map.flyTo([target.lat, target.lng], 18, { duration: 1.2 });
    // After fly animation completes, open the matching marker's popup
    setTimeout(() => {
      map.eachLayer(layer => {
        if (layer instanceof L.Marker) {
          const pos = layer.getLatLng();
          if (
            Math.abs(pos.lat - target.lat) < 0.0001 &&
            Math.abs(pos.lng - target.lng) < 0.0001
          ) {
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
              <div style={{ fontSize: 12, color: '#475569', marginBottom: 2 }}>Latitude: {Number(site.lat).toFixed(6)}</div>
              <div style={{ fontSize: 12, color: '#475569', marginBottom: 2 }}>Longitude: {Number(site.lng).toFixed(6)}</div>
              {site.alarm ? (
                <div style={{ fontSize: 12, color: '#b45309', marginTop: 6 }}>
                  <strong>Alarm:</strong> {site.alarm}
                </div>
              ) : null}
              {site.solution ? (
                <div style={{ fontSize: 12, color: '#475569', marginTop: 6 }}>
                  <strong>Solution:</strong> {site.solution}
                </div>
              ) : null}
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
          html: `<div style="
            width:${size}px;height:${size}px;border-radius:50%;
            background:#002266;color:#fff;border:2px solid #fff;
            box-shadow:0 2px 8px rgba(0,34,102,0.45);
            display:flex;align-items:center;justify-content:center;
            font-size:${count >= 100 ? 9 : 10}px;font-weight:700;font-family:sans-serif;
          ">${count}</div>`,
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

export default function CTOMap() {
  const [sites, setSites]   = useState([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery]   = useState('');
  const [flyTo, setFlyTo]   = useState(null);

  useEffect(() => {
    let mounted = true;
    apiGet('/api/cto/map-data')
      .then((data) => {
        if (!mounted) return;
        const nextSites = Array.isArray(data?.sites) ? data.sites : [];
        setSites(
          nextSites.filter((site) => {
            const lat = Number(site.lat);
            const lng = Number(site.lng);
            return Number.isFinite(lat) && Number.isFinite(lng) && lat !== 0 && lng !== 0;
          })
        );
      })
      .catch(() => { if (mounted) setSites([]); })
      .finally(() => { if (mounted) setLoading(false); });
    return () => { mounted = false; };
  }, []);

  const averageCenter = useMemo(() => {
    if (!sites.length) return INDIA_CENTER;
    const { lat, lng } = sites.reduce(
      (acc, site) => ({ lat: acc.lat + Number(site.lat), lng: acc.lng + Number(site.lng) }),
      { lat: 0, lng: 0 }
    );
    return [lat / sites.length, lng / sites.length];
  }, [sites]);

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

      <div style={{ position: 'relative', height: '84vh', width: '100%', background: 'var(--border, #e2e8f0)' }}>

        {/* ── Search bar overlay ── */}
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
              outline: 'none', background: 'var(--bg-card, #fff)', boxSizing: 'border-box', color: 'var(--text)',
            }}
          />
          {results.length > 0 && (
            <div style={{
              background: 'var(--bg-card, #fff)', borderRadius: 10, marginTop: 4,
              boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
              border: '1px solid var(--border)', overflow: 'hidden',
            }}>
              {results.map(s => (
                <div
                  key={s.site_id}
                  onClick={() => { setFlyTo(s); setQuery(s.site_id); }}
                  style={{
                    padding: '8px 14px', cursor: 'pointer', fontSize: 13,
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    borderBottom: '1px solid var(--border)', background: 'var(--bg-card, #fff)',
                  }}
                  onMouseEnter={e => e.currentTarget.style.background = 'var(--bg, #f8fafc)'}
                  onMouseLeave={e => e.currentTarget.style.background = 'var(--bg-card, #fff)'}
                >
                  <span style={{ fontWeight: 600, color: '#002266' }}>{s.site_id}</span>
                  <span style={{
                    fontSize: 11, padding: '2px 7px', borderRadius: 999, fontWeight: 600,
                    background: ['down','off_air'].includes(String(s.status).toLowerCase()) ? 'rgba(220,38,38,0.1)' : 'rgba(22,163,106,0.1)',
                    color:      ['down','off_air'].includes(String(s.status).toLowerCase()) ? '#dc2626' : '#16a34a',
                  }}>
                    {statusLabel(s.status)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>

        <MapContainer center={averageCenter} zoom={5} style={{ height: '100%', width: '100%' }} scrollWheelZoom>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          />
          <MapBounds sites={sites} />
          <FlyToSite target={flyTo} />
          <SiteMarkers sites={sites} />
        </MapContainer>
      </div>
    </>
  );
}
