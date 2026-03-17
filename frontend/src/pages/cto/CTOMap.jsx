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
  return L.divIcon({
    className: 'cto-site-marker',
    html: `<div style="width:14px;height:14px;border-radius:999px;background:${color};border:2px solid #fff;box-shadow:0 2px 8px rgba(15,23,42,0.28)"></div>`,
    iconSize: [14, 14],
    iconAnchor: [7, 7],
  });
}

function MapBounds({ sites }) {
  const map = useMap();

  useEffect(() => {
    if (!sites.length) {
      map.setView(INDIA_CENTER, 5);
      return;
    }
    const bounds = L.latLngBounds(sites.map((site) => [site.lat, site.lng]));
    map.fitBounds(bounds, { padding: [28, 28], maxZoom: 12 });
  }, [map, sites]);

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

  return <MarkerClusterGroup chunkedLoading>{markers}</MarkerClusterGroup>;
});

export default function CTOMap() {
  const [sites, setSites] = useState([]);
  const [loading, setLoading] = useState(true);

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
      .catch(() => {
        if (mounted) setSites([]);
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, []);

  const averageCenter = useMemo(() => {
    if (!sites.length) return INDIA_CENTER;
    const { lat, lng } = sites.reduce(
      (acc, site) => ({ lat: acc.lat + Number(site.lat), lng: acc.lng + Number(site.lng) }),
      { lat: 0, lng: 0 }
    );
    return [lat / sites.length, lng / sites.length];
  }, [sites]);

  if (loading) {
    return <div className="page-loader" style={{ minHeight: '84vh' }}><div className="spinner" /></div>;
  }

  return (
    <div style={{ height: '84vh', width: '100%', background: '#e2e8f0' }}>
      <MapContainer center={averageCenter} zoom={5} style={{ height: '100%', width: '100%' }} scrollWheelZoom>
        <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <MapBounds sites={sites} />
        <SiteMarkers sites={sites} />
      </MapContainer>
    </div>
  );
}
