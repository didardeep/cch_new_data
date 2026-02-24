import { useState, useEffect, useCallback } from 'react';
import { getToken } from '../../api';

const API_BASE = process.env.REACT_APP_API_URL || '';

export default function DataUpload() {
  const [siteFile, setSiteFile] = useState(null);
  const [kpiFile, setKpiFile] = useState(null);
  const [kpiName, setKpiName] = useState('');
  const [siteResult, setSiteResult] = useState(null);
  const [kpiResult, setKpiResult] = useState(null);
  const [uploading, setUploading] = useState({ sites: false, kpi: false });
  const [kpiList, setKpiList] = useState([]);
  const [siteCount, setSiteCount] = useState(0);
  const [error, setError] = useState('');

  const fetchKpiList = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/admin/uploaded-kpis`, {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (d.kpis) setKpiList(d.kpis);
      if (d.site_count !== undefined) setSiteCount(d.site_count);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchKpiList(); }, [fetchKpiList]);

  const uploadSites = async () => {
    if (!siteFile) return;
    setUploading(p => ({ ...p, sites: true }));
    setSiteResult(null);
    setError('');
    try {
      const form = new FormData();
      form.append('file', siteFile);
      const resp = await fetch(`${API_BASE}/api/admin/upload-sites`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setSiteResult(d);
        setSiteFile(null);
        fetchKpiList();
      } else {
        setError(d.error || 'Upload failed');
      }
    } catch (e) {
      setError('Upload failed: ' + e.message);
    }
    setUploading(p => ({ ...p, sites: false }));
  };

  const uploadKpi = async () => {
    if (!kpiFile || !kpiName.trim()) return;
    setUploading(p => ({ ...p, kpi: true }));
    setKpiResult(null);
    setError('');
    try {
      const form = new FormData();
      form.append('file', kpiFile);
      form.append('kpi_name', kpiName.trim());
      const resp = await fetch(`${API_BASE}/api/admin/upload-kpi`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setKpiResult(d);
        setKpiFile(null);
        setKpiName('');
        fetchKpiList();
      } else {
        setError(d.error || 'Upload failed');
      }
    } catch (e) {
      setError('Upload failed: ' + e.message);
    }
    setUploading(p => ({ ...p, kpi: false }));
  };

  return (
    <div>
      <div className="page-header">
        <h1>Data Upload</h1>
        <p>Upload telecom site data and KPI Excel files for network diagnosis.</p>
      </div>

      {error && (
        <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8, padding: '12px 16px', marginBottom: 20, color: '#dc2626', fontSize: 13, fontWeight: 600 }}>
          {error}
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        {/* Site Data Upload */}
        <div className="section-card">
          <div className="section-card-header">
            <h3>Site Data Upload</h3>
          </div>
          <div className="section-card-body">
            <p style={{ fontSize: 13, color: '#64748b', marginBottom: 16 }}>
              Upload an Excel file (.xlsx) with columns: <strong>Site_ID, Latitude, Longitude, Zone</strong>
            </p>
            <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8, padding: 16, marginBottom: 16 }}>
              <div style={{ fontSize: 12, color: '#64748b', marginBottom: 8 }}>Sites in database: <strong style={{ color: '#00338D', fontSize: 16 }}>{siteCount}</strong></div>
            </div>
            <input
              type="file"
              accept=".xlsx,.xls"
              onChange={e => { setSiteFile(e.target.files[0]); setSiteResult(null); setError(''); }}
              style={{ display: 'block', marginBottom: 12, fontSize: 13 }}
            />
            <button
              className="btn btn-primary btn-sm"
              onClick={uploadSites}
              disabled={!siteFile || uploading.sites}
            >
              {uploading.sites ? 'Uploading...' : 'Upload Site Data'}
            </button>
            {siteResult && (
              <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
                <strong style={{ color: '#16a34a' }}>Upload Successful</strong>
                <div style={{ marginTop: 6, color: '#475569' }}>
                  Created: {siteResult.created} &middot; Updated: {siteResult.updated} &middot; Total: {siteResult.total}
                </div>
                {siteResult.skipped?.length > 0 && (
                  <div style={{ marginTop: 6, color: '#d97706', fontSize: 12 }}>
                    Skipped: {siteResult.skipped.slice(0, 5).join('; ')}
                    {siteResult.skipped.length > 5 && ` ... and ${siteResult.skipped.length - 5} more`}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* KPI Data Upload */}
        <div className="section-card">
          <div className="section-card-header">
            <h3>KPI Data Upload</h3>
          </div>
          <div className="section-card-body">
            <p style={{ fontSize: 13, color: '#64748b', marginBottom: 16 }}>
              Upload one Excel file per KPI with columns: <strong>Date, Hour, Site_ID, Value</strong>
            </p>
            <div style={{ marginBottom: 12 }}>
              <label style={{ fontSize: 12, fontWeight: 600, color: '#475569', display: 'block', marginBottom: 4 }}>KPI Name</label>
              <input
                type="text"
                placeholder="e.g., CSSR, CDR, HOSR, Throughput_DL..."
                value={kpiName}
                onChange={e => setKpiName(e.target.value)}
                style={{ width: '100%', padding: '8px 12px', border: '1px solid #e2e8f0', borderRadius: 6, fontSize: 13 }}
              />
            </div>
            <input
              type="file"
              accept=".xlsx,.xls"
              onChange={e => { setKpiFile(e.target.files[0]); setKpiResult(null); setError(''); }}
              style={{ display: 'block', marginBottom: 12, fontSize: 13 }}
            />
            <button
              className="btn btn-primary btn-sm"
              onClick={uploadKpi}
              disabled={!kpiFile || !kpiName.trim() || uploading.kpi}
            >
              {uploading.kpi ? 'Uploading...' : 'Upload KPI Data'}
            </button>
            {kpiResult && (
              <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
                <strong style={{ color: '#16a34a' }}>Upload Successful</strong>
                <div style={{ marginTop: 6, color: '#475569' }}>
                  KPI: <strong>{kpiResult.kpi_name}</strong> &middot; Rows inserted: {kpiResult.inserted}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Uploaded KPIs list */}
      {kpiList.length > 0 && (
        <div className="section-card" style={{ marginTop: 24 }}>
          <div className="section-card-header">
            <h3>Uploaded KPIs ({kpiList.length})</h3>
          </div>
          <div className="section-card-body">
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 10 }}>
              {kpiList.map((k, i) => (
                <div key={i} style={{
                  background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8,
                  padding: '10px 14px', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                }}>
                  <span style={{ fontSize: 13, fontWeight: 600, color: '#0f172a' }}>{k.name}</span>
                  <span style={{ fontSize: 12, color: '#64748b' }}>{k.rows.toLocaleString()} rows</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
