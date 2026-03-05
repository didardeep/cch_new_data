import { useState, useEffect, useCallback } from 'react';
import { getToken } from '../../api';

const API_BASE = process.env.REACT_APP_API_URL || '';

export default function DataUpload() {
  const [siteFile, setSiteFile] = useState(null);
  const [siteLevelFile, setSiteLevelFile] = useState(null);
  const [cellLevelFile, setCellLevelFile] = useState(null);
  const [siteResult, setSiteResult] = useState(null);
  const [siteLevelResult, setSiteLevelResult] = useState(null);
  const [cellLevelResult, setCellLevelResult] = useState(null);
  const [uploading, setUploading] = useState({ sites: false, siteLevel: false, cellLevel: false });
  const [deleting, setDeleting] = useState({ sites: false, siteLevel: false, cellLevel: false });
  const [siteKpiList, setSiteKpiList] = useState([]);
  const [cellKpiList, setCellKpiList] = useState([]);
  const [siteCount, setSiteCount] = useState(0);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const fetchKpiList = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/admin/uploaded-kpis`, {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (d.site_kpis) setSiteKpiList(d.site_kpis);
      if (d.cell_kpis) setCellKpiList(d.cell_kpis);
      if (d.site_count !== undefined) setSiteCount(d.site_count);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchKpiList(); }, [fetchKpiList]);

  const uploadSites = async () => {
    if (!siteFile) return;
    setUploading(p => ({ ...p, sites: true }));
    setSiteResult(null); setError(''); setSuccess('');
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
    } catch (e) { setError('Upload failed: ' + e.message); }
    setUploading(p => ({ ...p, sites: false }));
  };

  const deleteSites = async () => {
    if (!window.confirm('Delete ALL site data from database? This cannot be undone.')) return;
    setDeleting(p => ({ ...p, sites: true }));
    setError(''); setSuccess('');
    try {
      const resp = await fetch(`${API_BASE}/api/admin/delete-sites`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (resp.ok) {
        setSuccess(`Deleted ${d.deleted} sites from database.`);
        setSiteResult(null);
        fetchKpiList();
      } else { setError(d.error || 'Delete failed'); }
    } catch (e) { setError('Delete failed: ' + e.message); }
    setDeleting(p => ({ ...p, sites: false }));
  };

  const uploadSiteLevel = async () => {
    if (!siteLevelFile) return;
    setUploading(p => ({ ...p, siteLevel: true }));
    setSiteLevelResult(null); setError(''); setSuccess('');
    try {
      const form = new FormData();
      form.append('file', siteLevelFile);
      const resp = await fetch(`${API_BASE}/api/admin/upload-kpi-site-level`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setSiteLevelResult(d);
        setSiteLevelFile(null);
        fetchKpiList();
      } else { setError(d.error || 'Upload failed'); }
    } catch (e) { setError('Upload failed: ' + e.message); }
    setUploading(p => ({ ...p, siteLevel: false }));
  };

  const deleteSiteLevel = async () => {
    if (!window.confirm('Delete ALL site-level KPI data from database? This cannot be undone.')) return;
    setDeleting(p => ({ ...p, siteLevel: true }));
    setError(''); setSuccess('');
    try {
      const resp = await fetch(`${API_BASE}/api/admin/delete-kpi-site-level`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (resp.ok) {
        setSuccess(`Deleted ${d.deleted} site-level KPI records.`);
        setSiteLevelResult(null);
        fetchKpiList();
      } else { setError(d.error || 'Delete failed'); }
    } catch (e) { setError('Delete failed: ' + e.message); }
    setDeleting(p => ({ ...p, siteLevel: false }));
  };

  const uploadCellLevel = async () => {
    if (!cellLevelFile) return;
    setUploading(p => ({ ...p, cellLevel: true }));
    setCellLevelResult(null); setError(''); setSuccess('');
    try {
      const form = new FormData();
      form.append('file', cellLevelFile);
      const resp = await fetch(`${API_BASE}/api/admin/upload-kpi-cell-level`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setCellLevelResult(d);
        setCellLevelFile(null);
        fetchKpiList();
      } else { setError(d.error || 'Upload failed'); }
    } catch (e) { setError('Upload failed: ' + e.message); }
    setUploading(p => ({ ...p, cellLevel: false }));
  };

  const deleteCellLevel = async () => {
    if (!window.confirm('Delete ALL cell-level KPI data from database? This cannot be undone.')) return;
    setDeleting(p => ({ ...p, cellLevel: true }));
    setError(''); setSuccess('');
    try {
      const resp = await fetch(`${API_BASE}/api/admin/delete-kpi-cell-level`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (resp.ok) {
        setSuccess(`Deleted ${d.deleted} cell-level KPI records.`);
        setCellLevelResult(null);
        fetchKpiList();
      } else { setError(d.error || 'Delete failed'); }
    } catch (e) { setError('Delete failed: ' + e.message); }
    setDeleting(p => ({ ...p, cellLevel: false }));
  };

  const totalSiteRows = siteKpiList.reduce((sum, k) => sum + k.rows, 0);
  const totalCellRows = cellKpiList.reduce((sum, k) => sum + k.rows, 0);

  return (
    <div>
      <div className="page-header">
        <h1>Data Upload</h1>
        <p>Upload telecom site data and KPI Excel workbooks for network diagnosis.</p>
      </div>

      {error && (
        <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8, padding: '12px 16px', marginBottom: 20, color: '#dc2626', fontSize: 13, fontWeight: 600 }}>
          {error}
        </div>
      )}
      {success && (
        <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '12px 16px', marginBottom: 20, color: '#16a34a', fontSize: 13, fontWeight: 600 }}>
          {success}
        </div>
      )}

      {/* ── Site Data Upload ────────────────────────────────────────── */}
      <div className="section-card" style={{ marginBottom: 24 }}>
        <div className="section-card-header">
          <h3>Site Data Upload</h3>
        </div>
        <div className="section-card-body">
          <p style={{ fontSize: 13, color: '#64748b', marginBottom: 16 }}>
            Upload an Excel file (.xlsx) with columns: <strong>Site_ID, Latitude, Longitude, Zone, Status, Alarm, Solution</strong>
          </p>
          <div style={{ display: 'flex', gap: 16, alignItems: 'center', marginBottom: 16 }}>
            <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8, padding: '10px 16px' }}>
              <div style={{ fontSize: 12, color: '#64748b' }}>Sites in database</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#00338D' }}>{siteCount}</div>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <input type="file" accept=".xlsx,.xls"
              onChange={e => { setSiteFile(e.target.files[0]); setSiteResult(null); setError(''); setSuccess(''); }}
              style={{ fontSize: 13 }} />
            <button className="btn btn-primary btn-sm" onClick={uploadSites}
              disabled={!siteFile || uploading.sites}>
              {uploading.sites ? 'Uploading...' : 'Upload Site Data'}
            </button>
            <button className="btn btn-sm" onClick={deleteSites}
              disabled={deleting.sites || siteCount === 0}
              style={{ background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: siteCount === 0 ? 'not-allowed' : 'pointer', opacity: siteCount === 0 ? 0.5 : 1 }}>
              {deleting.sites ? 'Deleting...' : 'Delete All Sites'}
            </button>
          </div>
          {siteResult && (
            <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
              <strong style={{ color: '#16a34a' }}>Upload Successful</strong>
              <div style={{ marginTop: 6, color: '#475569' }}>
                Total rows processed: {siteResult.total ?? 0}
              </div>
              {siteResult.skipped?.length > 0 && (
                <div style={{ marginTop: 6, color: '#d97706', fontSize: 12 }}>
                  Skipped ({siteResult.skipped.length}): {siteResult.skipped.slice(0, 5).join('; ')}
                  {siteResult.skipped.length > 5 && ` ... and ${siteResult.skipped.length - 5} more`}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── KPI Data Upload ─────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginBottom: 24 }}>

        {/* Site Level Data Upload */}
        <div className="section-card">
          <div className="section-card-header">
            <h3>Site Level Data Upload</h3>
          </div>
          <div className="section-card-body">
            <p style={{ fontSize: 13, color: '#64748b', marginBottom: 12 }}>
              Upload an Excel workbook (.xlsx) with <strong>27 sheets</strong>. Each sheet name = KPI name.<br/>
              Sheet columns: <strong>Site_ID</strong>, then <strong>date columns</strong> with values.
            </p>
            <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8, padding: '10px 14px', marginBottom: 12 }}>
              <div style={{ fontSize: 12, color: '#64748b' }}>Site-level KPIs uploaded: <strong style={{ color: '#00338D' }}>{siteKpiList.length}</strong></div>
              <div style={{ fontSize: 12, color: '#64748b' }}>Total records: <strong style={{ color: '#00338D' }}>{totalSiteRows.toLocaleString()}</strong></div>
            </div>
            <input type="file" accept=".xlsx,.xls"
              onChange={e => { setSiteLevelFile(e.target.files[0]); setSiteLevelResult(null); setError(''); setSuccess(''); }}
              style={{ display: 'block', marginBottom: 12, fontSize: 13 }} />
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={uploadSiteLevel}
                disabled={!siteLevelFile || uploading.siteLevel}>
                {uploading.siteLevel ? 'Uploading...' : 'Upload Site Level Data'}
              </button>
              <button className="btn btn-sm" onClick={deleteSiteLevel}
                disabled={deleting.siteLevel || siteKpiList.length === 0}
                style={{ background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: siteKpiList.length === 0 ? 'not-allowed' : 'pointer', opacity: siteKpiList.length === 0 ? 0.5 : 1 }}>
                {deleting.siteLevel ? 'Deleting...' : 'Delete All'}
              </button>
            </div>
            {siteLevelResult && (
              <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
                <strong style={{ color: '#16a34a' }}>Upload Successful</strong>
                <div style={{ marginTop: 6, color: '#475569' }}>
                  {siteLevelResult.message || 'Site-level KPI upload completed.'}
                </div>
                {siteLevelResult.errors?.length > 0 && (
                  <div style={{ marginTop: 6, color: '#d97706', fontSize: 12 }}>
                    {siteLevelResult.errors.join('; ')}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Cell Level Data Upload */}
        <div className="section-card">
          <div className="section-card-header">
            <h3>Cell Level Data Upload</h3>
          </div>
          <div className="section-card-body">
            <p style={{ fontSize: 13, color: '#64748b', marginBottom: 12 }}>
              Upload an Excel workbook (.xlsx) with <strong>27 sheets</strong>. Each sheet name = KPI name.<br/>
              Sheet columns: <strong>Site_ID, Cell_ID, Cell_Site_ID</strong>, then <strong>date columns</strong> with values.
            </p>
            <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8, padding: '10px 14px', marginBottom: 12 }}>
              <div style={{ fontSize: 12, color: '#64748b' }}>Cell-level KPIs uploaded: <strong style={{ color: '#00338D' }}>{cellKpiList.length}</strong></div>
              <div style={{ fontSize: 12, color: '#64748b' }}>Total records: <strong style={{ color: '#00338D' }}>{totalCellRows.toLocaleString()}</strong></div>
            </div>
            <input type="file" accept=".xlsx,.xls"
              onChange={e => { setCellLevelFile(e.target.files[0]); setCellLevelResult(null); setError(''); setSuccess(''); }}
              style={{ display: 'block', marginBottom: 12, fontSize: 13 }} />
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-primary btn-sm" onClick={uploadCellLevel}
                disabled={!cellLevelFile || uploading.cellLevel}>
                {uploading.cellLevel ? 'Uploading...' : 'Upload Cell Level Data'}
              </button>
              <button className="btn btn-sm" onClick={deleteCellLevel}
                disabled={deleting.cellLevel || cellKpiList.length === 0}
                style={{ background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: cellKpiList.length === 0 ? 'not-allowed' : 'pointer', opacity: cellKpiList.length === 0 ? 0.5 : 1 }}>
                {deleting.cellLevel ? 'Deleting...' : 'Delete All'}
              </button>
            </div>
            {cellLevelResult && (
              <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
                <strong style={{ color: '#16a34a' }}>Upload Successful</strong>
                <div style={{ marginTop: 6, color: '#475569' }}>
                  {cellLevelResult.message || 'Cell-level KPI upload completed.'}
                </div>
                {cellLevelResult.errors?.length > 0 && (
                  <div style={{ marginTop: 6, color: '#d97706', fontSize: 12 }}>
                    {cellLevelResult.errors.join('; ')}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── Uploaded KPIs Lists ─────────────────────────────────────── */}
      {siteKpiList.length > 0 && (
        <div className="section-card" style={{ marginBottom: 24 }}>
          <div className="section-card-header">
            <h3>Uploaded Site-Level KPIs ({siteKpiList.length})</h3>
          </div>
          <div className="section-card-body">
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 10 }}>
              {siteKpiList.map((k, i) => (
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

      {cellKpiList.length > 0 && (
        <div className="section-card">
          <div className="section-card-header">
            <h3>Uploaded Cell-Level KPIs ({cellKpiList.length})</h3>
          </div>
          <div className="section-card-body">
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 10 }}>
              {cellKpiList.map((k, i) => (
                <div key={i} style={{
                  background: '#fdf4ff', border: '1px solid #e9d5ff', borderRadius: 8,
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
