import { useState, useEffect, useCallback } from 'react';
import { getToken } from '../../api';

const API_BASE = process.env.REACT_APP_API_URL || '';

export default function DataUpload() {
  const [siteFile, setSiteFile] = useState(null);
  const [siteLevelFile, setSiteLevelFile] = useState(null);
  const [cellLevelFile, setCellLevelFile] = useState(null);
  const [networkKpiFile, setNetworkKpiFile] = useState(null);
  const [siteResult, setSiteResult] = useState(null);
  const [siteLevelResult, setSiteLevelResult] = useState(null);
  const [cellLevelResult, setCellLevelResult] = useState(null);
  const [networkKpiResult, setNetworkKpiResult] = useState(null);
  const [uploading, setUploading] = useState({ sites: false, siteLevel: false, cellLevel: false, networkKpi: false });
  const [deleting, setDeleting] = useState({ sites: false, siteLevel: false, cellLevel: false, networkKpi: false });
  const [siteKpiList, setSiteKpiList] = useState([]);
  const [cellKpiList, setCellKpiList] = useState([]);
  const [networkKpiCount, setNetworkKpiCount] = useState(0);
  const [siteCount, setSiteCount] = useState(0);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  // ── NEW: flexible KPI upload state (Core + Revenue)
  const [coreKpiFile, setCoreKpiFile] = useState(null);
  const [revenueKpiFile, setRevenueKpiFile] = useState(null);
  const [coreKpiResult, setCoreKpiResult] = useState(null);
  const [revenueKpiResult, setRevenueKpiResult] = useState(null);
  const [uploadingFlex, setUploadingFlex] = useState({ core: false, revenue: false });
  const [deletingFlex, setDeletingFlex] = useState({ core: false, revenue: false });
  const [coreKpiStatus, setCoreKpiStatus] = useState(null);
  const [revenueKpiStatus, setRevenueKpiStatus] = useState(null);

  // ── Business KPI upload (Site Users + Site Revenue)
  const [businessKpiFile, setBusinessKpiFile] = useState(null);
  const [businessKpiResult, setBusinessKpiResult] = useState(null);
  const [uploadingBusiness, setUploadingBusiness] = useState(false);
  const [deletingBusiness, setDeletingBusiness] = useState(false);
  const [businessKpiStatus, setBusinessKpiStatus] = useState(null);

  // ── Transport KPI upload (inside Core section)
  const [transportFile, setTransportFile] = useState(null);
  const [transportResult, setTransportResult] = useState(null);
  const [uploadingTransport, setUploadingTransport] = useState(false);
  const [deletingTransport, setDeletingTransport] = useState(false);
  const [transportStatus, setTransportStatus] = useState(null);

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

  const fetchNetworkKpiCount = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/network/summary`, {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      if (resp.ok) {
        const d = await resp.json();
        setNetworkKpiCount(d.total_sites ?? 0);
      }
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchKpiList(); fetchNetworkKpiCount(); }, [fetchKpiList, fetchNetworkKpiCount]);

  // ── NEW: fetch flexible KPI status for Core and Revenue
  const fetchFlexStatus = useCallback(async (kpiType, setter) => {
    try {
      const resp = await fetch(`${API_BASE}/api/admin/flexible-kpi-status?type=${kpiType}`, {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      if (resp.ok) setter(await resp.json());
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    fetchFlexStatus('core', setCoreKpiStatus);
    fetchFlexStatus('revenue', setRevenueKpiStatus);
  }, [fetchFlexStatus]);

  const fetchBusinessKpiStatus = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/admin/shared-site-workbook-summary`, {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      if (resp.ok) setBusinessKpiStatus(await resp.json());
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchBusinessKpiStatus(); }, [fetchBusinessKpiStatus]);

  const uploadBusinessKpi = async () => {
    if (!businessKpiFile) return;
    setUploadingBusiness(true);
    setBusinessKpiResult(null); setError(''); setSuccess('');
    try {
      const form = new FormData();
      form.append('file', businessKpiFile);
      const resp = await fetch(`${API_BASE}/api/admin/upload-shared-site-workbook`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setBusinessKpiResult(d);
        setBusinessKpiFile(null);
        fetchBusinessKpiStatus();
      } else { setError(d.error || 'Upload failed'); }
    } catch (e) { setError('Upload failed: ' + e.message); }
    setUploadingBusiness(false);
  };

  const deleteBusinessKpi = async () => {
    if (!window.confirm('Delete all Site Users & Site Revenue data? This cannot be undone.')) return;
    setDeletingBusiness(true); setError(''); setSuccess('');
    try {
      const resp = await fetch(`${API_BASE}/api/admin/delete-shared-site-workbook`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (resp.ok) { setSuccess(`Deleted ${d.deleted} records.`); setBusinessKpiResult(null); fetchBusinessKpiStatus(); }
      else { setError(d.error || 'Delete failed'); }
    } catch (e) { setError('Delete failed: ' + e.message); }
    setDeletingBusiness(false);
  };

  // ── Transport KPI helpers ─────────────────────────────────────────────────
  const fetchTransportStatus = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/api/admin/transport-kpi-status`, {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      if (resp.ok) setTransportStatus(await resp.json());
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchTransportStatus(); }, [fetchTransportStatus]);

  const uploadTransport = async () => {
    if (!transportFile) return;
    setUploadingTransport(true);
    setTransportResult(null); setError(''); setSuccess('');
    try {
      const form = new FormData();
      form.append('file', transportFile);
      const resp = await fetch(`${API_BASE}/api/admin/upload-transport-data`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setTransportResult(d);
        setTransportFile(null);
        setSuccess(`Transport data uploaded: ${d.records_processed?.toLocaleString()} records, ${d.unique_sites?.toLocaleString()} sites.`);
        fetchTransportStatus();
      } else {
        setError(d.error || 'Transport upload failed');
      }
    } catch (e) { setError('Upload failed: ' + e.message); }
    setUploadingTransport(false);
  };

  const deleteTransport = async () => {
    if (!window.confirm('Delete ALL transport KPI data? This cannot be undone.')) return;
    setDeletingTransport(true);
    setError(''); setSuccess('');
    try {
      const resp = await fetch(`${API_BASE}/api/admin/delete-transport-data`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (resp.ok) {
        setSuccess(`Deleted ${d.deleted?.toLocaleString()} transport records.`);
        setTransportStatus(null);
        setTransportResult(null);
      } else { setError(d.error || 'Delete failed'); }
    } catch (e) { setError('Delete failed: ' + e.message); }
    setDeletingTransport(false);
  };

  const uploadFlexKpi = async (kpiType, file, setFile, setResult, setter) => {
    if (!file) return;
    setUploadingFlex(p => ({ ...p, [kpiType]: true }));
    setResult(null); setError(''); setSuccess('');
    try {
      const form = new FormData();
      form.append('file', file);
      const resp = await fetch(`${API_BASE}/api/admin/upload-flexible-kpi?type=${kpiType}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setResult(d);
        setFile(null);
        fetchFlexStatus(kpiType, setter);
      } else { setError(d.error || 'Upload failed'); }
    } catch (e) { setError('Upload failed: ' + e.message); }
    setUploadingFlex(p => ({ ...p, [kpiType]: false }));
  };

  const deleteFlexKpi = async (kpiType, setResult, setter) => {
    if (!window.confirm(`Delete ALL ${kpiType} KPI data? This cannot be undone.`)) return;
    setDeletingFlex(p => ({ ...p, [kpiType]: true }));
    setError(''); setSuccess('');
    try {
      const resp = await fetch(`${API_BASE}/api/admin/delete-flexible-kpi?type=${kpiType}`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (resp.ok) {
        setSuccess(`Deleted ${d.deleted} ${kpiType} KPI records.`);
        setResult(null);
        fetchFlexStatus(kpiType, setter);
      } else { setError(d.error || 'Delete failed'); }
    } catch (e) { setError('Delete failed: ' + e.message); }
    setDeletingFlex(p => ({ ...p, [kpiType]: false }));
  };

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

  const uploadNetworkKpi = async () => {
    if (!networkKpiFile) return;
    setUploading(p => ({ ...p, networkKpi: true }));
    setNetworkKpiResult(null); setError(''); setSuccess('');
    try {
      const form = new FormData();
      form.append('file', networkKpiFile);
      const resp = await fetch(`${API_BASE}/api/admin/upload-network-data`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${getToken()}` },
        body: form,
      });
      const d = await resp.json();
      if (resp.ok) {
        setNetworkKpiResult(d);
        setNetworkKpiFile(null);
        fetchNetworkKpiCount();
      } else { setError(d.error || 'Upload failed'); }
    } catch (e) { setError('Upload failed: ' + e.message); }
    setUploading(p => ({ ...p, networkKpi: false }));
  };

  const deleteNetworkKpi = async () => {
    if (!window.confirm('Delete ALL network analytics KPI data? This cannot be undone.')) return;
    setDeleting(p => ({ ...p, networkKpi: true }));
    setError(''); setSuccess('');
    try {
      const resp = await fetch(`${API_BASE}/api/admin/delete-network-data`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const d = await resp.json();
      if (resp.ok) {
        setSuccess(`Deleted ${d.deleted} network KPI records.`);
        setNetworkKpiResult(null);
        fetchNetworkKpiCount();
      } else { setError(d.error || 'Delete failed'); }
    } catch (e) { setError('Delete failed: ' + e.message); }
    setDeleting(p => ({ ...p, networkKpi: false }));
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

      {/* ── Site Data Upload ─────────────────────────────────────────── */}
      <div className="section-card" style={{ marginBottom: 24 }}>
        <div className="section-card-header">
          <h3>Site Data Upload</h3>
        </div>
        <div className="section-card-body">
          <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 16 }}>
            Upload an Excel file (.xlsx) containing site data.
          </p>
          <div style={{ display: 'flex', gap: 16, alignItems: 'center', marginBottom: 16 }}>
            <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 16px' }}>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Sites in database</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--primary)' }}>{siteCount}</div>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <input type="file" accept=".xlsx,.xlsm"
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
              <div style={{ marginTop: 6, color: 'var(--text-secondary)' }}>
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
            <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 }}>
              Upload an Excel workbook (.xlsx/.xlsm) with <strong>27 sheets</strong>. Each sheet name = KPI name.<br/>
              Sheet columns: <strong>Site_ID</strong>, then <strong>date columns</strong> with values.
            </p>
            <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 14px', marginBottom: 12 }}>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Site-level KPIs uploaded: <strong style={{ color: 'var(--primary)' }}>{siteKpiList.length}</strong></div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Total records: <strong style={{ color: 'var(--primary)' }}>{totalSiteRows.toLocaleString()}</strong></div>
            </div>
            <input type="file" accept=".xlsx,.xlsm"
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
                <div style={{ marginTop: 6, color: 'var(--text-secondary)' }}>
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
            <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 }}>
              Upload an Excel workbook (.xlsx/.xlsm) with <strong>27 sheets</strong>. Each sheet name = KPI name.<br/>
              Sheet columns: <strong>Site_ID, Cell_ID, Cell_Site_ID</strong>, then <strong>date columns</strong> with values.
            </p>
            <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 14px', marginBottom: 12 }}>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Cell-level KPIs uploaded: <strong style={{ color: 'var(--primary)' }}>{cellKpiList.length}</strong></div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Total records: <strong style={{ color: 'var(--primary)' }}>{totalCellRows.toLocaleString()}</strong></div>
            </div>
            <input type="file" accept=".xlsx,.xlsm"
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
                <div style={{ marginTop: 6, color: 'var(--text-secondary)' }}>
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

      {/* ── Network Analytics KPI Upload ─────────────────────────────── */}
      <div className="section-card" style={{ marginBottom: 24, borderTop: '3px solid var(--primary)' }}>
        <div className="section-card-header" style={{ background: 'linear-gradient(135deg, #f0f4ff 0%, #e8f0fe 100%)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="2"/><path d="M16.24 7.76a6 6 0 010 8.49M7.76 16.24a6 6 0 010-8.49M20.49 3.51a12 12 0 010 16.99M3.51 20.49a12 12 0 010-16.99"/>
            </svg>
            <h3 style={{ color: 'var(--primary)', margin: 0 }}>Network Analytics KPI Upload</h3>
            <span style={{ marginLeft: 8, background: 'var(--primary)', color: '#fff', fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 20 }}>NEW</span>
          </div>
        </div>
        <div className="section-card-body">
          <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 }}>
            Upload a flat Excel/CSV file for the <strong>Predictive Network Analysis</strong> dashboard visible to agents.
            Each row = one KPI snapshot. Columns are detected automatically — no fixed schema required.
          </p>
          <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 14px', marginBottom: 14, fontSize: 12, color: '#1e40af' }}>
            <strong>Required columns:</strong> <code>Site_ID</code>, <code>Timestamp</code><br/>
            <strong>Optional columns:</strong> Region, Cluster, Cell_ID, Latitude, Longitude, Technology,
            Active_Users, PRB_Utilization, RSRP, SINR, Throughput_DL, Throughput_UL,
            Packet_Loss, Latency, Call_Drop_Rate, Availability, Traffic_Volume
          </div>
          <div style={{ display: 'flex', gap: 16, alignItems: 'center', marginBottom: 14 }}>
            <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 16px' }}>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Unique sites in analytics DB</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--primary)' }}>{networkKpiCount}</div>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <input type="file" accept=".xlsx,.xls,.csv"
              onChange={e => { setNetworkKpiFile(e.target.files[0]); setNetworkKpiResult(null); setError(''); setSuccess(''); }}
              style={{ fontSize: 13 }} />
            <button className="btn btn-primary btn-sm" onClick={uploadNetworkKpi}
              disabled={!networkKpiFile || uploading.networkKpi}
              style={{ background: 'var(--primary)', borderColor: 'var(--primary)' }}>
              {uploading.networkKpi ? 'Uploading...' : 'Upload Network KPI Data'}
            </button>
            <button className="btn btn-sm" onClick={deleteNetworkKpi}
              disabled={deleting.networkKpi || networkKpiCount === 0}
              style={{ background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: networkKpiCount === 0 ? 'not-allowed' : 'pointer', opacity: networkKpiCount === 0 ? 0.5 : 1 }}>
              {deleting.networkKpi ? 'Deleting...' : 'Delete All Network KPIs'}
            </button>
          </div>
          {networkKpiResult && (
            <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
              <strong style={{ color: '#16a34a' }}>Upload Successful</strong>
              <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: 8 }}>
                <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, padding: '8px 12px' }}>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Records Processed</div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: 'var(--primary)' }}>{(networkKpiResult.records_processed ?? 0).toLocaleString()}</div>
                </div>
                <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, padding: '8px 12px' }}>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Columns Detected</div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: 'var(--primary)' }}>{(networkKpiResult.columns_detected ?? []).length}</div>
                </div>
                {networkKpiResult.time_range?.from && (
                  <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, padding: '8px 12px', gridColumn: 'span 2' }}>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Data Time Range</div>
                    <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-secondary)' }}>
                      {networkKpiResult.time_range.from?.slice(0, 19)} → {networkKpiResult.time_range.to?.slice(0, 19)}
                    </div>
                  </div>
                )}
              </div>
              {networkKpiResult.extra_columns?.length > 0 && (
                <div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-secondary)' }}>
                  Extra columns stored in JSONB: <em>{networkKpiResult.extra_columns.join(', ')}</em>
                </div>
              )}
            </div>
          )}
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
                  <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{k.name}</span>
                  <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{k.rows.toLocaleString()} rows</span>
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
                  <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>{k.name}</span>
                  <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{k.rows.toLocaleString()} rows</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ── NEW: Core KPI Upload (flexible — only Site_ID mandatory) ── */}
      <div className="section-card" style={{ marginTop: 24, borderTop: '3px solid #7c3aed' }}>
        <div className="section-card-header" style={{ background: 'linear-gradient(135deg, #f5f3ff 0%, #ede9fe 100%)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ fontSize: 18 }}>🖥</span>
            <h3 style={{ color: '#7c3aed', margin: 0 }}>Core KPI Upload</h3>
            <span style={{ marginLeft: 8, background: '#7c3aed', color: '#fff', fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 20 }}>FLEXIBLE</span>
          </div>
        </div>
        <div className="section-card-body">
          <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 }}>
            Upload your Core Network KPI file (Excel or CSV) for the <strong>Core Network tab</strong> in the
            agent's Network Analysis dashboard. Only <code>Site_ID</code> is mandatory — all other columns
            (Auth Success Rate, CPU Utilization, Attach Success Rate, PDP Bearer SR, or any custom columns)
            are <strong>auto-detected</strong>, typed and stored. Each upload replaces the previous data.
          </p>
          <div style={{ background: '#f5f3ff', border: '1px solid #ddd6fe', borderRadius: 8, padding: '10px 14px', marginBottom: 14, fontSize: 12, color: '#5b21b6' }}>
            <strong>Only mandatory column:</strong> <code>Site_ID</code> (case-insensitive) — all other column names, types and units are identified automatically.
          </div>

          {/* Status row */}
          {coreKpiStatus && (
            <div style={{ display: 'flex', gap: 12, marginBottom: 14, flexWrap: 'wrap' }}>
              {[
                { label: 'Unique Sites', value: coreKpiStatus.unique_sites ?? 0, color: '#7c3aed' },
                { label: 'Columns Detected', value: coreKpiStatus.unique_columns ?? 0, color: '#6d28d9' },
                { label: 'Total Records', value: (coreKpiStatus.total_rows ?? 0).toLocaleString(), color: '#059669' },
                ...(coreKpiStatus.date_range?.from ? [{ label: 'Date Range', value: `${coreKpiStatus.date_range.from} → ${coreKpiStatus.date_range.to}`, color: '#0369a1' }] : []),
              ].map((s, i) => (
                <div key={i} style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 16px' }}>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.label}</div>
                  <div style={{ fontSize: 20, fontWeight: 700, color: s.color }}>{s.value}</div>
                </div>
              ))}
            </div>
          )}

          {/* Detected columns */}
          {coreKpiStatus?.columns?.length > 0 && (
            <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 14px', marginBottom: 14 }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 700, marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Stored Columns</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                {coreKpiStatus.columns.map((c, i) => (
                  <span key={i} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px', borderRadius: 12, fontSize: 11, fontWeight: 600, background: '#ede9fe', border: '1px solid #ddd6fe', color: '#5b21b6', margin: '2px 0' }}>
                    {c.column_label || c.column_name}
                    {c.unit && <span style={{ fontSize: 9, opacity: 0.7 }}>({c.unit})</span>}
                    <span style={{ fontSize: 9, padding: '0 4px', borderRadius: 6, background: c.column_type === 'numeric' ? '#dcfce7' : '#fef3c7', color: c.column_type === 'numeric' ? '#166534' : '#92400e', fontWeight: 700 }}>{c.column_type}</span>
                  </span>
                ))}
              </div>
            </div>
          )}

          <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <input type="file" accept=".xlsx,.xls,.csv"
              onChange={e => { setCoreKpiFile(e.target.files[0]); setCoreKpiResult(null); setError(''); setSuccess(''); }}
              style={{ fontSize: 13 }} />
            <button className="btn btn-primary btn-sm"
              onClick={() => uploadFlexKpi('core', coreKpiFile, setCoreKpiFile, setCoreKpiResult, setCoreKpiStatus)}
              disabled={!coreKpiFile || uploadingFlex.core}
              style={{ background: '#7c3aed', borderColor: '#7c3aed' }}>
              {uploadingFlex.core ? 'Uploading…' : 'Upload Core KPI Data'}
            </button>
            <button className="btn btn-sm"
              onClick={() => deleteFlexKpi('core', setCoreKpiResult, setCoreKpiStatus)}
              disabled={deletingFlex.core || !coreKpiStatus?.unique_sites}
              style={{ background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: !coreKpiStatus?.unique_sites ? 'not-allowed' : 'pointer', opacity: !coreKpiStatus?.unique_sites ? 0.5 : 1 }}>
              {deletingFlex.core ? 'Deleting…' : 'Delete All'}
            </button>
          </div>

          {coreKpiResult && (
            <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
              <strong style={{ color: '#16a34a' }}>✅ Upload Successful</strong>
              <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 8 }}>
                {[
                  { label: 'Rows in File', value: coreKpiResult.rows_in_file ?? 0 },
                  { label: 'Records Inserted', value: coreKpiResult.records_inserted ?? 0 },
                  { label: 'Unique Sites', value: coreKpiResult.unique_sites ?? 0 },
                  { label: 'Columns Found', value: (coreKpiResult.columns_detected ?? []).length },
                ].map((s, i) => (
                  <div key={i} style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, padding: '8px 12px' }}>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.label}</div>
                    <div style={{ fontSize: 18, fontWeight: 700, color: '#7c3aed' }}>{typeof s.value === 'number' ? s.value.toLocaleString() : s.value}</div>
                  </div>
                ))}
              </div>
              {coreKpiResult.columns_detected?.length > 0 && (
                <div style={{ marginTop: 10, fontSize: 12, color: 'var(--text-secondary)' }}>
                  <strong>Auto-detected columns:</strong> {coreKpiResult.columns_detected.join(', ')}
                </div>
              )}
            </div>
          )}

          {/* ── Transport KPI Upload — nested inside Core section ── */}
          <div style={{ marginTop: 24, paddingTop: 20, borderTop: '1px dashed #c4b5fd' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12 }}>
              <span style={{ fontSize: 18 }}>🔌</span>
              <div>
                <h4 style={{ margin: 0, color: '#5b21b6', fontSize: 14, fontWeight: 700 }}>Transport KPI Upload</h4>
                <p style={{ margin: 0, fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>
                  Only <code>Site_ID</code> is mandatory — Zone, Backhaul Type, Utilization, Latency, Jitter, Packet Loss etc. are <strong>auto-detected</strong>.
                </p>
              </div>
            </div>

            {/* Transport status row */}
            {transportStatus && (
              <div style={{ display: 'flex', gap: 12, marginBottom: 14, flexWrap: 'wrap' }}>
                {[
                  { label: 'Unique Sites',      value: transportStatus.unique_sites ?? 0,                    color: '#5b21b6' },
                  { label: 'Total Records',      value: (transportStatus.total_rows ?? 0).toLocaleString(),   color: '#6d28d9' },
                  { label: 'Columns Detected',   value: transportStatus.unique_columns ?? 0,                  color: '#059669' },
                ].map((s, i) => (
                  <div key={i} style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '9px 14px' }}>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.label}</div>
                    <div style={{ fontSize: 18, fontWeight: 700, color: s.color }}>{s.value}</div>
                  </div>
                ))}
              </div>
            )}

            {/* Detected columns badges */}
            {transportStatus?.columns?.length > 0 && (
              <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '9px 13px', marginBottom: 14 }}>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 700, marginBottom: 7, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Stored Columns</div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                  {transportStatus.columns.map((c, i) => (
                    <span key={i} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px', borderRadius: 12, fontSize: 11, fontWeight: 600, background: '#ede9fe', border: '1px solid #ddd6fe', color: '#5b21b6' }}>
                      {c}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Format hint */}
            <div style={{ background: '#f5f3ff', border: '1px solid #ddd6fe', borderRadius: 8, padding: '9px 13px', marginBottom: 14, fontSize: 12, color: '#5b21b6' }}>
              <strong>Accepted columns (all optional except Site_ID):</strong>{' '}
              Site_ID, Zone / Cluster, Backhaul_Type, Link_Capacity (Mbps), Avg_Utilization (%), Peak_Utilization (%),
              Packet_Loss (%), Avg_Latency (ms), Jitter (ms), Availability (%), Error_Rate (%), Throughput_Efficiency (%), Alarms
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
              <input
                type="file"
                accept=".xlsx,.xls,.csv"
                onChange={e => { setTransportFile(e.target.files[0]); setTransportResult(null); setError(''); setSuccess(''); }}
                style={{ fontSize: 13 }}
              />
              <button
                className="btn btn-primary btn-sm"
                onClick={uploadTransport}
                disabled={!transportFile || uploadingTransport}
                style={{ background: '#5b21b6', borderColor: '#5b21b6' }}
              >
                {uploadingTransport ? 'Uploading…' : '⬆ Upload Transport KPI Data'}
              </button>
              <button
                className="btn btn-sm"
                onClick={deleteTransport}
                disabled={deletingTransport || !transportStatus?.unique_sites}
                style={{
                  background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6,
                  padding: '6px 14px', fontSize: 12, fontWeight: 600,
                  cursor: !transportStatus?.unique_sites ? 'not-allowed' : 'pointer',
                  opacity: !transportStatus?.unique_sites ? 0.5 : 1,
                }}
              >
                {deletingTransport ? 'Deleting…' : 'Delete All'}
              </button>
            </div>

            {transportResult && (
              <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
                <strong style={{ color: '#16a34a' }}>✅ Transport Upload Successful</strong>
                <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 8 }}>
                  {[
                    { label: 'Records Inserted', value: transportResult.records_processed ?? 0 },
                    { label: 'Unique Sites',      value: transportResult.unique_sites ?? 0 },
                    { label: 'Columns Detected',  value: (transportResult.columns_detected ?? []).length },
                  ].map((s, i) => (
                    <div key={i} style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, padding: '8px 12px' }}>
                      <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.label}</div>
                      <div style={{ fontSize: 18, fontWeight: 700, color: '#5b21b6' }}>{typeof s.value === 'number' ? s.value.toLocaleString() : s.value}</div>
                    </div>
                  ))}
                </div>
                {transportResult.columns_detected?.length > 0 && (
                  <div style={{ marginTop: 10, fontSize: 12, color: 'var(--text-secondary)' }}>
                    <strong>Auto-detected columns:</strong> {transportResult.columns_detected.join(', ')}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── NEW: Revenue KPI Upload (flexible — only Site_ID mandatory) ── */}
      <div className="section-card" style={{ marginTop: 24, borderTop: '3px solid #059669' }}>
        <div className="section-card-header" style={{ background: 'linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ fontSize: 18 }}>💰</span>
            <h3 style={{ color: '#059669', margin: 0 }}>Revenue KPI Upload</h3>
            <span style={{ marginLeft: 8, background: '#059669', color: '#fff', fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 20 }}>FLEXIBLE</span>
          </div>
        </div>
        <div className="section-card-body">
          <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 }}>
            Upload your Revenue &amp; Financial KPI file (Excel or CSV) for the <strong>Revenue tab</strong> in the
            agent's Network Analysis dashboard. Only <code>Site_ID</code> is mandatory — typical columns like
            Revenue, OpEx, Subscribers, EBITDA, Site Category, Zone are all <strong>auto-detected</strong>.
            Each upload replaces the previous data.
          </p>
          <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '10px 14px', marginBottom: 14, fontSize: 12, color: '#166534' }}>
            <strong>Only mandatory column:</strong> <code>Site_ID</code> (case-insensitive) — all other column names, types and units are identified automatically.
          </div>

          {/* Status row */}
          {revenueKpiStatus && (
            <div style={{ display: 'flex', gap: 12, marginBottom: 14, flexWrap: 'wrap' }}>
              {[
                { label: 'Unique Sites', value: revenueKpiStatus.unique_sites ?? 0, color: '#059669' },
                { label: 'Columns Detected', value: revenueKpiStatus.unique_columns ?? 0, color: '#047857' },
                { label: 'Total Records', value: (revenueKpiStatus.total_rows ?? 0).toLocaleString(), color: '#7c3aed' },
              ].map((s, i) => (
                <div key={i} style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 16px' }}>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.label}</div>
                  <div style={{ fontSize: 20, fontWeight: 700, color: s.color }}>{s.value}</div>
                </div>
              ))}
            </div>
          )}

          {/* Detected columns */}
          {revenueKpiStatus?.columns?.length > 0 && (
            <div style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 14px', marginBottom: 14 }}>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 700, marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.05em' }}>Stored Columns</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                {revenueKpiStatus.columns.map((c, i) => (
                  <span key={i} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px', borderRadius: 12, fontSize: 11, fontWeight: 600, background: '#dcfce7', border: '1px solid #bbf7d0', color: '#166534', margin: '2px 0' }}>
                    {c.column_label || c.column_name}
                    {c.unit && <span style={{ fontSize: 9, opacity: 0.7 }}>({c.unit})</span>}
                    <span style={{ fontSize: 9, padding: '0 4px', borderRadius: 6, background: c.column_type === 'numeric' ? '#e0f2fe' : '#fef3c7', color: c.column_type === 'numeric' ? '#0369a1' : '#92400e', fontWeight: 700 }}>{c.column_type}</span>
                  </span>
                ))}
              </div>
            </div>
          )}

          <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
            <input type="file" accept=".xlsx,.xls,.csv"
              onChange={e => { setRevenueKpiFile(e.target.files[0]); setRevenueKpiResult(null); setError(''); setSuccess(''); }}
              style={{ fontSize: 13 }} />
            <button className="btn btn-primary btn-sm"
              onClick={() => uploadFlexKpi('revenue', revenueKpiFile, setRevenueKpiFile, setRevenueKpiResult, setRevenueKpiStatus)}
              disabled={!revenueKpiFile || uploadingFlex.revenue}
              style={{ background: '#059669', borderColor: '#059669' }}>
              {uploadingFlex.revenue ? 'Uploading…' : 'Upload Revenue KPI Data'}
            </button>
            <button className="btn btn-sm"
              onClick={() => deleteFlexKpi('revenue', setRevenueKpiResult, setRevenueKpiStatus)}
              disabled={deletingFlex.revenue || !revenueKpiStatus?.unique_sites}
              style={{ background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: !revenueKpiStatus?.unique_sites ? 'not-allowed' : 'pointer', opacity: !revenueKpiStatus?.unique_sites ? 0.5 : 1 }}>
              {deletingFlex.revenue ? 'Deleting…' : 'Delete All'}
            </button>
          </div>

          {revenueKpiResult && (
            <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
              <strong style={{ color: '#16a34a' }}>✅ Upload Successful</strong>
              <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 8 }}>
                {[
                  { label: 'Rows in File', value: revenueKpiResult.rows_in_file ?? 0 },
                  { label: 'Records Inserted', value: revenueKpiResult.records_inserted ?? 0 },
                  { label: 'Unique Sites', value: revenueKpiResult.unique_sites ?? 0 },
                  { label: 'Columns Found', value: (revenueKpiResult.columns_detected ?? []).length },
                ].map((s, i) => (
                  <div key={i} style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, padding: '8px 12px' }}>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.label}</div>
                    <div style={{ fontSize: 18, fontWeight: 700, color: '#059669' }}>{typeof s.value === 'number' ? s.value.toLocaleString() : s.value}</div>
                  </div>
                ))}
              </div>
              {revenueKpiResult.columns_detected?.length > 0 && (
                <div style={{ marginTop: 10, fontSize: 12, color: 'var(--text-secondary)' }}>
                  <strong>Auto-detected columns:</strong> {revenueKpiResult.columns_detected.join(', ')}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── Business KPI Upload (Site Users + Site Revenue) ── */}
      <div className="section-card" style={{ marginTop: 24 }}>
        <div className="section-card-header">
          <h3>Business KPI Upload</h3>
          <p style={{ margin: 0, fontSize: 13, color: 'var(--text-muted)' }}>
            Upload an Excel workbook with sheets named <strong>Site Users</strong> and <strong>Site Revenue</strong>.
            Each sheet: one Site ID column + date columns with values.
          </p>
        </div>
        <div className="section-card-body">

          {/* Status */}
          <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap' }}>
            {[
              { label: 'Records in DB', value: businessKpiStatus?.total_records ?? 0, color: 'var(--primary)' },
              { label: 'Sites with Data', value: businessKpiStatus?.total_sites ?? 0, color: '#10b981' },
            ].map((s, i) => (
              <div key={i} style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 18px', minWidth: 140 }}>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 2 }}>{s.label}</div>
                <div style={{ fontSize: 22, fontWeight: 700, color: s.color }}>{s.value.toLocaleString()}</div>
              </div>
            ))}
          </div>

          {/* File format hint */}
          <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 14px', marginBottom: 16, fontSize: 12, color: 'var(--text-muted)' }}>
            <strong style={{ color: 'var(--text)' }}>Required Excel format:</strong>
            <ul style={{ margin: '6px 0 0 16px', lineHeight: 1.8 }}>
              <li>Sheet 1 name: <code style={{ background: 'var(--border)', padding: '1px 5px', borderRadius: 3 }}>Site Users</code></li>
              <li>Sheet 2 name: <code style={{ background: 'var(--border)', padding: '1px 5px', borderRadius: 3 }}>Site Revenue</code></li>
              <li>Columns: <code style={{ background: 'var(--border)', padding: '1px 5px', borderRadius: 3 }}>Site_ID</code> + date columns (e.g. 2024-03-01, 2024-03-02…)</li>
            </ul>
          </div>

          {/* Upload controls */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
            <input type="file" accept=".xlsx,.xlsm"
              onChange={e => setBusinessKpiFile(e.target.files[0] || null)}
              style={{ fontSize: 13 }} />
            <button className="btn btn-primary btn-sm" onClick={uploadBusinessKpi}
              disabled={!businessKpiFile || uploadingBusiness}>
              {uploadingBusiness ? 'Uploading…' : 'Upload Business KPI'}
            </button>
            <button className="btn btn-sm"
              onClick={deleteBusinessKpi}
              disabled={deletingBusiness || !businessKpiStatus?.total_records}
              style={{ background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: !businessKpiStatus?.total_records ? 'not-allowed' : 'pointer', opacity: !businessKpiStatus?.total_records ? 0.5 : 1 }}>
              {deletingBusiness ? 'Deleting…' : 'Delete All'}
            </button>
          </div>

          {/* Result */}
          {businessKpiResult && (
            <div style={{ marginTop: 14, background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: 12, fontSize: 13 }}>
              <strong style={{ color: '#16a34a' }}>✅ Upload Successful</strong>
              <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 8 }}>
                {[
                  { label: 'Records Inserted', value: businessKpiResult.inserted ?? 0 },
                  { label: 'KPIs Processed', value: businessKpiResult.kpis_processed ?? 0 },
                ].map((s, i) => (
                  <div key={i} style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, padding: '8px 12px' }}>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{s.label}</div>
                    <div style={{ fontSize: 18, fontWeight: 700, color: '#059669' }}>{s.value.toLocaleString()}</div>
                  </div>
                ))}
              </div>
              {businessKpiResult.kpi_summary?.map((k, i) => (
                <div key={i} style={{ marginTop: 6, fontSize: 12, color: 'var(--text-secondary)' }}>
                  <strong>{k.name}:</strong> {k.rows.toLocaleString()} rows
                </div>
              ))}
              {businessKpiResult.errors?.length > 0 && (
                <div style={{ marginTop: 8, color: '#dc2626', fontSize: 12 }}>
                  {businessKpiResult.errors.map((e, i) => <div key={i}>{e}</div>)}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

    </div>
  );
}