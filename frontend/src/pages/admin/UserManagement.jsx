import { useState, useEffect, useRef } from 'react';
import { API_BASE, apiGet, apiPost, apiPut, apiDelete, getToken } from '../../api';

/* ── Customer tier config ───────────────────────────────────────────────────── */
const TIER_CFG = {
  platinum: { bg: '#f5f3ff', color: '#6d28d9', border: '#c4b5fd', label: 'Platinum', priority: 'Critical' },
  gold:     { bg: '#fffbeb', color: '#b45309', border: '#fde68a', label: 'Gold',     priority: 'High'     },
  silver:   { bg: '#f1f5f9', color: '#475569', border: '#cbd5e1', label: 'Silver',   priority: 'Medium'   },
  bronze:   { bg: '#fff7ed', color: '#c2410c', border: '#fdba74', label: 'Bronze',   priority: 'Low'      },
};

function TierBadge({ tier }) {
  const t = TIER_CFG[tier] || TIER_CFG.bronze;
  return (
    <span style={{
      display: 'inline-block', fontSize: 11, fontWeight: 700,
      padding: '3px 8px', borderRadius: 6,
      background: t.bg, color: t.color, border: `1px solid ${t.border}`,
      textTransform: 'uppercase', letterSpacing: '0.04em',
    }}>
      {t.label}
    </span>
  );
}

/* ── Staff sub-component ────────────────────────────────────────────────────── */
function StaffTab() {
  const [users, setUsers]               = useState([]);
  const [loading, setLoading]           = useState(true);
  const [roleFilter, setRoleFilter]     = useState('');
  const [search, setSearch]             = useState('');
  const [showAdd, setShowAdd]           = useState(false);
  const [addForm, setAddForm]           = useState({ name: '', email: '', password: '', role: 'human_agent', phone_number: '' });
  const [addError, setAddError]         = useState('');
  const [addLoading, setAddLoading]     = useState(false);
  const [editingId, setEditingId]       = useState(null);
  const [editData, setEditData]         = useState({});
  const [editError, setEditError]       = useState('');
  const [deleteConfirm, setDeleteConfirm] = useState(null);
  const [uploadResult, setUploadResult] = useState(null);
  const [uploadLoading, setUploadLoading] = useState(false);
  const fileInputRef = useRef(null);

  const loadUsers = () => {
    const params = new URLSearchParams();
    if (roleFilter) params.append('role', roleFilter);
    if (search) params.append('search', search);
    apiGet(`/api/admin/users?${params.toString()}`).then(d => {
      setUsers((d?.users || []).filter(u => u.role !== 'customer'));
      setLoading(false);
    });
  };

  useEffect(() => { loadUsers(); }, [roleFilter]); // eslint-disable-line

  const validatePassword = (pw) => {
    if (pw.length < 7) return 'Password must be at least 7 characters long';
    if (!/[A-Z]/.test(pw)) return 'Password must contain at least 1 uppercase letter';
    if (!/[!@#$%^&*()_+\-=[\]{};':"\\|,.<>/?`~]/.test(pw)) return 'Password must contain at least 1 special character';
    return null;
  };

  const handleAdd = async (e) => {
    e.preventDefault();
    setAddError('');
    const pwErr = validatePassword(addForm.password);
    if (pwErr) { setAddError(pwErr); return; }
    setAddLoading(true);
    try {
      const res = await apiPost('/api/admin/users', addForm);
      if (res.error) { setAddError(res.error); }
      else {
        setShowAdd(false);
        setAddForm({ name: '', email: '', password: '', role: 'human_agent', phone_number: '' });
        loadUsers();
      }
    } catch { setAddError('Something went wrong'); }
    setAddLoading(false);
  };

  const handleEdit = (u) => {
    setEditingId(u.id);
    setEditData({ name: u.name, email: u.email, role: u.role, password: '' });
    setEditError('');
  };

  const handleUpdate = async (id) => {
    setEditError('');
    const payload = { name: editData.name, email: editData.email, role: editData.role };
    if (editData.password) {
      const pwErr = validatePassword(editData.password);
      if (pwErr) { setEditError(pwErr); return; }
      payload.password = editData.password;
    }
    try {
      const res = await apiPut(`/api/admin/users/${id}`, payload);
      if (res.error) { setEditError(res.error); return; }
      setEditingId(null);
      setEditData({});
      loadUsers();
    } catch { setEditError('Something went wrong'); }
  };

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploadLoading(true);
    setUploadResult(null);
    const formData = new FormData();
    formData.append('file', file);
    try {
      const token = getToken();
      const resp = await fetch(`${API_BASE}/api/admin/users/upload`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` },
        body: formData,
      });
      const data = await resp.json();
      if (data.error) {
        setUploadResult({ type: 'error', message: data.error });
      } else {
        setUploadResult({ type: 'success', message: data.message, skipped: data.skipped || [] });
        loadUsers();
      }
    } catch { setUploadResult({ type: 'error', message: 'Upload failed' }); }
    setUploadLoading(false);
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const handleDelete = async (id) => {
    try {
      const data = await apiDelete(`/api/admin/users/${id}`);
      if (data?.error) { alert(data.error); }
      else { setDeleteConfirm(null); loadUsers(); }
    } catch { alert('Failed to delete user'); }
  };

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  return (
    <div className="table-card">
      <div className="table-header">
        <h3>Staff ({users.length})</h3>
        <div className="table-filters">
          <select className="filter-select" value={roleFilter} onChange={e => setRoleFilter(e.target.value)}>
            <option value="">All Roles</option>
            <option value="manager">Manager</option>
            <option value="human_agent">Human Agent</option>
            <option value="cto">CTO</option>
            <option value="admin">Admin</option>
          </select>
          <form onSubmit={e => { e.preventDefault(); loadUsers(); }} style={{ display: 'flex', gap: 6 }}>
            <input type="text" className="filter-input" placeholder="Search by name or email..."
              value={search} onChange={e => setSearch(e.target.value)} />
            <button type="submit" className="btn btn-primary btn-sm">Search</button>
          </form>
          <button className="btn btn-primary btn-sm" onClick={() => { setShowAdd(!showAdd); setAddError(''); }}>
            {showAdd ? 'Cancel' : '+ Add User'}
          </button>
          <input type="file" ref={fileInputRef} accept=".xlsx,.xlsm" onChange={handleUpload} style={{ display: 'none' }} />
          <button className="btn btn-outline btn-sm" disabled={uploadLoading} onClick={() => fileInputRef.current?.click()}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 4 }}>
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>
            </svg>
            {uploadLoading ? 'Uploading...' : 'Upload Excel'}
          </button>
        </div>
      </div>

      {uploadResult && (
        <div style={{
          background: uploadResult.type === 'success' ? '#ecfdf5' : '#fef2f2',
          border: `1px solid ${uploadResult.type === 'success' ? '#a7f3d0' : '#fecaca'}`,
          borderRadius: 8, padding: '12px 16px', marginBottom: 16, fontSize: 13,
          color: uploadResult.type === 'success' ? '#047857' : '#dc2626',
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span style={{ fontWeight: 600 }}>{uploadResult.message}</span>
            <button onClick={() => setUploadResult(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 16, color: 'inherit' }}>x</button>
          </div>
          {uploadResult.skipped?.length > 0 && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#b45309' }}>
              <strong>Skipped rows:</strong>
              {uploadResult.skipped.map((s, i) => <div key={i}>{s}</div>)}
            </div>
          )}
          {uploadResult.type === 'success' && (
            <div style={{ marginTop: 6, fontSize: 11, color: '#64748b' }}>
              Default password for new users: <strong>Welcome@123</strong>
            </div>
          )}
        </div>
      )}

      {showAdd && (
        <div style={{ background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 10, padding: 20, margin: '0 0 16px' }}>
          <h4 style={{ fontSize: 15, fontWeight: 700, marginBottom: 14, color: '#1e293b' }}>Add New Staff User</h4>
          {addError && <div className="form-error" style={{ marginBottom: 10 }}>{addError}</div>}
          <form onSubmit={handleAdd} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div className="form-group" style={{ margin: 0 }}>
              <label style={{ fontSize: 12, fontWeight: 600 }}>Name</label>
              <input type="text" className="form-input" placeholder="Full name" required
                value={addForm.name} onChange={e => setAddForm(f => ({ ...f, name: e.target.value }))} />
            </div>
            <div className="form-group" style={{ margin: 0 }}>
              <label style={{ fontSize: 12, fontWeight: 600 }}>Email</label>
              <input type="email" className="form-input" placeholder="user@example.com" required
                value={addForm.email} onChange={e => setAddForm(f => ({ ...f, email: e.target.value }))} />
            </div>
            <div className="form-group" style={{ margin: 0 }}>
              <label style={{ fontSize: 12, fontWeight: 600 }}>Password</label>
              <input type="password" className="form-input" placeholder="Min 7 chars, 1 upper, 1 special" required minLength={7}
                value={addForm.password} onChange={e => setAddForm(f => ({ ...f, password: e.target.value }))} />
            </div>
            <div className="form-group" style={{ margin: 0 }}>
              <label style={{ fontSize: 12, fontWeight: 600 }}>Role</label>
              <select className="form-input" value={addForm.role}
                onChange={e => setAddForm(f => ({ ...f, role: e.target.value }))}>
                <option value="manager">Manager</option>
                <option value="human_agent">Human Agent</option>
                <option value="cto">CTO</option>
                <option value="admin">Admin</option>
              </select>
            </div>
            <div className="form-group" style={{ margin: 0 }}>
              <label style={{ fontSize: 12, fontWeight: 600 }}>Phone Number <span style={{ color: '#94a3b8', fontWeight: 400 }}>(optional)</span></label>
              <input type="tel" className="form-input" placeholder="+923001234567"
                value={addForm.phone_number} onChange={e => setAddForm(f => ({ ...f, phone_number: e.target.value }))} />
            </div>
            <div style={{ gridColumn: '1 / -1' }}>
              <button type="submit" className="btn btn-primary btn-sm" disabled={addLoading}>
                {addLoading ? 'Adding...' : 'Add User'}
              </button>
            </div>
          </form>
        </div>
      )}

      {editError && <div className="form-error" style={{ margin: '0 0 10px' }}>{editError}</div>}

      {users.length === 0 ? (
        <div className="empty-state">
          <h4>No users found</h4>
          <p>Try adjusting your filters or add a new user</p>
        </div>
      ) : (
        <div className="table-scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>Employee ID</th>
                <th>Name</th>
                <th>Email</th>
                <th>Phone</th>
                <th>Role</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.map(u => (
                <tr key={u.id}>
                  <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#64748b', fontWeight: 600 }}>{u.employee_id || '—'}</td>
                  <td>
                    {editingId === u.id ? (
                      <input type="text" className="form-input" style={{ padding: '4px 8px', fontSize: 13 }}
                        value={editData.name} onChange={e => setEditData(d => ({ ...d, name: e.target.value }))} />
                    ) : (
                      <span style={{ fontWeight: 500, fontSize: 13 }}>{u.name}</span>
                    )}
                  </td>
                  <td>
                    {editingId === u.id ? (
                      <input type="email" className="form-input" style={{ padding: '4px 8px', fontSize: 13 }}
                        value={editData.email} onChange={e => setEditData(d => ({ ...d, email: e.target.value }))} />
                    ) : (
                      <span style={{ fontSize: 13, color: '#64748b' }}>{u.email}</span>
                    )}
                  </td>
                  <td style={{ fontSize: 12, color: '#475569', whiteSpace: 'nowrap' }}>{u.phone_number || '—'}</td>
                  <td>
                    {editingId === u.id ? (
                      <select className="filter-select" value={editData.role}
                        onChange={e => setEditData(d => ({ ...d, role: e.target.value }))}>
                        <option value="manager">Manager</option>
                        <option value="human_agent">Human Agent</option>
                        <option value="cto">CTO</option>
                        <option value="admin">Admin</option>
                      </select>
                    ) : (
                      <span className={`badge badge-${u.role === 'admin' ? 'escalated' : u.role === 'manager' ? 'active' : u.role === 'cto' ? 'in_progress' : u.role === 'human_agent' ? 'pending' : 'resolved'}`}
                        style={{ textTransform: 'capitalize' }}>
                        {u.role === 'human_agent' ? 'Human Agent' : u.role}
                      </span>
                    )}
                  </td>
                  <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                    {u.created_at ? new Date(u.created_at).toLocaleDateString() : '—'}
                  </td>
                  <td>
                    {editingId === u.id ? (
                      <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                        <input type="password" className="form-input" placeholder="New password (optional)"
                          style={{ padding: '4px 8px', fontSize: 11, width: 140 }}
                          value={editData.password} onChange={e => setEditData(d => ({ ...d, password: e.target.value }))} />
                        <button className="btn btn-success btn-sm" onClick={() => handleUpdate(u.id)}>Save</button>
                        <button className="btn btn-ghost btn-sm" onClick={() => { setEditingId(null); setEditData({}); setEditError(''); }}>Cancel</button>
                      </div>
                    ) : deleteConfirm === u.id ? (
                      <div style={{ display: 'flex', gap: 4 }}>
                        <button className="btn btn-sm" style={{ background: '#fef2f2', color: '#dc2626', border: '1px solid #fecaca', borderRadius: 6, fontSize: 11, padding: '4px 10px', cursor: 'pointer', fontFamily: 'inherit', fontWeight: 500 }}
                          onClick={() => handleDelete(u.id)}>Confirm</button>
                        <button className="btn btn-ghost btn-sm" onClick={() => setDeleteConfirm(null)}>No</button>
                      </div>
                    ) : (
                      <div style={{ display: 'flex', gap: 4 }}>
                        <button className="btn btn-outline btn-sm" onClick={() => handleEdit(u)}>Edit</button>
                        <button className="btn btn-sm" style={{ background: '#fef2f2', color: '#dc2626', border: '1px solid #fecaca', borderRadius: 6, fontSize: 11, padding: '4px 10px', cursor: 'pointer', fontFamily: 'inherit', fontWeight: 500 }}
                          onClick={() => setDeleteConfirm(u.id)}>Delete</button>
                      </div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

/* ── Customers sub-component ────────────────────────────────────────────────── */
function CustomersTab() {
  const [customers, setCustomers]       = useState([]);
  const [loading, setLoading]           = useState(true);
  const [search, setSearch]             = useState('');
  const [tierFilter, setTierFilter]     = useState('');
  const [savingTier, setSavingTier]     = useState(null);   // user id being saved
  const [tierEdits, setTierEdits]       = useState({});     // { [userId]: newTier }
  const [tierMsg, setTierMsg]           = useState({});     // { [userId]: 'Saved!' }

  const loadCustomers = () => {
    const params = new URLSearchParams();
    params.append('role', 'customer');
    if (search) params.append('search', search);
    setLoading(true);
    apiGet(`/api/admin/users?${params.toString()}`).then(d => {
      let list = d?.users || [];
      if (tierFilter) list = list.filter(u => (u.user_type || 'bronze') === tierFilter);
      setCustomers(list);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => { loadCustomers(); }, [tierFilter]); // eslint-disable-line

  const editedTier = (u) => tierEdits[u.id] ?? (u.user_type || 'bronze');

  const handleTierSave = async (u) => {
    const newTier = editedTier(u);
    if (newTier === (u.user_type || 'bronze')) return; // no change
    setSavingTier(u.id);
    try {
      await apiPut(`/api/admin/users/${u.id}`, { user_type: newTier });
      setTierMsg(m => ({ ...m, [u.id]: 'Saved!' }));
      setCustomers(prev => prev.map(c => c.id === u.id ? { ...c, user_type: newTier } : c));
      setTierEdits(e => { const n = { ...e }; delete n[u.id]; return n; });
      setTimeout(() => setTierMsg(m => { const n = { ...m }; delete n[u.id]; return n; }), 2000);
    } catch (err) {
      setTierMsg(m => ({ ...m, [u.id]: err?.message || 'Failed' }));
    }
    setSavingTier(null);
  };

  const tierCounts = Object.fromEntries(
    Object.keys(TIER_CFG).map(k => [k, customers.filter(c => (c.user_type || 'bronze') === k).length])
  );

  if (loading) return <div className="page-loader" style={{ minHeight: 200 }}><div className="spinner" /></div>;

  return (
    <div>
      {/* Tier summary cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 12, marginBottom: 20 }}>
        {Object.entries(TIER_CFG).map(([key, cfg]) => (
          <div
            key={key}
            onClick={() => setTierFilter(tierFilter === key ? '' : key)}
            style={{
              background: tierFilter === key ? cfg.bg : '#fff',
              border: `2px solid ${tierFilter === key ? cfg.border : '#e2e8f0'}`,
              borderRadius: 10, padding: '14px 16px', cursor: 'pointer',
              transition: 'all 0.15s',
            }}
          >
            <div style={{ fontSize: 11, fontWeight: 700, color: cfg.color, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 }}>
              {cfg.label}
            </div>
            <div style={{ fontSize: 26, fontWeight: 800, color: '#0f172a', lineHeight: 1 }}>
              {tierCounts[key] || 0}
            </div>
            <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 4 }}>
              Priority floor: {cfg.priority}
            </div>
          </div>
        ))}
      </div>

      <div className="table-card">
        <div className="table-header">
          <h3>
            {tierFilter
              ? `${TIER_CFG[tierFilter]?.label} Customers (${customers.length})`
              : `All Customers (${customers.length})`}
          </h3>
          <div className="table-filters">
            <form onSubmit={e => { e.preventDefault(); loadCustomers(); }} style={{ display: 'flex', gap: 6 }}>
              <input type="text" className="filter-input" placeholder="Search by name or email..."
                value={search} onChange={e => setSearch(e.target.value)} />
              <button type="submit" className="btn btn-primary btn-sm">Search</button>
            </form>
            {tierFilter && (
              <button className="btn btn-ghost btn-sm" onClick={() => setTierFilter('')}>Clear Filter</button>
            )}
          </div>
        </div>

        {customers.length === 0 ? (
          <div className="empty-state">
            <h4>No customers found</h4>
            <p>{tierFilter ? `No ${TIER_CFG[tierFilter]?.label} customers yet` : 'Try adjusting your search'}</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Email</th>
                  <th>Phone</th>
                  <th>Current Tier</th>
                  <th>Priority Floor</th>
                  <th>Joined</th>
                  <th style={{ minWidth: 200 }}>Change Tier</th>
                </tr>
              </thead>
              <tbody>
                {customers.map(u => {
                  const currentTier = u.user_type || 'bronze';
                  const edited = editedTier(u);
                  const isDirty = edited !== currentTier;
                  const isSaving = savingTier === u.id;
                  const msg = tierMsg[u.id];

                  return (
                    <tr key={u.id}>
                      <td style={{ fontWeight: 500, fontSize: 13 }}>{u.name}</td>
                      <td style={{ fontSize: 12, color: '#64748b' }}>{u.email}</td>
                      <td style={{ fontSize: 12, color: '#475569' }}>{u.phone_number || '—'}</td>
                      <td>
                        <TierBadge tier={currentTier} />
                      </td>
                      <td>
                        <span style={{ fontSize: 12, color: '#64748b' }}>
                          {TIER_CFG[currentTier]?.priority || 'Low'}
                        </span>
                      </td>
                      <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                        {u.created_at ? new Date(u.created_at).toLocaleDateString() : '—'}
                      </td>
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          <select
                            className="filter-select"
                            value={edited}
                            onChange={e => setTierEdits(t => ({ ...t, [u.id]: e.target.value }))}
                            style={{ minWidth: 100 }}
                          >
                            <option value="bronze">Bronze</option>
                            <option value="silver">Silver</option>
                            <option value="gold">Gold</option>
                            <option value="platinum">Platinum</option>
                          </select>
                          {isDirty && (
                            <button
                              className="btn btn-success btn-sm"
                              onClick={() => handleTierSave(u)}
                              disabled={isSaving}
                              style={{ whiteSpace: 'nowrap' }}
                            >
                              {isSaving ? 'Saving...' : 'Save'}
                            </button>
                          )}
                          {msg && (
                            <span style={{
                              fontSize: 11, fontWeight: 600,
                              color: msg === 'Saved!' ? '#047857' : '#dc2626',
                            }}>
                              {msg}
                            </span>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Main page ──────────────────────────────────────────────────────────────── */
export default function UserManagement() {
  const [tab, setTab] = useState('staff');

  return (
    <div>
      <div className="page-header">
        <h1>User Management</h1>
        <p>Manage staff accounts and customer tier assignments</p>
      </div>

      {/* Tab toggle */}
      <div style={{ display: 'flex', gap: 2, marginBottom: 20, background: '#f1f5f9', borderRadius: 10, padding: 4, width: 'fit-content' }}>
        {[
          { key: 'staff',     label: 'Staff & Agents' },
          { key: 'customers', label: 'Customers & Tiers' },
        ].map(({ key, label }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            style={{
              padding: '8px 20px', borderRadius: 8, border: 'none', cursor: 'pointer',
              fontSize: 13, fontWeight: 600, transition: 'all 0.15s',
              background: tab === key ? '#fff' : 'transparent',
              color: tab === key ? '#00338D' : '#64748b',
              boxShadow: tab === key ? '0 1px 4px rgba(0,0,0,0.08)' : 'none',
              fontFamily: 'inherit',
            }}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'staff'     && <StaffTab />}
      {tab === 'customers' && <CustomersTab />}
    </div>
  );
}
