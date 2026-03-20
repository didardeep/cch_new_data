import { useState, useEffect } from 'react';
import { useAuth } from '../AuthContext';
import { apiPut } from '../api';

export default function SettingsPage() {
  const { user, updateUser } = useAuth();

  // Account details state
  const [name, setName] = useState(user?.name || '');
  const [email, setEmail] = useState(user?.email || '');
  const [phone, setPhone] = useState(user?.phone_number || '');
  const [detailsError, setDetailsError] = useState('');
  const [detailsSuccess, setDetailsSuccess] = useState('');
  const [savingDetails, setSavingDetails] = useState(false);

  // Password state
  const [currentPwd, setCurrentPwd] = useState('');
  const [newPwd, setNewPwd] = useState('');
  const [confirmPwd, setConfirmPwd] = useState('');
  const [pwdError, setPwdError] = useState('');
  const [pwdSuccess, setPwdSuccess] = useState('');
  const [savingPwd, setSavingPwd] = useState(false);

  // Sync fields if user changes
  useEffect(() => {
    setName(user?.name || '');
    setEmail(user?.email || '');
    setPhone(user?.phone_number || '');
  }, [user]);

  const handleSaveDetails = async (e) => {
    e.preventDefault();
    setDetailsError(''); setDetailsSuccess('');
    if (!name.trim()) { setDetailsError('Name is required.'); return; }
    if (!email.trim()) { setDetailsError('Email is required.'); return; }
    setSavingDetails(true);
    try {
      const res = await apiPut('/api/user/settings', {
        name: name.trim(),
        email: email.trim().toLowerCase(),
        phone_number: phone.trim(),
      });
      if (res.error) { setDetailsError(res.error); return; }
      updateUser(res.user);
      setDetailsSuccess('Account details updated successfully.');
    } catch {
      setDetailsError('Failed to save changes. Please try again.');
    } finally {
      setSavingDetails(false);
    }
  };

  const handleChangePassword = async (e) => {
    e.preventDefault();
    setPwdError(''); setPwdSuccess('');
    if (!currentPwd) { setPwdError('Current password is required.'); return; }
    if (newPwd.length < 6) { setPwdError('New password must be at least 6 characters.'); return; }
    if (newPwd !== confirmPwd) { setPwdError('Passwords do not match.'); return; }
    setSavingPwd(true);
    try {
      const res = await apiPut('/api/user/password', {
        current_password: currentPwd,
        new_password: newPwd,
      });
      if (res.error) { setPwdError(res.error); return; }
      setPwdSuccess('Password changed successfully.');
      setCurrentPwd(''); setNewPwd(''); setConfirmPwd('');
    } catch {
      setPwdError('Failed to change password. Please try again.');
    } finally {
      setSavingPwd(false);
    }
  };

  const inputStyle = {
    width: '100%', padding: '10px 13px', fontSize: 14,
    border: '1px solid #d1d5db', borderRadius: 8,
    outline: 'none', background: '#f9fafb', color: '#1e293b',
    boxSizing: 'border-box', transition: 'border-color 0.15s',
  };

  const labelStyle = {
    display: 'block', fontSize: 12.5, fontWeight: 600,
    color: '#475569', marginBottom: 6, letterSpacing: 0.2,
  };

  const cardStyle = {
    background: '#fff', borderRadius: 12,
    border: '1px solid #e2e8f0',
    boxShadow: '0 1px 4px rgba(0,0,0,0.05)',
    overflow: 'hidden',
  };

  const cardHeaderStyle = {
    padding: '20px 24px 18px',
    borderBottom: '1px solid #f1f5f9',
    background: '#fafbfc',
  };

  return (
    <div>
      {/* Page header */}
      <div className="page-header">
        <h1>Settings</h1>
        <p>Manage your account details and security preferences.</p>
      </div>

      {/* Profile info strip */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 16,
        background: '#fff', border: '1px solid #e2e8f0',
        borderRadius: 12, padding: '18px 24px', marginBottom: 28,
        boxShadow: '0 1px 4px rgba(0,0,0,0.04)',
      }}>
        <div style={{
          width: 52, height: 52, borderRadius: '50%',
          background: '#00338d', color: '#fff',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: 22, fontWeight: 700, flexShrink: 0,
        }}>
          {user?.name?.charAt(0)?.toUpperCase() || 'U'}
        </div>
        <div>
          <div style={{ fontSize: 16, fontWeight: 700, color: '#0f172a' }}>{user?.name}</div>
          <div style={{ fontSize: 13, color: '#64748b', marginTop: 2 }}>{user?.email}</div>
        </div>
        <div style={{ marginLeft: 'auto' }}>
          <span style={{
            display: 'inline-block', padding: '4px 12px',
            background: '#f0f4ff', color: '#00338d',
            borderRadius: 20, fontSize: 12, fontWeight: 600,
            textTransform: 'capitalize', border: '1px solid #c7d7fa',
          }}>
            {user?.role === 'human_agent' ? 'Human Agent' : user?.role}
          </span>
        </div>
      </div>

      {/* Two-column layout */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, alignItems: 'start' }}>

        {/* Account Details card */}
        <div style={cardStyle}>
          <div style={cardHeaderStyle}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <div style={{
                width: 34, height: 34, borderRadius: 8,
                background: '#f0f4ff', border: '1px solid #c7d7fa',
                display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#00338d',
              }}>
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>
                </svg>
              </div>
              <div>
                <div style={{ fontSize: 15, fontWeight: 700, color: '#0f172a' }}>Account Details</div>
                <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 1 }}>Update your name, email and phone</div>
              </div>
            </div>
          </div>
          <form onSubmit={handleSaveDetails} style={{ padding: '24px' }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
              <div>
                <label style={labelStyle}>Full Name</label>
                <input style={inputStyle} value={name} onChange={e => setName(e.target.value)} placeholder="Your full name" />
              </div>
              <div>
                <label style={labelStyle}>Email Address</label>
                <input style={inputStyle} type="email" value={email} onChange={e => setEmail(e.target.value)} placeholder="your@email.com" />
              </div>
              <div>
                <label style={labelStyle}>Phone Number</label>
                <input style={inputStyle} value={phone} onChange={e => setPhone(e.target.value)} placeholder="+91 XXXXX XXXXX" />
              </div>

              {detailsError && (
                <div style={{ fontSize: 13, color: '#dc2626', background: '#fef2f2', padding: '10px 14px', borderRadius: 8, border: '1px solid #fecaca' }}>
                  {detailsError}
                </div>
              )}
              {detailsSuccess && (
                <div style={{ fontSize: 13, color: '#16a34a', background: '#f0fdf4', padding: '10px 14px', borderRadius: 8, border: '1px solid #bbf7d0' }}>
                  {detailsSuccess}
                </div>
              )}

              <button
                type="submit"
                disabled={savingDetails}
                style={{
                  padding: '10px 0', fontSize: 14, fontWeight: 600,
                  background: savingDetails ? '#94a3b8' : '#00338d',
                  color: '#fff', border: 'none', borderRadius: 8,
                  cursor: savingDetails ? 'not-allowed' : 'pointer',
                  transition: 'background 0.15s', width: '100%',
                }}
              >
                {savingDetails ? 'Saving…' : 'Save Changes'}
              </button>
            </div>
          </form>
        </div>

        {/* Change Password card */}
        <div style={cardStyle}>
          <div style={cardHeaderStyle}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <div style={{
                width: 34, height: 34, borderRadius: 8,
                background: '#fff7ed', border: '1px solid #fed7aa',
                display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#ea580c',
              }}>
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                </svg>
              </div>
              <div>
                <div style={{ fontSize: 15, fontWeight: 700, color: '#0f172a' }}>Change Password</div>
                <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 1 }}>Update your login password</div>
              </div>
            </div>
          </div>
          <form onSubmit={handleChangePassword} style={{ padding: '24px' }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
              <div>
                <label style={labelStyle}>Current Password</label>
                <input style={inputStyle} type="password" value={currentPwd} onChange={e => setCurrentPwd(e.target.value)} placeholder="Enter current password" autoComplete="current-password" />
              </div>
              <div>
                <label style={labelStyle}>New Password</label>
                <input style={inputStyle} type="password" value={newPwd} onChange={e => setNewPwd(e.target.value)} placeholder="Minimum 6 characters" autoComplete="new-password" />
              </div>
              <div>
                <label style={labelStyle}>Confirm New Password</label>
                <input style={inputStyle} type="password" value={confirmPwd} onChange={e => setConfirmPwd(e.target.value)} placeholder="Re-enter new password" autoComplete="new-password" />
              </div>

              {pwdError && (
                <div style={{ fontSize: 13, color: '#dc2626', background: '#fef2f2', padding: '10px 14px', borderRadius: 8, border: '1px solid #fecaca' }}>
                  {pwdError}
                </div>
              )}
              {pwdSuccess && (
                <div style={{ fontSize: 13, color: '#16a34a', background: '#f0fdf4', padding: '10px 14px', borderRadius: 8, border: '1px solid #bbf7d0' }}>
                  {pwdSuccess}
                </div>
              )}

              <button
                type="submit"
                disabled={savingPwd}
                style={{
                  padding: '10px 0', fontSize: 14, fontWeight: 600,
                  background: savingPwd ? '#94a3b8' : '#ea580c',
                  color: '#fff', border: 'none', borderRadius: 8,
                  cursor: savingPwd ? 'not-allowed' : 'pointer',
                  transition: 'background 0.15s', width: '100%',
                }}
              >
                {savingPwd ? 'Saving…' : 'Change Password'}
              </button>
            </div>
          </form>
        </div>

      </div>
    </div>
  );
}
