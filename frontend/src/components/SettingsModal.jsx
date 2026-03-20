import { useState, useEffect } from 'react';
import { useAuth } from '../AuthContext';
import { apiPut } from '../api';

export default function SettingsModal({ onClose }) {
  const { user, updateUser } = useAuth();
  const [tab, setTab] = useState('details');

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

  // Reset messages on tab switch
  useEffect(() => {
    setDetailsError(''); setDetailsSuccess('');
    setPwdError(''); setPwdSuccess('');
  }, [tab]);

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
    if (newPwd !== confirmPwd) { setPwdError('New passwords do not match.'); return; }
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

  // Close on backdrop click
  const handleBackdrop = (e) => {
    if (e.target === e.currentTarget) onClose();
  };

  const inputStyle = {
    width: '100%', padding: '9px 12px', fontSize: 13.5,
    border: '1px solid #d1d5db', borderRadius: 8,
    outline: 'none', background: '#f9fafb', color: '#1e293b',
    boxSizing: 'border-box',
    transition: 'border-color 0.15s',
  };

  const labelStyle = {
    display: 'block', fontSize: 12, fontWeight: 600,
    color: '#475569', marginBottom: 5, letterSpacing: 0.2,
  };

  const btnPrimary = {
    padding: '9px 22px', fontSize: 13.5, fontWeight: 600,
    background: '#00338d', color: '#fff', border: 'none',
    borderRadius: 8, cursor: 'pointer', transition: 'background 0.15s',
  };

  const tabBtn = (active) => ({
    flex: 1, padding: '9px 0', fontSize: 13, fontWeight: active ? 600 : 500,
    color: active ? '#00338d' : '#64748b',
    background: 'none', border: 'none', borderBottom: active ? '2px solid #00338d' : '2px solid transparent',
    cursor: 'pointer', transition: 'all 0.15s',
  });

  return (
    <div
      onClick={handleBackdrop}
      style={{
        position: 'fixed', inset: 0, zIndex: 1000,
        background: 'rgba(15,23,42,0.45)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        backdropFilter: 'blur(2px)',
      }}
    >
      <div style={{
        background: '#fff', borderRadius: 16,
        width: '100%', maxWidth: 440,
        boxShadow: '0 20px 60px rgba(0,0,0,0.2)',
        overflow: 'hidden',
      }}>
        {/* Header */}
        <div style={{
          padding: '18px 24px 14px',
          borderBottom: '1px solid #e2e8f0',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        }}>
          <div>
            <div style={{ fontSize: 16, fontWeight: 700, color: '#0f172a' }}>Account Settings</div>
            <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 2 }}>{user?.email}</div>
          </div>
          <button
            onClick={onClose}
            style={{
              width: 30, height: 30, borderRadius: '50%', border: 'none',
              background: '#f1f5f9', cursor: 'pointer', display: 'flex',
              alignItems: 'center', justifyContent: 'center',
              color: '#64748b', fontSize: 18, lineHeight: 1,
            }}
          >×</button>
        </div>

        {/* Tabs */}
        <div style={{ display: 'flex', borderBottom: '1px solid #e2e8f0' }}>
          <button style={tabBtn(tab === 'details')} onClick={() => setTab('details')}>
            Account Details
          </button>
          <button style={tabBtn(tab === 'password')} onClick={() => setTab('password')}>
            Change Password
          </button>
        </div>

        {/* Body */}
        <div style={{ padding: '22px 24px 24px' }}>
          {tab === 'details' && (
            <form onSubmit={handleSaveDetails}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                <div>
                  <label style={labelStyle}>Full Name</label>
                  <input
                    style={inputStyle} value={name}
                    onChange={e => setName(e.target.value)}
                    placeholder="Your full name"
                  />
                </div>
                <div>
                  <label style={labelStyle}>Email Address</label>
                  <input
                    style={inputStyle} type="email" value={email}
                    onChange={e => setEmail(e.target.value)}
                    placeholder="your@email.com"
                  />
                </div>
                <div>
                  <label style={labelStyle}>Phone Number</label>
                  <input
                    style={inputStyle} value={phone}
                    onChange={e => setPhone(e.target.value)}
                    placeholder="+91 XXXXX XXXXX"
                  />
                </div>
                {detailsError && (
                  <div style={{ fontSize: 12.5, color: '#dc2626', background: '#fef2f2', padding: '8px 12px', borderRadius: 7 }}>
                    {detailsError}
                  </div>
                )}
                {detailsSuccess && (
                  <div style={{ fontSize: 12.5, color: '#16a34a', background: '#f0fdf4', padding: '8px 12px', borderRadius: 7 }}>
                    {detailsSuccess}
                  </div>
                )}
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 4 }}>
                  <button type="button" onClick={onClose} style={{
                    ...btnPrimary, background: '#f1f5f9', color: '#475569',
                  }}>Cancel</button>
                  <button type="submit" disabled={savingDetails} style={{
                    ...btnPrimary, opacity: savingDetails ? 0.7 : 1,
                  }}>
                    {savingDetails ? 'Saving…' : 'Save Changes'}
                  </button>
                </div>
              </div>
            </form>
          )}

          {tab === 'password' && (
            <form onSubmit={handleChangePassword}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                <div>
                  <label style={labelStyle}>Current Password</label>
                  <input
                    style={inputStyle} type="password" value={currentPwd}
                    onChange={e => setCurrentPwd(e.target.value)}
                    placeholder="Enter current password"
                    autoComplete="current-password"
                  />
                </div>
                <div>
                  <label style={labelStyle}>New Password</label>
                  <input
                    style={inputStyle} type="password" value={newPwd}
                    onChange={e => setNewPwd(e.target.value)}
                    placeholder="Minimum 6 characters"
                    autoComplete="new-password"
                  />
                </div>
                <div>
                  <label style={labelStyle}>Confirm New Password</label>
                  <input
                    style={inputStyle} type="password" value={confirmPwd}
                    onChange={e => setConfirmPwd(e.target.value)}
                    placeholder="Re-enter new password"
                    autoComplete="new-password"
                  />
                </div>
                {pwdError && (
                  <div style={{ fontSize: 12.5, color: '#dc2626', background: '#fef2f2', padding: '8px 12px', borderRadius: 7 }}>
                    {pwdError}
                  </div>
                )}
                {pwdSuccess && (
                  <div style={{ fontSize: 12.5, color: '#16a34a', background: '#f0fdf4', padding: '8px 12px', borderRadius: 7 }}>
                    {pwdSuccess}
                  </div>
                )}
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 4 }}>
                  <button type="button" onClick={onClose} style={{
                    ...btnPrimary, background: '#f1f5f9', color: '#475569',
                  }}>Cancel</button>
                  <button type="submit" disabled={savingPwd} style={{
                    ...btnPrimary, opacity: savingPwd ? 0.7 : 1,
                  }}>
                    {savingPwd ? 'Saving…' : 'Change Password'}
                  </button>
                </div>
              </div>
            </form>
          )}
        </div>
      </div>
    </div>
  );
}
