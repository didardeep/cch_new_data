import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import { apiPost } from '../api';

export default function LoginPage() {
  const [loginType, setLoginType] = useState(null); // 'customer' or 'employee'
  const [step, setStep] = useState('choose'); // 'choose' | 'email' | 'credentials'
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [otp, setOtp] = useState('');
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');
  const [loading, setLoading] = useState(false);
  const [resendCooldown, setResendCooldown] = useState(0);
  const navigate = useNavigate();
  const { login } = useAuth();

  const handleChoose = (type) => {
    setLoginType(type);
    setStep('email');
    setError('');
    setInfo('');
  };

  const handleEmailSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setInfo('');
    setLoading(true);

    if (loginType === 'employee') {
      try {
        const data = await apiPost('/api/auth/send-otp', { email });
        if (data.error) {
          setError(data.error);
        } else {
          setStep('credentials');
          setInfo('A verification code has been sent to your email.');
          startResendCooldown();
        }
      } catch {
        setError('Something went wrong. Please try again.');
      }
    } else {
      setStep('credentials');
    }
    setLoading(false);
  };

  const handleLoginSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setInfo('');
    setLoading(true);

    try {
      const body = { email, password };
      if (loginType === 'employee') body.otp = otp;

      const data = await apiPost('/api/auth/login', body);
      if (data.error) {
        setError(data.error);
      } else {
        login(data.token, data.user);
        const role = data.user.role;
        if (role === 'customer') navigate('/customer/dashboard');
        else if (role === 'manager') navigate('/manager/dashboard');
        else if (role === 'admin') navigate('/admin/dashboard');
        else if (role === 'human_agent') navigate('/agent/dashboard');
        else navigate('/cto/dashboard');
      }
    } catch {
      setError('Something went wrong. Please try again.');
    }
    setLoading(false);
  };

  const startResendCooldown = () => {
    setResendCooldown(60);
    const interval = setInterval(() => {
      setResendCooldown(prev => {
        if (prev <= 1) { clearInterval(interval); return 0; }
        return prev - 1;
      });
    }, 1000);
  };

  const handleResendOtp = async () => {
    if (resendCooldown > 0) return;
    setError('');
    setInfo('');
    setLoading(true);

    try {
      const data = await apiPost('/api/auth/send-otp', { email });
      if (data.error) {
        setError(data.error);
      } else {
        setInfo('A new verification code has been sent to your email.');
        setOtp('');
        startResendCooldown();
      }
    } catch {
      setError('Failed to resend code. Please try again.');
    }
    setLoading(false);
  };

  const handleBackToChoose = () => {
    setStep('choose');
    setLoginType(null);
    setEmail('');
    setPassword('');
    setOtp('');
    setError('');
    setInfo('');
    setResendCooldown(0);
  };

  const handleBackToEmail = () => {
    setStep('email');
    setPassword('');
    setOtp('');
    setError('');
    setInfo('');
    setResendCooldown(0);
  };

  const getSubtitle = () => {
    if (step === 'choose') return 'Choose how you want to sign in';
    if (step === 'email') return loginType === 'employee' ? 'Enter your employee email' : 'Enter your email to continue';
    return 'Complete your sign in';
  };

  return (
    <div className="auth-page">
      <div className="auth-card">
        <div className="auth-header">
          <img src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg" alt="KPMG" style={{ height: 40 }} />
          <h2>Welcome Back</h2>
          <p>{getSubtitle()}</p>
        </div>

        {error && <div className="form-error">{error}</div>}
        {info && (
          <div style={{
            background: '#ecfdf5', border: '1px solid #a7f3d0',
            borderRadius: 8, padding: '10px 14px', color: '#047857',
            fontSize: 13, marginBottom: 16,
          }}>
            {info}
          </div>
        )}

        {step === 'choose' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <button
              className="btn btn-primary"
              style={{
                width: '100%', padding: '14px 20px', fontSize: 15,
                display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 10,
              }}
              onClick={() => handleChoose('customer')}
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/>
                <circle cx="12" cy="7" r="4"/>
              </svg>
              Login as Customer
            </button>
            <button
              className="btn"
              style={{
                width: '100%', padding: '14px 20px', fontSize: 15,
                background: '#483698', color: '#fff', border: 'none', borderRadius: 8,
                display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 10,
                cursor: 'pointer',
              }}
              onClick={() => handleChoose('employee')}
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="2" y="7" width="20" height="14" rx="2" ry="2"/>
                <path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>
              </svg>
              Login as Employee
            </button>
          </div>
        )}

        {step === 'email' && (
          <form onSubmit={handleEmailSubmit}>
            <div className="form-group">
              <label>Email Address</label>
              <input
                type="email"
                className="form-input"
                placeholder="you@example.com"
                value={email}
                onChange={e => setEmail(e.target.value)}
                required
                autoFocus
              />
            </div>
            <button
              type="submit"
              className="btn btn-primary"
              style={{ width: '100%', marginTop: 8 }}
              disabled={loading}
            >
              {loading ? (loginType === 'employee' ? 'Sending OTP...' : 'Checking...') : 'Continue'}
            </button>
            <button
              type="button"
              onClick={handleBackToChoose}
              className="btn btn-ghost"
              style={{ width: '100%', marginTop: 8, fontSize: 13 }}
            >
              Back
            </button>
          </form>
        )}

        {step === 'credentials' && (
          <form onSubmit={handleLoginSubmit}>
            <div className="form-group">
              <label>Email Address</label>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <input
                  type="email"
                  className="form-input"
                  value={email}
                  disabled
                  style={{ flex: 1, background: '#f1f5f9', color: '#64748b' }}
                />
                <button
                  type="button"
                  onClick={handleBackToEmail}
                  className="btn btn-ghost btn-sm"
                  style={{ whiteSpace: 'nowrap' }}
                >
                  Change
                </button>
              </div>
            </div>

            <div className="form-group">
              <label>Password</label>
              <input
                type="password"
                className="form-input"
                placeholder="Enter your password"
                value={password}
                onChange={e => setPassword(e.target.value)}
                required
                autoFocus
              />
            </div>

            {loginType === 'employee' && (
              <div className="form-group">
                <label>Verification Code</label>
                <input
                  type="text"
                  className="form-input"
                  placeholder="Enter 6-digit code"
                  value={otp}
                  onChange={e => {
                    const val = e.target.value.replace(/\D/g, '').slice(0, 6);
                    setOtp(val);
                  }}
                  required
                  maxLength={6}
                  inputMode="numeric"
                  autoComplete="one-time-code"
                  style={{ letterSpacing: 4, fontSize: 18, textAlign: 'center' }}
                />
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 6 }}>
                  <small style={{ color: '#6b7280', fontSize: 12 }}>Check your email for the code</small>
                  <button
                    type="button"
                    onClick={handleResendOtp}
                    disabled={resendCooldown > 0 || loading}
                    style={{
                      background: 'none', border: 'none',
                      cursor: resendCooldown > 0 ? 'default' : 'pointer',
                      color: resendCooldown > 0 ? '#94a3b8' : '#00338D',
                      fontSize: 12, fontWeight: 600, fontFamily: 'inherit',
                    }}
                  >
                    {resendCooldown > 0 ? `Resend in ${resendCooldown}s` : 'Resend Code'}
                  </button>
                </div>
              </div>
            )}

            <button
              type="submit"
              className="btn btn-primary"
              style={{ width: '100%', marginTop: 8 }}
              disabled={loading}
            >
              {loading ? 'Signing in...' : 'Sign In'}
            </button>
            <button
              type="button"
              onClick={handleBackToChoose}
              className="btn btn-ghost"
              style={{ width: '100%', marginTop: 8, fontSize: 13 }}
            >
              Back
            </button>
          </form>
        )}

        <div className="auth-footer">
          Don't have an account? <Link to="/register">Register here</Link>
        </div>
      </div>
    </div>
  );
}
