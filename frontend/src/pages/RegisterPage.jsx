import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuth } from '../AuthContext';
import { apiPost } from '../api';

export default function RegisterPage() {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [phone, setPhone] = useState('');  // ← NEW
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();
  const { login } = useAuth();

  const validatePassword = (pw) => {
    if (pw.length < 7) return 'Password must be at least 7 characters long';
    if (!/[A-Z]/.test(pw)) return 'Password must contain at least 1 uppercase letter';
    if (!/[!@#$%^&*()_+\-=[\]{};':"\\|,.<>/?`~]/.test(pw)) return 'Password must contain at least 1 special character';
    return null;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');

    const pwErr = validatePassword(password);
    if (pwErr) { setError(pwErr); return; }

    setLoading(true);

    try {
      // ← UPDATED: Now sends phone_number
      const data = await apiPost('/api/auth/register', { name, email, phone_number: phone, password });
      if (data.error) {
        setError(data.error);
      } else {
        login(data.token, data.user);
        navigate('/customer/dashboard');
      }
    } catch {
      setError('Something went wrong. Please try again.');
    }
    setLoading(false);
  };

  return (
    <div className="auth-page">
      <div className="auth-card">
        <div className="auth-header">
          <img src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg" alt="KPMG" style={{ height: 40 }} />
          <h2>Create Account</h2>
          <p>Register as a customer to get started</p>
        </div>

        {error && <div className="form-error">{error}</div>}

        <form onSubmit={handleSubmit}>
          <div className="form-group">
            <label>Full Name</label>
            <input
              type="text"
              className="form-input"
              placeholder="John Doe"
              value={name}
              onChange={e => setName(e.target.value)}
              required
            />
          </div>
          <div className="form-group">
            <label>Email Address</label>
            <input
              type="email"
              className="form-input"
              placeholder="you@example.com"
              value={email}
              onChange={e => setEmail(e.target.value)}
              required
            />
          </div>

          {/* ← NEW: Phone Number Field */}
          <div className="form-group">
            <label>Phone Number (WhatsApp)</label>
            <input
              type="tel"
              className="form-input"
              placeholder="+919876543210"
              value={phone}
              onChange={e => setPhone(e.target.value)}
              required
              pattern="^\+?[1-9]\d{1,14}$"
              title="Enter phone number with country code (e.g., +919876543210)"
            />
            <small style={{ color: '#6b7280', fontSize: '12px' }}>
              Include country code (e.g., +91 for India)
            </small>
          </div>

          <div className="form-group">
            <label>Password</label>
            <input
              type="password"
              className="form-input"
              placeholder="Min 7 chars, 1 uppercase, 1 special"
              value={password}
              onChange={e => setPassword(e.target.value)}
              minLength={7}
              required
            />
            <small style={{ color: '#6b7280', fontSize: '12px' }}>
              Min 7 characters, 1 uppercase letter, 1 special character
            </small>
          </div>
          <button
            type="submit"
            className="btn btn-primary"
            style={{ width: '100%', marginTop: 8 }}
            disabled={loading}
          >
            {loading ? 'Creating account...' : 'Create Account'}
          </button>
        </form>

        <div className="auth-footer">
          Already have an account? <Link to="/login">Sign in</Link>
        </div>
      </div>
    </div>
  );
}