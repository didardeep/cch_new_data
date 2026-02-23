import { useNavigate } from 'react-router-dom';
import { useAuth } from '../AuthContext';

export default function HomePage() {
  const navigate = useNavigate();
  const { user } = useAuth();

  const handleGetStarted = () => {
    navigate('/login');
  };

  return (
    <div className="home-page">
      <nav className="home-nav">
        <div className="home-logo">
          <img src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg" alt="KPMG" style={{ height: 32, filter: 'brightness(0) invert(1)' }} />
          Customer Handling
        </div>
        <div className="home-nav-actions">
          <button className="btn btn-outline btn-sm" style={{ color: '#ffffff', borderColor: 'rgba(255,255,255,0.5)' }} onClick={() => navigate('/login')}>Login</button>
          <button className="btn btn-primary btn-sm" onClick={() => navigate('/register')}>Register</button>
        </div>
      </nav>

      <div className="home-hero">
        <div className="home-hero-content">
          <div className="home-badge">
            AI-Powered Customer Support
          </div>

          <h1>
            Smart Telecom<br />
            <span>Complaint Resolution</span>
          </h1>

          <p>
            AI-powered complaint handling for telecom. Get instant resolutions, multilingual chat support,
            and automatic escalation to human agents when needed.
          </p>

          <div className="home-features">
            <div className="home-feature-card">
              <div className="home-feature-icon-box">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
              </div>
              <h4>Multilingual AI Chat</h4>
              <p>Chat in your preferred language with semantic understanding</p>
            </div>
            <div className="home-feature-card">
              <div className="home-feature-icon-box">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
              </div>
              <h4>Auto Escalation</h4>
              <p>Unresolved issues are escalated with full context automatically</p>
            </div>
            <div className="home-feature-card">
              <div className="home-feature-icon-box">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>
              </div>
              <h4>Manager Dashboard</h4>
              <p>Track tickets, monitor chats, and manage your team</p>
            </div>
            <div className="home-feature-card">
              <div className="home-feature-icon-box">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
              </div>
              <h4>Instant Resolutions</h4>
              <p>Step-by-step troubleshooting for common telecom issues</p>
            </div>
          </div>

          <button className="btn btn-primary btn-lg" onClick={handleGetStarted}>
            Get Started →
          </button>
        </div>
      </div>
    </div>
  );
}
