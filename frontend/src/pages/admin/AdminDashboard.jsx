import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiGet } from '../../api';
import { useAuth } from '../../AuthContext';
import { useTheme } from '../../ThemeContext';

export default function AdminDashboard() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const { user } = useAuth();
  const navigate = useNavigate();
  const { isDark } = useTheme();

  useEffect(() => {
    apiGet('/api/admin/dashboard').then(d => {
      setData(d);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const s = data?.stats || {};
  const users = data?.user_breakdown || [];
  const cats = data?.category_breakdown || [];

  return (
    <div>
      <div className="page-header">
        <h1>Admin Dashboard</h1>
        <p>Welcome back, {user?.name}. Complete system overview.</p>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon primary"></div></div>
          <div className="stat-card-label">Total Users</div>
          <div className="stat-card-value">{s.total_users || 0}</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon success"></div></div>
          <div className="stat-card-label">Total Chats</div>
          <div className="stat-card-value">{s.total_chats || 0}</div>
          <div className="stat-card-sub">{s.active_chats || 0} active now</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon warning"></div></div>
          <div className="stat-card-label">Total Tickets</div>
          <div className="stat-card-value">{s.total_tickets || 0}</div>
          <div className="stat-card-sub">{s.pending_tickets || 0} pending</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon danger"></div></div>
          <div className="stat-card-label">Critical / High</div>
          <div className="stat-card-value">{s.critical_tickets || 0} / {s.high_tickets || 0}</div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 20 }}>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon success"></div></div>
          <div className="stat-card-label">Resolution Rate</div>
          <div className="stat-card-value">{s.resolution_rate || 0}%</div>
          <div className="stat-card-sub">{s.resolved_chats || 0} resolved</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon primary"></div></div>
          <div className="stat-card-label">CSAT Score</div>
          <div className="stat-card-value">{s.csat_score || 0}%</div>
          <div className="stat-card-sub">{s.total_feedback || 0} responses</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon warning"></div></div>
          <div className="stat-card-label">Avg Rating</div>
          <div className="stat-card-value">{s.avg_rating || 0}/5</div>
          <div className="stat-card-sub">{s.total_feedback || 0} feedbacks</div>
        </div>
        <div className="stat-card">
          <div className="stat-card-header"><div className="stat-card-icon danger"></div></div>
          <div className="stat-card-label">Escalated</div>
          <div className="stat-card-value">{s.escalated_chats || 0}</div>
        </div>
      </div>

      {users.length > 0 && (
        <div className="section-card" style={{ marginTop: 24 }}>
          <div className="section-card-header">
            <h3>Users by Role</h3>
          </div>
          <div className="section-card-body">
            {users.map((u, i) => (
              <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: i < users.length - 1 ? `1px solid ${isDark ? '#334155' : '#f0f2f5'}` : 'none' }}>
                <span style={{ fontSize: 14, fontWeight: 500, textTransform: 'capitalize' }}>{u.role}</span>
                <span style={{ fontSize: 14, fontWeight: 700, color: isDark ? '#60a5fa' : '#00338D' }}>{u.count}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {cats.length > 0 && (
        <div className="section-card" style={{ marginTop: 24 }}>
          <div className="section-card-header">
            <h3>Issues by Category</h3>
          </div>
          <div className="section-card-body">
            {cats.map((c, i) => (
              <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: i < cats.length - 1 ? `1px solid ${isDark ? '#334155' : '#f0f2f5'}` : 'none' }}>
                <span style={{ fontSize: 14, fontWeight: 500 }}>{c.name || 'Uncategorized'}</span>
                <span style={{ fontSize: 14, fontWeight: 700, color: isDark ? '#60a5fa' : '#00338D' }}>{c.count}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div style={{ display: 'flex', gap: 12, marginTop: 20 }}>
        <button className="btn btn-primary btn-sm" onClick={() => navigate('/admin/users')}>Manage Users</button>
        <button className="btn btn-outline btn-sm" onClick={() => navigate('/admin/tickets')}>View Tickets</button>
        <button className="btn btn-outline btn-sm" onClick={() => navigate('/admin/tracking')}>Issue Tracking</button>
        <button className="btn btn-outline btn-sm" onClick={() => navigate('/admin/feedback')}>View Feedback</button>
      </div>
    </div>
  );
}
