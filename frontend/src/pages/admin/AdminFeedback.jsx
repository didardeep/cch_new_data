import { useState, useEffect } from 'react';
import { apiGet } from '../../api';

export default function AdminFeedback() {
  const [feedbacks, setFeedbacks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    apiGet('/api/admin/feedback').then(d => {
      setFeedbacks(d?.feedbacks || []);
      setLoading(false);
    });
  }, []);

  if (loading) return <div className="page-loader"><div className="spinner" /></div>;

  const stars = (n) => '★'.repeat(n) + '☆'.repeat(5 - n);

  return (
    <div>
      <div className="page-header">
        <h1>Customer Feedback</h1>
        <p>All feedback submissions from customers</p>
      </div>

      <div className="table-card">
        <div className="table-header">
          <h3>All Feedback ({feedbacks.length})</h3>
        </div>

        {feedbacks.length === 0 ? (
          <div className="empty-state">
            <h4>No feedback yet</h4>
            <p>Feedback will appear here once customers submit ratings</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Customer</th>
                  <th>Session</th>
                  <th>Category</th>
                  <th>Rating</th>
                  <th>Comment</th>
                  <th>Date</th>
                </tr>
              </thead>
              <tbody>
                {feedbacks.map(f => (
                  <tr key={f.id}>
                    <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#64748b' }}>#{f.id}</td>
                    <td style={{ fontWeight: 500, fontSize: 13 }}>{f.user_name}</td>
                    <td>
                      {f.chat_session_id ? (
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: '#00338D', fontWeight: 600 }}>
                          #{f.chat_session_id}
                        </span>
                      ) : 'â€”'}
                    </td>
                    <td style={{ fontSize: 13 }}>{f.session_sector || '—'}</td>
                    <td>
                      <span style={{ color: '#f59e0b', fontSize: 14, letterSpacing: 1 }}>
                        {stars(f.rating)}
                      </span>
                    </td>
                    <td style={{ maxWidth: 250, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 13 }}>
                      {f.comment || '—'}
                    </td>
                    <td style={{ fontSize: 12, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                      {f.created_at ? new Date(f.created_at).toLocaleString() : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

