import { useState, useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { apiPost, apiGet } from '../../api';

export default function FeedbackPage() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const sessionId = searchParams.get('session');

  const [session, setSession] = useState(null);
  const [rating, setRating] = useState(0);
  const [comment, setComment] = useState('');
  const [submitted, setSubmitted] = useState(false);
  const [feedbacks, setFeedbacks] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    apiGet('/api/feedback/list').then(d => {
      if (d?.feedbacks) setFeedbacks(d.feedbacks);
    });
  }, [submitted]);

  // Load session details if session ID is provided
  useEffect(() => {
    if (sessionId) {
      apiGet(`/api/chat/session/${sessionId}`).then(d => {
        if (d?.session) setSession(d.session);
      });
    }
  }, [sessionId]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (rating === 0) return;
    setLoading(true);
    await apiPost('/api/feedback', {
      rating,
      comment,
      chat_session_id: sessionId ? parseInt(sessionId) : undefined,
    });
    setRating(0);
    setComment('');
    setSubmitted(true);
    setLoading(false);
    // Clear session param after submitting so the form resets to general
    if (sessionId) {
      setTimeout(() => {
        navigate('/customer/feedback', { replace: true });
      }, 2000);
    } else {
      setTimeout(() => setSubmitted(false), 3000);
    }
  };

  return (
    <div>
      <div className="page-header">
        <h1>Provide Feedback</h1>
        <p>{sessionId ? 'Rate your recent support experience' : 'Help us improve our service by sharing your experience'}</p>
      </div>

      {submitted && (
        <div className="toast-success">Thank you for your feedback!</div>
      )}

      {!submitted && (
        <div className="feedback-card">
          {session && (
            <div style={{
              background: '#f0f4ff', border: '1px solid #c7d2fe', borderRadius: 8,
              padding: '12px 16px', marginBottom: 18, fontSize: 13, color: '#1e293b',
            }}>
              <strong style={{ color: '#00338D' }}>Session #{session.id}</strong>
              {session.subprocess_name && <span> &mdash; {session.subprocess_name}</span>}
              {session.sector_name && !session.subprocess_name && <span> &mdash; {session.sector_name}</span>}
              {session.query_text && (
                <div style={{ marginTop: 4, color: '#475569', fontSize: 12 }}>
                  {session.query_text.length > 100 ? session.query_text.slice(0, 100) + '...' : session.query_text}
                </div>
              )}
            </div>
          )}

          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label>Rate your experience</label>
              <div className="star-rating">
                {[1, 2, 3, 4, 5].map(n => (
                  <button key={n} type="button"
                    className={n <= rating ? 'active' : ''}
                    onClick={() => setRating(n)}>
                    ★
                  </button>
                ))}
              </div>
              <span style={{ fontSize: 13, color: '#64748b' }}>
                {rating === 0 ? 'Click a star to rate' : `${rating}/5`}
              </span>
            </div>

            <div className="form-group">
              <label>Your Comments (optional)</label>
              <textarea
                className="feedback-textarea"
                placeholder="Tell us about your experience..."
                value={comment}
                onChange={e => setComment(e.target.value)}
              />
            </div>

            <button type="submit" className="btn btn-primary" disabled={rating === 0 || loading}>
              {loading ? 'Submitting...' : 'Submit Feedback'}
            </button>
          </form>
        </div>
      )}

      {feedbacks.length > 0 && (
        <div className="feedback-list">
          <h3 style={{ fontSize: 16, fontWeight: 700, marginBottom: 14, color: '#1e293b' }}>
            Your Past Feedback
          </h3>
          {feedbacks.map(fb => (
            <div key={fb.id} className="feedback-item">
              <div className="feedback-item-header">
                <div className="feedback-item-stars">
                  {'★'.repeat(fb.rating)}{'☆'.repeat(5 - fb.rating)}
                </div>
                <div className="feedback-item-date">
                  {fb.created_at ? new Date(fb.created_at).toLocaleDateString() : ''}
                </div>
              </div>
              {fb.comment && (
                <div className="feedback-item-comment">{fb.comment}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
