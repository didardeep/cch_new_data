import { useState, useEffect, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { apiGet, apiPost } from '../../api';

/* ── SVG icon set ─────────────────────────────────────────────────────── */
const IC = {
  back: (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="15 18 9 12 15 6" />
    </svg>
  ),
  refresh: (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 4 23 10 17 10" /><polyline points="1 20 1 14 7 14" />
      <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
    </svg>
  ),
  send: (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="22" y1="2" x2="11" y2="13" /><polygon points="22 2 15 22 11 13 2 9 22 2" />
    </svg>
  ),
  agent: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" /><circle cx="12" cy="7" r="4" />
    </svg>
  ),
  bot: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="11" width="18" height="10" rx="2" />
      <path d="M12 1v4M8 15h.01M16 15h.01M9 3h6" />
    </svg>
  ),
  customer: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" />
      <path d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75" />
    </svg>
  ),
  chat: (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  ),
  clip: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <polyline points="14 2 14 8 20 8" />
    </svg>
  ),
  question: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
      <line x1="12" y1="17" x2="12.01" y2="17" />
    </svg>
  ),
  notes: (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
      <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
    </svg>
  ),
};

/* ── Bubble style by sender ───────────────────────────────────────────── */
const BUBBLE = {
  agent: {
    wrapper: { alignSelf: 'flex-end', alignItems: 'flex-end' },
    bubble: {
      background: 'var(--primary, #00338d)',
      color: '#fff',
      borderRadius: '16px 4px 16px 16px',
    },
    labelColor: 'var(--primary, #00338d)',
    icon: IC.agent,
    label: 'You (Agent)',
  },
  bot: {
    wrapper: { alignSelf: 'flex-start', alignItems: 'flex-start' },
    bubble: {
      background: '#eff6ff',
      color: '#1e293b',
      borderRadius: '4px 16px 16px 16px',
      border: '1px solid #bfdbfe',
    },
    labelColor: '#1d4ed8',
    icon: IC.bot,
    label: 'AI Assistant',
  },
  customer: {
    wrapper: { alignSelf: 'flex-start', alignItems: 'flex-start' },
    bubble: {
      background: '#f1f5f9',
      color: '#1e293b',
      borderRadius: '4px 16px 16px 16px',
      border: '1px solid #e2e8f0',
    },
    labelColor: '#475569',
    icon: IC.customer,
    label: 'Customer',
  },
};

/* ── Main Component ───────────────────────────────────────────────────── */
export default function AgentChatView() {
  const { sessionId } = useParams();
  const navigate = useNavigate();

  const [session, setSession] = useState(null);
  const [messages, setMessages] = useState([]);
  const [customer, setCustomer] = useState(null);
  const [newMessage, setNewMessage] = useState('');
  const [sending, setSending] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [activePanel, setActivePanel] = useState(null);
  const bottomRef = useRef(null);
  const pollingRef = useRef(null);

  const stopPolling = () => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  };

  const fetchChat = useCallback(async () => {
    try {
      const data = await apiGet(`/api/agent/chat/${sessionId}`);

      // null = 401 was received; apiGet already redirected to /login
      if (!data) { stopPolling(); return; }

      // 403 or other API error returned as JSON body
      if (data.error || data.message) {
        stopPolling();
        setError(data.error || data.message || 'Access denied.');
        setLoading(false);
        return;
      }

      setSession(data.session);
      setMessages(data.messages || []);
      // Merge customer object with session-embedded user fields as fallback
      const s = data.session || {};
      setCustomer({
        name:  data.customer?.name  || s.user_name  || '—',
        email: data.customer?.email || s.user_email || '—',
        phone: data.customer?.phone || s.user_phone || 'Not provided',
      });
      setLoading(false);
    } catch {
      stopPolling();
      setError('Failed to load chat session.');
      setLoading(false);
    }
  }, [sessionId]);

  useEffect(() => {
    fetchChat();
    pollingRef.current = setInterval(fetchChat, 8000); // poll every 8 s
    return () => stopPolling();
  }, [fetchChat]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSend = async () => {
    if (!newMessage.trim()) return;
    setSending(true);
    try {
      const data = await apiPost(`/api/agent/chat/${sessionId}/message`, {
        content: newMessage.trim(),
      });
      setMessages(prev => [...prev, data.message]);
      setNewMessage('');
    } catch {
      alert('Failed to send message.');
    } finally {
      setSending(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  /* ── Loading / Error states ─────────────────────────────────────── */
  if (loading) return (
    <div className="empty-state" style={{ height: 400 }}>
      <div className="empty-state-icon" style={{ color: 'var(--primary)' }}>{IC.chat}</div>
      <div className="empty-state-text">Loading conversation…</div>
    </div>
  );

  if (error) return (
    <div style={{
      background: '#fef2f2', border: '1px solid #fecaca',
      borderRadius: 10, padding: 20, color: '#dc2626',
    }}>
      {error}
    </div>
  );

  /* ── Render ─────────────────────────────────────────────────────── */
  return (
    <div style={{ display: 'flex', gap: 20, height: 'calc(100vh - 80px)', overflow: 'hidden' }}>

      {/* ── Chat panel ──────────────────────────────────────────────── */}
      <div style={{
        flex: 1,
        minWidth: 0,          /* prevents flex child from overflowing parent */
        display: 'flex',
        flexDirection: 'column',
        background: '#fff',
        borderRadius: 12,
        boxShadow: '0 2px 8px rgba(0,0,0,0.07)',
        border: '1px solid var(--border)',
        overflow: 'hidden',
      }}>
        {/* Header */}
        <div style={{
          background: 'linear-gradient(135deg, var(--primary, #00338d) 0%, #1a56db 100%)',
          padding: '14px 20px',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          flexShrink: 0,
        }}>
          <button
            onClick={() => navigate(-1)}
            className="btn btn-ghost btn-sm"
            style={{
              color: '#fff',
              borderColor: 'rgba(255,255,255,0.3)',
              background: 'rgba(255,255,255,0.12)',
              display: 'flex', alignItems: 'center', gap: 5,
            }}
          >
            {IC.back} Back
          </button>

          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{
              color: '#fff', fontWeight: 700, fontSize: 14,
              display: 'flex', alignItems: 'center', gap: 8,
            }}>
              {IC.chat}
              Chat Session #{sessionId}
            </div>
            <div style={{ color: 'rgba(255,255,255,0.72)', fontSize: 11, marginTop: 2 }}>
              {[session?.sector_name, session?.subprocess_name].filter(Boolean).join(' — ')}
              {session?.status && (
                <span style={{
                  marginLeft: 10,
                  padding: '2px 8px', borderRadius: 10,
                  background: session.status === 'escalated'
                    ? 'rgba(239,68,68,0.35)'
                    : 'rgba(34,197,94,0.35)',
                  color: '#fff', fontSize: 10, fontWeight: 600,
                  textTransform: 'uppercase',
                }}>
                  {session.status}
                </span>
              )}
            </div>
          </div>

          <button
            onClick={fetchChat}
            className="btn btn-ghost btn-sm"
            style={{
              color: '#fff',
              borderColor: 'rgba(255,255,255,0.3)',
              background: 'rgba(255,255,255,0.12)',
              display: 'flex', alignItems: 'center', gap: 5,
            }}
          >
            {IC.refresh} Refresh
          </button>
        </div>

        {/* Message list */}
        <div style={{
          flex: 1, overflowY: 'auto', padding: '20px 24px',
          display: 'flex', flexDirection: 'column', gap: 14,
        }}>
          {messages.length === 0 && (
            <div className="empty-state" style={{ flex: 1 }}>
              <div className="empty-state-icon" style={{ color: 'var(--primary)' }}>{IC.chat}</div>
              <div className="empty-state-text">No messages in this session yet</div>
            </div>
          )}

          {messages.map((msg) => {
            const cfg = BUBBLE[msg.sender] || BUBBLE.customer;
            return (
              <div key={msg.id} style={{
                display: 'flex', flexDirection: 'column',
                maxWidth: '72%',
                ...cfg.wrapper,
              }}>
                {/* Sender label row */}
                <div style={{
                  fontSize: 10,
                  color: cfg.labelColor,
                  marginBottom: 4,
                  display: 'flex', alignItems: 'center', gap: 4,
                  fontWeight: 600,
                }}>
                  {cfg.icon}
                  <span>{cfg.label}</span>
                  <span style={{ opacity: 0.5, fontWeight: 400 }}>·</span>
                  <span style={{ opacity: 0.7, fontWeight: 400 }}>
                    {msg.created_at
                      ? new Date(msg.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                      : ''}
                  </span>
                </div>

                {/* Bubble */}
                <div style={{
                  padding: '10px 14px',
                  fontSize: 13,
                  lineHeight: 1.65,
                  wordBreak: 'break-word',
                  boxShadow: '0 1px 3px rgba(0,0,0,0.06)',
                  ...cfg.bubble,
                }}>
                  {msg.content}
                </div>
              </div>
            );
          })}
          <div ref={bottomRef} />
        </div>

        {/* Input row */}
        <div style={{
          borderTop: '1px solid var(--border)',
          padding: '14px 20px',
          display: 'flex', gap: 10, alignItems: 'flex-end',
          flexShrink: 0,
          background: '#fafbfc',
        }}>
          <textarea
            value={newMessage}
            onChange={e => setNewMessage(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Type your reply… (Enter to send, Shift+Enter for new line)"
            rows={2}
            style={{
              flex: 1,
              padding: '10px 14px',
              borderRadius: 8,
              border: '1px solid var(--border)',
              fontSize: 13,
              resize: 'none',
              fontFamily: 'inherit',
              lineHeight: 1.5,
              outline: 'none',
              color: 'var(--text)',
              background: '#fff',
              transition: 'border-color 0.2s',
            }}
            onFocus={e => (e.target.style.borderColor = 'var(--primary, #00338d)')}
            onBlur={e => (e.target.style.borderColor = 'var(--border)')}
          />
          <button
            onClick={handleSend}
            disabled={sending || !newMessage.trim()}
            className="btn btn-primary"
            style={{
              display: 'flex', alignItems: 'center', gap: 6,
              opacity: sending || !newMessage.trim() ? 0.5 : 1,
              cursor: sending || !newMessage.trim() ? 'not-allowed' : 'pointer',
            }}
          >
            {IC.send}
            {sending ? 'Sending…' : 'Send'}
          </button>
        </div>
      </div>

      {/* ── Right side panel — Info Buttons ─────────────────────────── */}
      <div style={{
        width: 230, flexShrink: 0,
        height: '100%',
        display: 'flex', flexDirection: 'column',
        gap: 8,
      }}>
        <div style={{
          fontSize: 9, fontWeight: 700, color: '#94a3b8',
          textTransform: 'uppercase', letterSpacing: 1.2,
          paddingLeft: 2, marginBottom: 2,
        }}>
          Session Info
        </div>

        {[
          {
            key: 'customer',
            icon: IC.customer,
            label: 'Customer Info',
            sublabel: customer?.name || '—',
            iconColor: 'var(--primary, #00338d)',
            iconBg: '#eff6ff',
            hoverBorder: '#1a56db',
          },
          {
            key: 'session',
            icon: IC.clip,
            label: 'Session Details',
            sublabel: session ? `#${session.id} · ${(session.status || '').toUpperCase()}` : '—',
            iconColor: 'var(--primary, #00338d)',
            iconBg: '#eef2ff',
            hoverBorder: '#1a56db',
          },
          {
            key: 'query',
            icon: IC.question,
            label: 'Original Query',
            sublabel: session?.query_text
              ? session.query_text.slice(0, 28) + (session.query_text.length > 28 ? '…' : '')
              : 'Not recorded',
            iconColor: '#1d4ed8',
            iconBg: '#eff6ff',
            hoverBorder: '#1d4ed8',
          },
          {
            key: 'summary',
            icon: IC.notes,
            label: 'AI Summary',
            sublabel: session?.summary
              ? session.summary.slice(0, 28) + (session.summary.length > 28 ? '…' : '')
              : 'Not available',
            iconColor: '#15803d',
            iconBg: '#f0fdf4',
            hoverBorder: '#15803d',
          },
          {
            key: 'legend',
            icon: IC.chat,
            label: 'Message Legend',
            sublabel: 'Colour reference guide',
            iconColor: '#475569',
            iconBg: '#f1f5f9',
            hoverBorder: '#475569',
          },
        ].map(p => (
          <button
            key={p.key}
            onClick={() => setActivePanel(p.key)}
            style={{
              display: 'flex', alignItems: 'center', gap: 12,
              padding: '11px 14px',
              background: '#fff',
              border: '1px solid var(--border)',
              borderRadius: 10,
              cursor: 'pointer',
              textAlign: 'left',
              width: '100%',
              boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
              transition: 'box-shadow 0.15s, border-color 0.15s',
            }}
            onMouseEnter={e => {
              e.currentTarget.style.boxShadow = '0 3px 10px rgba(0,0,0,0.1)';
              e.currentTarget.style.borderColor = p.hoverBorder;
            }}
            onMouseLeave={e => {
              e.currentTarget.style.boxShadow = '0 1px 3px rgba(0,0,0,0.05)';
              e.currentTarget.style.borderColor = 'var(--border)';
            }}
          >
            <div style={{
              width: 36, height: 36, borderRadius: 9,
              background: p.iconBg,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              flexShrink: 0, color: p.iconColor,
            }}>
              {p.icon}
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text)', marginBottom: 2 }}>
                {p.label}
              </div>
              <div style={{
                fontSize: 10, color: '#94a3b8',
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              }}>
                {p.sublabel}
              </div>
            </div>
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#cbd5e1" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="9 18 15 12 9 6" />
            </svg>
          </button>
        ))}
      </div>

      {/* ── Info Panel Modal ──────────────────────────────────────────── */}
      {activePanel && (
        <div
          style={{
            position: 'fixed', inset: 0,
            background: 'rgba(15,23,42,0.45)',
            backdropFilter: 'blur(3px)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            zIndex: 1000,
          }}
          onClick={() => setActivePanel(null)}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              background: '#fff',
              borderRadius: 16,
              width: 420, maxWidth: '90vw',
              maxHeight: '82vh',
              overflowY: 'auto',
              boxShadow: '0 24px 80px rgba(0,0,0,0.22)',
            }}
          >
            {/* Modal header */}
            {(() => {
              const HEADERS = {
                customer: { icon: IC.customer, bg: '#eff6ff', color: 'var(--primary,#00338d)', title: 'Customer Info' },
                session:  { icon: IC.clip,     bg: '#eef2ff', color: 'var(--primary,#00338d)', title: 'Session Details' },
                query:    { icon: IC.question, bg: '#eff6ff', color: '#1d4ed8',                title: 'Original Query' },
                summary:  { icon: IC.notes,    bg: '#f0fdf4', color: '#15803d',                title: 'AI Summary' },
                legend:   { icon: IC.chat,     bg: '#f1f5f9', color: '#475569',                title: 'Message Legend' },
              };
              const h = HEADERS[activePanel];
              return (
                <div style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '16px 20px',
                  borderBottom: '1px solid var(--border)',
                  position: 'sticky', top: 0, background: '#fff', zIndex: 1,
                  borderRadius: '16px 16px 0 0',
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <div style={{
                      width: 32, height: 32, borderRadius: 8,
                      background: h.bg,
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      color: h.color,
                    }}>
                      {h.icon}
                    </div>
                    <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--text)' }}>{h.title}</span>
                  </div>
                  <button
                    onClick={() => setActivePanel(null)}
                    style={{
                      width: 28, height: 28, borderRadius: 6,
                      background: '#f1f5f9', border: 'none',
                      cursor: 'pointer', display: 'flex', alignItems: 'center',
                      justifyContent: 'center', color: '#64748b',
                      fontSize: 14, fontWeight: 700, lineHeight: 1,
                    }}
                  >
                    ✕
                  </button>
                </div>
              );
            })()}

            {/* Modal body */}
            <div style={{ padding: '22px 24px' }}>

              {/* ── Customer Info ── */}
              {activePanel === 'customer' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
                  {[
                    ['Name',  customer?.name  || '—'],
                    ['Email', customer?.email || '—'],
                    ['Phone', customer?.phone || customer?.phone_number || 'Not provided'],
                  ].map(([label, value]) => (
                    <div key={label} style={{
                      padding: '12px 14px',
                      background: '#f8fafc',
                      borderRadius: 9,
                      border: '1px solid var(--border)',
                    }}>
                      <div style={{
                        fontSize: 10, color: '#94a3b8',
                        textTransform: 'uppercase', letterSpacing: 1, marginBottom: 5, fontWeight: 600,
                      }}>
                        {label}
                      </div>
                      <div style={{
                        fontSize: 14, color: 'var(--text)', fontWeight: 600,
                        wordBreak: 'break-word', overflowWrap: 'break-word',
                      }}>
                        {value}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* ── Session Details ── */}
              {activePanel === 'session' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
                  {[
                    ['Session ID', session?.id ? `#${session.id}` : '—'],
                    ['Category',   session?.sector_name    || '—'],
                    ['Issue Type', session?.subprocess_name || '—'],
                    ['Language',   session?.language        || 'English'],
                    ['Status',     (session?.status || '—').toUpperCase()],
                    ['Created',    session?.created_at
                      ? new Date(session.created_at).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
                      : '—'],
                  ].map(([label, value], i, arr) => (
                    <div key={label} style={{
                      display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
                      gap: 12,
                      padding: '11px 0',
                      borderBottom: i < arr.length - 1 ? '1px solid #f1f5f9' : 'none',
                    }}>
                      <span style={{ fontSize: 12, color: '#64748b', fontWeight: 500, flexShrink: 0, width: 90 }}>
                        {label}
                      </span>
                      <span style={{
                        fontSize: 13, color: 'var(--text)', fontWeight: 600,
                        textAlign: 'right', wordBreak: 'break-word',
                        color: label === 'Status' && session?.status === 'escalated' ? '#dc2626'
                          : label === 'Status' && session?.status === 'resolved'   ? '#15803d'
                          : 'var(--text)',
                      }}>
                        {value}
                      </span>
                    </div>
                  ))}
                </div>
              )}

              {/* ── Original Query ── */}
              {activePanel === 'query' && (
                session?.query_text ? (
                  <div style={{
                    background: '#eff6ff', borderRadius: 10,
                    padding: '14px 16px',
                    border: '1px solid #bfdbfe',
                  }}>
                    <p style={{ margin: 0, fontSize: 13, color: '#1e293b', lineHeight: 1.8, wordBreak: 'break-word' }}>
                      {session.query_text}
                    </p>
                  </div>
                ) : (
                  <div style={{ textAlign: 'center', padding: '32px 0', color: '#94a3b8', fontSize: 13 }}>
                    No query text recorded for this session.
                  </div>
                )
              )}

              {/* ── AI Summary ── */}
              {activePanel === 'summary' && (
                session?.summary ? (
                  <div style={{
                    background: '#f0fdf4', borderRadius: 10,
                    padding: '14px 16px',
                    border: '1px solid #bbf7d0',
                  }}>
                    <p style={{ margin: 0, fontSize: 13, color: '#1e293b', lineHeight: 1.8, wordBreak: 'break-word' }}>
                      {session.summary}
                    </p>
                  </div>
                ) : (
                  <div style={{ textAlign: 'center', padding: '32px 0', color: '#94a3b8', fontSize: 13 }}>
                    No AI summary available for this session.
                  </div>
                )
              )}

              {/* ── Message Legend ── */}
              {activePanel === 'legend' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  {[
                    { color: 'var(--primary, #00338d)', bg: '#eff6ff', label: 'Agent (you)',   desc: 'Messages sent by you as the support agent' },
                    { color: '#1d4ed8',                  bg: '#dbeafe', label: 'AI Assistant', desc: 'Automated responses generated by the AI bot' },
                    { color: '#475569',                  bg: '#f1f5f9', label: 'Customer',     desc: 'Messages sent by the customer' },
                  ].map(({ color, bg, label, desc }) => (
                    <div key={label} style={{
                      display: 'flex', alignItems: 'center', gap: 14,
                      padding: '12px 14px',
                      background: '#f8fafc',
                      borderRadius: 9,
                      border: '1px solid var(--border)',
                    }}>
                      <div style={{
                        width: 16, height: 16, borderRadius: 5,
                        background: bg, border: `2px solid ${color}`,
                        flexShrink: 0,
                      }} />
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--text)' }}>{label}</div>
                        <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>{desc}</div>
                      </div>
                    </div>
                  ))}
                </div>
              )}

            </div>
          </div>
        </div>
      )}
    </div>
  );
}
