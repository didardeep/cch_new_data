import { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { getToken, apiGet, apiPost, API_BASE } from '../../api';
import { useAuth } from '../../AuthContext';
import '../../styles/chatbot.css';
import { io } from 'socket.io-client';

const SOCKET_URL = process.env.REACT_APP_API_URL || 'http://localhost:5500';

// Default: Siem Reap Province, Svay Dankum Commune, Cambodia
const DEFAULT_LATITUDE = 13.3633;
const DEFAULT_LONGITUDE = 103.8564;

async function chatApiCall(endpoint, body) {
  const token = getToken();
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const resp = await fetch(`${API_BASE}${endpoint}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  return resp.json();
}

function formatResolution(text) {
  return text
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\n/g, '<br>');
}

function isMobileNetworkIssue(sectorName, subprocessName) {
  if (!sectorName || !subprocessName) return false;
  const sector = sectorName.toLowerCase();
  const sub = subprocessName.toLowerCase();
  return sector.includes('mobile services') && sub.includes('network / signal problems');
}

function isBroadbandSector(sectorKey, sectorName) {
  if (!sectorKey && !sectorName) return false;
  if (String(sectorKey) === '2') return true;
  return sectorName?.toLowerCase().includes('broadband / internet services');
}

function reduceBroadbandSubprocesses(subprocesses, sectorKey) {
  if (String(sectorKey) !== '2') return subprocesses;
  const desired = [
    { slug: 'wifi',   label: 'WiFi Signal Slow',      match: (n) => n.includes('wifi') || n.includes('slow') },
    { slug: 'billing',label: 'Billing and Plan Issues', match: (n) => n.includes('bill') || n.includes('plan') },
    { slug: 'drops',  label: 'Frequent Disconnections', match: (n) => n.includes('disconnect') || n.includes('drop') },
    { slug: 'other',  label: 'Others',               match: (n) => n.includes('other') },
  ];
  const entries = Object.entries(subprocesses || {}).map(([k, v]) => [k, String(v)]);
  const picked = {};
  desired.forEach((d) => {
    const found = entries.find(([, name]) => d.match(name.toLowerCase()));
    if (found) picked[found[0]] = d.label;
    else picked[`bb-${d.slug}`] = d.label; // synthetic key fallback
  });
  return picked;
}

function limitSubprocesses(subprocesses) {
  const entries = Object.entries(subprocesses);
  const others = entries.filter(([, v]) => v === 'Others' || v.toLowerCase().includes('other'));
  const major = entries.filter(([, v]) => v !== 'Others' && !v.toLowerCase().includes('other'));
  return Object.fromEntries([...major.slice(0, 5), ...others]);
}

const SUBPROCESS_FOLLOWUP_OPTIONS = {
  'network / signal problems': ['Internet / Mobile Data', 'Call Failure', 'Call Drop'],
};

function getFollowupOptions(subprocessName) {
  if (!subprocessName) return [];
  return SUBPROCESS_FOLLOWUP_OPTIONS[subprocessName.trim().toLowerCase()] || [];
}

const PERSIST_TYPES = new Set([
  'bot','system','sector-menu','subprocess-grid','network-subissue-grid',
  'location-question','location-prompt','location-success','location-required',
  'signal-offer','signal-codes','screenshot-upload','speed-test',
  'connection-check-offer','live-connection','resolution','diagnosis-result',
  'handoff','post-actions','unsat-options','email-action','email-sent',
  'agent-resolved','broadband-diagnostic','thankyou','exit-box',
  'non-telecom-warning','live-agent-message','user-image',
]);

function sanitizePayload(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'function') return undefined;
  if (typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(sanitizePayload);
  const out = {};
  Object.entries(value).forEach(([k, v]) => { if (typeof v !== 'function') out[k] = sanitizePayload(v); });
  return out;
}

function stripHtml(html = '') {
  if (!html) return '';
  const tmp = document.createElement('div');
  tmp.innerHTML = html;
  return (tmp.textContent || tmp.innerText || '').trim();
}

const CACHE_PREFIX = 'chat_session_cache_';
const cacheKey = (sessionId) => `${CACHE_PREFIX}${sessionId}`;

function loadCachedSession(sessionId) {
  if (!sessionId) return null;
  try { const raw = localStorage.getItem(cacheKey(sessionId)); return raw ? JSON.parse(raw) : null; }
  catch { return null; }
}

function saveCachedSession(sessionId, payload) {
  if (!sessionId) return;
  try { localStorage.setItem(cacheKey(sessionId), JSON.stringify(payload)); } catch {}
}

function clearCachedSession(sessionId) {
  if (!sessionId) return;
  try { localStorage.removeItem(cacheKey(sessionId)); } catch {}
}

function clearStoredSession() {
  // Only remove the active-session pointer — keep the cache intact so the
  // user can resume any previous chat from the Dashboard with full UI state.
  localStorage.removeItem('chat_session_id');
}

const isConnectionSummary = (msg) => {
  if (!msg || msg.type !== 'user') return false;
  const txt = msg.text || msg.html || '';
  return typeof txt === 'string' && txt.trim().toLowerCase().startsWith('connection check');
};

// ══════════════════════════════════════════════════════════════════
// MODULE-SCOPE CARDS — defined OUTSIDE ChatSupport so React never
// sees a new component type on re-render (prevents infinite loops).
// All parent-scope callbacks are received as props.
// ══════════════════════════════════════════════════════════════════

function SpeedTestCard({ msgId, groupId, disabled, disableGroup, addMessage, fetchSolution, saveMessage, stateRef, updateMessage, initialPhase, initialResults, initialReported }) {
  const [phase, setPhase]       = useState(initialPhase   || 'idle');
  const [results, setResults]   = useState(initialResults || null);
  const [reported, setReported] = useState(initialReported || false);
  const iframeRef               = useRef(null);
  const isFirstRender           = useRef(true);

  // Persist phase/results/reported into the cache whenever they change.
  useEffect(() => {
    if (isFirstRender.current) { isFirstRender.current = false; return; }
    if (updateMessage && msgId) updateMessage(msgId, { phase, results, reported });
  }, [phase, results, reported, msgId, updateMessage]);

  // Listen for postMessage from our self-hosted speed test iframe
  useEffect(() => {
    const handler = (e) => {
      if (!e.data || e.data.type !== 'speedtest-results') return;
      const { download, upload, ping } = e.data;
      setResults({ download, upload, ping });
      setPhase('done');
    };
    window.addEventListener('message', handler);
    return () => window.removeEventListener('message', handler);
  }, []);

  // Send auth token into iframe once it loads
  const handleIframeLoad = () => {
    const token = getToken();
    if (iframeRef.current && token) {
      iframeRef.current.contentWindow.postMessage({ type: 'set-token', token }, '*');
    }
    setPhase('running');
  };

  const handleUseResults = () => {
    if (!results || reported) return;
    const parts = [];
    if (results.download != null) parts.push(`Download: ${results.download} Mbps`);
    if (results.upload   != null) parts.push(`Upload: ${results.upload} Mbps`);
    if (results.ping     != null) parts.push(`Ping: ${results.ping} ms`);
    const summary = `Speed test results — ${parts.join(', ')}`;
    setReported(true);
    disableGroup(groupId);
    saveMessage('user', summary, { current_step: stateRef.current.step });
    stateRef.current.diagnosisSummary = (stateRef.current.diagnosisSummary ? stateRef.current.diagnosisSummary + ' | ' : '') + summary;
    addMessage({ type: 'user', text: summary });
    addMessage({ type: 'bot', html: `Thanks! Using your speed test results to provide a tailored solution.` });
    fetchSolution(`${summary}. Please help diagnose and fix my original issue.`);
  };

  const quality = (dl) => {
    if (!dl) return null;
    if (dl >= 100) return { label: 'Excellent', color: '#00875a' };
    if (dl >= 50)  return { label: 'Good',      color: '#00875a' };
    if (dl >= 20)  return { label: 'Fair',       color: '#c87d0a' };
    if (dl >= 5)   return { label: 'Poor',       color: '#c42b1c' };
    return { label: 'Very Poor', color: '#c42b1c' };
  };
  const q = quality(results?.download);

  return (
    <div className="speed-test-card" style={{ padding: '12px 14px' }}>
      {/* Header */}
      <div className="speed-test-card__header" style={{ marginBottom: 8 }}>
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#005EB8" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
        </svg>
        <span className="speed-test-card__title" style={{ fontSize: 12 }}>Network Speed Test</span>
        {phase === 'done' && q && (
          <span style={{ marginLeft: 'auto', fontSize: 10, fontWeight: 700, color: q.color, background: q.color + '18', padding: '2px 8px', borderRadius: 99 }}>
            {q.label}
          </span>
        )}
      </div>

      {/* Compact iframe — hidden when results already exist (restored from cache) */}
      {phase !== 'done' && (
        <div style={{ borderRadius: 10, overflow: 'hidden', border: '1px solid #1e3a5f', marginBottom: 8, height: 220 }}>
          <iframe
            ref={iframeRef}
            src={`${SOCKET_URL}/api/speedtest-widget?Run`}
            title="Speed Test"
            onLoad={handleIframeLoad}
            style={{ width: '100%', height: '100%', border: 'none', display: 'block' }}
          />
        </div>
      )}

      {/* Results row — shown after test completes */}
      {phase === 'done' && results && !reported && (
        <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '8px 12px', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ flex: 1, fontSize: 11, color: '#166534', fontWeight: 600 }}>
            ⬇ {results.download} &nbsp;⬆ {results.upload} &nbsp;📡 {results.ping ?? '—'} ms
          </div>
          <button
            onClick={handleUseResults}
            disabled={disabled}
            style={{ background: '#00875a', color: '#fff', border: 'none', borderRadius: 7, padding: '6px 14px', fontSize: 11, fontWeight: 700, fontFamily: 'inherit', cursor: disabled ? 'not-allowed' : 'pointer', whiteSpace: 'nowrap' }}
          >
            Use Results &amp; Get Help
          </button>
        </div>
      )}

      {reported && (
        <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '8px 12px', marginBottom: 8, fontSize: 11, color: '#166534', fontWeight: 600 }}>
          ✓ Results submitted — AI is analysing your connection.
        </div>
      )}

      {phase === 'running' && (
        <div style={{ fontSize: 10, color: '#8596ab', textAlign: 'center' }}>
          Test running — results will auto-detect when complete
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────

function LiveConnectionCard({ groupId, disabled, autoStart, saveMessage, stateRef, sessionIdRef, disableGroup, addMessage, fetchSolution }) {
  const [phase, setPhase]     = useState('running');
  const [results, setResults] = useState(null);
  const [reported, setReported] = useState(false);
  const iframeRef             = useRef(null);

  useEffect(() => {
    const handler = (e) => {
      if (!e.data || e.data.type !== 'speedtest-results') return;
      const { download, upload, ping } = e.data;
      setResults({ download, upload, ping });
      setPhase('done');
    };
    window.addEventListener('message', handler);
    return () => window.removeEventListener('message', handler);
  }, []);

  const handleIframeLoad = () => {
    const token = getToken();
    if (iframeRef.current && token) {
      iframeRef.current.contentWindow.postMessage({ type: 'set-token', token }, '*');
    }
  };

  const handleUseResults = () => {
    if (!results || reported) return;
    const parts = [];
    if (results.download != null) parts.push(`Download: ${results.download} Mbps`);
    if (results.upload   != null) parts.push(`Upload: ${results.upload} Mbps`);
    if (results.ping     != null) parts.push(`Ping: ${results.ping} ms`);
    const summary = `Connection check results — ${parts.join(', ')}`;
    setReported(true);
    disableGroup(groupId);
    saveMessage('user', summary, { current_step: stateRef.current.step });
    stateRef.current.diagnosisSummary = (stateRef.current.diagnosisSummary ? stateRef.current.diagnosisSummary + ' | ' : '') + summary;
    addMessage({ type: 'user', text: summary });
    addMessage({ type: 'bot', html: `Thanks! Using your connection results to provide a tailored solution.` });
    fetchSolution(`${summary}. Now please help with my original issue.`);
  };

  const quality = (dl) => {
    if (!dl) return null;
    if (dl >= 100) return { label: 'Excellent', color: '#00875a' };
    if (dl >= 50)  return { label: 'Good',      color: '#00875a' };
    if (dl >= 20)  return { label: 'Fair',       color: '#c87d0a' };
    if (dl >= 5)   return { label: 'Poor',       color: '#c42b1c' };
    return { label: 'Very Poor', color: '#c42b1c' };
  };
  const q = quality(results?.download);

  return (
    <div className="speed-test-card" style={{ padding: '12px 14px' }}>
      <div className="speed-test-card__header" style={{ marginBottom: 8 }}>
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#005EB8" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M1 6s4-2 11-2 11 2 11 2"/><path d="M1 10s4-2 11-2 11 2 11 2"/>
          <circle cx="12" cy="15" r="3"/><line x1="12" y1="12" x2="12" y2="15"/>
        </svg>
        <span className="speed-test-card__title" style={{ fontSize: 12 }}>Live Connection Check</span>
        {phase === 'done' && q && (
          <span style={{ marginLeft: 'auto', fontSize: 10, fontWeight: 700, color: q.color, background: q.color + '18', padding: '2px 8px', borderRadius: 99 }}>
            {q.label}
          </span>
        )}
      </div>

      <div style={{ borderRadius: 10, overflow: 'hidden', border: '1px solid #1e3a5f', marginBottom: 8, height: 220 }}>
        <iframe
          ref={iframeRef}
          src={`${SOCKET_URL}/api/speedtest-widget?Run`}
          title="Connection Check"
          onLoad={handleIframeLoad}
          style={{ width: '100%', height: '100%', border: 'none', display: 'block' }}
        />
      </div>

      {phase === 'done' && results && !reported && (
        <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '8px 12px', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ flex: 1, fontSize: 11, color: '#166534', fontWeight: 600 }}>
            ⬇ {results.download} &nbsp;⬆ {results.upload} &nbsp;📡 {results.ping ?? '—'} ms
          </div>
          <button onClick={handleUseResults} disabled={disabled}
            style={{ background: '#00875a', color: '#fff', border: 'none', borderRadius: 7, padding: '6px 14px', fontSize: 11, fontWeight: 700, fontFamily: 'inherit', cursor: disabled ? 'not-allowed' : 'pointer', whiteSpace: 'nowrap' }}>
            Use Results &amp; Get Help
          </button>
        </div>
      )}

      {reported && (
        <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '8px 12px', fontSize: 11, color: '#166534', fontWeight: 600 }}>
          ✓ Results submitted — AI is analysing your connection.
        </div>
      )}

      {phase === 'running' && (
        <div style={{ fontSize: 10, color: '#8596ab', textAlign: 'center' }}>
          Test running — results will auto-detect when complete
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────

function ConnectionCheckOffer({ msgId, groupId, disabled, queryText, disableGroup, addMessage, fetchSolution, stateRef, updateMessage, initialShowWidget, runDiagnostics, onDone, onSkip }) {
  const [showWidget, setShowWidget] = useState(initialShowWidget || false);
  const isFirstRender = useRef(true);

  // Persist showWidget state into the cache whenever it changes (skip initial render).
  useEffect(() => {
    if (isFirstRender.current) { isFirstRender.current = false; return; }
    if (updateMessage && msgId) updateMessage(msgId, { showWidget });
  }, [showWidget, msgId, updateMessage]);

  return (
    <div style={{ background: 'rgba(0,94,184,0.07)', border: '1px solid rgba(0,94,184,0.2)', borderRadius: 12, padding: '16px 18px', margin: '6px 0' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#005EB8" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M1 6s4-2 11-2 11 2 11 2"/><path d="M1 10s4-2 11-2 11 2 11 2"/><circle cx="12" cy="15" r="3"/>
        </svg>
        <span style={{ fontWeight: 700, fontSize: 14, color: '#00338D' }}>Connection issue detected</span>
      </div>

      {!showWidget && (
        <>
          <p style={{ fontSize: 13, color: '#3d5068', margin: '0 0 14px', lineHeight: 1.6 }}>
            It looks like you're experiencing a network issue. Would you like to run a quick live connection check first?
          </p>
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <button disabled={disabled}
              style={{ background: disabled ? '#8596ab' : '#005EB8', color: '#fff', border: 'none', borderRadius: 8, padding: '9px 20px', fontSize: 13, fontWeight: 600, fontFamily: 'inherit', cursor: disabled ? 'not-allowed' : 'pointer' }}
              onClick={() => {
                if (disabled) return;
                setShowWidget(true);
                stateRef.current.step = 'connection-check';
              }}>Check My Connection</button>
            <button disabled={disabled}
              style={{ background: 'transparent', color: '#005EB8', border: '1px solid #005EB8', borderRadius: 8, padding: '9px 20px', fontSize: 13, fontWeight: 600, fontFamily: 'inherit', cursor: disabled ? 'not-allowed' : 'pointer' }}
              onClick={() => {
                if (disabled) return;
                disableGroup(groupId);
                const afterDiag = () => onSkip ? onSkip(queryText) : fetchSolution(queryText);
                runDiagnostics ? runDiagnostics().then(afterDiag) : afterDiag();
              }}>
              Skip, just help me
            </button>
          </div>
        </>
      )}
      

      {showWidget && (
        <div style={{ marginTop: 8 }}>
          <div style={{ position: 'relative', width: '100%', maxWidth: 720, margin: '0 auto', paddingBottom: '30%', minHeight: 160, borderRadius: 10, overflow: 'hidden', border: '1px solid #1e3a5f' }}>
            <iframe
              src="https://openspeedtest.com/speedtest"
              title="Speed Test"
              sandbox="allow-scripts allow-same-origin"
              style={{ border: 'none', position: 'absolute', top: 0, left: 0, width: '100%', height: '100%', minHeight: 160, overflow: 'hidden' }}
            />
          </div>
          <div style={{ marginTop: 10, display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <button
              style={{ background: '#005EB8', color: '#fff', border: 'none', borderRadius: 8, padding: '8px 18px', fontSize: 12, fontWeight: 600, fontFamily: 'inherit', cursor: 'pointer' }}
              onClick={() => {
                disableGroup(groupId);
                const afterDiag = () => onDone ? onDone(queryText) : fetchSolution(queryText);
                runDiagnostics ? runDiagnostics().then(afterDiag) : afterDiag();
              }}>
              Done — Help me now
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ══════════════════════════════════════════════════════════════════════════════

export default function ChatSupport() {
  const { user } = useAuth();
  const navigate = useNavigate();

  const [handoffActive, setHandoffActive] = useState(false);
  const agentResolvedShownRef = useRef(false);

  const [initPhase, setInitPhase] = useState('loading');
  const [resumeCandidate, setResumeCandidate] = useState(null);
  const [resumeMessages, setResumeMessages] = useState([]);

  const [messages, setMessages] = useState([]);
  const [inputVisible, setInputVisible] = useState(false);
  const [inputPlaceholder, setInputPlaceholder] = useState('Describe your issue...');
  const [inputValue, setInputValue] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [disabledGroups, setDisabledGroups] = useState(new Set());
  const [pendingFeedback, setPendingFeedback] = useState([]);
  const [fbRating, setFbRating] = useState(0);
  const [fbComment, setFbComment] = useState('');
  const [fbSubmitting, setFbSubmitting] = useState(false);
  const [currentFbIdx, setCurrentFbIdx] = useState(0);
  const [searchParams] = useSearchParams();

  const [locationStatus, setLocationStatus] = useState('idle');
  const [screenshotUploading, setScreenshotUploading] = useState(false);
  const fileInputRef = useRef(null);

  const chatAreaRef = useRef(null);
  const inputRef = useRef(null);
  const sessionIdRef = useRef(null);
  const socketRef = useRef(null);
  const resumeNeededRef = useRef(false);
  const agentJoinedRef = useRef(false);
  const stateRef = useRef({
    step: 'welcome', sectorKey: null, sectorName: null,
    subprocessKey: null, subprocessName: null, subprocessSubType: null,
    language: 'English', queryText: '', resolution: '', attempt: 0,
    previousSolutions: [], diagnosisSummary: '', diagnosisRan: false,
    billingContext: null, connectionContext: null, planSpeedMbps: null,
  });
  const msgIdCounter = useRef(0);
  const nextId = () => ++msgIdCounter.current;

  const broadbandDiagStatusRef = useRef('idle');
  const broadbandDiagResultRef = useRef(null);
  const fetchSolutionRef = useRef(null);

  const scrollToBottom = useCallback(() => {
    setTimeout(() => { if (chatAreaRef.current) chatAreaRef.current.scrollTop = chatAreaRef.current.scrollHeight; }, 100);
  }, []);

  const disableGroup = useCallback((groupId) => {
    setDisabledGroups(prev => new Set([...prev, groupId]));
  }, []);

  const showInput = useCallback((placeholder) => {
    setInputVisible(true);
    setInputPlaceholder(placeholder || 'Describe your issue...');
    setTimeout(() => inputRef.current?.focus(), 200);
  }, []);

  const hideInput = useCallback(() => setInputVisible(false), []);

  const joinSocketSession = useCallback(() => {
    if (!socketRef.current || !sessionIdRef.current) return;
    const token = getToken();
    if (!token) return;
    socketRef.current.emit('join_session', { session_id: sessionIdRef.current, token });
  }, []);

  const markAgentMessagesSeen = useCallback(async (messageIds) => {
    if (!sessionIdRef.current || !Array.isArray(messageIds) || !messageIds.length) return;
    try {
      const token = getToken();
      await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/seen`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ message_ids: messageIds }),
      });
    } catch {}
  }, []);

  const ensureSession = useCallback(async ({ forceNew = false, step = null } = {}) => {
    try {
      const payload = { force_new: forceNew };
      if (step) payload.current_step = step;
      const data = await chatApiCall('/api/chat/session', payload);
      if (data.session) {
        sessionIdRef.current = data.session.id;
        localStorage.setItem('chat_session_id', String(data.session.id));
        if (forceNew) clearCachedSession(sessionIdRef.current);
        joinSocketSession();
      }
      return sessionIdRef.current;
    } catch { return null; }
  }, [joinSocketSession]);

  const saveMessage = useCallback(async (sender, content, meta = {}) => {
    if (!sessionIdRef.current) await ensureSession({ step: stateRef.current.step });
    if (!sessionIdRef.current) return;
    try {
      const contentPlain = sender === 'bot' ? stripHtml(meta.html || content || '') : content;
      const payloadForSave = meta.payload || (sender === 'bot' ? { type: meta.type || 'bot', html: meta.html || content, text: content } : null);
      await chatApiCall(`/api/chat/session/${sessionIdRef.current}/message`, {
        sender, content: contentPlain || '', current_step: stateRef.current.step, ...meta, payload: payloadForSave,
      });
      // Local cache is managed by addMessage (via setMessages callback) to preserve
      // correct message order. Updating it here after an async backend call would
      // append duplicates out-of-order after subsequent bot messages.
    } catch {}
  }, [ensureSession]);

  const addMessage = useCallback((msg) => {
    const id = nextId();
    const groupId = msg.groupId || id;

    const maybePersistBot = async () => {
      if (msg.type !== 'bot' && !PERSIST_TYPES.has(msg.type)) return;
      if (msg.skipPersist || msg.__hydrated) return;
      if (!sessionIdRef.current) await ensureSession({ step: stateRef.current.step });
      if (!sessionIdRef.current) return;
      const content = msg.text || stripHtml(msg.html) || msg.type || '';
      const payload = sanitizePayload(msg.payload || { type: msg.type, html: msg.html, text: msg.text, ...msg, id, groupId });
      try {
        await chatApiCall(`/api/chat/session/${sessionIdRef.current}/message`, {
          sender: 'bot', content, current_step: stateRef.current.step, payload,
        });
      } catch {}
    };

    const stampedPayload = sanitizePayload(msg.payload || { type: msg.type, html: msg.html, text: msg.text, ...msg, id, groupId });
    const stamped = { ...msg, id, groupId, payload: stampedPayload };

    setMessages(prev => {
      const base = isConnectionSummary(stamped) ? prev.filter(m => !isConnectionSummary(m)) : prev;
      const updated = [...base, stamped];
      if (sessionIdRef.current) saveCachedSession(sessionIdRef.current, { messages: updated, state: stateRef.current });
      return updated;
    });

    maybePersistBot();
    scrollToBottom();
    return groupId;
  }, [scrollToBottom, ensureSession]);

  // Lets interactive cards (SpeedTestCard, ConnectionCheckOffer) persist their
  // internal state back into the message cache so it survives a page refresh.
  const updateMessage = useCallback((msgId, updates) => {
    setMessages(prev => {
      const updated = prev.map(m => m.id === msgId ? { ...m, ...updates } : m);
      if (sessionIdRef.current) saveCachedSession(sessionIdRef.current, { messages: updated, state: stateRef.current });
      return updated;
    });
  }, []);

  const saveLocationToBackend = useCallback(async (_latitude, _longitude, _locationDescription) => {
    if (!sessionIdRef.current) return;
    // Customer location is always recorded as Phnom Penh — Chakto Mukh,
    // regardless of what the user actually shares (product decision).
    try {
      const token = getToken();
      const payload = {
        latitude: DEFAULT_LATITUDE,
        longitude: DEFAULT_LONGITUDE,
        location_description: 'Phnom Penh, Chakto Mukh',
        state_province: 'Phnom Penh',
        country: 'Cambodia',
      };
      await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/location`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify(payload),
      });
    } catch {}
  }, []);

  const requestLocation = useCallback((onSuccess) => {
    setLocationStatus('requesting');
    setLocationStatus('granted');
    saveLocationToBackend(DEFAULT_LATITUDE, DEFAULT_LONGITUDE);
    addMessage({ type: 'location-success', latitude: DEFAULT_LATITUDE, longitude: DEFAULT_LONGITUDE });
    if (onSuccess) onSuccess();
  }, [addMessage, saveLocationToBackend]);

  const afterLocationCaptured = useCallback(() => {
    setTimeout(() => {
      addMessage({ type: 'bot', html: `Thank you! Your location has been recorded.` });
      // For ALL mobile network issues (internet speed, call drop, call failure) — auto-trigger signal screenshot
      if (isMobileNetworkIssue(stateRef.current.sectorName, stateRef.current.subprocessName) && !stateRef.current.diagnosisRan) {
        addMessage({ type: 'bot', html: `To diagnose your network issue accurately, I need your device's <strong>signal readings</strong> (RSRP, SINR, RSRQ). Please follow the steps below and share a screenshot.` });
        setTimeout(() => {
          addMessage({ type: 'signal-codes' });
          setTimeout(() => addMessage({ type: 'screenshot-upload', groupId: nextId() }), 600);
        }, 500);
        stateRef.current.step = 'signal-capture';
      } else {
        addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
        showInput('Describe your issue in any language...');
        stateRef.current.step = 'query';
      }
    }, 500);
  }, [addMessage, showInput]);

  const buildConnectionContext = useCallback((billing, connection, quality) => {
    const parts = [];
    if (billing?.plan_speed_mbps) parts.push(`Plan speed: ${billing.plan_speed_mbps} Mbps`);
    if (quality?.speedMbps) { const pct = quality.speedPercent != null ? ` (${quality.speedPercent}% of plan, ${quality.speedLabel})` : ''; parts.push(`Measured speed: ${quality.speedMbps} Mbps${pct}`); }
    else if (quality?.speedError) parts.push(`Measured speed unavailable (${quality.speedError})`);
    if (quality?.latencyMs) parts.push(`Latency: ${quality.latencyMs} ms (${quality.latencyLabel})`);
    else if (quality?.latencyError) parts.push(`Latency unavailable (${quality.latencyError})`);
    if (quality?.connectionType) parts.push(`Connection type: ${quality.connectionType}`);
    if (connection?.line_quality) parts.push(`Line quality (NOC): ${connection.line_quality}`);
    if (connection?.router_status) parts.push(`Router status: ${connection.router_status}`);
    if (connection?.area_outage) parts.push('Area outage detected');
    return parts.join('. ');
  }, []);

  const runBroadbandDiagnostics = useCallback(async () => {
    if (!isBroadbandSector(stateRef.current.sectorKey, stateRef.current.sectorName)) return null;
    const name = (stateRef.current.subprocessName || '').toLowerCase();
    const isBilling = name.includes('billing');
    const isWifi = name.includes('wifi') || name.includes('signal slow');
    if ((!isBilling && !isWifi) || stateRef.current.step === 'exited') return null;
    if (broadbandDiagStatusRef.current === 'running' || broadbandDiagStatusRef.current === 'done') return broadbandDiagResultRef.current;

    broadbandDiagStatusRef.current = 'running';
    addMessage({ type: 'bot', html: 'Checking your billing and plan details...', skipPersist: true });
    setIsTyping(true);
    const diag = { billing: null, connection: null, quality: null, errors: {} };
    let planSpeed = null;

    try {
      try {
        diag.billing = await apiGet('/api/broadband/billing-check');
        planSpeed = diag.billing?.plan_speed_mbps || null;
        const billSummary = [];
        if (diag.billing?.plan_name) billSummary.push(`Plan: ${diag.billing.plan_name}`);
        if (diag.billing?.plan_speed_mbps) billSummary.push(`Speed: ${diag.billing.plan_speed_mbps} Mbps`);
        if (diag.billing?.account_active !== undefined) billSummary.push(`Account: ${diag.billing.account_active ? 'Active' : 'Inactive'}`);
        if (diag.billing?.bill_paid !== undefined) billSummary.push(`Bill paid: ${diag.billing.bill_paid ? 'Yes' : 'No'}`);
        if (diag.billing?.outstanding_amount) billSummary.push(`Outstanding: ${diag.billing.outstanding_amount}`);
        if (diag.billing?.fup_hit !== undefined) billSummary.push(`FUP hit: ${diag.billing.fup_hit ? 'Yes' : 'No'}`);
        stateRef.current.billingContext = billSummary.join('; ');
      } catch { diag.errors.billing = 'Billing check failed'; stateRef.current.billingContext = null; }

      try { diag.connection = await apiGet('/api/broadband/connection-check'); }
      catch { diag.errors.connection = 'Connection check failed'; }

      stateRef.current.connectionContext = buildConnectionContext(diag.billing, diag.connection, null) || null;
      stateRef.current.planSpeedMbps = planSpeed;
      broadbandDiagResultRef.current = diag;

      if (!stateRef.current.broadbandDiagShown) {
        addMessage({ type: 'broadband-diagnostic', billing: diag.billing, connection: diag.connection, quality: null, errors: diag.errors, planSpeed });
        stateRef.current.broadbandDiagShown = true;
      }
    } finally {
      setIsTyping(false);
      broadbandDiagStatusRef.current = 'done';
    }
  }, [addMessage, buildConnectionContext]);

  // Resolve the session and show a closing message + post-actions (shared by both Done/Skip handlers).
  const closeSessionWithMessage = useCallback(async (html) => {
    hideInput();
    addMessage({ type: 'bot', html });
    if (sessionIdRef.current) {
      try {
        const token = getToken();
        await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/resolve`, {
          method: 'PUT', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        });
      } catch {}
    }
    setTimeout(() => {
      addMessage({ type: 'bot', html: 'What would you like to do next?' });
      addMessage({ type: 'post-actions', groupId: nextId() });
    }, 700);
    stateRef.current.step = 'resolved';
  }, [addMessage, hideInput]);

  // Called when customer clicks "Done — Help me now" after running the speed test.
  // Checks billing issues first, then speed vs plan; closes chat if all looks good.
  const handleConnectionCheckDone = useCallback(async (queryText) => {
    const diag = broadbandDiagResultRef.current;
    const billing = diag?.billing || {};
    const connection = diag?.connection || {};

    // Account suspended — close with billing message
    if (billing.account_active === false) {
      await closeSessionWithMessage(
        'Your account is currently <strong>inactive or suspended</strong>. Please contact our billing team to reactivate your service. Your connection will be restored once the account is active again.'
      );
      return;
    }

    // Bill unpaid — close with payment message
    if (billing.bill_paid === false) {
      const amount = billing.outstanding_amount ? ` of <strong>$${billing.outstanding_amount}</strong>` : '';
      await closeSessionWithMessage(
        `We found an <strong>unpaid bill${amount}</strong> on your account. Service may be restricted until payment is cleared. Please pay your bill and your connection will restore within a few minutes of payment.`
      );
      return;
    }

    // Compare live speed against plan speed
    const planSpeed = billing.plan_speed_mbps || stateRef.current.planSpeedMbps;
    const measuredSpeed = connection.sync_speed_mbps;
    if (planSpeed && measuredSpeed) {
      const pct = Math.round((measuredSpeed / planSpeed) * 100);
      if (pct >= 70) {
        // Speed is good — close with positive message
        await closeSessionWithMessage(
          `Your connection is performing <strong>well</strong> — measured speed is <strong>${measuredSpeed} Mbps</strong> (${pct}% of your ${planSpeed} Mbps plan). Everything looks normal from our end. If a specific device or app still feels slow, it may be a local Wi-Fi or device issue.`
        );
        return;
      }
      // Speed is poor — tell the customer before going into solution
      addMessage({ type: 'bot', html: `Your measured speed is <strong>${measuredSpeed} Mbps</strong>, which is below your ${planSpeed} Mbps plan (${pct}%). Let me help you fix this.` });
    }

    // Speed poor or no speed data — proceed with AI solution
    await fetchSolutionRef.current(queryText);
  }, [closeSessionWithMessage, addMessage]);

  // Called when customer clicks "Skip, just help me" — checks billing issues only (no speed test ran).
  const handleConnectionCheckSkip = useCallback(async (queryText) => {
    const diag = broadbandDiagResultRef.current;
    const billing = diag?.billing || {};

    if (billing.account_active === false) {
      await closeSessionWithMessage(
        'Your account is currently <strong>inactive or suspended</strong>. Please contact our billing team to reactivate your service.'
      );
      return;
    }

    if (billing.bill_paid === false) {
      const amount = billing.outstanding_amount ? ` of <strong>$${billing.outstanding_amount}</strong>` : '';
      await closeSessionWithMessage(
        `We found an <strong>unpaid bill${amount}</strong> on your account. Please clear your dues to restore your connection.`
      );
      return;
    }

    await fetchSolutionRef.current(queryText);
  }, [closeSessionWithMessage]);

  const hydrateHistory = useCallback((history = []) => {
    const restored = [];
    let prevKey = null;

    history.forEach((m) => {
      const id = m.id || nextId();
      const groupId = m.groupId || id;

      if (m.type && m.type !== 'bot' && m.type !== 'user' && m.type !== 'system') {
        restored.push({ ...m, id, groupId, __hydrated: true }); return;
      }
      if (m.type === 'bot' || m.type === 'user' || m.type === 'system') {
        if (m.payload && m.payload.type && m.payload.type !== 'bot') { restored.push({ id, groupId, ...m.payload, __hydrated: true }); return; }
        restored.push({ ...m, id, groupId, __hydrated: true }); return;
      }

      const key = `${m.sender}|${m.content || ''}`.trim();
      if (key && key === prevKey) return;
      prevKey = key;

      if (m.payload && m.payload.type) { restored.push({ id, groupId, ...m.payload, __hydrated: true }); return; }
      if (m.content?.startsWith('__IMAGE__:')) { restored.push({ id, groupId, type: 'user-image', imageSrc: m.content.replace('__IMAGE__:', ''), __hydrated: true }); return; }
      if (m.content?.startsWith('data:image/')) { restored.push({ id, groupId, type: 'user-image', imageSrc: m.content, __hydrated: true }); return; }
      if (m.sender === 'agent') { if (m.content === '__AGENT_REQUEST_DIAGNOSIS__') return; restored.push({ id, groupId, type: 'live-agent-message', text: m.content, timestamp: m.created_at, __hydrated: true }); return; }
      if (m.sender === 'user') { restored.push({ id, groupId, type: 'user', text: m.content || '', __hydrated: true }); return; }
      if (m.sender === 'bot') {
        const content = m.content || '', html = formatResolution(content);
        restored.push(content.length > 150 ? { id, groupId, type: 'resolution', html, __hydrated: true } : { id, groupId, type: 'bot', html, __hydrated: true });
        return;
      }
      restored.push({ id, groupId, type: 'system', text: m.content || m.text || '', __hydrated: true });
    });

    const deduped = restored.reduce((acc, msg) => {
      if (isConnectionSummary(msg)) { const ei = acc.findIndex(isConnectionSummary); if (ei !== -1) acc.splice(ei, 1); }
      acc.push(msg); return acc;
    }, []);

    const wired = deduped.map((msg) => {
      if (msg.type === 'location-question') return {
        ...msg,
        onYes: () => {
          disableGroup(msg.groupId);
          addMessage({ type: 'user', text: "Yes, I'm at the issue location" });
          const lpg = nextId();
          addMessage({ type: 'location-prompt', groupId: lpg, onShare: () => { disableGroup(lpg); requestLocation(() => afterLocationCaptured(stateRef.current.subprocessName || '')); } });
          stateRef.current.step = 'location';
        },
        onNo: () => {
          disableGroup(msg.groupId);
          addMessage({ type: 'user', text: 'No, different location' });
          addMessage({ type: 'bot', html: 'No problem! Please <strong>type your area or sector name</strong> (e.g. "Chakto Mukh, Phnom Penh").' });
          showInput('Type your area or sector...');
          stateRef.current.step = 'location-manual';
        },
      };
      if (msg.type === 'location-prompt') return { ...msg, onShare: () => { disableGroup(msg.groupId); requestLocation(() => afterLocationCaptured(stateRef.current.subprocessName || '')); } };
      if (msg.type === 'location-required') return { ...msg, onRetry: () => { disableGroup(msg.groupId); requestLocation(() => afterLocationCaptured(stateRef.current.subprocessName || '')); } };
      return msg;
    });

    setMessages(wired);
    if (sessionIdRef.current) saveCachedSession(sessionIdRef.current, { messages: wired, state: stateRef.current });
  }, [disableGroup, addMessage, requestLocation, afterLocationCaptured, showInput]);

  const restoreSession = useCallback(async (opts = {}) => {
    const { sessionId: forcedSessionId = null, maxAgeMs = null } = opts || {};
    const token = getToken();
    if (!token) return false;
    try {
      let data = null;
      const storedId = forcedSessionId || localStorage.getItem('chat_session_id');
      if (storedId) {
        const resp = await fetch(`${API_BASE}/api/chat/session/${storedId}`, { headers: { 'Authorization': `Bearer ${token}` } });
        if (resp.ok) data = await resp.json();
      }
      if (!data || data.error) {
        const resp = await fetch(`${API_BASE}/api/chat/session/active`, { headers: { 'Authorization': `Bearer ${token}` } });
        if (resp.ok) data = await resp.json();
      }
      if (data && data.session) {
        // Age check — reject sessions older than maxAgeMs (used for direct chat navigation).
        // Dashboard restores (resume param) pass no maxAgeMs so they always succeed.
        if (maxAgeMs !== null) {
          const sessionTime = data.session.updated_at || data.session.created_at;
          if (sessionTime && Date.now() - new Date(sessionTime).getTime() > maxAgeMs) return false;
        }

        sessionIdRef.current = data.session.id;
        localStorage.setItem('chat_session_id', String(data.session.id));
        const cache = loadCachedSession(sessionIdRef.current);
        const dbMsgs = data.messages?.length ? data.messages : [];

        // Prefer DB messages if they have payloads saved (i.e. after the payload-storage fix).
        // Fall back to localStorage cache only if DB has no messages (e.g. very old sessions).
        const dbHasPayloads = dbMsgs.some(m => m.payload && m.payload.type);
        const toHydrate = (dbMsgs.length && dbHasPayloads) ? dbMsgs
                        : (cache?.messages?.length ? cache.messages : dbMsgs);

        if (toHydrate.length === 0) {
          try {
            const menuResp = await apiGet('/api/menu');
            addMessage({ type: 'bot', html: `<strong>Welcome to TeleBot Support!</strong><br>Say hello to get started!` });
            addMessage({ type: 'sector-menu', menu: menuResp?.menu || {}, groupId: nextId() });
            stateRef.current.step = 'greeting'; setInitPhase('chat'); showInput('Type your greeting here...'); joinSocketSession(); return true;
          } catch {}
        }

        if (cache?.state) stateRef.current = { ...stateRef.current, ...cache.state };
        setDisabledGroups(new Set(toHydrate.filter(m => m.groupId).map(m => m.groupId)));
        hydrateHistory(toHydrate);

        const maxAgentId = Math.max(0, ...dbMsgs.filter(m => m.sender === 'agent').map(m => m.id));
        if (maxAgentId > 0) lastSeenMsgIdRef.current = maxAgentId;

        stateRef.current.step = data.session.current_step || 'greeting';
        stateRef.current.sectorName = data.session.sector_name || stateRef.current.sectorName || null;
        stateRef.current.subprocessName = data.session.subprocess_name || stateRef.current.subprocessName || null;
        stateRef.current.queryText = data.session.query_text || stateRef.current.queryText || '';

        setHandoffActive(data.session.status === 'escalated');
        setInitPhase('chat');
        if (data.session.status === 'resolved') {
          hideInput();
        } else {
          showInput(data.session.status === 'escalated' ? 'Type your reply to the agent...' : 'Describe your issue...');
        }
        joinSocketSession();
        resumeNeededRef.current = true;
        return true;
      }
    } catch {}
    return false;
  }, [hydrateHistory, joinSocketSession, showInput, hideInput, addMessage]);

  const lastSeenMsgIdRef = useRef(0);

  useEffect(() => {
    if (!handoffActive) return;
    if (sessionIdRef.current) chatApiCall(`/api/chat/session/${sessionIdRef.current}/presence`, { present: true });
    const poll = async () => {
      if (!sessionIdRef.current) return;
      try {
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}`, { headers: { Authorization: `Bearer ${token}` } });
        if (!resp.ok) return;
        const data = await resp.json();
        const allMsgs = data.messages || [];
        const newAgentMsgs = allMsgs.filter(m => m.sender === 'agent' && m.id > lastSeenMsgIdRef.current);
        const sessionInfo = data.session || {};
        newAgentMsgs.forEach(m => {
          lastSeenMsgIdRef.current = Math.max(lastSeenMsgIdRef.current, m.id);
          if (m.content === '__AGENT_REQUEST_DIAGNOSIS__') {
            addMessage({ type: 'bot', html: 'Your support agent has requested a <strong>signal diagnosis</strong>. Please run it now:' });
            addMessage({ type: 'signal-offer', groupId: m.id }); return;
          }
          addMessage({ type: 'live-agent-message', text: m.content, timestamp: m.created_at });
        });
        if (newAgentMsgs.length > 0 && !agentJoinedRef.current) {
          agentJoinedRef.current = true;
          addMessage({ type: 'system', text: 'Agent connected. You can now chat below.' });
          showInput('Type your message for the agent...'); stateRef.current.step = 'live-agent';
        }
        if (sessionInfo.status === 'resolved' && !agentResolvedShownRef.current) {
          agentResolvedShownRef.current = true; setHandoffActive(false); hideInput(); stateRef.current.step = 'agent-resolved';
          const lastBot = [...allMsgs].reverse().find(m => m.sender === 'bot');
          addMessage({ type: 'agent-resolved', botMessage: lastBot?.content || 'Your support ticket has been resolved.' });
        }
      } catch {}
    };
    const iv = setInterval(poll, 6000);
    return () => { clearInterval(iv); if (sessionIdRef.current) chatApiCall(`/api/chat/session/${sessionIdRef.current}/presence`, { present: false }); };
  }, [handoffActive, addMessage, hideInput, showInput]);

  useEffect(() => {
    const token = getToken();
    if (!token) return;
    const s = io(SOCKET_URL, { transports: ['websocket', 'polling'] });
    socketRef.current = s;
    s.on('connect', () => { joinSocketSession(); });
    s.on('new_message', (m) => {
      if (!m || !sessionIdRef.current || m.session_id !== sessionIdRef.current || m.sender !== 'agent') return;
      if (m.id <= lastSeenMsgIdRef.current) return;
      lastSeenMsgIdRef.current = Math.max(lastSeenMsgIdRef.current, m.id);
      if (m.content === '__AGENT_REQUEST_DIAGNOSIS__') {
        addMessage({ type: 'bot', html: 'Your support agent has requested a <strong>signal diagnosis</strong>. Please run it now:' });
        addMessage({ type: 'signal-offer', groupId: m.id }); setHandoffActive(true); showInput('Type your reply to the agent...'); return;
      }
      addMessage({ type: 'live-agent-message', text: m.content, timestamp: m.created_at });
      markAgentMessagesSeen([m.id]); setHandoffActive(true); showInput('Type your reply to the agent...');
    });
    s.on('session_updated', (payload) => {
      if (!payload || payload.session_id !== sessionIdRef.current) return;
      if (payload.status === 'resolved' && !agentResolvedShownRef.current) {
        agentResolvedShownRef.current = true; setHandoffActive(false);
        addMessage({ type: 'agent-resolved', botMessage: 'Your support ticket has been resolved.' });
      }
    });
    return () => { s.disconnect(); socketRef.current = null; };
  }, [addMessage, joinSocketSession, markAgentMessagesSeen, showInput]);

  const startChat = useCallback(async (forceNew = false) => {
    setMessages([]); setDisabledGroups(new Set()); setLocationStatus('idle');
    setResumeCandidate(null); setResumeMessages([]);
    stateRef.current = {
      step: 'greeting', sectorKey: null, sectorName: null, subprocessKey: null, subprocessName: null,
      language: 'English', queryText: '', resolution: '', attempt: 0, previousSolutions: [],
      diagnosisSummary: '', diagnosisRan: false, billingContext: null, connectionContext: null,
      planSpeedMbps: null, broadbandDiagShown: false, connectionCheckOffered: false, subprocessSubType: null,
    };
    broadbandDiagStatusRef.current = 'idle'; broadbandDiagResultRef.current = null;
    sessionIdRef.current = null; agentResolvedShownRef.current = false; agentJoinedRef.current = false;
    setHandoffActive(false); hideInput(); setInitPhase('chat');
    if (forceNew) {
      clearStoredSession();
      try { const url = new URL(window.location.href); url.searchParams.delete('resume'); window.history.replaceState(null, '', url.toString()); } catch {}
    }
    await ensureSession({ forceNew, step: 'greeting' });
    if (sessionIdRef.current) clearCachedSession(sessionIdRef.current);
    setTimeout(() => { addMessage({ type: 'bot', html: `<strong>Welcome to TeleBot Support!</strong><br>Say hello to get started!` }); showInput('Type your greeting here...'); }, 500);
  }, [addMessage, hideInput, showInput, ensureSession]);

  const beginNewChat = useCallback(async () => { await startChat(true); }, [startChat]);

  const loadSectorMenu = useCallback(async () => {
    const token = getToken();
    setIsTyping(true);
    try {
      const resp = await fetch(`${API_BASE}/api/menu`, { headers: token ? { 'Authorization': `Bearer ${token}` } : {} });
      const data = await resp.json();
      addMessage({ type: 'sector-menu', menu: data.menu, groupId: nextId() });
      stateRef.current.step = 'sector';
    } finally { setIsTyping(false); }
  }, [addMessage]);

  const selectSector = useCallback(async (key, name, groupId) => {
    disableGroup(groupId);
    stateRef.current.sectorKey = key; stateRef.current.sectorName = name;
    stateRef.current.billingContext = null; stateRef.current.connectionContext = null; stateRef.current.planSpeedMbps = null;
    broadbandDiagStatusRef.current = 'idle'; broadbandDiagResultRef.current = null;
    addMessage({ type: 'user', text: name });
    saveMessage('user', name, { sector_name: name, current_step: 'sector' });
    addMessage({ type: 'system', text: `Selected: ${name}` });
    setIsTyping(true);
    let data = await chatApiCall('/api/subprocesses', { sector_key: key, language: stateRef.current.language });
    data = { ...data, subprocesses: reduceBroadbandSubprocesses(data.subprocesses, key) };
    setIsTyping(false);
    addMessage({ type: 'bot', html: `Great choice! Now please select the <strong>type of issue</strong> you're facing with <strong>${name}</strong>:` });
    addMessage({ type: 'subprocess-grid', subprocesses: limitSubprocesses(data.subprocesses), groupId: nextId() });
    stateRef.current.step = 'subprocess';
  }, [addMessage, disableGroup, saveMessage]);

  const beginLocationFlow = useCallback(async (selectedIssueLabel) => {
    if (!sessionIdRef.current) await ensureSession({ step: 'location' });
    addMessage({ type: 'bot', html: `You selected <strong>${selectedIssueLabel}</strong>.<br><br>To help us assist you better, we need to know about your location.` });
    const locQGroupId = nextId();
    addMessage({
      type: 'location-question', groupId: locQGroupId,
      onYes: () => {
        disableGroup(locQGroupId);
        addMessage({ type: 'user', text: "Yes, I'm at the issue location" });
        const lpg = nextId();
        addMessage({ type: 'location-prompt', groupId: lpg, onShare: () => { disableGroup(lpg); requestLocation(() => afterLocationCaptured(selectedIssueLabel)); } });
        stateRef.current.step = 'location';
      },
      onNo: () => {
        disableGroup(locQGroupId);
        addMessage({ type: 'user', text: 'No, different location' });
        addMessage({ type: 'bot', html: 'No problem! Please <strong>type your area or sector name</strong> (e.g. "Chakto Mukh, Phnom Penh").' });
        showInput('Type your area or sector...');
        stateRef.current.step = 'location-manual';
      },
    });
    stateRef.current.step = 'location-question';
  }, [addMessage, disableGroup, ensureSession, requestLocation, afterLocationCaptured, showInput]);

  useEffect(() => {
    if (!resumeNeededRef.current) return;
    const st = stateRef.current;
    if (st.step === 'location' || st.step === 'location-question') {
      resumeNeededRef.current = false;
      beginLocationFlow(st.subprocessName || st.subprocessSubType || 'your issue'); return;
    }
    if (st.step === 'signal-diagnosis') {
      resumeNeededRef.current = false;
      addMessage({ type: 'screenshot-upload', groupId: nextId() });
    }
  }, [beginLocationFlow, addMessage]);

  const autoRaiseTicket = useCallback(async (opts = {}) => {
    const { prefaceHtml } = opts;
    let refNum = '', assignedAgent = null, slaHours = null;
    if (sessionIdRef.current) {
      try {
        setIsTyping(true);
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) assignedAgent = data.assigned_agent;
      } catch {} finally { setIsTyping(false); }
    }
    const header = prefaceHtml || `Your ticket is being raised now.`;
    addMessage({
      type: 'bot',
      html: `${header}<br><br>Your ticket has been raised successfully!` +
        (refNum ? `<br>Reference: <strong>${refNum}</strong>` : '') +
        (assignedAgent
          ? `<br><br>We are connecting you to our expert. Your dedicated support agent is:<br>
             <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;">
               <div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>
               ${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.phone}</div>` : ''}
               ${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.email}</div>` : ''}
               ${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}
               ${slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : ''}
             </div>`
          : `<br><br>We are connecting you to a human agent. Our support team will reach out to you shortly.` +
            (slaHours ? `<br><span style="color:#16a34a;font-weight:600;">Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : '')) +
        `<br><br>The agent may send you messages below - please stay in this chat.<br>You can track your ticket from the dashboard.`,
    });
    addMessage({ type: 'system', text: 'Please wait — we are connecting you to a human agent.' });
    stateRef.current.step = 'live-agent';
    agentResolvedShownRef.current = false; agentJoinedRef.current = false;
    setHandoffActive(true); hideInput();
  }, [addMessage, hideInput]);

  const fetchSolution = useCallback(async (userQuery) => {
    const st = stateRef.current;
    st.attempt += 1;
    if (!sessionIdRef.current) await ensureSession({ step: 'query' });
    if (st.attempt === 1) st.queryText = userQuery;
    const msgMeta = { sector_name: st.sectorName, subprocess_name: st.subprocessName };
    if (st.attempt === 1) msgMeta.query_text = userQuery;   // only first query becomes the session issue
    saveMessage('user', userQuery, msgMeta);
    const effectiveQuery = st.subprocessSubType ? `${userQuery}\n\nIssue type: ${st.subprocessSubType}` : userQuery;
    setIsTyping(true);
    const resolveData = await chatApiCall('/api/resolve-step', {
      sector_key: st.sectorKey, subprocess_key: st.subprocessKey,
      selected_subprocess: st.subprocessName || undefined,
      query: effectiveQuery, language: st.language,
      previous_solutions: st.previousSolutions.slice(-10), attempt: st.attempt,
      original_query: st.attempt > 1 ? st.queryText : undefined,
      diagnosis_summary: st.diagnosisSummary || undefined,
      billing_context: st.billingContext || undefined,
      connection_context: st.connectionContext || undefined,
    });
    setIsTyping(false);
    if (resolveData.is_telecom === false) {
      saveMessage('bot', resolveData.resolution, { type: 'non-telecom-warning', html: formatResolution(resolveData.resolution) });
      addMessage({ type: 'non-telecom-warning', html: formatResolution(resolveData.resolution), skipPersist: true });
      addMessage({ type: 'bot', html: `Please describe a telecom-related issue so I can help you.` });
      showInput('Describe your telecom issue...'); st.attempt -= 1; return;
    }
    st.resolution = resolveData.resolution; st.previousSolutions.push(resolveData.resolution);
    const resHtml = formatResolution(resolveData.resolution);
    saveMessage('bot', resolveData.resolution, { resolution: resolveData.resolution, language: st.language, type: 'resolution', html: resHtml });
    addMessage({ type: 'resolution', html: resHtml, skipPersist: true });
    if (st.attempt >= 6) { setTimeout(() => autoRaiseTicket(), 800); return; }
    setTimeout(() => { addMessage({ type: 'bot', html: `Did this help? If not, please describe what's still not working.` }); showInput('Type your response...'); }, 800);
    st.step = 'conversation';
  }, [addMessage, saveMessage, showInput, ensureSession, autoRaiseTicket]);
  fetchSolutionRef.current = fetchSolution;

  const selectSubprocess = useCallback(async (key, name, groupId) => {
    const followupOptions = getFollowupOptions(name);
    if (followupOptions.length > 0) {
      disableGroup(groupId);
      stateRef.current.subprocessKey = key; stateRef.current.subprocessName = name;
      stateRef.current.subprocessSubType = null; stateRef.current.attempt = 0; stateRef.current.previousSolutions = [];
      addMessage({ type: 'user', text: name });
      saveMessage('user', name, { subprocess_name: name, sector_name: stateRef.current.sectorName, current_step: 'subprocess' });
      addMessage({ type: 'bot', html: `You selected <strong>${name}</strong>. Please choose the <strong>specific issue type</strong>:` });
      addMessage({ type: 'network-subissue-grid', options: followupOptions, groupId: nextId() });
      stateRef.current.step = 'network-subissue'; return;
    }
    disableGroup(groupId);
    stateRef.current.subprocessKey = key; stateRef.current.subprocessSubType = null;
    stateRef.current.subprocessName = name; stateRef.current.attempt = 0; stateRef.current.previousSolutions = [];
    addMessage({ type: 'user', text: name });
    saveMessage('user', name, { subprocess_name: name, sector_name: stateRef.current.sectorName, current_step: 'subprocess' });
    if (isMobileNetworkIssue(stateRef.current.sectorName, name)) { await beginLocationFlow(name); return; }
    addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
    showInput('Describe your issue in any language...'); stateRef.current.step = 'query';
  }, [addMessage, disableGroup, beginLocationFlow, saveMessage, showInput]);

  const selectNetworkSubissue = useCallback(async (name, groupId) => {
    disableGroup(groupId);
    stateRef.current.subprocessSubType = name;
    const finalSubprocessName = `${stateRef.current.subprocessName || 'Network / Signal Problems'} - ${name}`;
    stateRef.current.subprocessName = finalSubprocessName; stateRef.current.attempt = 0; stateRef.current.previousSolutions = [];
    addMessage({ type: 'user', text: name });
    saveMessage('user', name, { subprocess_name: finalSubprocessName, sector_name: stateRef.current.sectorName, current_step: 'subprocess' });
    if (isMobileNetworkIssue(stateRef.current.sectorName, finalSubprocessName)) { await beginLocationFlow(finalSubprocessName); return; }
    addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
    showInput('Describe your issue in any language...'); stateRef.current.step = 'query';
  }, [addMessage, disableGroup, beginLocationFlow, saveMessage, showInput]);

  const sendMessage = useCallback(async () => {
    const text = inputValue.trim();
    if (!text) return;
    addMessage({ type: 'user', text });
    setInputValue('');
    if (handoffActive || ['human_handoff', 'escalated', 'live-agent'].includes(stateRef.current.step)) { saveMessage('user', text); return; }
    hideInput();

    let userSaved = false;
    const saveUserOnce = (meta = {}) => { if (userSaved) return; saveMessage('user', text, meta); userSaved = true; };

    // ── Speed test keyword shortcut — works in any sector / step ──────────────
    const lowerText = text.toLowerCase();
    const speedKeywords = ['speed test', 'speedtest', 'check speed', 'test speed', 'internet speed', 'network speed', 'run speed', 'check my speed', 'check internet', 'test my internet', 'test internet'];
    if (speedKeywords.some(kw => lowerText.includes(kw))) {
      saveUserOnce({ current_step: stateRef.current.step });
      addMessage({ type: 'bot', html: 'Sure! Let me run a live speed test for you. This will measure your <strong>ping</strong>, <strong>download</strong>, and <strong>upload</strong> speed against our servers.' });
      addMessage({ type: 'speed-test', groupId: nextId() });
      return;
    }
    // ─────────────────────────────────────────────────────────────────────────

    if (stateRef.current.step === 'greeting') {
      saveUserOnce({ current_step: 'greeting' });
      setIsTyping(true);
      let isGreeting = true;
      try { const g = await chatApiCall('/api/detect-greeting', { text }); isGreeting = g.is_greeting !== false; } catch {}
      setIsTyping(false);
      if (!isGreeting) { addMessage({ type: 'bot', html: `Please say hello to get started!` }); showInput('Type your greeting here...'); return; }
      const userName = user?.name || 'there';
      setIsTyping(true); await new Promise(r => setTimeout(r, 700)); setIsTyping(false);
      addMessage({ type: 'bot', html: `Hi dear ${userName}! Hope you're doing well. I'm your AI-powered telecom support assistant. How can I help you today? Please choose one of the options below to get started:` });
      setTimeout(() => loadSectorMenu(), 600); stateRef.current.step = 'sector'; return;
    }

    if (stateRef.current.step === 'location-manual') {
      hideInput();
      const MANUAL_LAT = DEFAULT_LATITUDE;
      const MANUAL_LNG = DEFAULT_LONGITUDE;
      saveLocationToBackend(MANUAL_LAT, MANUAL_LNG, text);
      addMessage({ type: 'bot', html: `Your area has been set to <strong>${text}</strong>.` });
      addMessage({ type: 'location-success', latitude: MANUAL_LAT, longitude: MANUAL_LNG });
      afterLocationCaptured(stateRef.current.subprocessName || '');
      return;
    }

    if (stateRef.current.step === 'conversation') {
      setIsTyping(true);
      let clf = { is_satisfied: false, mentions_signal: false };
      try { clf = await chatApiCall('/api/classify-response', { text }); } catch {}
      setIsTyping(false);
      if (clf.is_satisfied) {
        saveUserOnce({ current_step: 'conversation' }); addMessage({ type: 'thankyou' });
        if (sessionIdRef.current) { try { const token = getToken(); await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/resolve`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } }); } catch {} }
        setTimeout(() => { addMessage({ type: 'bot', html: `What would you like to do next?` }); addMessage({ type: 'post-actions', groupId: nextId() }); }, 800);
        stateRef.current.step = 'resolved'; return;
      }
      if (clf.mentions_signal && isMobileNetworkIssue(stateRef.current.sectorName, stateRef.current.subprocessName) && !stateRef.current.diagnosisRan) {
        addMessage({ type: 'bot', html: `It sounds like you're experiencing signal issues. Would you like to run a signal diagnosis?` });
        addMessage({ type: 'signal-offer', groupId: nextId() }); stateRef.current.step = 'signal-offer'; return;
      }
      stateRef.current.step = 'query';
    }

    if (stateRef.current.language === 'English') {
      try { const ld = await chatApiCall('/api/detect-language', { text }); stateRef.current.language = ld.language || 'English'; } catch {}
      addMessage({ type: 'system', text: `Language detected: ${stateRef.current.language}` });
    }

    if (stateRef.current.attempt > 0 && !stateRef.current.diagnosisRan && isMobileNetworkIssue(stateRef.current.sectorName, stateRef.current.subprocessName)) {
      let clf2 = { mentions_signal: false };
      try { clf2 = await chatApiCall('/api/classify-response', { text }); } catch {}
      if (clf2.mentions_signal) {
        saveUserOnce({ current_step: stateRef.current.step });
        addMessage({ type: 'bot', html: `It sounds like you're experiencing signal issues. Would you like to run a signal diagnosis?` });
        addMessage({ type: 'signal-offer', groupId: nextId() }); stateRef.current.step = 'signal-offer'; return;
      }
    }

    if (isBroadbandSector(stateRef.current.sectorKey, stateRef.current.sectorName)) {
      if (stateRef.current.step === 'query' && !stateRef.current.connectionCheckOffered) {
        const lt = text.toLowerCase();
        const heuristic = lt.includes('slow') || lt.includes('speed') || lt.includes('wifi') || lt.includes('wi-fi') || lt.includes('signal');
        let clf3 = { mentions_connection_issue: false };
        try { clf3 = await chatApiCall('/api/broadband/classify-connection-issue', { text }); } catch {}
        if (heuristic || clf3.mentions_connection_issue) {
          stateRef.current.connectionCheckOffered = true; saveUserOnce({ current_step: 'query' });
          addMessage({ type: 'connection-check-offer', groupId: nextId(), queryText: text }); return;
        }
      }
      await runBroadbandDiagnostics();
    }

    await fetchSolution(text);
  }, [inputValue, addMessage, hideInput, fetchSolution, loadSectorMenu, user, handoffActive, saveMessage, runBroadbandDiagnostics]);

  const handleSendEmail = useCallback(async (groupId) => {
    disableGroup(groupId);
    if (!sessionIdRef.current) return;
    addMessage({ type: 'system', text: 'Sending summary to your email...' });
    try {
      setIsTyping(true);
      const token = getToken();
      const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/send-summary-email`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } });
      const data = await resp.json();
      if (resp.ok) addMessage({ type: 'email-sent', message: data.message });
      else addMessage({ type: 'system', text: data.error || 'Failed to send email.' });
    } catch { addMessage({ type: 'system', text: 'Failed to send email. Please try again later.' }); }
    finally { setIsTyping(false); }
  }, [addMessage, disableGroup]);

  const handleSignalDiagnosis = useCallback((groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Run signal diagnosis' });
    addMessage({ type: 'bot', html: `Let's diagnose your signal. Please follow the steps below to get your signal information.` });
    setTimeout(() => { addMessage({ type: 'signal-codes' }); setTimeout(() => addMessage({ type: 'screenshot-upload', groupId: nextId() }), 600); }, 500);
    stateRef.current.step = 'signal-diagnosis';
  }, [addMessage, disableGroup]);

  const handleRaiseTicket = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Raise a ticket' });
    saveMessage('user', 'Raise a ticket');
    let refNum = '', assignedAgent = null, slaHours = null;
    if (sessionIdRef.current) {
      try {
        setIsTyping(true);
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) assignedAgent = data.assigned_agent;
      } catch {} finally { setIsTyping(false); }
    }
    addMessage({
      type: 'bot',
      html: `Your ticket is being raised now.<br><br>Your ticket has been raised successfully!` +
        (refNum ? `<br>Reference: <strong>${refNum}</strong>` : '') +
        (assignedAgent
          ? `<br><br>Dedicated support agent:<br><div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;"><div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.phone}</div>` : ''}${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.email}</div>` : ''}${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}${slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">SLA: ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : ''}</div>`
          : `<br><br>Our support team will reach out to you shortly.${slaHours ? `<br><span style="color:#16a34a;font-weight:600;">SLA: ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : ''}`) +
        `<br>You can track your ticket from the dashboard.`,
    });
    stateRef.current.step = 'escalated'; agentResolvedShownRef.current = false; setHandoffActive(true);
  }, [addMessage, disableGroup, saveMessage]);

  const handleBackToMenu = useCallback((groupId) => {
    disableGroup(groupId);
    if (['resolved', 'agent-resolved'].includes(stateRef.current.step)) { addMessage({ type: 'user', text: 'Main Menu' }); beginNewChat(); return; }
    stateRef.current.attempt = 0; stateRef.current.previousSolutions = [];
    stateRef.current.billingContext = null; stateRef.current.connectionContext = null; stateRef.current.planSpeedMbps = null;
    broadbandDiagStatusRef.current = 'idle'; broadbandDiagResultRef.current = null;
    addMessage({ type: 'user', text: 'Main Menu' });
    addMessage({ type: 'bot', html: `Sure! Please select your <strong>telecom service category</strong>:` });
    setTimeout(() => loadSectorMenu(), 400); stateRef.current.step = 'sector';
  }, [addMessage, beginNewChat, disableGroup, loadSectorMenu]);

  const handleExit = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Exit' });
    const isHandoffMode = handoffActive || ['human_handoff', 'escalated', 'live-agent'].includes(stateRef.current.step);
    if (isHandoffMode) {
      addMessage({ type: 'system', text: agentJoinedRef.current ? 'A human agent is connected to this ticket. Please stay here to continue the live chat.' : 'Please wait — we are connecting you to a human agent.' });
      if (agentJoinedRef.current) showInput('Type your message for the agent...'); else hideInput();
      stateRef.current.step = 'live-agent'; return;
    }
    hideInput();
    const currentSessionId = sessionIdRef.current;
    if (currentSessionId) {
      addMessage({ type: 'system', text: 'Sending chat summary to your email & WhatsApp...' });
      try { const token = getToken(); const resp = await fetch(`${API_BASE}/api/chat/session/${currentSessionId}/send-summary-email`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } }); const data = await resp.json(); if (resp.ok) addMessage({ type: 'email-sent', message: data.message }); } catch {}
      if (!handoffActive) { try { const token = getToken(); await fetch(`${API_BASE}/api/chat/session/${currentSessionId}/resolve`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } }); } catch {} }
    }
    stateRef.current.step = 'exited';
    setTimeout(() => { addMessage({ type: 'exit-box' }); if (currentSessionId) navigate(`/customer/feedback?session=${currentSessionId}`); }, 800);
  }, [addMessage, disableGroup, hideInput, handoffActive, showInput, navigate]);

  const handleScreenshotUpload = useCallback(async (file) => {
    if (!file || !file.type.startsWith('image/')) { addMessage({ type: 'system', text: 'Please upload a valid image file (PNG or JPG).' }); addMessage({ type: 'screenshot-upload', groupId: nextId() }); return; }
    if (file.size > 5 * 1024 * 1024) { addMessage({ type: 'system', text: 'Image is too large. Please upload a screenshot under 5MB.' }); addMessage({ type: 'screenshot-upload', groupId: nextId() }); return; }
    const reader = new FileReader();
    reader.onload = async () => {
      const base64String = reader.result.split(',')[1];
      addMessage({ type: 'user-image', imageSrc: reader.result }); saveMessage('user', reader.result);
      setScreenshotUploading(true); setIsTyping(true);
      try {
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/analyze-signal`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` }, body: JSON.stringify({ image: base64String, image_data_url: reader.result }) });
        const data = await resp.json();
        setIsTyping(false); setScreenshotUploading(false);
        if (resp.ok && data.diagnosis) {
          addMessage({ type: 'diagnosis-result', diagnosis: data.diagnosis });
          saveMessage('bot', data.diagnosis.summary || `Signal: ${data.diagnosis.overall_label}`);
          stateRef.current.diagnosisSummary = data.diagnosis.summary || `Signal: ${data.diagnosis.overall_label}`;
          stateRef.current.diagnosisRan = true;
          setTimeout(() => {
            if (data.diagnosis.overall_status === 'red') { autoRaiseTicket({ prefaceHtml: 'Your signal is really poor. Your ticket is being raised now.' }); return; }
            addMessage({ type: 'bot', html: 'Thank you for uploading your signal screenshot. I have analyzed your network parameters. Let me suggest a solution...' });
            fetchSolution(`My signal diagnosis shows: ${stateRef.current.diagnosisSummary}`);
          }, 800);
        } else { addMessage({ type: 'system', text: data.error || 'Failed to analyze the screenshot.' }); addMessage({ type: 'screenshot-upload', groupId: nextId() }); }
      } catch { setIsTyping(false); setScreenshotUploading(false); addMessage({ type: 'system', text: 'An error occurred while analyzing the screenshot.' }); addMessage({ type: 'screenshot-upload', groupId: nextId() }); }
    };
    reader.readAsDataURL(file);
  }, [addMessage, saveMessage, fetchSolution, autoRaiseTicket]);

  const handleRetry = useCallback((groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'I want to describe my issue again' });
    addMessage({ type: 'bot', html: `Sure! Please <strong>describe your issue again</strong> with as much detail as possible.` });
    showInput('Describe your issue in more detail...'); stateRef.current.step = 'query';
  }, [addMessage, disableGroup, showInput]);

  const handleHumanHandoff = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Connect me to a human agent' });
    saveMessage('user', 'Connect me to a human agent');
    let refNum = 'TC-' + Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).substring(2, 6).toUpperCase();
    let assignedAgent = null, slaHours = null;
    if (sessionIdRef.current) {
      try {
        setIsTyping(true);
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` } });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) assignedAgent = data.assigned_agent;
      } catch {} finally { setIsTyping(false); }
    }
    addMessage({ type: 'handoff', sectorName: stateRef.current.sectorName || 'Telecom', subprocessName: stateRef.current.subprocessName || 'General', queryText: stateRef.current.queryText || 'N/A', refNum, assignedAgent });
    setTimeout(() => {
      addMessage({
        type: 'bot',
        html: `Your ticket is being raised now.<br><br>Your request has been submitted. Reference: <strong>${refNum}</strong><br><br>` +
          (assignedAgent ? `Dedicated support agent:<br><div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;"><div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.phone}</div>` : ''}${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.email}</div>` : ''}${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}${slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">SLA: ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : ''}</div>` : `Our support team will contact you shortly.${slaHours ? `<br><span style="color:#16a34a;font-weight:600;">SLA: ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : ''}`) +
          `<br><br>The agent may send you messages below — please stay in this chat.`,
      });
    }, 1500);
    addMessage({ type: 'system', text: 'Please wait — we are connecting you to a human agent.' });
    stateRef.current.step = 'live-agent'; agentResolvedShownRef.current = false; agentJoinedRef.current = false;
    setHandoffActive(true); hideInput();
  }, [addMessage, disableGroup, saveMessage, hideInput]);

  const handleKeyDown = useCallback((e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); } }, [sendMessage]);
  const handleInputChange = useCallback((e) => { setInputValue(e.target.value); e.target.style.height = 'auto'; e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px'; }, []);

  const resumeChat = useCallback(async (session, msgs) => {
    setResumeCandidate(null); setResumeMessages([]); setInitPhase('chat'); setMessages([]); setDisabledGroups(new Set()); hideInput();
    sessionIdRef.current = session.id;
    stateRef.current.sectorName = session.sector_name || null; stateRef.current.subprocessName = session.subprocess_name || null;
    stateRef.current.language = session.language || 'English'; stateRef.current.queryText = session.query_text || ''; stateRef.current.resolution = session.resolution || '';
    try {
      const token = getToken();
      const menuResp = await fetch(`${API_BASE}/api/menu`, { headers: token ? { 'Authorization': `Bearer ${token}` } : {} });
      const menuData = await menuResp.json();
      for (const [key, sector] of Object.entries(menuData.menu)) { if (sector.name === session.sector_name) { stateRef.current.sectorKey = key; break; } }
      if (stateRef.current.sectorKey && session.subprocess_name) {
        const spData = await chatApiCall('/api/subprocesses', { sector_key: stateRef.current.sectorKey, language: 'English' });
        for (const [key, name] of Object.entries(spData.subprocesses)) { if (name === session.subprocess_name || session.subprocess_name.startsWith(`${name} - `)) { stateRef.current.subprocessKey = key; break; } }
      }
    } catch {}
    setMessages([{ type: 'system', text: `Resuming your previous chat session #${session.id}`, id: nextId(), groupId: nextId() }]);
    const botResolutions = [], newMsgs = [];
    for (const m of msgs) {
      const id = nextId();
      if (m.sender === 'user') { newMsgs.push(m.content?.startsWith('data:image/') ? { type: 'user-image', imageSrc: m.content, id, groupId: id } : { type: 'user', text: m.content, id, groupId: id }); }
      else if (m.sender === 'bot') { if (m.content.length > 150) { botResolutions.push(m.content); newMsgs.push({ type: 'resolution', html: formatResolution(m.content), id, groupId: id }); } else newMsgs.push({ type: 'bot', html: formatResolution(m.content), id, groupId: id }); }
      else newMsgs.push({ type: 'system', text: m.content, id, groupId: id });
    }
    stateRef.current.previousSolutions = botResolutions; stateRef.current.attempt = botResolutions.length;
    setMessages(prev => [...prev, ...newMsgs]); scrollToBottom();
    if (!msgs.length && session.status === 'active') { addMessage({ type: 'bot', html: `<strong>Welcome to TeleBot Support!</strong><br>Say hello to get started!` }); showInput('Type your greeting here...'); stateRef.current.step = 'greeting'; return; }
    // Restore step + input state without adding duplicate bot messages
    setTimeout(() => {
      const step = session.current_step || 'greeting';
      stateRef.current.step = step;
      if (session.status === 'resolved') { hideInput(); stateRef.current.step = 'view-only'; return; }
      if (session.status === 'escalated') { agentResolvedShownRef.current = false; agentJoinedRef.current = false; setHandoffActive(true); hideInput(); stateRef.current.step = 'live-agent'; return; }
      if (step === 'conversation' || step === 'query') { showInput('Type your response...'); }
      else if (step === 'greeting') { showInput('Type your greeting here...'); }
      else if (step === 'live-agent') { hideInput(); setHandoffActive(true); }
      else { showInput('Type your response...'); }
    }, 400);
  }, [addMessage, hideInput, loadSectorMenu, scrollToBottom, showInput]);

  const proceedAfterFeedback = useCallback(async () => {
    const resumeId = searchParams.get('resume');
    if (resumeId) { try { const data = await apiGet(`/api/chat/session/${resumeId}`); if (data?.session) { resumeChat(data.session, data.messages || []); return; } } catch {} }
    setInitPhase('start-gate');
  }, [searchParams, resumeChat]);

  const handleFeedbackSubmit = useCallback(async () => {
    if (fbRating === 0) return;
    const session = pendingFeedback[currentFbIdx];
    if (!session) return;
    setFbSubmitting(true);
    await apiPost('/api/feedback', { chat_session_id: session.id, rating: fbRating, comment: fbComment });
    setFbSubmitting(false); setFbRating(0); setFbComment('');
    if (currentFbIdx + 1 < pendingFeedback.length) setCurrentFbIdx(prev => prev + 1);
    else proceedAfterFeedback();
  }, [fbRating, fbComment, pendingFeedback, currentFbIdx, proceedAfterFeedback]);

  const initialized = useRef(false);
  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    (async () => {
      const params = new URLSearchParams(window.location.search);
      const resumeId = params.get('resume');
      if (resumeId) {
        // Try restoreSession first — uses localStorage cache so all visual components
        // (sector-menu, subprocess-grid, diagnostic cards, etc.) are fully restored.
        const restored = await restoreSession({ sessionId: resumeId });
        if (restored) return;
        // Fallback: if restoreSession failed (e.g. network error), use resumeChat with DB messages.
        try { const data = await apiGet(`/api/chat/session/${resumeId}`); if (data?.session) { resumeChat(data.session, data.messages || []); return; } } catch {}
      }
      // Check for resolved sessions needing feedback before anything else
      try {
        const fbData = await apiGet('/api/customer/pending-feedback');
        if (fbData?.sessions?.length) {
          localStorage.removeItem('chat_session_id');
          setPendingFeedback(fbData.sessions);
          setCurrentFbIdx(0);
          setInitPhase('feedback-gate');
          return;
        }
      } catch {}
      // If there's an active session in localStorage, auto-resume it on refresh.
      const storedId = localStorage.getItem('chat_session_id');
      if (storedId) {
        const restored = await restoreSession({ sessionId: storedId });
        if (restored) return;
        try { const data = await apiGet(`/api/chat/session/${storedId}`); if (data?.session && data.session.status === 'active') { resumeChat(data.session, data.messages || []); return; } } catch {}
      }
      setInitPhase('start-gate');
    })();
  }, [searchParams, resumeChat, restoreSession]);

  // ══════════════════════════════════════════════════════════════════
  // RENDER HELPERS
  // ══════════════════════════════════════════════════════════════════

  const renderMessage = (msg) => {
    const isDisabled = disabledGroups.has(msg.groupId);

    // Props shared by the card components
    const cardProps = { saveMessage, stateRef, sessionIdRef, disableGroup, addMessage, fetchSolution };

    switch (msg.type) {
      case 'connection-check-offer':
        return <ConnectionCheckOffer key={msg.id} msgId={msg.id} groupId={msg.groupId} disabled={isDisabled} queryText={msg.queryText} disableGroup={disableGroup} addMessage={addMessage} fetchSolution={fetchSolution} stateRef={stateRef} updateMessage={updateMessage} initialShowWidget={msg.showWidget || false} runDiagnostics={runBroadbandDiagnostics} onDone={handleConnectionCheckDone} onSkip={handleConnectionCheckSkip} />;
      case 'live-connection':
        return <LiveConnectionCard key={msg.id} groupId={msg.groupId} disabled={isDisabled} autoStart={msg.autoStart} {...cardProps} />;
      case 'speed-test':
        return <SpeedTestCard key={msg.id} msgId={msg.id} groupId={msg.groupId} disabled={isDisabled} {...cardProps} updateMessage={updateMessage} initialPhase={msg.phase} initialResults={msg.results} initialReported={msg.reported} />;
      case 'bot':
        return <div key={msg.id} className="message bot" dangerouslySetInnerHTML={{ __html: msg.html }} />;
      case 'user':
        return (
          <div key={msg.id} className="message user">
            <span>{msg.text}</span>
            {msg.liveChat && (
              <span className={`msg-tick ${msg.seen ? 'msg-tick--seen' : 'msg-tick--sent'}`}>
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
              </span>
            )}
          </div>
        );
      case 'system':
        return <div key={msg.id} className="message system">{msg.text}</div>;
      case 'sector-menu':
        return (
          <div key={msg.id} className="menu-container">
            {Object.entries(msg.menu).map(([key, sector]) => (
              <button key={key} className={`menu-card${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && selectSector(key, sector.name, msg.groupId)}>
                <div className="card-icon">{sector.icon}</div><div className="card-label">{sector.name}</div><div className="card-arrow">&rsaquo;</div>
              </button>
            ))}
          </div>
        );
      case 'subprocess-grid':
        return (
          <div key={msg.id} className="subprocess-grid">
            {Object.entries(msg.subprocesses).map(([sk, sname], idx) => {
              const isOthers = sname === 'Others' || sname.toLowerCase().includes('other');
              return (
                <button key={sk} className={`subprocess-chip${isOthers ? ' others' : ''}${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && selectSubprocess(sk, sname, msg.groupId)}>
                  <div className="chip-num">{isOthers ? '···' : idx + 1}</div><div className="chip-label">{sname}</div><div className="chip-arrow">›</div>
                </button>
              );
            })}
          </div>
        );
      case 'network-subissue-grid':
        return (
          <div key={msg.id} className="subprocess-grid">
            {(msg.options || []).map((name, idx) => (
              <button key={`${name}-${idx}`} className={`subprocess-chip${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && selectNetworkSubissue(name, msg.groupId)}>
                <div className="chip-num">{idx + 1}</div><div className="chip-label">{name}</div><div className="chip-arrow">›</div>
              </button>
            ))}
          </div>
        );
      case 'resolution':
        return <div key={msg.id} className="resolution-box"><h4>Resolution Steps</h4><div dangerouslySetInnerHTML={{ __html: msg.html }} /></div>;
      case 'non-telecom-warning':
        return <div key={msg.id} className="non-telecom-warning" dangerouslySetInnerHTML={{ __html: msg.html }} />;
      case 'satisfaction': case 'solution-actions':
        return (
          <div key={msg.id} className="satisfaction-container">
            <button className={`sat-btn yes${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Main Menu</button>
            <button className={`sat-btn no${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit</button>
          </div>
        );
      case 'signal-offer':
        return (
          <div key={msg.id} className="satisfaction-container">
            <button className={`sat-btn yes${isDisabled ? ' disabled' : ''}`} onClick={() => { if (isDisabled) return; disableGroup(msg.groupId); handleSignalDiagnosis(msg.groupId); }}>Yes, Run Diagnosis</button>
            <button className={`sat-btn yes${isDisabled ? ' disabled' : ''}`} style={{ background: '#005EB8' }} onClick={() => { if (isDisabled) return; disableGroup(msg.groupId); addMessage({ type: 'speed-test', groupId: nextId() }); }}>Run Speed Test</button>
            <button className={`sat-btn no${isDisabled ? ' disabled' : ''}`} onClick={() => { if (isDisabled) return; disableGroup(msg.groupId); addMessage({ type: 'user', text: 'No, continue chatting' }); addMessage({ type: 'bot', html: `No problem! Please describe what's still not working.` }); showInput('Type your response...'); stateRef.current.step = 'conversation'; }}>No, Continue</button>
          </div>
        );
      case 'thankyou':
        return <div key={msg.id} className="thankyou-box"><div className="ty-icon"></div><div className="ty-title">Thank You!</div><div className="ty-msg">We're glad we could help resolve your issue.<br/>If you face any other telecom issues, feel free to come back anytime!</div></div>;
      case 'post-actions':
        return (
          <div key={msg.id} className="post-feedback-actions">
            <button className={`action-btn menu-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Main Menu</button>
            <button className={`action-btn exit-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit</button>
          </div>
        );
      case 'exit-box':
        return <div key={msg.id} className="exit-box"><div className="exit-icon"></div><div className="exit-title">Goodbye!</div><div className="exit-msg">Thank you for using Customer Handling.<br/>Have a great day! Click <strong>Restart</strong> anytime to start a new session.</div></div>;
      case 'location-question':
        return (
          <div key={msg.id} style={{ background: 'rgba(0,145,218,0.12)', border: '1px solid rgba(0,145,218,0.25)', borderRadius: 10, padding: '20px 22px', margin: '6px 0', textAlign: 'center' }}>
            <div style={{ fontWeight: 700, fontSize: 14, color: '#00338D', marginBottom: 8 }}>Location Check</div>
            <div style={{ fontSize: 13, color: '#3d5068', marginBottom: 16, lineHeight: 1.6 }}>Are you currently at the same location where you're experiencing this issue?</div>
            <div style={{ display: 'flex', gap: 10, justifyContent: 'center' }}>
              <button onClick={() => !isDisabled && msg.onYes && msg.onYes()} disabled={isDisabled} style={{ background: isDisabled ? '#8596ab' : '#0091DA', color: '#fff', border: 'none', borderRadius: 8, padding: '11px 26px', fontSize: 13, fontWeight: 600, cursor: isDisabled ? 'not-allowed' : 'pointer' }}>Yes, I'm here</button>
              <button onClick={() => !isDisabled && msg.onNo && msg.onNo()} disabled={isDisabled} style={{ background: isDisabled ? '#8596ab' : '#fff', color: isDisabled ? '#fff' : '#0091DA', border: '1px solid #0091DA', borderRadius: 8, padding: '11px 26px', fontSize: 13, fontWeight: 600, cursor: isDisabled ? 'not-allowed' : 'pointer' }}>No, different location</button>
            </div>
          </div>
        );
      case 'location-prompt':
        return (
          <div key={msg.id} style={{ background: 'rgba(0,145,218,0.12)', borderRadius: 10, padding: '20px 22px', margin: '6px 0', textAlign: 'center', boxShadow: '0 2px 8px rgba(0,145,218,0.25)' }}>
            <div style={{ fontWeight: 700, fontSize: 14, color: '#0f1d33', marginBottom: 8 }}>Location Access Required</div>
            <div style={{ fontSize: 13, color: '#3d5068', marginBottom: 16, lineHeight: 1.6 }}>To diagnose your issue, we need your current location. This is <strong>required</strong> to continue.</div>
            <button onClick={() => !isDisabled && msg.onShare && msg.onShare()} disabled={isDisabled} style={{ background: isDisabled ? '#a0c4e8' : '#0091DA', color: '#fff', border: 'none', borderRadius: 8, padding: '11px 26px', fontSize: 13, fontWeight: 700, fontFamily: 'inherit', cursor: isDisabled ? 'not-allowed' : 'pointer', margin: '0 auto', display: 'flex', alignItems: 'center', gap: 8 }}>Share My Location</button>
          </div>
        );
      case 'location-required':
        return (
          <div key={msg.id} style={{ background: '#fff', border: '1px solid #d8e0ec', borderLeft: '3px solid #c42b1c', borderRadius: 10, padding: '20px 22px', margin: '6px 0', textAlign: 'center' }}>
            <div style={{ fontWeight: 700, fontSize: 14, color: '#c42b1c', marginBottom: 8 }}>Location Access Denied</div>
            <div style={{ fontSize: 12, color: '#8596ab', marginBottom: 16 }}>Please allow location access in your browser settings, then try again.</div>
            <button onClick={() => !isDisabled && msg.onRetry && msg.onRetry()} disabled={isDisabled} style={{ background: isDisabled ? '#8596ab' : '#c42b1c', color: '#fff', border: 'none', borderRadius: 8, padding: '11px 26px', fontSize: 13, fontWeight: 600, fontFamily: 'inherit', cursor: isDisabled ? 'not-allowed' : 'pointer', display: 'block', margin: '0 auto' }}>Try Again</button>
          </div>
        );
      case 'location-success': {
        const hasCoords = typeof msg.latitude === 'number' && typeof msg.longitude === 'number';
        return (
          <div key={msg.id} style={{ background: '#fff', border: '1px solid #d8e0ec', borderLeft: '3px solid #00875a', borderRadius: 10, padding: '14px 18px', margin: '6px 0', display: 'flex', alignItems: 'center', gap: 14 }}>
            <div style={{ fontSize: 18, color: '#00875a', fontWeight: 700 }}>&#10003;</div>
            <div>
              <div style={{ fontWeight: 700, fontSize: 13, color: '#00875a' }}>Location Captured Successfully</div>
              {hasCoords && <div style={{ fontSize: 12, color: '#3d5068', marginTop: 4 }}>Lat: <strong>{msg.latitude?.toFixed(6)}</strong> &nbsp;|&nbsp; Long: <strong>{msg.longitude?.toFixed(6)}</strong></div>}
            </div>
          </div>
        );
      }
      case 'signal-codes':
        return (
          <div key={msg.id} style={{ background: 'rgba(0,145,218,0.12)', borderRadius: 10, padding: '18px 20px', margin: '6px 0', boxShadow: '0 2px 8px rgba(0,145,218,0.25)' }}>
            <div style={{ fontWeight: 700, fontSize: 14, color: '#0f1d33', marginBottom: 10 }}>Signal Diagnosis</div>
            <div style={{ fontSize: 13, color: '#3d5068', lineHeight: 1.6, marginBottom: 12 }}>Dial one of these codes on your phone and take a screenshot:</div>
            {[{ code: '*#0011#', desc: 'Samsung' }, { code: '*#*#4636#*#*', desc: 'Android' }, { code: '*3001#12345#*', desc: 'iPhone' }].map((item, i) => (
              <div key={i} style={{ background: 'rgba(0,145,218,0.1)', border: '1px solid rgba(0,145,218,0.25)', borderRadius: 8, padding: '9px 14px', display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                <code style={{ fontWeight: 700, color: '#0f1d33', fontSize: 14 }}>{item.code}</code>
                <span style={{ fontSize: 11, color: '#8596ab' }}>{item.desc}</span>
              </div>
            ))}
          </div>
        );
      case 'screenshot-upload':
        return (
          <div key={msg.id} style={{ background: 'rgba(0,145,218,0.12)', borderRadius: 10, padding: '16px 20px', margin: '6px 0', textAlign: 'center', boxShadow: '0 2px 8px rgba(0,145,218,0.25)' }}>
            <div style={{ fontSize: 13, color: '#3d5068', marginBottom: 14 }}>Upload your signal information screenshot:</div>
            <button onClick={() => { if (!isDisabled && !screenshotUploading) { fileInputRef.current?.click(); fileInputRef.current._groupId = msg.groupId; } }} disabled={isDisabled || screenshotUploading}
              style={{ background: isDisabled ? '#a0c4e8' : '#0091DA', color: '#fff', border: 'none', borderRadius: 8, padding: '11px 24px', fontSize: 13, fontWeight: 700, fontFamily: 'inherit', cursor: isDisabled ? 'not-allowed' : 'pointer', display: 'inline-flex', alignItems: 'center', gap: 8 }}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
              Upload Screenshot
            </button>
            {!isDisabled && <button onClick={() => { disableGroup(msg.groupId); addMessage({type:'bot',html:'No problem. Please <strong>describe your specific issue</strong> so I can help.'}); showInput('Describe your issue...'); stateRef.current.step='query'; }}
              style={{ background: 'transparent', color: '#64748b', border: '1px solid #cbd5e1', borderRadius: 8, padding: '7px 18px', fontSize: 12, fontWeight: 600, fontFamily: 'inherit', cursor: 'pointer', marginLeft: 8 }}>
              Skip
            </button>}
            <div style={{ fontSize: 11, color: '#8596ab', marginTop: 8 }}>PNG, JPG, JPEG (max 5MB)</div>
          </div>
        );
      case 'broadband-diagnostic': {
        const billing = msg.billing || {}, errors = msg.errors || {};
        const planSpeed = msg.planSpeed || billing.plan_speed_mbps || null;
        const accountActive = billing.account_active !== false;
        return (
          <div key={msg.id} style={{ background: '#f7f9fc', border: '1px solid #d8e0ec', borderLeft: '4px solid #00338d', borderRadius: 12, padding: '16px 18px', margin: '8px 0' }}>
            <div style={{ fontWeight: 700, fontSize: 14, color: '#0f1d33', marginBottom: 6 }}>Billing & Plan Details</div>
            <div style={{ background: '#fff', border: '1px solid #d8e0ec', borderRadius: 10, padding: '12px 14px', marginTop: 10 }}>
              <div style={{ fontSize: 11, letterSpacing: '0.04em', color: '#00338d', fontWeight: 700, textTransform: 'uppercase', marginBottom: 6 }}>Plan Details</div>
              {errors.billing ? <div style={{ color: '#c42b1c', fontSize: 12 }}>{errors.billing}</div> : (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: '8px 12px', fontSize: 12, color: '#1a2b42' }}>
                  <div><span style={{ color: '#8596ab' }}>Plan:</span> <strong>{billing.plan_name || 'Not on record'}</strong></div>
                  <div><span style={{ color: '#8596ab' }}>Speed:</span> <strong>{planSpeed != null ? `${planSpeed} Mbps` : 'Not on record'}</strong></div>
                  <div><span style={{ color: '#8596ab' }}>Account:</span> <strong style={{ color: accountActive ? '#0f1d33' : '#c42b1c' }}>{accountActive ? 'Active' : 'Inactive'}</strong></div>
                  <div><span style={{ color: '#8596ab' }}>Bill Paid:</span> <strong style={{ color: billing.bill_paid === false ? '#c42b1c' : '#0f1d33' }}>{billing.bill_paid === false ? 'No' : 'Yes'}</strong></div>
                </div>
              )}
            </div>
          </div>
        );
      }
      case 'user-image':
        return <div key={msg.id} style={{ display: 'flex', justifyContent: 'flex-end' }}><div style={{ maxWidth: 240, borderRadius: 12, overflow: 'hidden', border: '1px solid #d8e0ec' }}><img src={msg.imageSrc} alt="Uploaded screenshot" style={{ width: '100%', display: 'block' }} /></div></div>;
      case 'diagnosis-result': {
        const d = msg.diagnosis;
        const overallColor = { green: '#00875a', amber: '#c87d0a', red: '#c42b1c', unknown: '#8596ab' };
        const overallBg = { green: '#f0fdf4', amber: '#fffbeb', red: '#fef2f2', unknown: '#f7f9fc' };
        const status = d.overall_status || 'unknown';
        return (
          <div key={msg.id} style={{ background: overallBg[status], border: '1px solid #d8e0ec', borderLeft: `4px solid ${overallColor[status]}`, borderRadius: 10, padding: '18px 20px', margin: '6px 0' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
              <div style={{ background: overallColor[status], color: '#fff', borderRadius: 12, padding: '4px 14px', fontSize: 12, fontWeight: 700 }}>Signal: {d.overall_label || 'Unknown'}</div>
              {d.is_busy_hour && <div style={{ background: '#c87d0a', color: '#fff', borderRadius: 12, padding: '4px 14px', fontSize: 12, fontWeight: 700 }}>Peak Hours</div>}
            </div>
            <div style={{ fontSize: 13, color: '#1a2b42', lineHeight: 1.6 }}>{d.summary}</div>
            {d.nearest_sites?.length > 0 && (
              <div style={{ marginTop: 14, borderTop: '1px solid #d8e0ec', paddingTop: 14 }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: '#00338D', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 10 }}>Nearest Tower Sites</div>
                {d.nearest_sites.map((site, idx) => {
                  const sc = site.status === 'ON AIR' ? '#00875a' : '#c42b1c';
                  return (
                    <div key={idx} style={{ background: '#fff', border: '1px solid #d8e0ec', borderRadius: 8, padding: '12px 14px', marginBottom: idx < d.nearest_sites.length - 1 ? 8 : 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                        <span style={{ fontWeight: 700, fontSize: 13, color: '#0f1d33' }}>{site.site_id}</span>
                        <span style={{ fontSize: 11, fontWeight: 700, color: sc, background: site.status === 'ON AIR' ? 'rgba(0,135,90,0.08)' : 'rgba(196,43,28,0.08)', padding: '2px 10px', borderRadius: 6 }}>{site.status}</span>
                      </div>
                      <div style={{ fontSize: 12, color: '#3d5068', lineHeight: 1.5 }}><span style={{ color: '#8596ab' }}>Distance:</span> {site.distance_km} km{site.alarm && site.alarm !== 'None' ? ` | Alarm: ${site.alarm}` : ''}</div>
                      {site.solution && site.solution !== 'No action required' && <div style={{ fontSize: 12, color: '#00338D', marginTop: 4, fontWeight: 600 }}>Action: {site.solution}</div>}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        );
      }
      case 'post-diagnosis-actions':
        return (
          <div key={msg.id} className="post-feedback-actions">
            <button className={`action-btn menu-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Continue Chat</button>
            <button className={`action-btn exit-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit</button>
            <button className={`sat-btn ticket${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleRaiseTicket(msg.groupId)}>Raise a Ticket</button>
          </div>
        );
      case 'unsat-options':
        return (
          <div key={msg.id} className="unsat-options">
            {[
              { cls: 'retry', title: 'Describe Again', desc: 'Provide more details for better resolution steps', fn: () => handleRetry(msg.groupId) },
              { cls: 'human', title: 'Connect to Human Agent', desc: 'A support ticket will be raised for you', fn: () => handleHumanHandoff(msg.groupId) },
              { cls: 'newc', title: 'Main Menu', desc: 'Go back to the main service category menu', fn: () => handleBackToMenu(msg.groupId) },
              { cls: 'exit', title: 'Exit', desc: 'End this session', fn: () => handleExit(msg.groupId) },
            ].map((opt, i) => (
              <button key={i} className={`unsat-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && opt.fn()}>
                <div className={`o-icon ${opt.cls}`} />
                <div className="o-info"><div className="o-title">{opt.title}</div><div className="o-desc">{opt.desc}</div></div>
                <div className="o-arrow">&rsaquo;</div>
              </button>
            ))}
          </div>
        );
      case 'email-action':
        return <div key={msg.id} className="email-action-container"><button className={`email-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleSendEmail(msg.groupId)}><span className="email-btn-icon"></span>Send Summary to My Email</button></div>;
      case 'email-sent':
        return <div key={msg.id} className="email-sent-box"><div className="email-sent-icon"></div><div className="email-sent-text">{msg.message}</div></div>;
      case 'handoff':
        return (
          <div key={msg.id} className="handoff-box">
            <h4>Human Agent Request Submitted</h4>
            <div className="handoff-row"><span className="h-label">Category</span><span className="h-value">{msg.sectorName}</span></div>
            <div className="handoff-row"><span className="h-label">Issue Type</span><span className="h-value">{msg.subprocessName}</span></div>
            <div className="handoff-row"><span className="h-label">Complaint</span><span className="h-value">{msg.queryText}</span></div>
            {msg.assignedAgent
              ? <><div className="handoff-row"><span className="h-label">Status</span><span className="h-value" style={{ color: '#22c55e', fontWeight: 700 }}>Agent Assigned</span></div><div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 14px', margin: '10px 0 6px' }}><div style={{ fontSize: 14, fontWeight: 700 }}>{msg.assignedAgent.name}</div>{msg.assignedAgent.phone && <div style={{ fontSize: 13, color: '#0ea5e9', marginTop: 4 }}>Phone: {msg.assignedAgent.phone}</div>}</div></>
              : <div className="handoff-row"><span className="h-label">Status</span><span className="h-value status-pending">Pending Agent Assignment</span></div>
            }
            <div className="handoff-ref">Reference No: {msg.refNum}</div>
          </div>
        );
      case 'live-agent-message':
        return (
          <div key={msg.id} style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', maxWidth: '80%' }}>
            <div style={{ fontSize: 10, color: '#00338d', fontWeight: 600, marginBottom: 4, display: 'flex', alignItems: 'center', gap: 4 }}>
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
              Support Agent
              {msg.timestamp && <span style={{ opacity: 0.6, fontWeight: 400 }}>· {new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>}
            </div>
            <div style={{ background: '#eff6ff', border: '1px solid #93c5fd', borderRadius: '4px 16px 16px 16px', padding: '10px 14px', fontSize: 13, color: '#1e293b', lineHeight: 1.6, wordBreak: 'break-word' }}>{msg.text}</div>
          </div>
        );
      case 'agent-resolved':
        return (
          <div key={msg.id} style={{ background: 'linear-gradient(135deg,#f0fdf4,#dcfce7)', border: '2px solid #22c55e', borderRadius: 14, padding: '20px 22px', margin: '8px 0' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
              <div style={{ width: 36, height: 36, borderRadius: '50%', background: '#22c55e', display: 'flex', alignItems: 'center', justifyContent: 'center' }}><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg></div>
              <div><div style={{ fontWeight: 700, fontSize: 15, color: '#15803d' }}>Issue Resolved</div><div style={{ fontSize: 11, color: '#16a34a', marginTop: 2 }}>Your support ticket has been closed</div></div>
            </div>
            <p style={{ margin: '0 0 16px', fontSize: 13, color: '#1e293b', lineHeight: 1.65 }}>{msg.botMessage}</p>
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              <button className="action-btn menu-btn" onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Main Menu</button>
              <button className="action-btn exit-btn" onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit Chat</button>
            </div>
          </div>
        );
      default: return null;
    }
  };

  const renderResumePrompt = () => {
    const session = resumeCandidate;
    if (!session) return null;
    const lastMsg = resumeMessages.length > 0 ? resumeMessages[resumeMessages.length - 1] : null;
    const isActiveSession = session.status === 'active';
    return (
      <div className="gate-overlay">
        <div className="gate-card resume-gate">
          <div className="gate-icon">&#128172;</div>
          <h2 className="gate-title">{isActiveSession ? 'Active Chat Found' : 'Previous Chat Found'}</h2>
          <p className="gate-subtitle">{isActiveSession ? 'You have an active chat session. Would you like to continue or start a new one?' : 'Would you like to open this chat history or start a new chat?'}</p>
          <div className="gate-session-info">
            <div className="gate-session-row"><span className="gate-label">Session</span><span className="gate-value">#{session.id}</span></div>
            {session.sector_name && <div className="gate-session-row"><span className="gate-label">Category</span><span className="gate-value">{session.sector_name}</span></div>}
            {session.subprocess_name && <div className="gate-session-row"><span className="gate-label">Issue Type</span><span className="gate-value">{session.subprocess_name}</span></div>}
            <div className="gate-session-row"><span className="gate-label">Started</span><span className="gate-value">{session.created_at ? new Date(session.created_at).toLocaleString() : 'N/A'}</span></div>
            {lastMsg && <div className="gate-summary"><span className="gate-label">Last message</span><p>{lastMsg.content?.length > 120 ? lastMsg.content.slice(0, 120) + '...' : lastMsg.content}</p></div>}
          </div>
          <div className="gate-actions">
            <button className="gate-btn gate-btn-primary" onClick={() => resumeChat(resumeCandidate, resumeMessages)}>{isActiveSession ? 'Continue Chat' : 'Open Chat'}</button>
            <button className="gate-btn gate-btn-secondary" onClick={() => beginNewChat()}>Start New Chat</button>
          </div>
        </div>
      </div>
    );
  };

  const renderFeedbackGate = () => {
    const session = pendingFeedback[currentFbIdx];
    if (!session) return null;
    const total = pendingFeedback.length;
    const current = currentFbIdx + 1;

    // List view: show all pending sessions
    if (!fbRating && currentFbIdx === 0 && total > 0) {
      return (
        <div className="gate-overlay">
          <div className="gate-card resume-gate" style={{ maxWidth: 520 }}>
            <div className="gate-icon" style={{ fontSize: 32 }}>&#9733;</div>
            <h2 className="gate-title">Feedback Required</h2>
            <p className="gate-subtitle">
              You have {total} resolved session{total > 1 ? 's' : ''} awaiting feedback. Please rate your experience to continue.
            </p>
            <div style={{ maxHeight: 300, overflowY: 'auto', width: '100%', marginBottom: 16 }}>
              {pendingFeedback.map((s, idx) => (
                <div key={s.id}
                  onClick={() => setCurrentFbIdx(idx)}
                  style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    padding: '12px 16px', margin: '6px 0', borderRadius: 10, cursor: 'pointer',
                    background: idx === currentFbIdx ? '#eff6ff' : '#f8fafc',
                    border: idx === currentFbIdx ? '2px solid #3b82f6' : '1px solid #e2e8f0',
                    transition: 'all 0.15s',
                  }}
                >
                  <div>
                    <div style={{ fontWeight: 600, fontSize: 13, color: '#1e293b' }}>
                      Ticket #{s.ticket_id} — {s.subprocess_name || s.sector_name || 'Support'}
                    </div>
                    <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>
                      {s.query_text ? (s.query_text.length > 80 ? s.query_text.slice(0, 80) + '...' : s.query_text) : 'No description'}
                    </div>
                  </div>
                  <div style={{ fontSize: 11, color: '#94a3b8', whiteSpace: 'nowrap', marginLeft: 12 }}>
                    {s.resolved_at ? new Date(s.resolved_at).toLocaleDateString() : ''}
                  </div>
                </div>
              ))}
            </div>
            <p style={{ fontSize: 12, color: '#64748b', marginBottom: 8 }}>Click a session above, then rate below</p>
            {/* Rating form inline */}
            <div style={{ width: '100%', background: '#f0f4ff', borderRadius: 10, padding: '16px', border: '1px solid #c7d2fe' }}>
              <div style={{ fontWeight: 600, fontSize: 13, color: '#1e293b', marginBottom: 8 }}>
                Rate: Ticket #{session.ticket_id} — {session.subprocess_name || session.sector_name || 'Support'}
              </div>
              <div style={{ display: 'flex', gap: 6, marginBottom: 10 }}>
                {[1, 2, 3, 4, 5].map(n => (
                  <button key={n} type="button" onClick={() => setFbRating(n)}
                    style={{
                      fontSize: 26, background: 'none', border: 'none', cursor: 'pointer',
                      color: n <= fbRating ? '#f59e0b' : '#cbd5e1', transition: 'color 0.15s',
                    }}
                  >&#9733;</button>
                ))}
                <span style={{ fontSize: 12, color: '#64748b', alignSelf: 'center', marginLeft: 4 }}>
                  {fbRating > 0 ? `${fbRating}/5` : ''}
                </span>
              </div>
              <textarea
                placeholder="Optional comment..."
                value={fbComment}
                onChange={e => setFbComment(e.target.value)}
                rows={2}
                style={{
                  width: '100%', borderRadius: 8, border: '1px solid #e2e8f0', padding: '8px 12px',
                  fontSize: 13, resize: 'none', marginBottom: 10, fontFamily: 'inherit',
                }}
              />
              <button
                onClick={handleFeedbackSubmit}
                disabled={fbRating === 0 || fbSubmitting}
                style={{
                  width: '100%', padding: '10px', borderRadius: 8, fontWeight: 600, fontSize: 13,
                  background: fbRating > 0 ? '#3b82f6' : '#e2e8f0',
                  color: fbRating > 0 ? '#fff' : '#94a3b8',
                  border: 'none', cursor: fbRating > 0 ? 'pointer' : 'not-allowed',
                }}
              >
                {fbSubmitting ? 'Submitting...' : `Submit Feedback (${current}/${total})`}
              </button>
            </div>
          </div>
        </div>
      );
    }

    // Single session feedback (when navigating through the list after first submission)
    return (
      <div className="gate-overlay">
        <div className="gate-card resume-gate" style={{ maxWidth: 460 }}>
          <div className="gate-icon" style={{ fontSize: 32 }}>&#9733;</div>
          <h2 className="gate-title">Rate Your Experience ({current}/{total})</h2>
          <div style={{ width: '100%', background: '#f0f4ff', borderRadius: 10, padding: '16px', border: '1px solid #c7d2fe', marginBottom: 16 }}>
            <div style={{ fontWeight: 600, fontSize: 13, color: '#1e293b' }}>
              Ticket #{session.ticket_id} — {session.subprocess_name || session.sector_name || 'Support'}
            </div>
            {session.query_text && (
              <div style={{ fontSize: 12, color: '#64748b', marginTop: 4 }}>
                {session.query_text.length > 100 ? session.query_text.slice(0, 100) + '...' : session.query_text}
              </div>
            )}
          </div>
          <div style={{ display: 'flex', gap: 6, marginBottom: 12, justifyContent: 'center' }}>
            {[1, 2, 3, 4, 5].map(n => (
              <button key={n} type="button" onClick={() => setFbRating(n)}
                style={{
                  fontSize: 30, background: 'none', border: 'none', cursor: 'pointer',
                  color: n <= fbRating ? '#f59e0b' : '#cbd5e1', transition: 'color 0.15s',
                }}
              >&#9733;</button>
            ))}
          </div>
          <textarea
            placeholder="Optional comment..."
            value={fbComment}
            onChange={e => setFbComment(e.target.value)}
            rows={2}
            style={{
              width: '100%', borderRadius: 8, border: '1px solid #e2e8f0', padding: '8px 12px',
              fontSize: 13, resize: 'none', marginBottom: 12, fontFamily: 'inherit',
            }}
          />
          <button
            onClick={handleFeedbackSubmit}
            disabled={fbRating === 0 || fbSubmitting}
            style={{
              width: '100%', padding: '10px', borderRadius: 8, fontWeight: 600, fontSize: 13,
              background: fbRating > 0 ? '#3b82f6' : '#e2e8f0',
              color: fbRating > 0 ? '#fff' : '#94a3b8',
              border: 'none', cursor: fbRating > 0 ? 'pointer' : 'not-allowed',
            }}
          >
            {fbSubmitting ? 'Submitting...' : `Submit Feedback (${current}/${total})`}
          </button>
        </div>
      </div>
    );
  };

  const renderStartGate = () => (
    <div className="gate-overlay">
      <div className="gate-card resume-gate">
        <h2 className="gate-title">Start a New Chat</h2>
        <p className="gate-subtitle">Begin a fresh support conversation whenever you are ready.</p>
        <div className="gate-actions"><button className="gate-btn gate-btn-primary" onClick={() => beginNewChat()}>Start New Chat</button></div>
      </div>
    </div>
  );

  return (
    <div className="chat-support-page">
      <div className="app-container">
        <div className="header">
          <img src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg" alt="KPMG" style={{ height: 24 }} />
          <div className="header-info"><h1>Customer Handling</h1><p>AI-powered multilingual support</p></div>
          <div className="status-dot" />
          {initPhase === 'chat' && <button className="restart-btn" onClick={beginNewChat}>Start New Chat</button>}
        </div>

        {initPhase === 'loading' && (
          <div className="chat-area" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div className="typing-indicator visible"><div className="typing-dot"/><div className="typing-dot"/><div className="typing-dot"/></div>
          </div>
        )}
        {initPhase === 'resume-prompt' && <div className="chat-area">{renderResumePrompt()}</div>}
        {initPhase === 'feedback-gate' && <div className="chat-area">{renderFeedbackGate()}</div>}
        {initPhase === 'start-gate' && <div className="chat-area">{renderStartGate()}</div>}

        {initPhase === 'chat' && (
          <>
            <div className="chat-area" ref={chatAreaRef}>
              {messages.map(renderMessage)}
              {isTyping && <div className="typing-indicator visible"><div className="typing-dot"/><div className="typing-dot"/><div className="typing-dot"/></div>}
            </div>
            <input type="file" accept="image/*" ref={fileInputRef} style={{ display: 'none' }}
              onChange={(e) => {
                const file = e.target.files[0];
                if (file) { const gid = fileInputRef.current?._groupId; if (gid) disableGroup(gid); handleScreenshotUpload(file); e.target.value = ''; }
              }}
            />
            {inputVisible && (
              <div className="input-area">
                <div className="input-row">
                  <textarea ref={inputRef} value={inputValue} onChange={handleInputChange} onKeyDown={handleKeyDown} placeholder={inputPlaceholder} rows={1} />
                  <button className="send-btn" onClick={sendMessage} disabled={!inputValue.trim()}>
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
                  </button>
                </div>
                <div className="input-hint">Press Enter to send &middot; Supports any language</div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}