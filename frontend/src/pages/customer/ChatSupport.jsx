import { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { getToken, apiGet, apiPost, API_BASE } from '../../api';
import { useAuth } from '../../AuthContext';
import '../../styles/chatbot.css';
import { io } from 'socket.io-client';

const SOCKET_URL = process.env.REACT_APP_API_URL || 'http://localhost:5500';

const DEFAULT_LATITUDE = 28.4595;
const DEFAULT_LONGITUDE = 77.0266;

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

function isNetworkIssue(subprocessName) {
  if (!subprocessName) return false;
  const name = subprocessName.toLowerCase();
  return name.includes('network') || name.includes('signal');
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

function limitSubprocesses(subprocesses) {
  const entries = Object.entries(subprocesses);
  const others = entries.filter(([, v]) => v === 'Others' || v.toLowerCase().includes('other'));
  const major = entries.filter(([, v]) => v !== 'Others' && !v.toLowerCase().includes('other'));
  return Object.fromEntries([...major.slice(0, 5), ...others]);
}

const SUBPROCESS_FOLLOWUP_OPTIONS = {
  'network / signal problems': [
    'Internet / Mobile Data',
    'Call Failure',
    'Call Drop',
  ],
};

function getFollowupOptions(subprocessName) {
  if (!subprocessName) return [];
  const key = subprocessName.trim().toLowerCase();
  return SUBPROCESS_FOLLOWUP_OPTIONS[key] || [];
}

// ── CHANGE 1: expanded PERSIST_TYPES to cover every renderable message type ──
const PERSIST_TYPES = new Set([
  'bot',
  'system',
  'sector-menu',
  'subprocess-grid',
  'network-subissue-grid',
  'location-question',
  'location-prompt',
  'location-success',
  'location-required',
  'signal-offer',
  'signal-codes',
  'screenshot-upload',
  'resolution',
  'diagnosis-result',
  'handoff',
  'post-actions',
  'unsat-options',
  'email-action',
  'email-sent',
  'agent-resolved',
  'broadband-diagnostic',
  'thankyou',
  'exit-box',
  'non-telecom-warning',
  'live-agent-message',
  'user-image',
]);

function sanitizePayload(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'function') return undefined;
  if (typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(sanitizePayload);
  const out = {};
  Object.entries(value).forEach(([k, v]) => {
    if (typeof v === 'function') return;
    out[k] = sanitizePayload(v);
  });
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
  try {
    const raw = localStorage.getItem(cacheKey(sessionId));
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function saveCachedSession(sessionId, payload) {
  if (!sessionId) return;
  try {
    localStorage.setItem(cacheKey(sessionId), JSON.stringify(payload));
  } catch {}
}

function clearCachedSession(sessionId) {
  if (!sessionId) return;
  try {
    localStorage.removeItem(cacheKey(sessionId));
  } catch {}
}

function clearStoredSession() {
  const sid = localStorage.getItem('chat_session_id');
  if (sid) clearCachedSession(sid);
  localStorage.removeItem('chat_session_id');
}

export default function ChatSupport() {
  const { user } = useAuth();
  const navigate = useNavigate();

  const [handoffActive, setHandoffActive] = useState(false);
  const agentResolvedShownRef = useRef(false);
  const [wsConnected, setWsConnected] = useState(false);

  const [initPhase, setInitPhase] = useState('loading');
  const [startChoice, setStartChoice] = useState(true);
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
  const locationRetryRef = useRef(null);

  const [screenshotUploading, setScreenshotUploading] = useState(false);
  const fileInputRef = useRef(null);

  const chatAreaRef = useRef(null);
  const inputRef = useRef(null);
  const sessionIdRef = useRef(null);
  const socketRef = useRef(null);
  const resumeNeededRef = useRef(false);
  const agentJoinedRef = useRef(false);
  const stateRef = useRef({
    step: 'welcome',
    sectorKey: null,
    sectorName: null,
    subprocessKey: null,
    subprocessName: null,
    subprocessSubType: null,
    language: 'English',
    queryText: '',
    resolution: '',
    attempt: 0,
    previousSolutions: [],
    diagnosisSummary: '',
    diagnosisRan: false,
    billingContext: null,
    connectionContext: null,
    planSpeedMbps: null,
  });
  const msgIdCounter = useRef(0);
  const nextId = () => ++msgIdCounter.current;

  const broadbandDiagStatusRef = useRef('idle');
  const broadbandDiagResultRef = useRef(null);

  const isRecentSession = useCallback((session) => {
    if (!session?.created_at) return false;
    const created = new Date(session.created_at).getTime();
    if (Number.isNaN(created)) return false;
    const hours = (Date.now() - created) / (1000 * 60 * 60);
    return hours <= 24;
  }, []);

  const scrollToBottom = useCallback(() => {
    setTimeout(() => {
      if (chatAreaRef.current) {
        chatAreaRef.current.scrollTop = chatAreaRef.current.scrollHeight;
      }
    }, 100);
  }, []);

  const disableGroup = useCallback((groupId) => {
    setDisabledGroups(prev => new Set([...prev, groupId]));
  }, []);

  const showInput = useCallback((placeholder) => {
    setInputVisible(true);
    setInputPlaceholder(placeholder || 'Describe your issue...');
    setTimeout(() => inputRef.current?.focus(), 200);
  }, []);

  const hideInput = useCallback(() => {
    setInputVisible(false);
  }, []);

  const joinSocketSession = useCallback(() => {
    if (!socketRef.current || !sessionIdRef.current) return;
    const token = getToken();
    if (!token) return;
    socketRef.current.emit('join_session', { session_id: sessionIdRef.current, token });
  }, []);

  const markAgentMessagesSeen = useCallback(async (messageIds) => {
    if (!sessionIdRef.current) return;
    if (!Array.isArray(messageIds) || messageIds.length === 0) return;
    try {
      const token = getToken();
      await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/seen`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({ message_ids: messageIds }),
      });
    } catch (e) {}
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
    } catch (e) {
      return null;
    }
  }, [joinSocketSession]);

  const saveMessage = useCallback(async (sender, content, meta = {}) => {
    if (!sessionIdRef.current) {
      await ensureSession({ step: stateRef.current.step });
    }
    if (!sessionIdRef.current) return;
    try {
      const contentPlain = sender === 'bot' ? stripHtml(meta.html || content || '') : content;
      const payloadForSave = meta.payload || (sender === 'bot'
        ? { type: meta.type || 'bot', html: meta.html || content, text: content }
        : null);
      await chatApiCall(`/api/chat/session/${sessionIdRef.current}/message`, {
        sender,
        content: contentPlain || '',
        current_step: stateRef.current.step,
        ...meta,
        payload: payloadForSave,
      });
      const cached = loadCachedSession(sessionIdRef.current) || {};
      const merged = Array.isArray(cached.messages) ? cached.messages : [];
      const type = sender === 'bot' ? 'bot' : sender === 'agent' ? 'live-agent-message' : 'user';
      merged.push({ type, text: content, html: meta.html || content, payload: payloadForSave });
      saveCachedSession(sessionIdRef.current, { messages: merged, state: stateRef.current });
    } catch (e) {}
  }, [ensureSession]);

  // ── CHANGE 2: addMessage now saves full sanitized payload for ALL types ─────
  const addMessage = useCallback((msg) => {
    const id = nextId();
    const groupId = msg.groupId || id;

    const maybePersistBot = async () => {
      if (msg.type !== 'bot' && !PERSIST_TYPES.has(msg.type)) return;
      if (msg.skipPersist) return;
      if (msg.__hydrated) return;
      if (!sessionIdRef.current) {
        await ensureSession({ step: stateRef.current.step });
      }
      if (!sessionIdRef.current) return;
      const content = msg.text || stripHtml(msg.html) || msg.type || '';
      // Sanitize strips functions so payload is safe to serialise
      const payload = sanitizePayload(
        msg.payload || { type: msg.type, html: msg.html, text: msg.text, ...msg, id, groupId }
      );
      try {
        await chatApiCall(`/api/chat/session/${sessionIdRef.current}/message`, {
          sender: 'bot',
          content,
          current_step: stateRef.current.step,
          payload,
        });
      } catch {}
    };

    // Always store a full serialisable payload in the stamped message so the
    // localStorage cache can reconstruct every card type on refresh.
    const stampedPayload = sanitizePayload(
      msg.payload || { type: msg.type, html: msg.html, text: msg.text, ...msg, id, groupId }
    );
    const stamped = { ...msg, id, groupId, payload: stampedPayload };

    setMessages(prev => {
      const updated = [...prev, stamped];
      if (sessionIdRef.current) {
        saveCachedSession(sessionIdRef.current, { messages: updated, state: stateRef.current });
      }
      return updated;
    });

    maybePersistBot();
    scrollToBottom();
    return groupId;
  }, [scrollToBottom, ensureSession]);

  // ══════════════════════════════════════════════════════════════════
  // LOCATION FUNCTIONS
  // ══════════════════════════════════════════════════════════════════

  const saveLocationToBackend = useCallback(async (latitude, longitude) => {
    if (!sessionIdRef.current) return;
    try {
      const token = getToken();
      await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/location`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({ latitude, longitude }),
      });
    } catch (e) {}
  }, []);

  const requestLocation = useCallback((onSuccess) => {
    setLocationStatus('requesting');
    const latitude = DEFAULT_LATITUDE;
    const longitude = DEFAULT_LONGITUDE;
    setLocationStatus('granted');
    saveLocationToBackend(latitude, longitude);
    addMessage({
      type: 'location-success',
      latitude,
      longitude,
    });
    if (onSuccess) onSuccess();
  }, [addMessage, saveLocationToBackend, disableGroup]);

  const afterLocationCaptured = useCallback(() => {
    setTimeout(() => {
      addMessage({ type: 'bot', html: `Thank you! Your location has been recorded.` });
      addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
      showInput('Describe your issue in any language...');
      stateRef.current.step = 'query';
    }, 500);
  }, [addMessage, showInput]);

  // ── Broadband diagnostics ──────────────────────────────────────────────────
  const classifySpeed = (speedMbps, planSpeedMbps) => {
    if (typeof speedMbps !== 'number' || speedMbps <= 0 || !planSpeedMbps) {
      return { label: 'Unknown', percent: null };
    }
    const percent = Math.round((speedMbps / planSpeedMbps) * 100);
    if (percent >= 80) return { label: 'Good', percent };
    if (percent >= 40) return { label: 'Degraded', percent };
    return { label: 'Poor', percent };
  };

  const classifyLatency = (latencyMs) => {
    if (typeof latencyMs !== 'number' || Number.isNaN(latencyMs) || latencyMs <= 0) return 'Unknown';
    if (latencyMs < 50) return 'Good';
    if (latencyMs <= 150) return 'Moderate';
    return 'High';
  };

  const measurePing = useCallback(async () => {
    const token = getToken();
    const start = performance.now();
    const resp = await fetch(`${API_BASE}/api/broadband/ping?ts=${Date.now()}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      cache: 'no-store',
    });
    if (!resp.ok) throw new Error('Ping request failed');
    await resp.json();
    return performance.now() - start;
  }, []);

  const measureDownloadSpeed = useCallback(async () => {
    const token = getToken();
    const start = performance.now();
    const resp = await fetch(`${API_BASE}/api/broadband/speedtest-file?ts=${Date.now()}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      cache: 'no-store',
    });
    if (!resp.ok) throw new Error('Speed test file request failed');
    let bytes = 0;
    if (resp.body?.getReader) {
      const reader = resp.body.getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        bytes += (value && value.length) || 0;
      }
    } else {
      const buf = await resp.arrayBuffer();
      bytes = buf.byteLength;
    }
    const durationSec = (performance.now() - start) / 1000;
    const mbps = durationSec > 0 ? (bytes * 8) / (durationSec * 1e6) : 0;
    return { mbps: Number(mbps.toFixed(1)), bytes, durationSec };
  }, []);

  const runBrowserQualityChecks = useCallback(async (planSpeedMbps) => {
    const connectionType = navigator.connection?.effectiveType || 'unknown';
    let latencyMs = null;
    let latencyError = null;
    let speedMbps = null;
    let speedError = null;

    try { latencyMs = Math.round(await measurePing()); }
    catch (e) { latencyError = e?.message || 'Latency check failed'; }

    try { const speed = await measureDownloadSpeed(); speedMbps = speed.mbps; }
    catch (e) { speedError = e?.message || 'Speed test failed'; }

    const speedMeta = classifySpeed(speedMbps, planSpeedMbps);
    const latencyLabel = classifyLatency(latencyMs);

    return {
      speedMbps,
      speedPercent: speedMeta.percent,
      speedLabel: speedMeta.label,
      latencyMs,
      latencyLabel,
      connectionType,
      latencyError,
      speedError,
    };
  }, [measurePing, measureDownloadSpeed]);

  const buildConnectionContext = useCallback((billing, connection, quality) => {
    const parts = [];
    if (billing?.plan_speed_mbps) parts.push(`Plan speed: ${billing.plan_speed_mbps} Mbps`);
    if (quality?.speedMbps) {
      const pct = quality.speedPercent != null ? ` (${quality.speedPercent}% of plan, ${quality.speedLabel})` : '';
      parts.push(`Measured speed: ${quality.speedMbps} Mbps${pct}`);
    } else if (quality?.speedError) {
      parts.push(`Measured speed unavailable (${quality.speedError})`);
    }
    if (quality?.latencyMs) {
      parts.push(`Latency: ${quality.latencyMs} ms (${quality.latencyLabel})`);
    } else if (quality?.latencyError) {
      parts.push(`Latency unavailable (${quality.latencyError})`);
    }
    if (quality?.connectionType) parts.push(`Connection type: ${quality.connectionType}`);
    if (connection?.line_quality) parts.push(`Line quality (NOC): ${connection.line_quality}`);
    if (connection?.router_status) parts.push(`Router status: ${connection.router_status}`);
    if (connection?.area_outage) parts.push('Area outage detected');
    return parts.join('. ');
  }, []);

  const runBroadbandDiagnostics = useCallback(async () => {
    if (!isBroadbandSector(stateRef.current.sectorKey, stateRef.current.sectorName)) return null;
    if (stateRef.current.step === 'exited') return null;
    if (broadbandDiagStatusRef.current === 'running') return broadbandDiagResultRef.current;
    if (broadbandDiagStatusRef.current === 'done') return broadbandDiagResultRef.current;

    broadbandDiagStatusRef.current = 'running';
    addMessage({ type: 'bot', html: 'Running broadband diagnostics (billing, connection, and browser checks)...', skipPersist: true });
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
        if (diag.billing?.bill_paid !== undefined) billSummary.push(`Bill paid: ${diag.billing.bill_paid ? 'Yes' : 'No'}`);
        if (diag.billing?.fup_hit !== undefined) billSummary.push(`FUP hit: ${diag.billing.fup_hit ? 'Yes' : 'No'}`);
        stateRef.current.billingContext = billSummary.join('; ');
      } catch (e) {
        diag.errors.billing = 'Billing check failed';
        stateRef.current.billingContext = null;
      }

      try {
        diag.connection = await apiGet('/api/broadband/connection-check');
      } catch (e) {
        diag.errors.connection = 'Connection check failed';
      }

      try {
        diag.quality = await runBrowserQualityChecks(planSpeed);
      } catch (e) {
        diag.errors.quality = 'Browser checks failed';
      }

      const connectionContext = buildConnectionContext(diag.billing, diag.connection, diag.quality);
      stateRef.current.connectionContext = connectionContext || null;
      stateRef.current.planSpeedMbps = planSpeed;

      broadbandDiagResultRef.current = diag;
      if (!stateRef.current.broadbandDiagShown) {
        addMessage({
          type: 'broadband-diagnostic',
          billing: diag.billing,
          connection: diag.connection,
          quality: diag.quality,
          errors: diag.errors,
          planSpeed,
        });
        stateRef.current.broadbandDiagShown = true;
      }
      return diag;
    } finally {
      setIsTyping(false);
      broadbandDiagStatusRef.current = 'done';
    }
  }, [addMessage, buildConnectionContext, runBrowserQualityChecks]);

  // ── CHANGE 3: fully rewritten hydrateHistory ───────────────────────────────
  // Restores every message type correctly from both localStorage cache
  // (which has full typed payloads) and DB history (which has sender/content).
  // Interactive cards get their onClick handlers re-attached after restore.
  const hydrateHistory = useCallback((history = []) => {
    const restored = [];
    let prevKey = null;

    history.forEach((m) => {
      const id      = m.id      || nextId();
      const groupId = m.groupId || id;

      // ── Already a typed message from localStorage cache ──────────────────
      // These come back with type set and full payload data intact.
      if (m.type && m.type !== 'bot' && m.type !== 'user' && m.type !== 'system') {
        restored.push({ ...m, id, groupId, __hydrated: true });
        return;
      }

      // ── Plain typed bot/user/system from cache ───────────────────────────
      if (m.type === 'bot' || m.type === 'user' || m.type === 'system') {
        // If it also has a payload with a richer type, use that instead
        if (m.payload && m.payload.type && m.payload.type !== 'bot') {
          restored.push({ id, groupId, ...m.payload, __hydrated: true });
          return;
        }
        restored.push({ ...m, id, groupId, __hydrated: true });
        return;
      }

      // ── DB message — deduplicate consecutive identical messages ───────────
      const key = `${m.sender}|${m.content || ''}`.trim();
      if (key && key === prevKey) return;
      prevKey = key;

      // ── DB message with payload — reconstruct full card from payload ──────
      if (m.payload && m.payload.type) {
        restored.push({ id, groupId, ...m.payload, __hydrated: true });
        return;
      }

      // ── Image messages ────────────────────────────────────────────────────
      if (m.content?.startsWith('__IMAGE__:')) {
        restored.push({
          id, groupId,
          type:       'user-image',
          imageSrc:   m.content.replace('__IMAGE__:', ''),
          __hydrated: true,
        });
        return;
      }
      if (m.content?.startsWith('data:image/')) {
        restored.push({
          id, groupId,
          type:       'user-image',
          imageSrc:   m.content,
          __hydrated: true,
        });
        return;
      }

      // ── Agent messages ────────────────────────────────────────────────────
      if (m.sender === 'agent') {
        if (m.content === '__AGENT_REQUEST_DIAGNOSIS__') return;
        restored.push({
          id, groupId,
          type:       'live-agent-message',
          text:        m.content,
          timestamp:   m.created_at,
          __hydrated: true,
        });
        return;
      }

      // ── User messages ─────────────────────────────────────────────────────
      if (m.sender === 'user') {
        restored.push({
          id, groupId,
          type:       'user',
          text:        m.content || '',
          __hydrated: true,
        });
        return;
      }

      // ── Bot messages — resolution box if long, bubble if short ───────────
      if (m.sender === 'bot') {
        const content = m.content || '';
        const html    = formatResolution(content);
        if (content.length > 150) {
          restored.push({ id, groupId, type: 'resolution', html, __hydrated: true });
        } else {
          restored.push({ id, groupId, type: 'bot', html, __hydrated: true });
        }
        return;
      }

      // ── Fallback: system ──────────────────────────────────────────────────
      restored.push({
        id, groupId,
        type:       'system',
        text:        m.content || m.text || '',
        __hydrated: true,
      });
    });

    // ── Re-attach onClick handlers to interactive cards ────────────────────
    // Functions cannot be JSON-serialised so they must be re-wired after
    // every restore. Cards that are already past their interactive moment
    // are left disabled so the user cannot re-trigger them.
    const wired = restored.map((msg) => {

      if (msg.type === 'location-question') {
        return {
          ...msg,
          onYes: () => {
            disableGroup(msg.groupId);
            addMessage({ type: 'user', text: "Yes, I'm at the issue location" });
            const locPromptGroupId = nextId();
            addMessage({
              type:    'location-prompt',
              groupId: locPromptGroupId,
              onShare: () => {
                disableGroup(locPromptGroupId);
                requestLocation(() => {
                  afterLocationCaptured(stateRef.current.subprocessName || '');
                });
              },
            });
            stateRef.current.step = 'location';
          },
          onNo: () => {
            disableGroup(msg.groupId);
            addMessage({ type: 'user', text: 'No, different location' });
            addMessage({ type: 'system', text: 'Please describe the location of the issue.' });
            showInput('Describe the location...');
            stateRef.current.step = 'location-other';
          },
        };
      }

      if (msg.type === 'location-prompt') {
        return {
          ...msg,
          onShare: () => {
            disableGroup(msg.groupId);
            requestLocation(() => {
              afterLocationCaptured(stateRef.current.subprocessName || '');
            });
          },
        };
      }

      if (msg.type === 'location-required') {
        return {
          ...msg,
          onRetry: () => {
            disableGroup(msg.groupId);
            requestLocation(() => {
              afterLocationCaptured(stateRef.current.subprocessName || '');
            });
          },
        };
      }

      return msg;
    });

    setMessages(wired);
    if (sessionIdRef.current) {
      saveCachedSession(sessionIdRef.current, {
        messages: wired,
        state:    stateRef.current,
      });
    }
  }, [disableGroup, addMessage, requestLocation, afterLocationCaptured, showInput]);

  // ── CHANGE 4: restoreSession now prefers localStorage cache over DB ────────
  // The cache holds full typed payloads for every card. DB messages are used
  // only as a fallback when the cache is empty (e.g. different browser/device).
  const restoreSession = useCallback(async (opts = {}) => {
    const { sessionId: forcedSessionId = null } = opts || {};
    const token = getToken();
    if (!token) return false;
    try {
      let data = null;
      const storedId = forcedSessionId || localStorage.getItem('chat_session_id');
      if (storedId) {
        const resp = await fetch(`${API_BASE}/api/chat/session/${storedId}`, {
          headers: { 'Authorization': `Bearer ${token}` },
        });
        if (resp.ok) data = await resp.json();
      }
      if (!data || data.error) {
        const resp = await fetch(`${API_BASE}/api/chat/session/active`, {
          headers: { 'Authorization': `Bearer ${token}` },
        });
        if (resp.ok) data = await resp.json();
      }
      if (data && data.session) {
        sessionIdRef.current = data.session.id;
        localStorage.setItem('chat_session_id', String(data.session.id));

        // ── Try localStorage cache first ─────────────────────────────────
        const cache   = loadCachedSession(sessionIdRef.current);
        const dbMsgs  = data.messages && data.messages.length ? data.messages : [];

        // Cache messages have full typed payloads — always prefer them.
        // Fall back to DB messages only if cache is empty.
        const toHydrate = (cache?.messages && cache.messages.length > 0)
          ? cache.messages
          : dbMsgs;

        // Seed welcome/menu if session is genuinely empty
        if (toHydrate.length === 0) {
          try {
            const menuResp = await apiGet('/api/menu');
            const menu = menuResp?.menu || {};
            addMessage({ type: 'bot', html: `<strong>Welcome to TeleBot Support!</strong><br>Say hello to get started!` });
            const spGroupId = nextId();
            addMessage({ type: 'sector-menu', menu, groupId: spGroupId });
            stateRef.current.step = 'greeting';
            setInitPhase('chat');
            showInput('Type your greeting here...');
            joinSocketSession();
            return true;
          } catch {}
        }

        // Restore state from cache if available
        if (cache?.state) {
          stateRef.current = { ...stateRef.current, ...cache.state };
        }

        // All past interactive cards should start disabled on restore so the
        // user cannot accidentally re-click a sector or subprocess that was
        // already chosen earlier in the conversation.
        const allGroupIds = new Set(
          toHydrate
            .filter(m => m.groupId)
            .map(m => m.groupId)
        );
        setDisabledGroups(allGroupIds);

        hydrateHistory(toHydrate);

        const maxAgentId = Math.max(
          0,
          ...dbMsgs.filter(m => m.sender === 'agent').map(m => m.id)
        );
        if (maxAgentId > 0) lastSeenMsgIdRef.current = maxAgentId;

        stateRef.current.step        = data.session.current_step || 'greeting';
        stateRef.current.sectorName  = data.session.sector_name  || stateRef.current.sectorName  || null;
        stateRef.current.subprocessName = data.session.subprocess_name || stateRef.current.subprocessName || null;
        stateRef.current.queryText   = data.session.query_text   || stateRef.current.queryText   || '';

        setHandoffActive(data.session.status === 'escalated');
        setInitPhase('chat');

        const placeholder = data.session.status === 'escalated'
          ? 'Type your reply to the agent...'
          : 'Describe your issue...';
        showInput(placeholder);
        joinSocketSession();
        resumeNeededRef.current = true;
        return true;
      }
    } catch (e) {}
    return false;
  }, [hydrateHistory, joinSocketSession, showInput, addMessage]);

  // ── Poll for agent messages + resolution after handoff ──────────────────────
  const lastSeenMsgIdRef = useRef(0);

  useEffect(() => {
    if (!handoffActive) return;
    if (sessionIdRef.current) {
      chatApiCall(`/api/chat/session/${sessionIdRef.current}/presence`, { present: true });
    }
    const poll = async () => {
      if (!sessionIdRef.current) return;
      try {
        const token = getToken();
        const resp = await fetch(
          `${API_BASE}/api/chat/session/${sessionIdRef.current}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        if (!resp.ok) return;
        const data = await resp.json();
        const allMsgs = data.messages || [];
        const newAgentMsgs = allMsgs.filter(
          m => m.sender === 'agent' && m.id > lastSeenMsgIdRef.current
        );
        const sessionInfo = data.session || {};
        if (!handoffActive && (newAgentMsgs.length > 0 || sessionInfo.status === 'escalated')) {
          setHandoffActive(true);
        }
        newAgentMsgs.forEach(m => {
          lastSeenMsgIdRef.current = Math.max(lastSeenMsgIdRef.current, m.id);
          if (m.content === '__AGENT_REQUEST_DIAGNOSIS__') {
            addMessage({
              type: 'bot',
              html: 'Your support agent has requested a <strong>signal diagnosis</strong> to better assist you. Please run it now:',
            });
            addMessage({ type: 'signal-offer', groupId: m.id });
            return;
          }
          addMessage({ type: 'live-agent-message', text: m.content, timestamp: m.created_at });
        });
        if (newAgentMsgs.length > 0 && !agentJoinedRef.current) {
          agentJoinedRef.current = true;
          addMessage({ type: 'system', text: 'Agent connected. You can now chat below.' });
          showInput('Type your message for the agent...');
          stateRef.current.step = 'live-agent';
        }
        if (sessionInfo.status === 'resolved' && !agentResolvedShownRef.current) {
          agentResolvedShownRef.current = true;
          setHandoffActive(false);
          hideInput();
          stateRef.current.step = 'agent-resolved';
          const lastBot = [...allMsgs].reverse().find(m => m.sender === 'bot');
          addMessage({
            type: 'agent-resolved',
            botMessage: lastBot?.content || 'Your support ticket has been resolved.',
          });
        }
      } catch {}
    };
    const iv = setInterval(poll, 6000);
    return () => {
      clearInterval(iv);
      if (sessionIdRef.current) {
        chatApiCall(`/api/chat/session/${sessionIdRef.current}/presence`, { present: false });
      }
    };
  }, [handoffActive, addMessage, hideInput, showInput]);

  useEffect(() => {
    const token = getToken();
    if (!token) return;
    const s = io(SOCKET_URL, { transports: ['websocket', 'polling'] });
    socketRef.current = s;
    s.on('connect', () => {
      setWsConnected(true);
      joinSocketSession();
    });
    s.on('disconnect', () => setWsConnected(false));
    s.on('new_message', (m) => {
      if (!m || !sessionIdRef.current || m.session_id !== sessionIdRef.current) return;
      if (m.sender !== 'agent') return;
      if (m.id <= lastSeenMsgIdRef.current) return;
      lastSeenMsgIdRef.current = Math.max(lastSeenMsgIdRef.current, m.id);
      if (m.content === '__AGENT_REQUEST_DIAGNOSIS__') {
        addMessage({
          type: 'bot',
          html: 'Your support agent has requested a <strong>signal diagnosis</strong> to better assist you. Please run it now:',
        });
        addMessage({ type: 'signal-offer', groupId: m.id });
        setHandoffActive(true);
        showInput('Type your reply to the agent...');
        return;
      }
      addMessage({ type: 'live-agent-message', text: m.content, timestamp: m.created_at });
      markAgentMessagesSeen([m.id]);
      setHandoffActive(true);
      showInput('Type your reply to the agent...');
    });
    s.on('session_updated', (payload) => {
      if (!payload || payload.session_id !== sessionIdRef.current) return;
      if (payload.status === 'resolved' && !agentResolvedShownRef.current) {
        agentResolvedShownRef.current = true;
        setHandoffActive(false);
        addMessage({
          type: 'agent-resolved',
          botMessage: 'Your support ticket has been resolved.',
        });
      }
    });
    return () => {
      s.disconnect();
      socketRef.current = null;
    };
  }, [addMessage, joinSocketSession, markAgentMessagesSeen, showInput]);

  const startChat = useCallback(async (forceNew = false) => {
    setMessages([]);
    setDisabledGroups(new Set());
    setLocationStatus('idle');
    setResumeCandidate(null);
    setResumeMessages([]);
    stateRef.current = {
      step: 'greeting', sectorKey: null, sectorName: null,
      subprocessKey: null, subprocessName: null, language: 'English',
      queryText: '', resolution: '', attempt: 0, previousSolutions: [],
      diagnosisSummary: '', diagnosisRan: false,
      billingContext: null, connectionContext: null, planSpeedMbps: null,
      broadbandDiagShown: false,
    };
    broadbandDiagStatusRef.current = 'idle';
    broadbandDiagResultRef.current = null;
    sessionIdRef.current = null;
    agentResolvedShownRef.current = false;
    agentJoinedRef.current = false;
    setHandoffActive(false);
    hideInput();
    setInitPhase('chat');
    if (forceNew) {
      clearStoredSession();
      try {
        const url = new URL(window.location.href);
        url.searchParams.delete('resume');
        window.history.replaceState(null, '', url.toString());
      } catch {}
    }
    await ensureSession({ forceNew: forceNew, step: 'greeting' });
    if (sessionIdRef.current) clearCachedSession(sessionIdRef.current);
    setTimeout(() => {
      addMessage({ type: 'bot', html: `<strong>Welcome to TeleBot Support!</strong><br>Say hello to get started!` });
      showInput('Type your greeting here...');
    }, 500);
  }, [addMessage, hideInput, showInput, ensureSession]);

  const beginNewChat = useCallback(async () => {
    await startChat(true);
  }, [startChat]);

  const loadSectorMenu = useCallback(async () => {
    const token = getToken();
    const headers = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    setIsTyping(true);
    try {
      const resp = await fetch(`${API_BASE}/api/menu`, { headers });
      const data = await resp.json();
      const groupId = nextId();
      addMessage({ type: 'sector-menu', menu: data.menu, groupId });
      stateRef.current.step = 'sector';
    } finally {
      setIsTyping(false);
    }
  }, [addMessage]);

  const selectSector = useCallback(async (key, name, groupId) => {
    disableGroup(groupId);
    stateRef.current.sectorKey = key;
    stateRef.current.sectorName = name;
    stateRef.current.billingContext = null;
    stateRef.current.connectionContext = null;
    stateRef.current.planSpeedMbps = null;
    broadbandDiagStatusRef.current = 'idle';
    broadbandDiagResultRef.current = null;
    addMessage({ type: 'user', text: name });
    saveMessage('user', name, { sector_name: name, current_step: 'sector' });
    addMessage({ type: 'system', text: `Selected: ${name}` });
    setIsTyping(true);
    const data = await chatApiCall('/api/subprocesses', { sector_key: key, language: stateRef.current.language });
    setIsTyping(false);
    addMessage({ type: 'bot', html: `Great choice! Now please select the <strong>type of issue</strong> you're facing with <strong>${name}</strong>:` });
    const spGroupId = nextId();
    addMessage({ type: 'subprocess-grid', subprocesses: limitSubprocesses(data.subprocesses), groupId: spGroupId });
    stateRef.current.step = 'subprocess';
  }, [addMessage, disableGroup, saveMessage]);

  const beginLocationFlow = useCallback(async (selectedIssueLabel) => {
    if (!sessionIdRef.current) { await ensureSession({ step: 'location' }); }
    addMessage({
      type: 'bot',
      html: `You selected <strong>${selectedIssueLabel}</strong>.<br><br>To help us assist you better, we need to know about your location.`,
    });
    const locQGroupId = nextId();
    addMessage({
      type: 'location-question',
      groupId: locQGroupId,
      onYes: () => {
        disableGroup(locQGroupId);
        addMessage({ type: 'user', text: "Yes, I'm at the issue location" });
        const locPromptGroupId = nextId();
        addMessage({
          type: 'location-prompt',
          groupId: locPromptGroupId,
          onShare: () => {
            disableGroup(locPromptGroupId);
            requestLocation(() => { afterLocationCaptured(selectedIssueLabel); });
          },
        });
        stateRef.current.step = 'location';
      },
      onNo: () => {
        // No alternate location flow for now
      },
    });
    stateRef.current.step = 'location-question';
  }, [addMessage, disableGroup, showInput, ensureSession, requestLocation, afterLocationCaptured]);

  useEffect(() => {
    if (!resumeNeededRef.current) return;
    const st = stateRef.current;
    if (st.step === 'location' || st.step === 'location-question') {
      resumeNeededRef.current = false;
      beginLocationFlow(st.subprocessName || st.subprocessSubType || 'your issue');
      return;
    }
    if (st.step === 'signal-diagnosis') {
      resumeNeededRef.current = false;
      const uploadGroupId = nextId();
      addMessage({ type: 'screenshot-upload', groupId: uploadGroupId });
    }
  }, [beginLocationFlow, addMessage]);

  const autoRaiseTicket = useCallback(async (opts = {}) => {
    const { prefaceHtml } = opts;
    let refNum = '';
    let assignedAgent = null;
    let slaHours = null;
    if (sessionIdRef.current) {
      try {
        setIsTyping(true);
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) { assignedAgent = data.assigned_agent; }
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
    agentResolvedShownRef.current = false;
    agentJoinedRef.current = false;
    setHandoffActive(true);
    hideInput();
  }, [addMessage, setHandoffActive, hideInput]);

  const fetchSolution = useCallback(async (userQuery) => {
    const st = stateRef.current;
    st.attempt += 1;
    if (!sessionIdRef.current) { await ensureSession({ step: 'query' }); }
    if (st.attempt === 1) { st.queryText = userQuery; }
    saveMessage('user', userQuery, { query_text: userQuery, sector_name: st.sectorName, subprocess_name: st.subprocessName });
    const effectiveQuery = st.subprocessSubType
      ? `${userQuery}\n\nIssue type: ${st.subprocessSubType}`
      : userQuery;
    setIsTyping(true);
    const resolveData = await chatApiCall('/api/resolve-step', {
      sector_key: st.sectorKey,
      subprocess_key: st.subprocessKey,
      selected_subprocess: st.subprocessName || undefined,
      query: effectiveQuery,
      language: st.language,
      previous_solutions: st.previousSolutions.slice(-10),
      attempt: st.attempt,
      original_query: st.attempt > 1 ? st.queryText : undefined,
      diagnosis_summary: st.diagnosisSummary || undefined,
      billing_context: st.billingContext || undefined,
      connection_context: st.connectionContext || undefined,
    });
    setIsTyping(false);
    if (resolveData.is_telecom === false) {
      addMessage({ type: 'non-telecom-warning', html: formatResolution(resolveData.resolution) });
      saveMessage('bot', resolveData.resolution);
      addMessage({ type: 'bot', html: `Please describe a telecom-related issue so I can help you.` });
      showInput('Describe your telecom issue...');
      st.attempt -= 1;
      return;
    }
    st.resolution = resolveData.resolution;
    st.previousSolutions.push(resolveData.resolution);
    saveMessage('bot', resolveData.resolution, { resolution: resolveData.resolution, language: st.language });
    addMessage({ type: 'resolution', html: formatResolution(resolveData.resolution) });
    if (st.attempt >= 6) {
      setTimeout(() => { autoRaiseTicket(); }, 800);
      return;
    }
    setTimeout(() => {
      addMessage({ type: 'bot', html: `Did this help? If not, please describe what's still not working.` });
      showInput('Type your response...');
    }, 800);
    st.step = 'conversation';
  }, [addMessage, saveMessage, showInput, ensureSession, autoRaiseTicket]);

  const selectSubprocess = useCallback(async (key, name, groupId) => {
    const followupOptions = getFollowupOptions(name);
    if (followupOptions.length > 0) {
      disableGroup(groupId);
      stateRef.current.subprocessKey = key;
      stateRef.current.subprocessName = name;
      stateRef.current.subprocessSubType = null;
      stateRef.current.attempt = 0;
      stateRef.current.previousSolutions = [];
      addMessage({ type: 'user', text: name });
      saveMessage('user', name, { subprocess_name: name, sector_name: stateRef.current.sectorName, current_step: 'subprocess' });
      addMessage({
        type: 'bot',
        html: `You selected <strong>${name}</strong>. Please choose the <strong>specific issue type</strong>:`,
      });
      const subIssueGroupId = nextId();
      addMessage({ type: 'network-subissue-grid', options: followupOptions, groupId: subIssueGroupId });
      stateRef.current.step = 'network-subissue';
      return;
    }

    disableGroup(groupId);
    stateRef.current.subprocessKey = key;
    stateRef.current.subprocessSubType = null;
    stateRef.current.subprocessName = name;
    stateRef.current.attempt = 0;
    stateRef.current.previousSolutions = [];
    addMessage({ type: 'user', text: name });
    saveMessage('user', name, { subprocess_name: name, sector_name: stateRef.current.sectorName, current_step: 'subprocess' });
    if (isMobileNetworkIssue(stateRef.current.sectorName, name)) {
      await beginLocationFlow(name);
      return;
    }
    addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
    showInput('Describe your issue in any language...');
    stateRef.current.step = 'query';
  }, [addMessage, disableGroup, beginLocationFlow, saveMessage, showInput]);

  const selectNetworkSubissue = useCallback(async (name, groupId) => {
    disableGroup(groupId);
    stateRef.current.subprocessSubType = name;
    const baseName = stateRef.current.subprocessName || 'Network / Signal Problems';
    const finalSubprocessName = `${baseName} - ${name}`;
    stateRef.current.subprocessName = finalSubprocessName;
    stateRef.current.attempt = 0;
    stateRef.current.previousSolutions = [];
    addMessage({ type: 'user', text: name });
    saveMessage('user', name, { subprocess_name: finalSubprocessName, sector_name: stateRef.current.sectorName, current_step: 'subprocess' });
    if (isMobileNetworkIssue(stateRef.current.sectorName, finalSubprocessName)) {
      await beginLocationFlow(finalSubprocessName);
      return;
    }
    addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
    showInput('Describe your issue in any language...');
    stateRef.current.step = 'query';
  }, [addMessage, disableGroup, beginLocationFlow, saveMessage, showInput]);

  const sendMessage = useCallback(async () => {
    const text = inputValue.trim();
    if (!text) return;
    addMessage({ type: 'user', text });
    setInputValue('');
    if (handoffActive || ['human_handoff', 'escalated', 'live-agent'].includes(stateRef.current.step)) {
      saveMessage('user', text);
      return;
    }
    hideInput();

    let userSaved = false;
    const saveUserOnce = (meta = {}) => {
      if (userSaved) return;
      saveMessage('user', text, meta);
      userSaved = true;
    };

    if (handoffActive || stateRef.current.step === 'human_handoff' || stateRef.current.step === 'escalated') {
      setMessages(prev => {
        const updated = [...prev];
        for (let i = updated.length - 1; i >= 0; i--) {
          if (updated[i].type === 'user' && updated[i].text === text) {
            updated[i] = { ...updated[i], liveChat: true };
            break;
          }
        }
        return updated;
      });
      saveUserOnce({ current_step: stateRef.current.step });
      showInput('Type your reply to the agent...');
      return;
    }

    if (stateRef.current.step === 'greeting') {
      saveUserOnce({ current_step: 'greeting' });
      setIsTyping(true);
      let isGreeting = true;
      try {
        const greetData = await chatApiCall('/api/detect-greeting', { text });
        isGreeting = greetData.is_greeting !== false;
      } catch { isGreeting = true; }
      setIsTyping(false);
      if (!isGreeting) {
        addMessage({ type: 'bot', html: `Please say hello to get started!` });
        showInput('Type your greeting here...');
        return;
      }
      const userName = user?.name || 'there';
      setIsTyping(true);
      await new Promise(resolve => setTimeout(resolve, 700));
      setIsTyping(false);
      addMessage({ type: 'bot', html: `Hi dear ${userName}! Hope you're doing well. I'm your AI-powered telecom support assistant. How can I help you today? Please choose one of the options below to get started:` });
      setTimeout(() => loadSectorMenu(), 600);
      stateRef.current.step = 'sector';
      return;
    }

    if (stateRef.current.step === 'conversation') {
      setIsTyping(true);
      let classification = { is_satisfied: false, mentions_signal: false };
      try { classification = await chatApiCall('/api/classify-response', { text }); } catch {}
      setIsTyping(false);
      if (classification.is_satisfied) {
        saveUserOnce({ current_step: 'conversation' });
        addMessage({ type: 'thankyou' });
        if (sessionIdRef.current) {
          try {
            const token = getToken();
            await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/resolve`, {
              method: 'PUT',
              headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
            });
          } catch {}
        }
        setTimeout(() => {
          addMessage({ type: 'bot', html: `What would you like to do next?` });
          const actionGroupId = nextId();
          addMessage({ type: 'post-actions', groupId: actionGroupId });
        }, 800);
        stateRef.current.step = 'resolved';
        return;
      }
      if (classification.mentions_signal && isMobileNetworkIssue(stateRef.current.sectorName, stateRef.current.subprocessName) && !stateRef.current.diagnosisRan) {
        addMessage({ type: 'bot', html: `It sounds like you're experiencing signal issues. Would you like to run a signal diagnosis?` });
        const diagGroupId = nextId();
        addMessage({ type: 'signal-offer', groupId: diagGroupId });
        stateRef.current.step = 'signal-offer';
        return;
      }
      stateRef.current.step = 'query';
    }

    if (stateRef.current.language === 'English') {
      try {
        const langData = await chatApiCall('/api/detect-language', { text });
        stateRef.current.language = langData.language || 'English';
      } catch { stateRef.current.language = 'English'; }
      addMessage({ type: 'system', text: `Language detected: ${stateRef.current.language}` });
    }

    if (stateRef.current.attempt > 0 && !stateRef.current.diagnosisRan && isMobileNetworkIssue(stateRef.current.sectorName, stateRef.current.subprocessName)) {
      let classification = { mentions_signal: false };
      try { classification = await chatApiCall('/api/classify-response', { text }); } catch {}
      if (classification.mentions_signal) {
        saveUserOnce({ current_step: stateRef.current.step });
        addMessage({ type: 'bot', html: `It sounds like you're experiencing signal issues. Would you like to run a signal diagnosis?` });
        const diagGroupId = nextId();
        addMessage({ type: 'signal-offer', groupId: diagGroupId });
        stateRef.current.step = 'signal-offer';
        return;
      }
    }

    if (isBroadbandSector(stateRef.current.sectorKey, stateRef.current.sectorName)) {
      await runBroadbandDiagnostics();
    }

    await fetchSolution(text);
  }, [inputValue, addMessage, hideInput, fetchSolution, loadSectorMenu, user, afterLocationCaptured, handoffActive, saveMessage, runBroadbandDiagnostics]);

  const handleSendEmail = useCallback(async (groupId) => {
    disableGroup(groupId);
    if (!sessionIdRef.current) return;
    addMessage({ type: 'system', text: 'Sending summary to your email...' });
    try {
      setIsTyping(true);
      const token = getToken();
      const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/send-summary-email`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      });
      const data = await resp.json();
      if (resp.ok) { addMessage({ type: 'email-sent', message: data.message }); }
      else { addMessage({ type: 'system', text: data.error || 'Failed to send email.' }); }
    } catch { addMessage({ type: 'system', text: 'Failed to send email. Please try again later.' }); }
    finally { setIsTyping(false); }
  }, [addMessage, disableGroup]);

  const handleSignalDiagnosis = useCallback((groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Run signal diagnosis' });
    addMessage({ type: 'bot', html: `Let's diagnose your signal. Please follow the steps below to get your signal information.` });
    setTimeout(() => {
      addMessage({ type: 'signal-codes' });
      setTimeout(() => {
        const uploadGroupId = nextId();
        addMessage({ type: 'screenshot-upload', groupId: uploadGroupId });
      }, 600);
    }, 500);
    stateRef.current.step = 'signal-diagnosis';
  }, [addMessage, disableGroup]);

  const handleRaiseTicket = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Raise a ticket' });
    saveMessage('user', 'Raise a ticket');
    let refNum = '';
    let assignedAgent = null;
    let slaHours = null;
    if (sessionIdRef.current) {
      try {
        setIsTyping(true);
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) { assignedAgent = data.assigned_agent; }
      } catch {} finally { setIsTyping(false); }
    }
    addMessage({
      type: 'bot',
      html: `Your ticket is being raised now.` +
        `<br><br>Your ticket has been raised successfully!` +
        (refNum ? `<br>Reference: <strong>${refNum}</strong>` : '') +
        (assignedAgent
          ? `<br><br>We are connecting you to our expert. Your dedicated support agent is:<br>
             <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;">
               <div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>
               ${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">Phone: ${assignedAgent.phone}</div>` : ''}
               ${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">Email: ${assignedAgent.email}</div>` : ''}
               ${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}
               ${slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">SLA: Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : ''}
             </div>`
          : `<br><br>Our support team will reach out to you shortly.` +
            (slaHours ? `<br><span style="color:#16a34a;font-weight:600;">SLA: Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : '')) +
        `<br>You can track your ticket from the dashboard.`,
    });
    stateRef.current.step = 'escalated';
    agentResolvedShownRef.current = false;
    setHandoffActive(true);
  }, [addMessage, disableGroup, saveMessage, setHandoffActive]);

  const handleBackToMenu = useCallback((groupId) => {
    disableGroup(groupId);
    if (['resolved', 'agent-resolved'].includes(stateRef.current.step)) {
      addMessage({ type: 'user', text: 'Main Menu' });
      beginNewChat();
      return;
    }
    stateRef.current.attempt = 0;
    stateRef.current.previousSolutions = [];
    stateRef.current.billingContext = null;
    stateRef.current.connectionContext = null;
    stateRef.current.planSpeedMbps = null;
    broadbandDiagStatusRef.current = 'idle';
    broadbandDiagResultRef.current = null;
    addMessage({ type: 'user', text: 'Main Menu' });
    addMessage({ type: 'bot', html: `Sure! Please select your <strong>telecom service category</strong>:` });
    setTimeout(() => loadSectorMenu(), 400);
    stateRef.current.step = 'sector';
  }, [addMessage, beginNewChat, disableGroup, loadSectorMenu]);

  const handleExit = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Exit' });
    const isHandoffMode = handoffActive || ['human_handoff', 'escalated', 'live-agent'].includes(stateRef.current.step);
    if (isHandoffMode) {
      addMessage({
        type: 'system',
        text: agentJoinedRef.current
          ? 'A human agent is connected to this ticket. Please stay here to continue the live chat.'
          : 'Please wait — we are connecting you to a human agent.',
      });
      if (agentJoinedRef.current) {
        showInput('Type your message for the agent...');
      } else {
        hideInput();
      }
      stateRef.current.step = 'live-agent';
      return;
    }
    hideInput();
    const currentSessionId = sessionIdRef.current;
    if (currentSessionId) {
      addMessage({ type: 'system', text: 'Sending chat summary to your email & WhatsApp...' });
      try {
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${currentSessionId}/send-summary-email`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (resp.ok) { addMessage({ type: 'email-sent', message: data.message }); }
      } catch {}
      if (!handoffActive) {
        try {
          const token = getToken();
          await fetch(`${API_BASE}/api/chat/session/${currentSessionId}/resolve`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
          });
        } catch {}
      }
    }
    stateRef.current.step = 'exited';
    setTimeout(() => {
      addMessage({ type: 'exit-box' });
      if (currentSessionId) {
        navigate(`/customer/feedback?session=${currentSessionId}`);
      }
    }, 800);
  }, [addMessage, disableGroup, hideInput, handoffActive, showInput, navigate]);

  const handleScreenshotUpload = useCallback(async (file) => {
    if (!file || !file.type.startsWith('image/')) {
      addMessage({ type: 'system', text: 'Please upload a valid image file (PNG or JPG).' });
      const uploadGroupId = nextId();
      addMessage({ type: 'screenshot-upload', groupId: uploadGroupId });
      return;
    }
    if (file.size > 5 * 1024 * 1024) {
      addMessage({ type: 'system', text: 'Image is too large. Please upload a screenshot under 5MB.' });
      const uploadGroupId = nextId();
      addMessage({ type: 'screenshot-upload', groupId: uploadGroupId });
      return;
    }
    const reader = new FileReader();
    reader.onload = async () => {
      const base64String = reader.result.split(',')[1];
      addMessage({ type: 'user-image', imageSrc: reader.result });
      saveMessage('user', reader.result);
      setScreenshotUploading(true);
      setIsTyping(true);
      try {
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/analyze-signal`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
          body: JSON.stringify({ image: base64String, image_data_url: reader.result }),
        });
        const data = await resp.json();
        setIsTyping(false);
        setScreenshotUploading(false);
        if (resp.ok && data.diagnosis) {
          addMessage({ type: 'diagnosis-result', diagnosis: data.diagnosis });
          saveMessage('bot', data.diagnosis.summary || `Signal: ${data.diagnosis.overall_label}`);
          stateRef.current.diagnosisSummary = data.diagnosis.summary || `Signal: ${data.diagnosis.overall_label}`;
          stateRef.current.diagnosisRan = true;
          setTimeout(() => {
            if (data.diagnosis.overall_status === 'red') {
              autoRaiseTicket({ prefaceHtml: 'Your signal is really poor. Your ticket is being raised now.' });
              return;
            }
            addMessage({ type: 'bot', html: 'Thank you for uploading your signal screenshot. I have analyzed your network parameters and identified the issue. Let me suggest a solution based on your signal diagnosis...' });
            fetchSolution(`My signal diagnosis shows: ${stateRef.current.diagnosisSummary}`);
          }, 800);
        } else {
          addMessage({ type: 'system', text: data.error || 'Failed to analyze the screenshot. Please try again.' });
          const uploadGroupId = nextId();
          addMessage({ type: 'screenshot-upload', groupId: uploadGroupId });
        }
      } catch {
        setIsTyping(false);
        setScreenshotUploading(false);
        addMessage({ type: 'system', text: 'An error occurred while analyzing the screenshot. Please try again.' });
        const uploadGroupId = nextId();
        addMessage({ type: 'screenshot-upload', groupId: uploadGroupId });
      }
    };
    reader.readAsDataURL(file);
  }, [addMessage, saveMessage, fetchSolution, autoRaiseTicket]);

  const handleRetry = useCallback((groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'I want to describe my issue again' });
    addMessage({ type: 'bot', html: `Sure! Please <strong>describe your issue again</strong> with as much detail as possible.` });
    showInput('Describe your issue in more detail...');
    stateRef.current.step = 'query';
  }, [addMessage, disableGroup, showInput]);

  const handleHumanHandoff = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Connect me to a human agent' });
    saveMessage('user', 'Connect me to a human agent');
    let refNum = 'TC-' + Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).substring(2, 6).toUpperCase();
    let assignedAgent = null;
    let slaHours = null;
    if (sessionIdRef.current) {
      try {
        setIsTyping(true);
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) { assignedAgent = data.assigned_agent; }
      } catch {} finally { setIsTyping(false); }
    }
    addMessage({
      type: 'handoff',
      sectorName: stateRef.current.sectorName || 'Telecom',
      subprocessName: stateRef.current.subprocessName || 'General',
      queryText: stateRef.current.queryText || 'N/A',
      refNum,
      assignedAgent,
    });
    setTimeout(() => {
      const slaLine = slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">SLA: Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : '';
      const agentCard = assignedAgent
        ? `<br><br>We are connecting you to your dedicated support expert:<br>
           <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;">
             <div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>
             ${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">Phone: ${assignedAgent.phone}</div>` : ''}
             ${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">Email: ${assignedAgent.email}</div>` : ''}
             ${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}
             ${slaLine}
           </div>`
        : `<br><br>Our support team will contact you shortly.` + (slaHours ? `<br><span style="color:#16a34a;font-weight:600;">SLA: Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : '');
      addMessage({
        type: 'bot',
        html: `Your ticket is being raised now.` +
          `<br><br>Your request has been submitted and a support ticket has been raised.` + agentCard +
          `<br>Reference: <strong>${refNum}</strong><br><br>The agent may send you messages below - please stay in this chat.<br><br>What would you like to do next?`,
      });
    }, 1500);
    addMessage({ type: 'system', text: 'Please wait — we are connecting you to a human agent.' });
    stateRef.current.step = 'live-agent';
    agentResolvedShownRef.current = false;
    agentJoinedRef.current = false;
    setHandoffActive(true);
    hideInput();
  }, [addMessage, disableGroup, saveMessage, setHandoffActive, hideInput]);

  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  }, [sendMessage]);

  const handleInputChange = useCallback((e) => {
    setInputValue(e.target.value);
    e.target.style.height = 'auto';
    e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
  }, []);

  const resumeChat = useCallback(async (session, msgs) => {
    setResumeCandidate(null);
    setResumeMessages([]);
    setInitPhase('chat');
    setMessages([]);
    setDisabledGroups(new Set());
    hideInput();
    sessionIdRef.current = session.id;
    stateRef.current.sectorName     = session.sector_name     || null;
    stateRef.current.subprocessName = session.subprocess_name || null;
    stateRef.current.language       = session.language        || 'English';
    stateRef.current.queryText      = session.query_text      || '';
    stateRef.current.resolution     = session.resolution      || '';
    try {
      const token = getToken();
      const headers = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const menuResp = await fetch(`${API_BASE}/api/menu`, { headers });
      const menuData = await menuResp.json();
      for (const [key, sector] of Object.entries(menuData.menu)) {
        if (sector.name === session.sector_name) { stateRef.current.sectorKey = key; break; }
      }
      if (stateRef.current.sectorKey && session.subprocess_name) {
        const spData = await chatApiCall('/api/subprocesses', { sector_key: stateRef.current.sectorKey, language: 'English' });
        for (const [key, name] of Object.entries(spData.subprocesses)) {
          if (name === session.subprocess_name || session.subprocess_name.startsWith(`${name} - `)) {
            stateRef.current.subprocessKey = key;
            break;
          }
        }
      }
    } catch {}
    const resumeMsg = { type: 'system', text: `Resuming your previous chat session #${session.id}` };
    setMessages([{ ...resumeMsg, id: nextId(), groupId: nextId() }]);
    const botResolutions = [];
    const newMsgs = [];
    for (const m of msgs) {
      const id = nextId();
      if (m.sender === 'user') {
        if (m.content && m.content.startsWith('data:image/')) {
          newMsgs.push({ type: 'user-image', imageSrc: m.content, id, groupId: id });
        } else {
          newMsgs.push({ type: 'user', text: m.content, id, groupId: id });
        }
      } else if (m.sender === 'bot') {
        if (m.content.length > 150) {
          botResolutions.push(m.content);
          newMsgs.push({ type: 'resolution', html: formatResolution(m.content), id, groupId: id });
        } else {
          newMsgs.push({ type: 'bot', html: formatResolution(m.content), id, groupId: id });
        }
      } else {
        newMsgs.push({ type: 'system', text: m.content, id, groupId: id });
      }
    }
    stateRef.current.previousSolutions = botResolutions;
    stateRef.current.attempt           = botResolutions.length;
    setMessages(prev => [...prev, ...newMsgs]);
    scrollToBottom();
    if (!msgs.length && session.status === 'active') {
      addMessage({ type: 'bot', html: `<strong>Welcome to TeleBot Support!</strong><br>Say hello to get started!` });
      showInput('Type your greeting here...');
      stateRef.current.step = 'greeting';
      return;
    }
    setTimeout(async () => {
      if (session.status === 'resolved') {
        addMessage({ type: 'bot', html: `This chat session is <strong>${session.status}</strong>. You are viewing the complete chat history.` });
        addMessage({ type: 'bot', html: `Start a new chat if you need more help.` });
        hideInput();
        stateRef.current.step = 'view-only';
        return;
      }
      if (session.status === 'escalated') {
        addMessage({ type: 'bot', html: `Please wait — we are connecting you to a human agent.` });
        agentResolvedShownRef.current = false;
        agentJoinedRef.current = false;
        setHandoffActive(true);
        hideInput();
        stateRef.current.step = 'live-agent';
        return;
      }
      if (!session.sector_name) {
        addMessage({ type: 'bot', html: 'Please select your <strong>telecom service category</strong>:' });
        loadSectorMenu();
      } else if (!session.subprocess_name) {
        addMessage({ type: 'bot', html: `Please select the <strong>type of issue</strong> you're facing with <strong>${session.sector_name}</strong>:` });
        const data = await chatApiCall('/api/subprocesses', { sector_key: stateRef.current.sectorKey, language: stateRef.current.language });
        const spGroupId = nextId();
        addMessage({ type: 'subprocess-grid', subprocesses: limitSubprocesses(data.subprocesses), groupId: spGroupId });
        stateRef.current.step = 'subprocess';
      } else if (session.resolution) {
        addMessage({ type: 'bot', html: `Did this help? If not, please describe what's still not working.` });
        showInput('Type your response...');
        stateRef.current.step = 'conversation';
      } else {
        addMessage({ type: 'bot', html: 'Please <strong>describe your specific issue</strong> so I can provide the best resolution.' });
        showInput('Describe your issue in any language...');
        stateRef.current.step = 'query';
      }
    }, 400);
  }, [addMessage, hideInput, loadSectorMenu, scrollToBottom, showInput, setHandoffActive]);

  const handleFeedbackSubmit = useCallback(async () => {
    if (fbRating === 0) return;
    const session = pendingFeedback[currentFbIdx];
    if (!session) return;
    setFbSubmitting(true);
    await apiPost('/api/feedback', { chat_session_id: session.id, rating: fbRating, comment: fbComment });
    setFbSubmitting(false);
    setFbRating(0);
    setFbComment('');
    if (currentFbIdx + 1 < pendingFeedback.length) { setCurrentFbIdx(prev => prev + 1); }
    else { proceedAfterFeedback(); }
  }, [fbRating, fbComment, pendingFeedback, currentFbIdx]);

  const proceedAfterFeedback = useCallback(async () => {
    const resumeId = searchParams.get('resume');
    if (resumeId) {
      try {
        const data = await apiGet(`/api/chat/session/${resumeId}`);
        if (data?.session) {
          resumeChat(data.session, data.messages || []);
          return;
        }
      } catch {}
    }
    setInitPhase('start-gate');
  }, [searchParams, resumeChat]);

  // ── CHANGE 5: initialization now auto-restores active session on refresh ───
  // Instead of always showing the resume-prompt gate, we silently restore the
  // active session directly so the chat reappears immediately on refresh.
  const initialized = useRef(false);
  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    (async () => {
      const params   = new URLSearchParams(window.location.search);
      const resumeId = params.get('resume');

      // ── Explicit resume link ─────────────────────────────────────────────
      if (resumeId) {
        try {
          const data = await apiGet(`/api/chat/session/${resumeId}`);
          if (data?.session) {
            resumeChat(data.session, data.messages || []);
            return;
          }
        } catch {}
      }

      // ── Check for a stored session id in localStorage ────────────────────
      const storedId = localStorage.getItem('chat_session_id');
      if (storedId) {
        const restored = await restoreSession({ sessionId: storedId });
        if (restored) return;
      }

      // ── Check for any active session on the server ───────────────────────
      try {
        const active = await apiGet('/api/chat/session/active');
        if (active?.session && active.session.status !== 'resolved') {
          // If there is a localStorage cache for this session, restore silently.
          // Otherwise show the resume-prompt so the user can choose.
          const cache = loadCachedSession(active.session.id);
          if (cache?.messages && cache.messages.length > 0) {
            const restored = await restoreSession({ sessionId: active.session.id });
            if (restored) return;
          }
          setResumeCandidate(active.session);
          setResumeMessages(active.messages || []);
          setInitPhase('resume-prompt');
          return;
        }
      } catch {}

      setInitPhase('start-gate');
    })();
  }, [searchParams, resumeChat, restoreSession]);

  // ══════════════════════════════════════════════════════════════════
  // RENDER MESSAGES — unchanged from original
  // ══════════════════════════════════════════════════════════════════
  const renderMessage = (msg) => {
    const isDisabled = disabledGroups.has(msg.groupId);
    switch (msg.type) {
      case 'bot':
        return <div key={msg.id} className="message bot" dangerouslySetInnerHTML={{ __html: msg.html }} />;
      case 'user':
        return (
          <div key={msg.id} className="message user">
            <span>{msg.text}</span>
            {msg.liveChat && (
              <span className={`msg-tick ${msg.seen ? 'msg-tick--seen' : 'msg-tick--sent'}`} title={msg.seen ? 'Seen' : 'Sent'}>
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="20 6 9 17 4 12" />
                </svg>
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
              <button key={key} className={`menu-card${isDisabled ? ' disabled' : ''}`}
                onClick={() => !isDisabled && selectSector(key, sector.name, msg.groupId)}>
                <div className="card-icon">{sector.icon}</div>
                <div className="card-label">{sector.name}</div>
                <div className="card-arrow">&rsaquo;</div>
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
                <button key={sk} className={`subprocess-chip${isOthers ? ' others' : ''}${isDisabled ? ' disabled' : ''}`}
                  onClick={() => !isDisabled && selectSubprocess(sk, sname, msg.groupId)}>
                  <div className="chip-num">{isOthers ? '···' : idx + 1}</div>
                  <div className="chip-label">{sname}</div>
                  <div className="chip-arrow">›</div>
                </button>
              );
            })}
          </div>
        );
      case 'network-subissue-grid':
        return (
          <div key={msg.id} className="subprocess-grid">
            {(msg.options || []).map((name, idx) => (
              <button key={`${name}-${idx}`} className={`subprocess-chip${isDisabled ? ' disabled' : ''}`}
                onClick={() => !isDisabled && selectNetworkSubissue(name, msg.groupId)}>
                <div className="chip-num">{idx + 1}</div>
                <div className="chip-label">{name}</div>
                <div className="chip-arrow">›</div>
              </button>
            ))}
          </div>
        );
      case 'resolution':
        return (
          <div key={msg.id} className="resolution-box">
            <h4>Resolution Steps</h4>
            <div dangerouslySetInnerHTML={{ __html: msg.html }} />
          </div>
        );
      case 'non-telecom-warning':
        return <div key={msg.id} className="non-telecom-warning" dangerouslySetInnerHTML={{ __html: msg.html }} />;
      case 'satisfaction':
      case 'solution-actions':
        return (
          <div key={msg.id} className="satisfaction-container">
            <button className={`sat-btn yes${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Main Menu</button>
            <button className={`sat-btn no${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit</button>
          </div>
        );
      case 'signal-offer':
        return (
          <div key={msg.id} className="satisfaction-container">
            <button className={`sat-btn yes${isDisabled ? ' disabled' : ''}`}
              onClick={() => { if (isDisabled) return; disableGroup(msg.groupId); handleSignalDiagnosis(msg.groupId); }}>
              Yes, Run Diagnosis
            </button>
            <button className={`sat-btn no${isDisabled ? ' disabled' : ''}`}
              onClick={() => {
                if (isDisabled) return;
                disableGroup(msg.groupId);
                addMessage({ type: 'user', text: 'No, continue chatting' });
                addMessage({ type: 'bot', html: `No problem! Please describe what's still not working.` });
                showInput('Type your response...');
                stateRef.current.step = 'conversation';
              }}>
              No, Continue
            </button>
          </div>
        );
      case 'thankyou':
        return (
          <div key={msg.id} className="thankyou-box">
            <div className="ty-icon"></div>
            <div className="ty-title">Thank You!</div>
            <div className="ty-msg">We're glad we could help resolve your issue.<br />If you face any other telecom issues, feel free to come back anytime!</div>
          </div>
        );
      case 'post-actions':
        return (
          <div key={msg.id} className="post-feedback-actions">
            <button className={`action-btn menu-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Main Menu</button>
            <button className={`action-btn exit-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit</button>
          </div>
        );
      case 'exit-box':
        return (
          <div key={msg.id} className="exit-box">
            <div className="exit-icon"></div>
            <div className="exit-title">Goodbye!</div>
            <div className="exit-msg">Thank you for using Customer Handling.<br />Have a great day! Click <strong>Restart</strong> anytime to start a new session.</div>
          </div>
        );
      case 'location-question':
        return (
          <div key={msg.id} style={{ background: 'rgba(0, 145, 218, 0.12)', border: '1px solid rgba(0, 145, 218, 0.25)', borderRadius: '10px', padding: '20px 22px', margin: '6px 0', textAlign: 'center' }}>
            <div style={{ fontSize: '28px', color: '#0091DA', marginBottom: '6px' }}>
              <svg width="28" height="28" viewBox="0 0 24 24" fill="#0091DA" stroke="none">
                <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5S10.62 6.5 12 6.5s2.5 1.12 2.5 2.5S13.38 11.5 12 11.5z"/>
              </svg>
            </div>
            <div style={{ fontWeight: '700', fontSize: '14px', color: '#00338D', marginBottom: '8px' }}>Location Check</div>
            <div style={{ fontSize: '13px', color: '#3d5068', marginBottom: '16px', lineHeight: '1.6' }}>
              Are you currently at the same location where you're experiencing this issue?
            </div>
            <div style={{ display: 'flex', gap: '10px', justifyContent: 'center' }}>
              <button onClick={() => !isDisabled && msg.onYes && msg.onYes()} disabled={isDisabled}
                style={{ background: isDisabled ? '#8596ab' : '#0091DA', color: '#fff', border: 'none', borderRadius: '8px', padding: '11px 26px', fontSize: '13px', fontWeight: '600', cursor: isDisabled ? 'not-allowed' : 'pointer', transition: 'all 0.2s' }}>
                Yes, I'm here
              </button>
              <button onClick={() => !isDisabled && msg.onNo && msg.onNo()} disabled={isDisabled}
                style={{ background: isDisabled ? '#8596ab' : '#fff', color: isDisabled ? '#fff' : '#0091DA', border: '1px solid #0091DA', borderRadius: '8px', padding: '11px 26px', fontSize: '13px', fontWeight: '600', cursor: isDisabled ? 'not-allowed' : 'pointer', transition: 'all 0.2s' }}>
                No, different location
              </button>
            </div>
          </div>
        );
      case 'location-prompt':
        return (
          <div key={msg.id} style={{ background: 'rgba(0, 145, 218, 0.12)', border: 'none', borderRadius: '10px', padding: '20px 22px', margin: '6px 0', textAlign: 'center', boxShadow: '0 2px 8px rgba(0, 145, 218, 0.25)' }}>
            <div style={{ fontSize: '28px', color: '#0091DA', marginBottom: '6px' }}>
              <svg width="28" height="28" viewBox="0 0 24 24" fill="#0091DA" stroke="none">
                <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5S10.62 6.5 12 6.5s2.5 1.12 2.5 2.5S13.38 11.5 12 11.5z"/>
              </svg>
            </div>
            <div style={{ fontWeight: '700', fontSize: '14px', color: '#0f1d33', marginBottom: '8px' }}>Location Access Required</div>
            <div style={{ fontSize: '13px', color: '#3d5068', marginBottom: '16px', lineHeight: '1.6' }}>
              To diagnose your issue and check coverage in your area, we need your current location. This is <strong style={{ color: '#0f1d33' }}>required</strong> to continue.
            </div>
            <button onClick={() => !isDisabled && msg.onShare && msg.onShare()} disabled={isDisabled}
              style={{ background: isDisabled ? '#a0c4e8' : '#0091DA', color: '#fff', border: 'none', borderRadius: '8px', padding: '11px 26px', fontSize: '13px', fontWeight: '700', fontFamily: 'inherit', cursor: isDisabled ? 'not-allowed' : 'pointer', display: 'flex', alignItems: 'center', gap: '8px', margin: '0 auto', transition: 'all 0.18s ease' }}>
              Share My Location
            </button>
            <div style={{ fontSize: '11px', color: '#8596ab', marginTop: '10px' }}>
              Your location is only used for network diagnostics and stored securely.
            </div>
          </div>
        );
      case 'location-required':
        return (
          <div key={msg.id} style={{ background: '#ffffff', border: '1px solid #d8e0ec', borderLeft: '3px solid #c42b1c', borderRadius: '10px', padding: '20px 22px', margin: '6px 0', textAlign: 'center', boxShadow: '0 1px 2px rgba(0, 20, 60, 0.04)' }}>
            <div style={{ fontWeight: '700', fontSize: '14px', color: '#c42b1c', marginBottom: '8px' }}>Location Access Denied</div>
            <div style={{ fontSize: '13px', color: '#3d5068', marginBottom: '8px', lineHeight: '1.6' }}>
              Location access is <strong style={{ color: '#0f1d33' }}>mandatory</strong> to proceed with your network complaint.
            </div>
            <div style={{ fontSize: '12px', color: '#8596ab', marginBottom: '16px', lineHeight: '1.55' }}>
              Please click the <strong style={{ color: '#3d5068' }}>lock/location icon</strong> in your browser address bar and set Location to <strong style={{ color: '#3d5068' }}>"Allow"</strong>, then try again.
            </div>
            <button onClick={() => !isDisabled && msg.onRetry && msg.onRetry()} disabled={isDisabled}
              style={{ background: isDisabled ? '#8596ab' : '#c42b1c', color: '#fff', border: 'none', borderRadius: '8px', padding: '11px 26px', fontSize: '13px', fontWeight: '600', fontFamily: 'inherit', cursor: isDisabled ? 'not-allowed' : 'pointer', margin: '0 auto', display: 'block', boxShadow: '0 2px 6px rgba(196, 43, 28, 0.18)', transition: 'all 0.18s ease' }}>
              Try Again
            </button>
          </div>
        );
      case 'location-success': {
        const hasCoords = typeof msg.latitude === 'number' && typeof msg.longitude === 'number';
        return (
          <div key={msg.id} style={{ background: '#ffffff', border: '1px solid #d8e0ec', borderLeft: '3px solid #00875a', borderRadius: '10px', padding: '14px 18px', margin: '6px 0', display: 'flex', alignItems: 'center', gap: '14px', boxShadow: '0 1px 2px rgba(0, 20, 60, 0.04)' }}>
            <div style={{ fontSize: '18px', color: '#00875a', fontWeight: 700 }}>&#10003;</div>
            <div>
              <div style={{ fontWeight: '700', fontSize: '13px', color: '#00875a' }}>Location Captured Successfully</div>
              {hasCoords && (
                <div style={{ fontSize: '12px', color: '#3d5068', marginTop: '4px' }}>
                  Lat: <strong style={{ color: '#0f1d33' }}>{msg.latitude?.toFixed(6)}</strong> &nbsp;|&nbsp;
                  Long: <strong style={{ color: '#0f1d33' }}>{msg.longitude?.toFixed(6)}</strong>
                </div>
              )}
              {msg.description && (
                <div style={{ fontSize: '12px', color: '#3d5068', marginTop: '4px' }}>
                  Location: <strong style={{ color: '#0f1d33' }}>{msg.description}</strong>
                </div>
              )}
              <div style={{ fontSize: '11px', color: '#8596ab', marginTop: '2px' }}>Stored securely for network diagnostics</div>
            </div>
          </div>
        );
      }
      case 'signal-codes':
        return (
          <div key={msg.id} style={{ background: 'rgba(0, 145, 218, 0.12)', border: 'none', borderRadius: '10px', padding: '18px 20px', margin: '6px 0', boxShadow: '0 2px 8px rgba(0, 145, 218, 0.25)' }}>
            <div style={{ fontWeight: 700, fontSize: '14px', color: '#0f1d33', marginBottom: '10px' }}>Signal Diagnosis</div>
            <div style={{ fontSize: '13px', color: '#3d5068', lineHeight: 1.6, marginBottom: '12px' }}>
              Dial one of these codes on your phone and take a screenshot of the signal info screen:
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', marginBottom: '12px' }}>
              {[{ code: '*#0011#', desc: 'Samsung' }, { code: '*#*#4636#*#*', desc: 'Android' }, { code: '*3001#12345#*', desc: 'iPhone' }].map((item, i) => (
                <div key={i} style={{ background: 'rgba(0, 145, 218, 0.1)', border: '1px solid rgba(0, 145, 218, 0.25)', borderRadius: '8px', padding: '9px 14px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <code style={{ fontWeight: 700, color: '#0f1d33', fontSize: '14px' }}>{item.code}</code>
                  <span style={{ fontSize: '11px', color: '#8596ab' }}>{item.desc}</span>
                </div>
              ))}
            </div>
            <div style={{ fontSize: '12px', color: '#8596ab', lineHeight: 1.5 }}>
              Look for <strong style={{ color: '#0f1d33' }}>RSRP</strong>, <strong style={{ color: '#0f1d33' }}>SINR</strong>, and <strong style={{ color: '#0f1d33' }}>Cell ID</strong> on the screen, then upload the screenshot below.
            </div>
          </div>
        );
      case 'screenshot-upload':
        return (
          <div key={msg.id} style={{ background: 'rgba(0, 145, 218, 0.12)', border: 'none', borderRadius: '10px', padding: '16px 20px', margin: '6px 0', textAlign: 'center', boxShadow: '0 2px 8px rgba(0, 145, 218, 0.25)' }}>
            <div style={{ fontSize: '13px', color: '#3d5068', marginBottom: '14px' }}>Upload your signal information screenshot:</div>
            <button
              onClick={() => { if (!isDisabled && !screenshotUploading) { fileInputRef.current?.click(); fileInputRef.current._groupId = msg.groupId; } }}
              disabled={isDisabled || screenshotUploading}
              style={{ background: isDisabled ? '#a0c4e8' : '#0091DA', color: '#fff', border: 'none', borderRadius: '8px', padding: '11px 24px', fontSize: '13px', fontWeight: 700, fontFamily: 'inherit', cursor: isDisabled ? 'not-allowed' : 'pointer', display: 'inline-flex', alignItems: 'center', gap: '8px', transition: 'all 0.18s ease' }}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                <polyline points="17 8 12 3 7 8" />
                <line x1="12" y1="3" x2="12" y2="15" />
              </svg>
              Upload Screenshot
            </button>
            <div style={{ fontSize: '11px', color: '#8596ab', marginTop: '8px' }}>PNG, JPG, JPEG (max 5MB)</div>
          </div>
        );
      case 'broadband-diagnostic': {
        const billing     = msg.billing     || {};
        const quality     = msg.quality     || {};
        const errors      = msg.errors      || {};
        const planSpeed   = msg.planSpeed   || billing.plan_speed_mbps || null;
        const speedLabel  = quality.speedLabel  || 'Unknown';
        const speedColor  = speedLabel === 'Good' ? '#00875a' : speedLabel === 'Degraded' ? '#c87d0a' : '#c42b1c';
        const accountActive = billing.account_active !== false;
        const sectionStyle = { background: '#fff', border: '1px solid #d8e0ec', borderRadius: '10px', padding: '12px 14px', marginTop: '10px' };
        const labelStyle   = { fontSize: '11px', letterSpacing: '0.04em', color: '#00338d', fontWeight: 700, textTransform: 'uppercase', marginBottom: '6px' };
        return (
          <div key={msg.id} style={{ background: '#f7f9fc', border: '1px solid #d8e0ec', borderLeft: '4px solid #00338d', borderRadius: '12px', padding: '16px 18px', margin: '8px 0', boxShadow: '0 1px 2px rgba(0, 20, 60, 0.05)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
              <div style={{ width: 10, height: 10, borderRadius: '50%', background: '#00338d' }}></div>
              <div style={{ fontWeight: 700, fontSize: 14, color: '#0f1d33' }}>Broadband Diagnostic</div>
            </div>
            <div style={{ fontSize: 12, color: '#3d5068', marginBottom: '10px' }}>Plan info + quick browser speed check</div>
            <div style={sectionStyle}>
              <div style={labelStyle}>Plan Details</div>
              {errors.billing ? (
                <div style={{ color: '#c42b1c', fontSize: 12 }}>{errors.billing}</div>
              ) : (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: '8px 12px', fontSize: 12, color: '#1a2b42' }}>
                  <div><span style={{ color: '#8596ab' }}>Plan:</span> <strong style={{ color: '#0f1d33' }}>{billing.plan_name || 'N/A'}</strong></div>
                  <div><span style={{ color: '#8596ab' }}>Speed:</span> <strong style={{ color: '#0f1d33' }}>{planSpeed != null ? `${planSpeed} Mbps` : 'N/A'}</strong></div>
                  <div><span style={{ color: '#8596ab' }}>Account:</span> <strong style={{ color: accountActive ? '#0f1d33' : '#c42b1c' }}>{accountActive ? 'Active' : 'Inactive'}</strong></div>
                  <div><span style={{ color: '#8596ab' }}>Bill Paid:</span> <strong style={{ color: billing.bill_paid === false ? '#c42b1c' : '#0f1d33' }}>{billing.bill_paid === false ? 'No' : 'Yes'}</strong></div>
                </div>
              )}
            </div>
            <div style={sectionStyle}>
              <div style={labelStyle}>Speed Test</div>
              {errors.quality ? (
                <div style={{ color: '#c42b1c', fontSize: 12 }}>{errors.quality}</div>
              ) : (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '10px 12px', alignItems: 'center' }}>
                  <div style={{ fontSize: 12, color: '#1a2b42' }}>
                    <div style={{ color: '#8596ab', fontSize: 11 }}>Plan Speed</div>
                    <div style={{ fontSize: 14, fontWeight: 700, color: '#0f1d33' }}>{planSpeed != null ? `${planSpeed} Mbps` : 'n/a'}</div>
                  </div>
                  <div style={{ fontSize: 12, color: '#1a2b42' }}>
                    <div style={{ color: '#8596ab', fontSize: 11 }}>Measured Speed</div>
                    <div style={{ fontSize: 18, fontWeight: 800, color: '#0f1d33' }}>
                      {quality.speedMbps != null ? `${quality.speedMbps} Mbps` : 'n/a'}
                      {quality.speedPercent != null ? <span style={{ fontSize: 11, color: '#64748b', marginLeft: 6 }}>({quality.speedPercent}% of plan)</span> : null}
                    </div>
                    <div style={{ marginTop: 4, display: 'inline-block', padding: '4px 12px', borderRadius: 12, background: `${speedColor}15`, color: speedColor, fontWeight: 700, fontSize: 11 }}>{speedLabel}</div>
                  </div>
                </div>
              )}
              {quality.speedError && (
                <div style={{ marginTop: 8, fontSize: 11, color: '#c42b1c' }}>
                  Speed: {quality.speedError}
                </div>
              )}
            </div>
          </div>
        );
      }
      case 'user-image':
        return (
          <div key={msg.id} style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <div style={{ maxWidth: '240px', borderRadius: '12px', overflow: 'hidden', border: '1px solid #d8e0ec', boxShadow: '0 1px 3px rgba(0, 20, 60, 0.06)' }}>
              <img src={msg.imageSrc} alt="Uploaded screenshot" style={{ width: '100%', display: 'block' }} />
            </div>
          </div>
        );
      case 'diagnosis-result': {
        const d = msg.diagnosis;
        const overallColor = { green: '#00875a', amber: '#c87d0a', red: '#c42b1c', unknown: '#8596ab' };
        const overallBg    = { green: '#f0fdf4', amber: '#fffbeb', red: '#fef2f2', unknown: '#f7f9fc' };
        const status = d.overall_status || 'unknown';
        return (
          <div key={msg.id} style={{ background: overallBg[status], border: '1px solid #d8e0ec', borderLeft: `4px solid ${overallColor[status]}`, borderRadius: '10px', padding: '18px 20px', margin: '6px 0', boxShadow: '0 1px 2px rgba(0, 20, 60, 0.04)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '10px' }}>
              <div style={{ background: overallColor[status], color: '#fff', borderRadius: '12px', padding: '4px 14px', fontSize: '12px', fontWeight: 700 }}>
                Signal: {d.overall_label || 'Unknown'}
              </div>
              {d.is_busy_hour && (
                <div style={{ background: '#c87d0a', color: '#fff', borderRadius: '12px', padding: '4px 14px', fontSize: '12px', fontWeight: 700 }}>Peak Hours</div>
              )}
            </div>
            <div style={{ fontSize: '13px', color: '#1a2b42', lineHeight: '1.6' }}>{d.summary}</div>
            {d.nearest_sites && d.nearest_sites.length > 0 && (
              <div style={{ marginTop: '14px', borderTop: '1px solid #d8e0ec', paddingTop: '14px' }}>
                <div style={{ fontSize: '12px', fontWeight: 700, color: '#00338D', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: '10px' }}>Nearest Tower Sites</div>
                {d.nearest_sites.map((site, idx) => {
                  const sc = site.status === 'ON AIR' ? '#00875a' : '#c42b1c';
                  return (
                    <div key={idx} style={{ background: '#fff', border: '1px solid #d8e0ec', borderRadius: '8px', padding: '12px 14px', marginBottom: idx < d.nearest_sites.length - 1 ? '8px' : 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '6px' }}>
                        <span style={{ fontWeight: 700, fontSize: '13px', color: '#0f1d33' }}>{site.site_id}</span>
                        <span style={{ fontSize: '11px', fontWeight: 700, color: sc, background: site.status === 'ON AIR' ? 'rgba(0,135,90,0.08)' : 'rgba(196,43,28,0.08)', padding: '2px 10px', borderRadius: '6px' }}>{site.status}</span>
                      </div>
                      <div style={{ fontSize: '12px', color: '#3d5068', lineHeight: '1.5' }}>
                        <span style={{ color: '#8596ab' }}>Distance:</span> {site.distance_km} km
                        {site.alarm && site.alarm !== 'None' && <span style={{ marginLeft: '12px' }}><span style={{ color: '#8596ab' }}>Alarm:</span> {site.alarm}</span>}
                      </div>
                      {site.solution && site.solution !== 'No action required' && (
                        <div style={{ fontSize: '12px', color: '#00338D', marginTop: '4px', fontWeight: 600 }}>Action: {site.solution}</div>
                      )}
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
      case 'unsat-options': {
        const options = [
          { cls: 'retry', icon: '', title: 'Describe Again', desc: 'Provide more details for better resolution steps', fn: () => handleRetry(msg.groupId) },
          { cls: 'human', icon: '', title: 'Connect to Human Agent', desc: 'A support ticket will be raised for you', fn: () => handleHumanHandoff(msg.groupId) },
          { cls: 'newc',  icon: '', title: 'Main Menu', desc: 'Go back to the main service category menu', fn: () => handleBackToMenu(msg.groupId) },
          { cls: 'exit',  icon: '', title: 'Exit', desc: 'End this session', fn: () => handleExit(msg.groupId) },
        ];
        return (
          <div key={msg.id} className="unsat-options">
            {options.map((opt, i) => (
              <button key={i} className={`unsat-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && opt.fn()}>
                <div className={`o-icon ${opt.cls}`} dangerouslySetInnerHTML={{ __html: opt.icon }} />
                <div className="o-info"><div className="o-title">{opt.title}</div><div className="o-desc">{opt.desc}</div></div>
                <div className="o-arrow">&rsaquo;</div>
              </button>
            ))}
          </div>
        );
      }
      case 'email-action':
        return (
          <div key={msg.id} className="email-action-container">
            <button className={`email-btn${isDisabled ? ' disabled' : ''}`} onClick={() => !isDisabled && handleSendEmail(msg.groupId)}>
              <span className="email-btn-icon"></span>Send Summary to My Email
            </button>
          </div>
        );
      case 'email-sent':
        return (
          <div key={msg.id} className="email-sent-box">
            <div className="email-sent-icon"></div>
            <div className="email-sent-text">{msg.message}</div>
          </div>
        );
      case 'handoff':
        return (
          <div key={msg.id} className="handoff-box">
            <h4>Human Agent Request Submitted</h4>
            <div className="handoff-row"><span className="h-label">Category</span><span className="h-value">{msg.sectorName}</span></div>
            <div className="handoff-row"><span className="h-label">Issue Type</span><span className="h-value">{msg.subprocessName}</span></div>
            <div className="handoff-row"><span className="h-label">Complaint</span><span className="h-value">{msg.queryText}</span></div>
            {msg.assignedAgent ? (
              <>
                <div className="handoff-row"><span className="h-label">Status</span><span className="h-value" style={{ color: '#22c55e', fontWeight: 700 }}>Agent Assigned</span></div>
                <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 14px', margin: '10px 0 6px' }}>
                  <div style={{ fontSize: 12, color: '#1d4ed8', fontWeight: 700, marginBottom: 6 }}>Your Expert</div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: '#1e293b' }}>{msg.assignedAgent.name}</div>
                  {msg.assignedAgent.phone && <div style={{ fontSize: 13, color: '#0ea5e9', marginTop: 4 }}>Phone: {msg.assignedAgent.phone}</div>}
                  {msg.assignedAgent.employee_id && <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>ID: {msg.assignedAgent.employee_id}</div>}
                </div>
              </>
            ) : (
              <div className="handoff-row"><span className="h-label">Status</span><span className="h-value status-pending">Pending Agent Assignment</span></div>
            )}
            <div className="handoff-ref">Reference No: {msg.refNum}</div>
          </div>
        );
      case 'live-agent-message':
        return (
          <div key={msg.id} style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', maxWidth: '80%' }}>
            <div style={{ fontSize: 10, color: '#00338d', fontWeight: 600, marginBottom: 4, display: 'flex', alignItems: 'center', gap: 4 }}>
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" /><circle cx="12" cy="7" r="4" />
              </svg>
              Support Agent
              {msg.timestamp && <span style={{ opacity: 0.6, fontWeight: 400 }}>· {new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>}
            </div>
            <div style={{ background: '#eff6ff', border: '1px solid #93c5fd', borderRadius: '4px 16px 16px 16px', padding: '10px 14px', fontSize: 13, color: '#1e293b', lineHeight: 1.6, boxShadow: '0 1px 3px rgba(0,0,0,0.06)', wordBreak: 'break-word' }}>
              {msg.text}
            </div>
          </div>
        );
      case 'agent-resolved':
        return (
          <div key={msg.id} style={{ background: 'linear-gradient(135deg, #f0fdf4, #dcfce7)', border: '2px solid #22c55e', borderRadius: 14, padding: '20px 22px', margin: '8px 0' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
              <div style={{ width: 36, height: 36, borderRadius: '50%', background: '#22c55e', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12" /></svg>
              </div>
              <div>
                <div style={{ fontWeight: 700, fontSize: 15, color: '#15803d' }}>Issue Resolved</div>
                <div style={{ fontSize: 11, color: '#16a34a', marginTop: 2 }}>Your support ticket has been closed</div>
              </div>
            </div>
            <p style={{ margin: '0 0 16px', fontSize: 13, color: '#1e293b', lineHeight: 1.65 }}>{msg.botMessage}</p>
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              <button className="action-btn menu-btn" onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Main Menu</button>
              <button className="action-btn exit-btn" onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit Chat</button>
            </div>
          </div>
        );
      default:
        return null;
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
          <p className="gate-subtitle">
            {isActiveSession
              ? 'You have an active chat session. Would you like to continue or start a new one?'
              : 'Would you like to open this chat history or start a new chat?'}
          </p>
          <div className="gate-session-info">
            <div className="gate-session-row"><span className="gate-label">Session</span><span className="gate-value">#{session.id}</span></div>
            {session.sector_name && <div className="gate-session-row"><span className="gate-label">Category</span><span className="gate-value">{session.sector_name}</span></div>}
            {session.subprocess_name && <div className="gate-session-row"><span className="gate-label">Issue Type</span><span className="gate-value">{session.subprocess_name}</span></div>}
            <div className="gate-session-row"><span className="gate-label">Started</span><span className="gate-value">{session.created_at ? new Date(session.created_at).toLocaleString() : 'N/A'}</span></div>
            {lastMsg && <div className="gate-summary"><span className="gate-label">Last message</span><p>{lastMsg.content.length > 120 ? lastMsg.content.slice(0, 120) + '...' : lastMsg.content}</p></div>}
          </div>
          <div className="gate-actions">
            <button className="gate-btn gate-btn-primary" onClick={() => resumeChat(resumeCandidate, resumeMessages)}>
              {isActiveSession ? 'Continue Chat' : 'Open Chat'}
            </button>
            <button className="gate-btn gate-btn-secondary" onClick={() => beginNewChat()}>Start New Chat</button>
          </div>
        </div>
      </div>
    );
  };

  const renderStartGate = () => (
    <div className="gate-overlay">
      <div className="gate-card resume-gate">
        <h2 className="gate-title">Start a New Chat</h2>
        <p className="gate-subtitle">Begin a fresh support conversation whenever you are ready.</p>
        <div className="gate-actions">
          <button className="gate-btn gate-btn-primary" onClick={() => beginNewChat()}>
            Start New Chat
          </button>
        </div>
      </div>
    </div>
  );

  return (
    <div className="chat-support-page">
      <div className="app-container">
        <div className="header">
          <img src="https://upload.wikimedia.org/wikipedia/commons/d/db/KPMG_blue_logo.svg" alt="KPMG" style={{ height: 24 }} />
          <div className="header-info">
            <h1>Customer Handling</h1>
            <p>AI-powered multilingual support</p>
          </div>
          <div className="status-dot" />
          {initPhase === 'chat' && <button className="restart-btn" onClick={beginNewChat}>Restart</button>}
        </div>

        {initPhase === 'loading' && (
          <div className="chat-area" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div className="typing-indicator visible">
              <div className="typing-dot" /><div className="typing-dot" /><div className="typing-dot" />
            </div>
          </div>
        )}
        {initPhase === 'resume-prompt' && <div className="chat-area">{renderResumePrompt()}</div>}
        {initPhase === 'start-gate'    && <div className="chat-area">{renderStartGate()}</div>}

        {initPhase === 'chat' && (
          <>
            <div className="chat-area" ref={chatAreaRef}>
              {messages.map(renderMessage)}
              {isTyping && (
                <div className="typing-indicator visible">
                  <div className="typing-dot" /><div className="typing-dot" /><div className="typing-dot" />
                </div>
              )}
            </div>
            <input type="file" accept="image/*" ref={fileInputRef} style={{ display: 'none' }}
              onChange={(e) => {
                const file = e.target.files[0];
                if (file) {
                  const gid = fileInputRef.current?._groupId;
                  if (gid) disableGroup(gid);
                  handleScreenshotUpload(file);
                  e.target.value = '';
                }
              }}
            />
            {inputVisible && (
              <div className="input-area">
                <div className="input-row">
                  <textarea ref={inputRef} value={inputValue} onChange={handleInputChange}
                    onKeyDown={handleKeyDown} placeholder={inputPlaceholder} rows={1} />
                  <button className="send-btn" onClick={sendMessage} disabled={!inputValue.trim()}>
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <line x1="22" y1="2" x2="11" y2="13" />
                      <polygon points="22 2 15 22 11 13 2 9 22 2" />
                    </svg>
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

