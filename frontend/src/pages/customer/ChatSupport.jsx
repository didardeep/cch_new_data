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
  const sid = localStorage.getItem('chat_session_id');
  if (sid) clearCachedSession(sid);
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

function SpeedTestCard({ groupId, disabled, saveMessage, stateRef, disableGroup, addMessage, fetchSolution }) {
  const [phase, setPhase]           = useState('idle');
  const [pct, setPct]               = useState(0);
  const [phaseText, setPhaseText]   = useState('Ready to test');
  const [ping, setPing]             = useState(null);
  const [jitter, setJitter]         = useState(null);
  const [downMbps, setDownMbps]     = useState(null);
  const [upMbps, setUpMbps]         = useState(null);
  const [logLines, setLogLines]     = useState(['— Waiting to start —']);
  const [signalLevel, setSignalLevel] = useState(0);

  const appendLog = (line) => {
    const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
    setLogLines(prev => [...prev.filter(l => l !== '— Waiting to start —'), `[${ts}] ${line}`].slice(-20));
  };

  const getSignalLevel = (mbps) => {
    if (mbps >= 100) return 5; if (mbps >= 50) return 4;
    if (mbps >= 20) return 3;  if (mbps >= 5)  return 2; return 1;
  };
  const signalColor = (level) => level >= 4 ? '#00875a' : level >= 3 ? '#c87d0a' : '#c42b1c';
  const barHeights = [8, 13, 18, 23, 28];

  const runTest = async () => {
    if (phase === 'running') return;
    setPhase('running');
    setPct(0); setPing(null); setJitter(null); setDownMbps(null); setUpMbps(null);
    setSignalLevel(0); setLogLines([]); setPhaseText('Starting test…');
    appendLog('=== Network speed test started ===');

    const token = getToken();
    const authHeader = token ? { Authorization: `Bearer ${token}` } : {};
    const pingTimes = [];

    for (let i = 0; i < 8; i++) {
      try {
        const t0 = performance.now();
        await fetch(`${API_BASE}/api/broadband/ping?_=${Date.now()}`, { headers: authHeader, cache: 'no-store' });
        const rtt = performance.now() - t0;
        pingTimes.push(rtt);
        setPing(Math.round(pingTimes.reduce((a, b) => a + b) / pingTimes.length));
        appendLog(`Ping #${i + 1}: ${rtt.toFixed(1)} ms`);
      } catch { appendLog(`Ping #${i + 1}: error`); }
      setPct(Math.round(5 + (i / 8) * 20));
      await new Promise(r => setTimeout(r, 60));
    }
    if (pingTimes.length) {
      const avg = pingTimes.reduce((a, b) => a + b) / pingTimes.length;
      const jit = Math.sqrt(pingTimes.reduce((s, t) => s + (t - avg) ** 2, 0) / pingTimes.length);
      setPing(Math.round(avg)); setJitter(parseFloat(jit.toFixed(1)));
      appendLog(`Avg ping: ${avg.toFixed(1)} ms | Jitter: ${jit.toFixed(1)} ms`);
    }

    setPhaseText('Measuring download speed…');
    const downResults = [];
    for (let i = 0; i < 5; i++) {
      try {
        const t0 = performance.now();
        const resp = await fetch(`${API_BASE}/api/broadband/speedtest-file?_=${Date.now()}`, { headers: authHeader, cache: 'no-store' });
        let bytes = 0;
        if (resp.body?.getReader) {
          const reader = resp.body.getReader();
          while (true) { const { done, value } = await reader.read(); if (done) break; bytes += value?.length || 0; }
        } else { bytes = (await resp.arrayBuffer()).byteLength; }
        const mbps = parseFloat(((bytes * 8) / ((performance.now() - t0) / 1000 * 1e6)).toFixed(2));
        downResults.push(mbps);
        const avg = parseFloat((downResults.reduce((a, b) => a + b) / downResults.length).toFixed(2));
        setDownMbps(avg); setSignalLevel(getSignalLevel(avg));
        appendLog(`Download chunk ${i + 1}: ${mbps} Mbps`);
      } catch (e) { appendLog(`Download chunk ${i + 1}: error — ${e.message}`); }
      setPct(Math.round(25 + (i / 5) * 35));
    }
    const finalDown = downResults.length ? parseFloat((downResults.reduce((a, b) => a + b) / downResults.length).toFixed(2)) : 0;
    setDownMbps(finalDown); setSignalLevel(getSignalLevel(finalDown));
    appendLog(`Avg download: ${finalDown} Mbps`);

    setPhaseText('Measuring upload speed…');
    const uploadPayload = new Uint8Array(1 * 1024 * 1024);
    const upResults = [];
    for (let i = 0; i < 4; i++) {
      try {
        const t0 = performance.now();
        const resp = await fetch(`${API_BASE}/api/broadband/speedtest-upload`, {
          method: 'POST', headers: { ...authHeader, 'Content-Type': 'application/octet-stream' },
          body: uploadPayload, cache: 'no-store',
        });
        if (resp.ok) {
          const mbps = parseFloat(((uploadPayload.byteLength * 8) / ((performance.now() - t0) / 1000 * 1e6)).toFixed(2));
          upResults.push(mbps);
          setUpMbps(parseFloat((upResults.reduce((a, b) => a + b) / upResults.length).toFixed(2)));
          appendLog(`Upload chunk ${i + 1}: ${mbps} Mbps`);
        } else { appendLog(`Upload chunk ${i + 1}: HTTP ${resp.status}`); }
      } catch (e) { appendLog(`Upload chunk ${i + 1}: error — ${e.message}`); }
      setPct(Math.round(60 + (i / 4) * 38));
    }
    const finalUp = upResults.length ? parseFloat((upResults.reduce((a, b) => a + b) / upResults.length).toFixed(2)) : 0;
    setUpMbps(finalUp); appendLog(`Avg upload: ${finalUp} Mbps`);
    appendLog('=== Test complete ===');
    setPct(100); setPhaseText('Test complete'); setPhase('done');

    const summary = `Speed test results — Ping: ${Math.round(pingTimes.reduce((a,b)=>a+b,0)/Math.max(pingTimes.length,1))} ms, Download: ${finalDown} Mbps, Upload: ${finalUp} Mbps`;
    saveMessage('user', summary, { current_step: stateRef.current.step });
    stateRef.current.diagnosisSummary = (stateRef.current.diagnosisSummary ? stateRef.current.diagnosisSummary + ' | ' : '') + summary;
  };

  const level = signalLevel;
  const col = level > 0 ? signalColor(level) : '#d8e0ec';

  return (
    <div className="speed-test-card">
      <div className="speed-test-card__header">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={col} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>
        <span className="speed-test-card__title">Live Network Speed Test</span>
      </div>
      <div className="speed-test-card__subtitle">Measures your real ping, download and upload speed against our servers.</div>
      <div className="speed-test-signal">
        <div className="speed-test-signal__bars">
          {barHeights.map((h, i) => <div key={i} className="speed-test-signal__bar" style={{ height: h, background: i < level ? col : '#d8e0ec' }} />)}
        </div>
        <div>
          <div className="speed-test-signal__label" style={{ color: level > 0 ? col : '#8596ab' }}>
            {level === 0 ? 'Not tested' : level === 1 ? '< 5 Mbps' : level === 2 ? '5 – 20 Mbps' : level === 3 ? '20 – 50 Mbps' : level === 4 ? '50 – 100 Mbps' : '100+ Mbps'}
          </div>
          <div className="speed-test-signal__sublabel">{downMbps !== null ? `${downMbps} Mbps download` : 'Run test to measure'}</div>
        </div>
      </div>
      <div className="speed-test-metrics">
        {[
          { label: 'Ping', value: ping, unit: 'ms', color: '#005EB8', max: 300 },
          { label: 'Jitter', value: jitter, unit: 'ms', color: '#c87d0a', max: 100 },
          { label: 'Download', value: downMbps, unit: 'Mbps', color: '#00875a', max: 200 },
          { label: 'Upload', value: upMbps, unit: 'Mbps', color: '#483698', max: 100 },
        ].map(({ label, value, unit, color, max }) => (
          <div className="speed-metric" key={label}>
            <div className="speed-metric__label">{label}</div>
            <div className="speed-metric__value" style={{ color: value !== null ? '#0f1d33' : '#c8d0dc' }}>{value !== null ? value : '—'}</div>
            <div className="speed-metric__unit">{unit}</div>
            <div className="speed-metric__bar"><div className="speed-metric__bar-fill" style={{ width: value !== null ? `${Math.min(100, (value / max) * 100)}%` : '0%', background: color }} /></div>
          </div>
        ))}
      </div>
      <div className="speed-test-progress">
        <div className="speed-test-progress__labels"><span>{phaseText}</span><span>{pct}%</span></div>
        <div className="speed-test-progress__track"><div className="speed-test-progress__fill" style={{ width: `${pct}%` }} /></div>
      </div>
      <div className="speed-test-log">{logLines.join('\n')}</div>
      <div className="speed-test-actions">
        <button className="speed-test-btn" onClick={runTest} disabled={disabled || phase === 'running'}>
          {phase === 'running' ? 'Testing…' : phase === 'done' ? 'Re-run Test' : 'Run Speed Test'}
        </button>
        {phase === 'done' && (
          <button className="speed-test-btn speed-test-btn--secondary" onClick={() => {
            disableGroup(groupId);
            addMessage({ type: 'user', text: `Speed test complete — Download: ${downMbps} Mbps, Upload: ${upMbps} Mbps, Ping: ${ping} ms` });
            addMessage({ type: 'bot', html: `Thanks for running the speed test. Let me use these results to provide a better solution.` });
            fetchSolution(`My speed test shows — Download: ${downMbps} Mbps, Upload: ${upMbps} Mbps, Ping: ${ping} ms, Jitter: ${jitter} ms`);
          }}>Use Results &amp; Continue</button>
        )}
      </div>
      <div className="speed-test-note">All traffic stays within your company network. No data is sent externally.</div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────

function LiveConnectionCard({ groupId, disabled, autoStart, saveMessage, stateRef, sessionIdRef, disableGroup, addMessage, fetchSolution }) {
  const [phase, setPhase]         = useState('idle');
  const [pct, setPct]             = useState(0);
  const [phaseText, setPhaseText] = useState('Ready');
  const [pingVal, setPingVal]     = useState(null);
  const [jitterVal, setJitterVal] = useState(null);
  const [downVal, setDownVal]     = useState(null);
  const [upVal, setUpVal]         = useState(null);
  const [logLines, setLogLines]   = useState([]);
  const [sigLevel, setSigLevel]   = useState(0);
  const [showLog, setShowLog]     = useState(false);
  const [errorMsg, setErrorMsg]   = useState('');

  // startedRef prevents the autoStart effect from firing more than once.
  const startedRef = useRef(false);

  const log = useCallback((line) => {
    const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
    setLogLines(prev => [...prev.slice(-18), `[${ts}] ${line}`]);
  }, []);

  const sigColor = (lvl) => lvl >= 4 ? '#00875a' : lvl >= 3 ? '#c87d0a' : '#c42b1c';
  const sigLabel = (lvl, mbps) => {
    if (!mbps) return 'Not tested';
    return lvl >= 5 ? `${mbps} Mbps — Excellent` : lvl >= 4 ? `${mbps} Mbps — Good` : lvl >= 3 ? `${mbps} Mbps — Fair` : lvl >= 2 ? `${mbps} Mbps — Weak` : `${mbps} Mbps — Poor`;
  };
  const getLevel = (mbps) => mbps >= 100 ? 5 : mbps >= 50 ? 4 : mbps >= 20 ? 3 : mbps >= 5 ? 2 : 1;

  const fetchWithTimeout = async (url, opts = {}, ms = 5000) => {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), ms);
    try { return await fetch(url, { ...opts, signal: controller.signal }); }
    finally { clearTimeout(t); }
  };

  // run is stable — useCallback with [] deps. It reads no React state directly;
  // all setters are stable references and the parent props don't change identity.
  const run = useCallback(async () => {
    setErrorMsg(''); setPhase('running');
    setPct(0); setPingVal(null); setJitterVal(null); setDownVal(null); setUpVal(null);
    setSigLevel(0); setLogLines([]);
    setPhaseText('Measuring ping…');
    log('=== Connection check started ===');

    const token = getToken();
    const auth = token ? { Authorization: `Bearer ${token}` } : {};

    // ── Ping (8 rounds) ────────────────────────────────────────────────────────
    const times = [];
    let consecutiveFails = 0;
    for (let i = 0; i < 8; i++) {
      try {
        const t0 = performance.now();
        const res = await fetchWithTimeout(`${API_BASE}/api/broadband/ping?_=${Date.now()}`, { headers: auth, cache: 'no-store' });
        if (res.ok) {
          await res.json();
          times.push(performance.now() - t0);
          consecutiveFails = 0;
        } else { consecutiveFails += 1; log(`Ping #${i + 1}: HTTP ${res.status}`); }
      } catch (e) { consecutiveFails += 1; log(`Ping #${i + 1}: ${e.name === 'AbortError' ? 'Timeout' : e.message}`); }

      if (consecutiveFails >= 3) {
        log('Ping test failed repeatedly — please retry.');
        setPhase('error'); setPhaseText('Could not reach test server');
        setErrorMsg('Ping test failed repeatedly — please retry.'); setPct(100);
        return;
      }
      if (times.length) {
        const avg = times.reduce((a, b) => a + b) / times.length;
        setPingVal(Math.round(avg));
        setJitterVal(parseFloat(Math.sqrt(times.reduce((s, t) => s + (t - avg) ** 2, 0) / times.length).toFixed(1)));
      }
      setPct(Math.round(5 + (i / 8) * 20));
      await new Promise(r => setTimeout(r, 60));
    }
    if (times.length) log(`Avg ping: ${Math.round(times.reduce((a, b) => a + b) / times.length)} ms`);

    // ── Download (5 chunks) ────────────────────────────────────────────────────
    setPhaseText('Measuring download speed…');
    const downs = [];
    for (let i = 0; i < 5; i++) {
      try {
        const t0 = performance.now();
        const res = await fetchWithTimeout(`${API_BASE}/api/broadband/speedtest-file?_=${Date.now()}`, { headers: auth, cache: 'no-store' });
        if (!res.ok) { log(`Download chunk ${i + 1}: HTTP ${res.status}`); continue; }
        let bytes = 0;
        if (res.body?.getReader) {
          const rdr = res.body.getReader();
          while (true) { const { done, value } = await rdr.read(); if (done) break; bytes += value?.length || 0; }
        } else { bytes = (await res.arrayBuffer()).byteLength; }
        const mbps = parseFloat(((bytes * 8) / ((performance.now() - t0) / 1000 * 1e6)).toFixed(2));
        downs.push(mbps);
        const avg = parseFloat((downs.reduce((a, b) => a + b) / downs.length).toFixed(2));
        setDownVal(avg); setSigLevel(getLevel(avg));
        log(`Download chunk ${i + 1}: ${mbps} Mbps`);
      } catch (e) { log(`Download chunk ${i + 1}: ${e.message}`); }
      setPct(Math.round(25 + (i / 5) * 35));
    }
    const finalDown = downs.length ? parseFloat((downs.reduce((a, b) => a + b) / downs.length).toFixed(2)) : null;
    if (finalDown) log(`Avg download: ${finalDown} Mbps`);

    // ── Upload (3 chunks — skipped gracefully if endpoint missing) ─────────────
    setPhaseText('Measuring upload speed…');
    const payload = new Uint8Array(1 * 1024 * 1024);
    const ups = [];
    for (let i = 0; i < 3; i++) {
      try {
        const t0 = performance.now();
        const res = await fetchWithTimeout(`${API_BASE}/api/broadband/speedtest-upload`, {
          method: 'POST', headers: { ...auth, 'Content-Type': 'application/octet-stream' },
          body: payload, cache: 'no-store',
        });
        if (res.ok) {
          const mbps = parseFloat(((payload.byteLength * 8) / ((performance.now() - t0) / 1000 * 1e6)).toFixed(2));
          ups.push(mbps);
          setUpVal(parseFloat((ups.reduce((a, b) => a + b) / ups.length).toFixed(2)));
          log(`Upload chunk ${i + 1}: ${mbps} Mbps`);
        } else { log(`Upload chunk ${i + 1}: HTTP ${res.status} (skipped)`); }
      } catch (e) { log(`Upload chunk ${i + 1}: ${e.message} (skipped)`); }
      setPct(Math.round(60 + (i / 3) * 38));
    }
    const finalUp = ups.length ? parseFloat((ups.reduce((a, b) => a + b) / ups.length).toFixed(2)) : null;
    if (finalUp) log(`Avg upload: ${finalUp} Mbps`);

    log('=== Check complete ===');
    setPct(100); setPhaseText('Check complete'); setPhase('done');

    const finalPing = times.length ? Math.round(times.reduce((a, b) => a + b) / times.length) : null;
    const summary = `Connection check — Download: ${finalDown ?? 'N/A'} Mbps, Upload: ${finalUp ?? 'N/A'} Mbps, Ping: ${finalPing ?? 'N/A'} ms`;
    saveMessage('user', summary, { current_step: stateRef.current.step });
    stateRef.current.diagnosisSummary = (stateRef.current.diagnosisSummary ? stateRef.current.diagnosisSummary + ' | ' : '') + summary;
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Fires exactly once on mount if autoStart is true.
  useEffect(() => {
    if (autoStart && !startedRef.current) { startedRef.current = true; run(); }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const lvl = sigLevel;
  const col = lvl > 0 ? sigColor(lvl) : '#8596ab';
  const barHeights = [8, 13, 18, 23, 28];
  const visibleLog = showLog ? logLines : logLines.slice(-5);

  return (
    <div className="speed-test-card">
      <div className="speed-test-card__header">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={col} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M1 6s4-2 11-2 11 2 11 2"/><path d="M1 10s4-2 11-2 11 2 11 2"/>
          <circle cx="12" cy="15" r="3"/><line x1="12" y1="12" x2="12" y2="15"/>
        </svg>
        <span className="speed-test-card__title">Live Connection Check</span>
      </div>
      <div className="speed-test-card__subtitle">Measuring real-time download, upload and ping against our servers.</div>
      <div className="speed-test-signal">
        <div className="speed-test-signal__bars">
          {barHeights.map((h, i) => <div key={i} className="speed-test-signal__bar" style={{ height: h, background: i < lvl ? col : '#d8e0ec' }} />)}
        </div>
        <div>
          <div className="speed-test-signal__label" style={{ color: col }}>{sigLabel(lvl, downVal)}</div>
          <div className="speed-test-signal__sublabel">
            {phase === 'running' ? 'Measuring…' : phase === 'done' ? 'Based on live download speed' : 'Run check to measure'}
          </div>
        </div>
      </div>
      <div className="speed-test-metrics">
        {[
          { label: 'Ping', value: pingVal, unit: 'ms', color: '#005EB8', max: 300 },
          { label: 'Jitter', value: jitterVal, unit: 'ms', color: '#c87d0a', max: 80 },
          { label: 'Download', value: downVal, unit: 'Mbps', color: '#00875a', max: 200 },
          { label: 'Upload', value: upVal, unit: 'Mbps', color: '#483698', max: 100 },
        ].map(({ label, value, unit, color, max }) => (
          <div className="speed-metric" key={label}>
            <div className="speed-metric__label">{label}</div>
            <div className="speed-metric__value" style={{ color: value != null ? '#0f1d33' : '#c8d0dc' }}>{value != null ? value : '—'}</div>
            <div className="speed-metric__unit">{unit}</div>
            <div className="speed-metric__bar"><div className="speed-metric__bar-fill" style={{ width: value != null ? `${Math.min(100,(value/max)*100)}%` : '0%', background: color }} /></div>
          </div>
        ))}
      </div>
      <div className="speed-test-progress">
        <div className="speed-test-progress__labels"><span>{phaseText}</span><span>{pct}%</span></div>
        <div className="speed-test-progress__track"><div className="speed-test-progress__fill" style={{ width: `${pct}%` }} /></div>
      </div>
      <div className="speed-test-log">{visibleLog.join('\n') || '— Waiting to start —'}</div>
      {logLines.length > 5 && (
        <button type="button" className="speed-test-btn speed-test-btn--link" onClick={() => setShowLog(v => !v)}>
          {showLog ? 'Hide raw log' : 'View full log'}
        </button>
      )}
      <div className="speed-test-actions">
        <button className="speed-test-btn" onClick={run} disabled={disabled || phase === 'running'}>
          {phase === 'running' ? 'Checking…' : phase === 'done' ? 'Re-run Check' : phase === 'error' ? 'Retry Check' : 'Check My Connection'}
        </button>
        {phase === 'done' && (
          <button className="speed-test-btn speed-test-btn--secondary" onClick={() => {
            disableGroup(groupId);
            const summary = `Download: ${downVal} Mbps, Upload: ${upVal} Mbps, Ping: ${pingVal} ms`;
            addMessage({ type: 'user', text: `Connection check done — ${summary}` });
            fetchSolution(`My connection check results: ${summary}. Now please help with my original issue.`);
          }}>Use Results &amp; Get Help</button>
        )}
      </div>
      <div className="speed-test-note">
        {errorMsg ? <span style={{ color: '#c42b1c' }}>{errorMsg}</span> : 'All traffic stays within your company network.'}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────

function ConnectionCheckOffer({ groupId, disabled, queryText, disableGroup, addMessage, fetchSolution, stateRef }) {
  const nextIdLocal = useRef(0);
  const nid = () => ++nextIdLocal.current + Date.now(); // good-enough unique id for nested cards

  return (
    <div style={{ background: 'rgba(0,94,184,0.07)', border: '1px solid rgba(0,94,184,0.2)', borderRadius: 12, padding: '16px 18px', margin: '6px 0' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#005EB8" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M1 6s4-2 11-2 11 2 11 2"/><path d="M1 10s4-2 11-2 11 2 11 2"/><circle cx="12" cy="15" r="3"/>
        </svg>
        <span style={{ fontWeight: 700, fontSize: 14, color: '#00338D' }}>Connection issue detected</span>
      </div>
      <p style={{ fontSize: 13, color: '#3d5068', margin: '0 0 14px', lineHeight: 1.6 }}>
        It looks like you're experiencing a network issue. Would you like to run a quick live connection check first?
      </p>
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        <button disabled={disabled}
          style={{ background: disabled ? '#8596ab' : '#005EB8', color: '#fff', border: 'none', borderRadius: 8, padding: '9px 20px', fontSize: 13, fontWeight: 600, fontFamily: 'inherit', cursor: disabled ? 'not-allowed' : 'pointer' }}
          onClick={() => {
            if (disabled) return;
            disableGroup(groupId);
            addMessage({ type: 'user', text: 'Check my connection' });
            const ccGroupId = nid();
            addMessage({ type: 'live-connection', groupId: ccGroupId, autoStart: true });
            stateRef.current.step = 'connection-check';
          }}>Check My Connection</button>
        <button disabled={disabled}
          style={{ background: 'transparent', color: '#005EB8', border: '1px solid #005EB8', borderRadius: 8, padding: '9px 20px', fontSize: 13, fontWeight: 600, fontFamily: 'inherit', cursor: disabled ? 'not-allowed' : 'pointer' }}
          onClick={() => { if (disabled) return; disableGroup(groupId); fetchSolution(queryText); }}>
          Skip, just help me
        </button>
      </div>
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
      const cached = loadCachedSession(sessionIdRef.current) || {};
      const merged = Array.isArray(cached.messages) ? cached.messages.filter(m => !isConnectionSummary(m)) : [];
      const type = sender === 'bot' ? 'bot' : sender === 'agent' ? 'live-agent-message' : 'user';
      merged.push({ type, text: content, html: meta.html || content, payload: payloadForSave });
      saveCachedSession(sessionIdRef.current, { messages: merged, state: stateRef.current });
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

  const saveLocationToBackend = useCallback(async (latitude, longitude) => {
    if (!sessionIdRef.current) return;
    try {
      const token = getToken();
      await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/location`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ latitude, longitude }),
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
      addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
      showInput('Describe your issue in any language...');
      stateRef.current.step = 'query';
    }, 500);
  }, [addMessage, showInput]);

  const classifySpeed = (speedMbps, planSpeedMbps) => {
    if (typeof speedMbps !== 'number' || speedMbps <= 0) return { label: 'Unknown', percent: null };
    if (planSpeedMbps) {
      const percent = Math.round((speedMbps / planSpeedMbps) * 100);
      return percent >= 80 ? { label: 'Good', percent } : percent >= 40 ? { label: 'Degraded', percent } : { label: 'Poor', percent };
    }
    return speedMbps >= 50 ? { label: 'Good', percent: null } : speedMbps >= 10 ? { label: 'Degraded', percent: null } : { label: 'Poor', percent: null };
  };

  const classifyLatency = (latencyMs) => {
    if (typeof latencyMs !== 'number' || Number.isNaN(latencyMs) || latencyMs <= 0) return 'Unknown';
    return latencyMs < 50 ? 'Good' : latencyMs <= 150 ? 'Moderate' : 'High';
  };

  const measurePing = useCallback(async () => {
    const token = getToken();
    const start = performance.now();
    const resp = await fetch(`${API_BASE}/api/broadband/ping?ts=${Date.now()}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {}, cache: 'no-store',
    });
    if (!resp.ok) throw new Error(`Ping failed: ${resp.status}`);
    await resp.json();
    return performance.now() - start;
  }, []);

  const measureDownloadSpeed = useCallback(async () => {
    const token = getToken();
    const start = performance.now();
    const resp = await fetch(`${API_BASE}/api/broadband/speedtest-file?ts=${Date.now()}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {}, cache: 'no-store',
    });
    if (!resp.ok) throw new Error(`Speed test failed: ${resp.status}`);
    let bytes = 0;
    if (resp.body?.getReader) {
      const reader = resp.body.getReader();
      while (true) { const { done, value } = await reader.read(); if (done) break; bytes += value?.length || 0; }
    } else { bytes = (await resp.arrayBuffer()).byteLength; }
    if (bytes === 0) throw new Error('No bytes received');
    const durationSec = (performance.now() - start) / 1000;
    return { mbps: Number(((bytes * 8) / (durationSec * 1e6)).toFixed(2)), bytes, durationSec };
  }, []);

  const runBrowserQualityChecks = useCallback(async (planSpeedMbps) => {
    const connectionType = navigator.connection?.effectiveType || 'unknown';
    let latencyMs = null, latencyError = null, speedMbps = null, speedError = null;
    try { latencyMs = Math.round(await measurePing()); } catch (e) { latencyError = e?.message || 'Latency check failed'; }
    try { const speed = await measureDownloadSpeed(); speedMbps = speed.mbps; } catch (e) { speedError = e?.message || 'Speed test failed'; }
    const speedMeta = classifySpeed(speedMbps, planSpeedMbps);
    return { speedMbps, speedPercent: speedMeta.percent, speedLabel: speedMeta.label, latencyMs, latencyLabel: classifyLatency(latencyMs), connectionType, latencyError, speedError };
  }, [measurePing, measureDownloadSpeed]);

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
    if (!isBilling || stateRef.current.step === 'exited') return null;
    if (broadbandDiagStatusRef.current === 'running' || broadbandDiagStatusRef.current === 'done') return broadbandDiagResultRef.current;

    broadbandDiagStatusRef.current = 'running';
    addMessage({ type: 'bot', html: 'Running broadband billing diagnostics...', skipPersist: true });
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
      } catch { diag.errors.billing = 'Billing check failed'; stateRef.current.billingContext = null; }

      try { diag.connection = await apiGet('/api/broadband/connection-check'); }
      catch { diag.errors.connection = 'Connection check failed'; }

      try { diag.quality = await runBrowserQualityChecks(planSpeed); }
      catch { diag.errors.quality = 'Browser checks failed'; }

      stateRef.current.connectionContext = buildConnectionContext(diag.billing, diag.connection, diag.quality) || null;
      stateRef.current.planSpeedMbps = planSpeed;
      broadbandDiagResultRef.current = diag;

      if (!stateRef.current.broadbandDiagShown) {
        addMessage({ type: 'broadband-diagnostic', billing: diag.billing, connection: diag.connection, quality: diag.quality, errors: diag.errors, planSpeed });
        stateRef.current.broadbandDiagShown = true;
      }
      return diag;
    } finally {
      setIsTyping(false);
      broadbandDiagStatusRef.current = 'done';
    }
  }, [addMessage, buildConnectionContext, runBrowserQualityChecks]);

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
          addMessage({ type: 'system', text: 'Please describe the location of the issue.' });
          showInput('Describe the location...'); stateRef.current.step = 'location-other';
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
    const { sessionId: forcedSessionId = null } = opts || {};
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
        sessionIdRef.current = data.session.id;
        localStorage.setItem('chat_session_id', String(data.session.id));
        const cache = loadCachedSession(sessionIdRef.current);
        const dbMsgs = data.messages?.length ? data.messages : [];
        const toHydrate = cache?.messages?.length ? cache.messages : dbMsgs;

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
        showInput(data.session.status === 'escalated' ? 'Type your reply to the agent...' : 'Describe your issue...');
        joinSocketSession();
        resumeNeededRef.current = true;
        return true;
      }
    } catch {}
    return false;
  }, [hydrateHistory, joinSocketSession, showInput, addMessage]);

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
    const data = await chatApiCall('/api/subprocesses', { sector_key: key, language: stateRef.current.language });
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
      onNo: () => {},
    });
    stateRef.current.step = 'location-question';
  }, [addMessage, disableGroup, ensureSession, requestLocation, afterLocationCaptured]);

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
    saveMessage('user', userQuery, { query_text: userQuery, sector_name: st.sectorName, subprocess_name: st.subprocessName });
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
      addMessage({ type: 'non-telecom-warning', html: formatResolution(resolveData.resolution) });
      saveMessage('bot', resolveData.resolution);
      addMessage({ type: 'bot', html: `Please describe a telecom-related issue so I can help you.` });
      showInput('Describe your telecom issue...'); st.attempt -= 1; return;
    }
    st.resolution = resolveData.resolution; st.previousSolutions.push(resolveData.resolution);
    saveMessage('bot', resolveData.resolution, { resolution: resolveData.resolution, language: st.language });
    addMessage({ type: 'resolution', html: formatResolution(resolveData.resolution) });
    if (st.attempt >= 6) { setTimeout(() => autoRaiseTicket(), 800); return; }
    setTimeout(() => { addMessage({ type: 'bot', html: `Did this help? If not, please describe what's still not working.` }); showInput('Type your response...'); }, 800);
    st.step = 'conversation';
  }, [addMessage, saveMessage, showInput, ensureSession, autoRaiseTicket]);

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
          ? `<br><br>Dedicated support agent:<br><div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;"><div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">Phone: ${assignedAgent.phone}</div>` : ''}${slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">SLA: ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : ''}</div>`
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
          (assignedAgent ? `Dedicated agent: <strong>${assignedAgent.name}</strong>${assignedAgent.phone ? ` — ${assignedAgent.phone}` : ''}` : `Our support team will contact you shortly.`) +
          (slaHours ? `<br><span style="color:#16a34a;font-weight:600;">SLA: ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : '') +
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
    setTimeout(async () => {
      if (session.status === 'resolved') { addMessage({ type: 'bot', html: `This chat session is <strong>resolved</strong>. Start a new chat if you need more help.` }); hideInput(); stateRef.current.step = 'view-only'; return; }
      if (session.status === 'escalated') { addMessage({ type: 'bot', html: `Please wait — we are connecting you to a human agent.` }); agentResolvedShownRef.current = false; agentJoinedRef.current = false; setHandoffActive(true); hideInput(); stateRef.current.step = 'live-agent'; return; }
      if (!session.sector_name) { addMessage({ type: 'bot', html: 'Please select your <strong>telecom service category</strong>:' }); loadSectorMenu(); }
      else if (!session.subprocess_name) {
        addMessage({ type: 'bot', html: `Please select the <strong>type of issue</strong> with <strong>${session.sector_name}</strong>:` });
        const data = await chatApiCall('/api/subprocesses', { sector_key: stateRef.current.sectorKey, language: stateRef.current.language });
        addMessage({ type: 'subprocess-grid', subprocesses: limitSubprocesses(data.subprocesses), groupId: nextId() }); stateRef.current.step = 'subprocess';
      } else if (session.resolution) { addMessage({ type: 'bot', html: `Did this help? If not, please describe what's still not working.` }); showInput('Type your response...'); stateRef.current.step = 'conversation'; }
      else { addMessage({ type: 'bot', html: 'Please <strong>describe your specific issue</strong> so I can provide the best resolution.' }); showInput('Describe your issue in any language...'); stateRef.current.step = 'query'; }
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
      if (resumeId) { try { const data = await apiGet(`/api/chat/session/${resumeId}`); if (data?.session) { resumeChat(data.session, data.messages || []); return; } } catch {} }
      const storedId = localStorage.getItem('chat_session_id');
      if (storedId) { const restored = await restoreSession({ sessionId: storedId }); if (restored) return; }
      try {
        const active = await apiGet('/api/chat/session/active');
        if (active?.session && active.session.status !== 'resolved') {
          const cache = loadCachedSession(active.session.id);
          if (cache?.messages?.length) { const restored = await restoreSession({ sessionId: active.session.id }); if (restored) return; }
          setResumeCandidate(active.session); setResumeMessages(active.messages || []); setInitPhase('resume-prompt'); return;
        }
      } catch {}
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
        return <ConnectionCheckOffer key={msg.id} groupId={msg.groupId} disabled={isDisabled} queryText={msg.queryText} disableGroup={disableGroup} addMessage={addMessage} fetchSolution={fetchSolution} stateRef={stateRef} />;
      case 'live-connection':
        return <LiveConnectionCard key={msg.id} groupId={msg.groupId} disabled={isDisabled} autoStart={msg.autoStart} {...cardProps} />;
      case 'speed-test':
        return <SpeedTestCard key={msg.id} groupId={msg.groupId} disabled={isDisabled} {...cardProps} />;
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
            <div style={{ fontSize: 11, color: '#8596ab', marginTop: 8 }}>PNG, JPG, JPEG (max 5MB)</div>
          </div>
        );
      case 'broadband-diagnostic': {
        const billing = msg.billing || {}, quality = msg.quality || {}, errors = msg.errors || {};
        const planSpeed = msg.planSpeed || billing.plan_speed_mbps || null;
        const speedLabel = quality.speedLabel || 'Unknown';
        const speedColor = speedLabel === 'Good' ? '#00875a' : speedLabel === 'Degraded' ? '#c87d0a' : speedLabel === 'Poor' ? '#c42b1c' : '#8596ab';
        const accountActive = billing.account_active !== false;
        return (
          <div key={msg.id} style={{ background: '#f7f9fc', border: '1px solid #d8e0ec', borderLeft: '4px solid #00338d', borderRadius: 12, padding: '16px 18px', margin: '8px 0' }}>
            <div style={{ fontWeight: 700, fontSize: 14, color: '#0f1d33', marginBottom: 6 }}>Broadband Diagnostic</div>
            <div style={{ fontSize: 12, color: '#3d5068', marginBottom: 10 }}>Plan info + live browser speed measurement</div>
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
            <div style={{ background: '#fff', border: '1px solid #d8e0ec', borderRadius: 10, padding: '12px 14px', marginTop: 10 }}>
              <div style={{ fontSize: 11, letterSpacing: '0.04em', color: '#00338d', fontWeight: 700, textTransform: 'uppercase', marginBottom: 6 }}>Live Speed Measurement</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '10px 12px', alignItems: 'center' }}>
                {planSpeed != null && <div style={{ fontSize: 12 }}><div style={{ color: '#8596ab', fontSize: 11 }}>Plan Speed</div><div style={{ fontSize: 14, fontWeight: 700 }}>{planSpeed} Mbps</div></div>}
                <div style={{ fontSize: 12 }}>
                  <div style={{ color: '#8596ab', fontSize: 11 }}>Measured Download</div>
                  <div style={{ fontSize: 22, fontWeight: 800, color: quality.speedMbps != null ? '#0f1d33' : '#c42b1c' }}>{quality.speedMbps != null ? `${quality.speedMbps} Mbps` : 'Failed'}</div>
                  {quality.speedPercent != null && <div style={{ fontSize: 11, color: '#64748b' }}>{quality.speedPercent}% of plan speed</div>}
                  <div style={{ marginTop: 4, display: 'inline-block', padding: '3px 10px', borderRadius: 12, background: `${speedColor}18`, color: speedColor, fontWeight: 700, fontSize: 11 }}>{speedLabel}</div>
                </div>
                {quality.latencyMs != null && <div style={{ fontSize: 12 }}><div style={{ color: '#8596ab', fontSize: 11 }}>Ping / Latency</div><div style={{ fontSize: 22, fontWeight: 800, color: '#0f1d33' }}>{quality.latencyMs} ms</div><div style={{ fontSize: 11, color: '#64748b' }}>{quality.latencyLabel}</div></div>}
              </div>
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
          {initPhase === 'chat' && <button className="restart-btn" onClick={beginNewChat}>Restart</button>}
        </div>

        {initPhase === 'loading' && (
          <div className="chat-area" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div className="typing-indicator visible"><div className="typing-dot"/><div className="typing-dot"/><div className="typing-dot"/></div>
          </div>
        )}
        {initPhase === 'resume-prompt' && <div className="chat-area">{renderResumePrompt()}</div>}
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
