import { useState, useRef, useEffect, useCallback } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { API_BASE, getToken, apiGet, apiPost } from '../../api';
import { useAuth } from '../../AuthContext';
import '../../styles/chatbot.css';

<<<<<<< HEAD
const API_BASE = '';

// ── DEFAULT LOCATION (used instead of real GPS) ──────────────────────────────
// TODO: Remove this and uncomment the real geolocation logic when ready
const DEFAULT_LATITUDE = 28.4595;   // Gurgaon, Haryana, India
const DEFAULT_LONGITUDE = 77.0266;
// ─────────────────────────────────────────────────────────────────────────────

=======
>>>>>>> madhav
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

function limitSubprocesses(subprocesses) {
  const entries = Object.entries(subprocesses);
  const others = entries.filter(([, v]) => v === 'Others' || v.toLowerCase().includes('other'));
  const major = entries.filter(([, v]) => v !== 'Others' && !v.toLowerCase().includes('other'));
  return Object.fromEntries([...major.slice(0, 5), ...others]);
}

const SUBPROCESS_FOLLOWUP_OPTIONS = {
  'network / signal problems': [
    'Internet / Mobile Data',
    'Calls',
    'SMS / OTP',
    'Others',
  ],
};

function getFollowupOptions(subprocessName) {
  if (!subprocessName) return [];
  const key = subprocessName.trim().toLowerCase();
  return SUBPROCESS_FOLLOWUP_OPTIONS[key] || [];
}

export default function ChatSupport() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const [handoffActive, setHandoffActive] = useState(false);
  const agentResolvedShownRef = useRef(false);

  const [initPhase, setInitPhase] = useState('loading');
  const [pendingFeedback, setPendingFeedback] = useState([]);
  const [currentFbIdx, setCurrentFbIdx] = useState(0);
  const [activeSessionData, setActiveSessionData] = useState(null);
  const [activeSessionMsgs, setActiveSessionMsgs] = useState([]);
  const [fbRating, setFbRating] = useState(0);
  const [fbComment, setFbComment] = useState('');
  const [fbSubmitting, setFbSubmitting] = useState(false);

  const [messages, setMessages] = useState([]);
  const [inlineSubprocessPicker, setInlineSubprocessPicker] = useState(null);
  const [inputVisible, setInputVisible] = useState(false);
  const [inputPlaceholder, setInputPlaceholder] = useState('Describe your issue...');
  const [inputValue, setInputValue] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [disabledGroups, setDisabledGroups] = useState(new Set());

  const [locationStatus, setLocationStatus] = useState('idle');
  const locationRetryRef = useRef(null);

  const [screenshotUploading, setScreenshotUploading] = useState(false);
  const fileInputRef = useRef(null);

  const chatAreaRef = useRef(null);
  const inputRef = useRef(null);
  const sessionIdRef = useRef(null);
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
  });
  const msgIdCounter = useRef(0);

  const nextId = () => ++msgIdCounter.current;

  const scrollToBottom = useCallback(() => {
    setTimeout(() => {
      if (chatAreaRef.current) {
        chatAreaRef.current.scrollTop = chatAreaRef.current.scrollHeight;
      }
    }, 100);
  }, []);

  const addMessage = useCallback((msg) => {
    const id = nextId();
    const groupId = msg.groupId || id;
    setMessages(prev => [...prev, { ...msg, id, groupId }]);
    scrollToBottom();
    return groupId;
  }, [scrollToBottom]);

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

  const saveMessage = useCallback(async (sender, content, meta = {}) => {
    if (!sessionIdRef.current) return;
    try {
      await chatApiCall(`/api/chat/session/${sessionIdRef.current}/message`, {
        sender, content, ...meta,
      });
    } catch (e) {}
  }, []);

  const createSession = useCallback(async () => {
    try {
      const data = await chatApiCall('/api/chat/session', {});
      if (data.session) {
        sessionIdRef.current = data.session.id;
      }
    } catch (e) {}
  }, []);

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

  // ── Request Location — uses DEFAULT lat/long instead of real GPS ──────────
  // TODO: Uncomment the real geolocation block below and remove the default
  //       location block when GPS is ready to be used in production.
  const requestLocation = useCallback((onSuccess) => {
    setLocationStatus('requesting');

    // ── REAL GEOLOCATION (commented out) ──────────────────────────────────────
    // if (!navigator.geolocation) {
    //   addMessage({
    //     type: 'system',
    //     text: 'Your browser does not support location services. Please use a modern browser.',
    //   });
    //   return;
    // }
    //
    // navigator.geolocation.getCurrentPosition(
    //   // SUCCESS
    //   (position) => {
    //     const { latitude, longitude } = position.coords;
    //     setLocationStatus('granted');
    //     saveLocationToBackend(latitude, longitude);
    //     addMessage({
    //       type: 'location-success',
    //       latitude,
    //       longitude,
    //     });
    //     if (onSuccess) onSuccess();
    //   },
    //   // ERROR — user denied or error
    //   (error) => {
    //     setLocationStatus('denied');
    //     const locGroupId = nextId();
    //     addMessage({
    //       type: 'location-required',
    //       groupId: locGroupId,
    //       onRetry: () => {
    //         disableGroup(locGroupId);
    //         requestLocation(onSuccess);
    //       },
    //     });
    //   },
    //   {
    //     enableHighAccuracy: true,
    //     timeout: 15000,
    //     maximumAge: 0,
    //   }
    // );
    // ── END REAL GEOLOCATION ───────────────────────────────────────────────────

    // ── DEFAULT LOCATION (hardcoded fallback) ──────────────────────────────────
    // TODO: Remove this block when switching back to real GPS
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
    // ── END DEFAULT LOCATION ───────────────────────────────────────────────────

  }, [addMessage, saveLocationToBackend, disableGroup]);

  // ── Poll for agent messages + resolution after handoff ──
  const lastSeenMsgIdRef = useRef(0);

  useEffect(() => {
    if (!handoffActive) return;
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
        newAgentMsgs.forEach(m => {
          lastSeenMsgIdRef.current = Math.max(lastSeenMsgIdRef.current, m.id);
          addMessage({ type: 'live-agent-message', text: m.content, timestamp: m.created_at });
        });
        const s = data.session || {};
        if (s.status === 'resolved' && !agentResolvedShownRef.current) {
          agentResolvedShownRef.current = true;
          setHandoffActive(false);
          const lastBot = [...allMsgs].reverse().find(m => m.sender === 'bot');
          addMessage({
            type: 'agent-resolved',
            botMessage: lastBot?.content || 'Your support ticket has been resolved.',
          });
        }
      } catch {}
    };
    const iv = setInterval(poll, 6000);
    return () => clearInterval(iv);
  }, [handoffActive, addMessage]);

  const startChat = useCallback(async () => {
    setMessages([]);
    setInlineSubprocessPicker(null);
    setDisabledGroups(new Set());
    setLocationStatus('idle');
    stateRef.current = {
      step: 'greeting', sectorKey: null, sectorName: null,
      subprocessKey: null, subprocessName: null, language: 'English',
      queryText: '', resolution: '', attempt: 0, previousSolutions: [],
    };
    sessionIdRef.current = null;
    agentResolvedShownRef.current = false;
    setHandoffActive(false);
    hideInput();
    setInitPhase('chat');
    setTimeout(() => {
      addMessage({ type: 'bot', html: `<strong>Welcome to TeleBot Support!</strong><br>Say hello to get started!` });
      showInput('Type your greeting here...');
    }, 500);
  }, [addMessage, hideInput, showInput]);

  const loadSectorMenu = useCallback(async () => {
    const token = getToken();
    const headers = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const resp = await fetch(`${API_BASE}/api/menu`, { headers });
    const data = await resp.json();
    const groupId = nextId();
    addMessage({ type: 'sector-menu', menu: data.menu, groupId });
    stateRef.current.step = 'sector';
  }, [addMessage]);

  const selectSector = useCallback(async (key, name, groupId) => {
    disableGroup(groupId);
    setInlineSubprocessPicker(null);
    stateRef.current.sectorKey = key;
    stateRef.current.sectorName = name;
    addMessage({ type: 'user', text: name });
    addMessage({ type: 'system', text: `Selected: ${name}` });
    setIsTyping(true);
    const data = await chatApiCall('/api/subprocesses', { sector_key: key, language: stateRef.current.language });
    setIsTyping(false);
    addMessage({ type: 'bot', html: `Great choice! Now please select the <strong>type of issue</strong> you're facing with <strong>${name}</strong>:` });
    const spGroupId = nextId();
    addMessage({ type: 'subprocess-grid', subprocesses: limitSubprocesses(data.subprocesses), groupId: spGroupId });
    stateRef.current.step = 'subprocess';
  }, [addMessage, disableGroup]);

  const afterLocationCaptured = useCallback(() => {
    setTimeout(() => {
      addMessage({ type: 'bot', html: `Thank you! Your location has been recorded.` });
      addMessage({ type: 'bot', html: `Please <strong>describe your specific issue</strong> so I can provide the best resolution.` });
      showInput('Describe your issue in any language...');
      stateRef.current.step = 'query';
    }, 500);
  }, [addMessage, showInput]);

  const beginLocationFlow = useCallback(async (selectedIssueLabel) => {
    if (!sessionIdRef.current) { await createSession(); }
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
            // requestLocation will use DEFAULT_LATITUDE/DEFAULT_LONGITUDE (see above)
            requestLocation(() => { afterLocationCaptured(selectedIssueLabel); });
          },
        });
        stateRef.current.step = 'location';
      },
      onNo: () => {
        disableGroup(locQGroupId);
        addMessage({ type: 'user', text: "No, I'm at a different location" });
        addMessage({ type: 'bot', html: `Please describe the location where you're experiencing this issue (e.g., area, address, landmark):` });
        showInput('Describe your issue location...');
        stateRef.current.step = 'location-describe';
      },
    });
    stateRef.current.step = 'location-question';
  }, [addMessage, disableGroup, showInput, createSession, requestLocation, afterLocationCaptured]);

  const autoRaiseTicket = useCallback(async () => {
    addMessage({ type: 'bot', html: `I'm sorry I wasn't able to resolve your issue. Let me raise a ticket for you so our support team can help.` });
    let refNum = '';
    let assignedAgent = null;
    let slaHours = null;
    if (sessionIdRef.current) {
      try {
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) { assignedAgent = data.assigned_agent; }
      } catch {}
    }
    addMessage({
      type: 'bot',
      html: `Your ticket has been raised successfully!` +
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
          : `<br><br>Our support team will reach out to you shortly.` +
            (slaHours ? `<br><span style="color:#16a34a;font-weight:600;">Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : '')) +
        `<br>You can track your ticket from the dashboard.`,
    });
    setTimeout(() => {
      const actionGroupId = nextId();
      addMessage({ type: 'post-feedback-actions', groupId: actionGroupId });
    }, 1000);
    stateRef.current.step = 'escalated';
  }, [addMessage]);

  const fetchSolution = useCallback(async (userQuery) => {
    const st = stateRef.current;
    st.attempt += 1;
    if (!sessionIdRef.current) { await createSession(); }
    if (st.attempt === 1) { st.queryText = userQuery; }
    saveMessage('user', userQuery, { query_text: userQuery, sector_name: st.sectorName, subprocess_name: st.subprocessName });
    setIsTyping(true);
    const resolveData = await chatApiCall('/api/resolve-step', {
      sector_key: st.sectorKey,
      subprocess_key: st.subprocessKey,
      selected_subprocess: st.subprocessName || undefined,
      query: userQuery,
      language: st.language,
      previous_solutions: st.previousSolutions.slice(-10),
      attempt: st.attempt,
      original_query: st.attempt > 1 ? st.queryText : undefined,
      diagnosis_summary: st.diagnosisSummary || undefined,
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
  }, [addMessage, saveMessage, showInput, createSession, autoRaiseTicket]);

  const selectSubprocess = useCallback(async (key, name, groupId, selectedOption = null) => {
    const followupOptions = getFollowupOptions(name);
    if (followupOptions.length > 0 && !selectedOption) {
      setInlineSubprocessPicker({
        groupId,
        subprocessKey: key,
        subprocessName: name,
        options: followupOptions,
      });
      return;
    }

    disableGroup(groupId);
    setInlineSubprocessPicker(null);
    stateRef.current.subprocessKey = key;
    stateRef.current.subprocessSubType = selectedOption || null;
    const finalSubprocessName = selectedOption ? `${name} - ${selectedOption}` : name;
    stateRef.current.subprocessName = finalSubprocessName;
    stateRef.current.attempt = 0;
    stateRef.current.previousSolutions = [];
    addMessage({ type: 'user', text: finalSubprocessName });
    await beginLocationFlow(finalSubprocessName);
  }, [addMessage, disableGroup, beginLocationFlow]);

  const sendMessage = useCallback(async () => {
    const text = inputValue.trim();
    if (!text) return;
    addMessage({ type: 'user', text });
    setInputValue('');
    hideInput();

    if (stateRef.current.step === 'greeting') {
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

    if (stateRef.current.step === 'location-describe') {
      if (sessionIdRef.current) {
        try {
          const token = getToken();
          await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/location`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
            body: JSON.stringify({ location_description: text }),
          });
        } catch (e) {}
      }
      addMessage({ type: 'bot', html: `Thank you! Location noted: <strong>${text}</strong>` });
      afterLocationCaptured(stateRef.current.subprocessName);
      return;
    }

    if (stateRef.current.step === 'conversation') {
      setIsTyping(true);
      let classification = { is_satisfied: false, mentions_signal: false };
      try { classification = await chatApiCall('/api/classify-response', { text }); } catch {}
      setIsTyping(false);
      if (classification.is_satisfied) {
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
          addMessage({ type: 'post-feedback-actions', groupId: actionGroupId });
        }, 800);
        stateRef.current.step = 'resolved';
        return;
      }
      if (classification.mentions_signal && isNetworkIssue(stateRef.current.subprocessName) && !stateRef.current.diagnosisRan) {
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

    await fetchSolution(text);
  }, [inputValue, addMessage, hideInput, fetchSolution, loadSectorMenu, user, afterLocationCaptured]);

  const handleSendEmail = useCallback(async (groupId) => {
    disableGroup(groupId);
    if (!sessionIdRef.current) return;
    addMessage({ type: 'system', text: 'Sending summary to your email...' });
    try {
      const token = getToken();
      const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/send-summary-email`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      });
      const data = await resp.json();
      if (resp.ok) { addMessage({ type: 'email-sent', message: data.message }); }
      else { addMessage({ type: 'system', text: data.error || 'Failed to send email.' }); }
    } catch { addMessage({ type: 'system', text: 'Failed to send email. Please try again later.' }); }
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
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) { assignedAgent = data.assigned_agent; }
      } catch {}
    }
    addMessage({
      type: 'bot',
      html: `Your ticket has been raised successfully!` +
        (refNum ? `<br>Reference: <strong>${refNum}</strong>` : '') +
        (assignedAgent
          ? `<br><br>We are connecting you to our expert. Your dedicated support agent is:<br>
             <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;">
               <div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>
               ${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">📞 ${assignedAgent.phone}</div>` : ''}
               ${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">✉️ ${assignedAgent.email}</div>` : ''}
               ${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}
               ${slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">⏱ Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : ''}
             </div>`
          : `<br><br>Our support team will reach out to you shortly.` +
            (slaHours ? `<br><span style="color:#16a34a;font-weight:600;">⏱ Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : '')) +
        `<br>You can track your ticket from the dashboard.`,
    });
    setTimeout(() => { const ag = nextId(); addMessage({ type: 'post-feedback-actions', groupId: ag }); }, 1000);
    stateRef.current.step = 'escalated';
    agentResolvedShownRef.current = false;
    setHandoffActive(true);
  }, [addMessage, disableGroup, saveMessage, setHandoffActive]);

  const handleBackToMenu = useCallback((groupId) => {
    disableGroup(groupId);
    stateRef.current.attempt = 0;
    stateRef.current.previousSolutions = [];
    addMessage({ type: 'user', text: 'Main Menu' });
    addMessage({ type: 'bot', html: `Sure! Please select your <strong>telecom service category</strong>:` });
    setTimeout(() => loadSectorMenu(), 400);
    stateRef.current.step = 'sector';
  }, [addMessage, disableGroup, loadSectorMenu]);

  const handleExit = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Exit' });
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
    }
    stateRef.current.step = 'exited';
    setTimeout(() => {
      navigate(`/customer/feedback${currentSessionId ? `?session=${currentSessionId}` : ''}`);
    }, 1500);
  }, [addMessage, disableGroup, hideInput, navigate]);

  const handleScreenshotUpload = useCallback(async (file) => {
    if (!file || !file.type.startsWith('image/')) {
      addMessage({ type: 'system', text: 'Please upload a valid image file (PNG, JPG).' });
      return;
    }
    if (file.size > 5 * 1024 * 1024) {
      addMessage({ type: 'system', text: 'Image is too large. Please upload a screenshot under 5MB.' });
      return;
    }
    const reader = new FileReader();
    reader.onload = async () => {
      const base64String = reader.result.split(',')[1];
      addMessage({ type: 'user-image', imageSrc: reader.result });
      setScreenshotUploading(true);
      setIsTyping(true);
      try {
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/analyze-signal`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
          body: JSON.stringify({ image: base64String }),
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
              addMessage({ type: 'bot', html: 'Your signal is poor. Please try using your phone in an open area or near a window to improve reception. I am now raising a ticket so our support team can assist you directly.' });
              autoRaiseTicket();
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
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/escalate`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (data.ticket) { refNum = data.ticket.reference_number; slaHours = data.ticket.sla_hours || null; }
        if (data.assigned_agent) { assignedAgent = data.assigned_agent; }
      } catch {}
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
      const slaLine = slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">⏱ Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : '';
      const agentCard = assignedAgent
        ? `<br><br>We are connecting you to your dedicated support expert:<br>
           <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;">
             <div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>
             ${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">📞 ${assignedAgent.phone}</div>` : ''}
             ${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">✉️ ${assignedAgent.email}</div>` : ''}
             ${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}
             ${slaLine}
           </div>`
        : `<br><br>Our support team will contact you shortly.` + (slaHours ? `<br><span style="color:#16a34a;font-weight:600;">⏱ Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</span>` : '');
      addMessage({
        type: 'bot',
        html: `Your request has been submitted and a support ticket has been raised.` + agentCard +
          `<br>Reference: <strong>${refNum}</strong><br><br>The agent may send you messages below — please stay in this chat.<br><br>What would you like to do next?`,
      });
      setTimeout(() => { const ag = nextId(); addMessage({ type: 'post-feedback-actions', groupId: ag }); }, 400);
    }, 1500);
    stateRef.current.step = 'human_handoff';
    agentResolvedShownRef.current = false;
    setHandoffActive(true);
  }, [addMessage, disableGroup, saveMessage, setHandoffActive]);

  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  }, [sendMessage]);

  const handleInputChange = useCallback((e) => {
    setInputValue(e.target.value);
    e.target.style.height = 'auto';
    e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
  }, []);

  const resumeChat = useCallback(async (session, msgs) => {
    setInitPhase('chat');
    setMessages([]);
    setInlineSubprocessPicker(null);
    setDisabledGroups(new Set());
    hideInput();
    sessionIdRef.current = session.id;
    stateRef.current.sectorName = session.sector_name || null;
    stateRef.current.subprocessName = session.subprocess_name || null;
    stateRef.current.language = session.language || 'English';
    stateRef.current.queryText = session.query_text || '';
    stateRef.current.resolution = session.resolution || '';
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
        newMsgs.push({ type: 'user', text: m.content, id, groupId: id });
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
    stateRef.current.attempt = botResolutions.length;
    setMessages(prev => [...prev, ...newMsgs]);
    scrollToBottom();
    setTimeout(async () => {
      if (session.status && session.status !== 'active') {
        addMessage({
          type: 'bot',
          html: `This chat session is <strong>${session.status}</strong>. You are viewing the complete chat history.`,
        });
        addMessage({ type: 'bot', html: `Start a new chat if you need more help.` });
        hideInput();
        stateRef.current.step = 'view-only';
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
  }, [addMessage, hideInput, loadSectorMenu, scrollToBottom, showInput]);

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
        if (data?.session) { setActiveSessionData(data.session); setActiveSessionMsgs(data.messages || []); setInitPhase('resume-prompt'); return; }
      } catch {}
    }
    const activeData = await apiGet('/api/customer/active-session');
    if (activeData?.session) { setActiveSessionData(activeData.session); setActiveSessionMsgs(activeData.messages || []); setInitPhase('resume-prompt'); return; }
    startChat();
  }, [searchParams, startChat]);

  const initialized = useRef(false);
  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    (async () => {
      try {
        const fbData = await apiGet('/api/customer/pending-feedback');
        if (fbData?.sessions?.length > 0) { setPendingFeedback(fbData.sessions); setCurrentFbIdx(0); setInitPhase('feedback-gate'); return; }
      } catch {}
      const resumeId = searchParams.get('resume');
      if (resumeId) {
        try {
          const data = await apiGet(`/api/chat/session/${resumeId}`);
          if (data?.session) { setActiveSessionData(data.session); setActiveSessionMsgs(data.messages || []); setInitPhase('resume-prompt'); return; }
        } catch {}
      }
      const activeData = await apiGet('/api/customer/active-session');
      if (activeData?.session) { setActiveSessionData(activeData.session); setActiveSessionMsgs(activeData.messages || []); setInitPhase('resume-prompt'); return; }
      startChat();
    })();
  }, []);

  // ══════════════════════════════════════════════════════════════════
  // RENDER MESSAGES
  // ══════════════════════════════════════════════════════════════════
  const renderMessage = (msg) => {
    const isDisabled = disabledGroups.has(msg.groupId);
    switch (msg.type) {
      case 'bot':
        return <div key={msg.id} className="message bot" dangerouslySetInnerHTML={{ __html: msg.html }} />;
      case 'user':
        return <div key={msg.id} className="message user">{msg.text}</div>;
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
              const hasFollowup = getFollowupOptions(sname).length > 0;
              const isExpanded = inlineSubprocessPicker &&
                inlineSubprocessPicker.groupId === msg.groupId &&
                inlineSubprocessPicker.subprocessKey === sk;
              return (
                <div key={sk} style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  <button className={`subprocess-chip${isOthers ? ' others' : ''}${isDisabled ? ' disabled' : ''}`}
                    onClick={() => !isDisabled && selectSubprocess(sk, sname, msg.groupId)}>
                    <div className="chip-num">{isOthers ? '···' : idx + 1}</div>
                    <div className="chip-label">{sname}</div>
                    <div className="chip-arrow">›</div>
                  </button>
                  {hasFollowup && isExpanded && !isDisabled && (
                    <div style={{ marginTop: '-4px' }}>
                      <select
                        defaultValue=""
                        onChange={(e) => {
                          const selected = e.target.value;
                          if (!selected) return;
                          selectSubprocess(sk, sname, msg.groupId, selected);
                        }}
                        style={{ width: '100%', padding: '10px 12px', borderRadius: 8, border: '1px solid #cbd5e1', fontSize: 14 }}
                      >
                        <option value="" disabled>Select one option</option>
                        {(inlineSubprocessPicker.options || []).map((opt, optIdx) => (
                          <option key={`${opt}-${optIdx}`} value={opt}>{opt}</option>
                        ))}
                      </select>
                    </div>
                  )}
                </div>
              );
            })}
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
      case 'post-feedback-actions':
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

      // ── LOCATION QUESTION ──
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

      // ── LOCATION PROMPT — clicking "Share My Location" uses DEFAULT_LATITUDE/DEFAULT_LONGITUDE
      //    Real GPS is commented out in requestLocation() above.
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

      // ── LOCATION REQUIRED — never shown while default location is active;
      //    will reappear once real GPS is re-enabled.
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

      // ── LOCATION SUCCESS ──
      case 'location-success':
        return (
          <div key={msg.id} style={{ background: '#ffffff', border: '1px solid #d8e0ec', borderLeft: '3px solid #00875a', borderRadius: '10px', padding: '14px 18px', margin: '6px 0', display: 'flex', alignItems: 'center', gap: '14px', boxShadow: '0 1px 2px rgba(0, 20, 60, 0.04)' }}>
            <div style={{ fontSize: '18px', color: '#00875a', fontWeight: 700 }}>&#10003;</div>
            <div>
              <div style={{ fontWeight: '700', fontSize: '13px', color: '#00875a' }}>Location Captured Successfully</div>
              <div style={{ fontSize: '12px', color: '#3d5068', marginTop: '4px' }}>
                Lat: <strong style={{ color: '#0f1d33' }}>{msg.latitude?.toFixed(6)}</strong> &nbsp;|&nbsp;
                Long: <strong style={{ color: '#0f1d33' }}>{msg.longitude?.toFixed(6)}</strong>
              </div>
              <div style={{ fontSize: '11px', color: '#8596ab', marginTop: '2px' }}>Stored securely for network diagnostics</div>
            </div>
          </div>
        );

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
        const overallBg = { green: '#f0fdf4', amber: '#fffbeb', red: '#fef2f2', unknown: '#f7f9fc' };
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
          { cls: 'newc', icon: '', title: 'Main Menu', desc: 'Go back to the main service category menu', fn: () => handleBackToMenu(msg.groupId) },
          { cls: 'exit', icon: '', title: 'Exit', desc: 'End this session', fn: () => handleExit(msg.groupId) },
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
                <div className="handoff-row"><span className="h-label">Status</span><span className="h-value" style={{ color: '#22c55e', fontWeight: 700 }}>✅ Agent Assigned</span></div>
                <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 14px', margin: '10px 0 6px' }}>
                  <div style={{ fontSize: 12, color: '#1d4ed8', fontWeight: 700, marginBottom: 6 }}>🧑‍💼 Your Expert</div>
                  <div style={{ fontSize: 14, fontWeight: 700, color: '#1e293b' }}>{msg.assignedAgent.name}</div>
                  {msg.assignedAgent.phone && <div style={{ fontSize: 13, color: '#0ea5e9', marginTop: 4 }}>📞 {msg.assignedAgent.phone}</div>}
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

  const renderFeedbackGate = () => {
    const session = pendingFeedback[currentFbIdx];
    if (!session) return null;
    return (
      <div className="gate-overlay">
        <div className="gate-card feedback-gate">
          <div className="gate-icon">&#9733;</div>
          <h2 className="gate-title">Feedback Required</h2>
          <p className="gate-subtitle">
            Please rate your previous chat session before starting a new one.
            {pendingFeedback.length > 1 && <span className="gate-counter"> ({currentFbIdx + 1} of {pendingFeedback.length})</span>}
          </p>
          <div className="gate-session-info">
            <div className="gate-session-row"><span className="gate-label">Category</span><span className="gate-value">{session.sector_name || 'N/A'}</span></div>
            <div className="gate-session-row"><span className="gate-label">Issue</span><span className="gate-value">{session.subprocess_name || 'N/A'}</span></div>
            <div className="gate-session-row"><span className="gate-label">Status</span><span className={`badge badge-${session.status}`}>{session.status}</span></div>
            <div className="gate-session-row"><span className="gate-label">Date</span><span className="gate-value">{session.created_at ? new Date(session.created_at).toLocaleDateString() : 'N/A'}</span></div>
            {session.summary && <div className="gate-summary"><span className="gate-label">Summary</span><p>{session.summary}</p></div>}
          </div>
          <div className="gate-rating">
            <label>Rate your experience</label>
            <div className="gate-stars">
              {[1, 2, 3, 4, 5].map(n => (
                <button key={n} type="button" className={`gate-star${n <= fbRating ? ' active' : ''}`} onClick={() => setFbRating(n)}>&#9733;</button>
              ))}
            </div>
            <span className="gate-rating-label">{fbRating === 0 ? 'Click a star to rate' : `${fbRating}/5`}</span>
          </div>
          <div className="gate-comment">
            <label>Comments (optional)</label>
            <textarea placeholder="Tell us about your experience..." value={fbComment} onChange={e => setFbComment(e.target.value)} rows={3} />
          </div>
          <button className="gate-submit-btn" disabled={fbRating === 0 || fbSubmitting} onClick={handleFeedbackSubmit}>
            {fbSubmitting ? 'Submitting...' : 'Submit Feedback'}
          </button>
        </div>
      </div>
    );
  };

  const renderResumePrompt = () => {
    const session = activeSessionData;
    if (!session) return null;
    const lastMsg = activeSessionMsgs.length > 0 ? activeSessionMsgs[activeSessionMsgs.length - 1] : null;
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
            <button className="gate-btn gate-btn-primary" onClick={() => resumeChat(activeSessionData, activeSessionMsgs)}>
              {isActiveSession ? 'Continue Chat' : 'Open Chat'}
            </button>
            <button className="gate-btn gate-btn-secondary" onClick={() => startChat()}>Start New Chat</button>
          </div>
        </div>
      </div>
    );
  };

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
          {initPhase === 'chat' && <button className="restart-btn" onClick={startChat}>Restart</button>}
        </div>

        {initPhase === 'loading' && (
          <div className="chat-area" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div className="typing-indicator visible">
              <div className="typing-dot" /><div className="typing-dot" /><div className="typing-dot" />
            </div>
          </div>
        )}
        {initPhase === 'feedback-gate' && <div className="chat-area">{renderFeedbackGate()}</div>}
        {initPhase === 'resume-prompt' && <div className="chat-area">{renderResumePrompt()}</div>}

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


