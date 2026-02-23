import { useState, useRef, useEffect, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { getToken, apiGet, apiPost } from '../../api';
import { useAuth } from '../../AuthContext';
import '../../styles/chatbot.css';

const API_BASE = '';

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

function limitSubprocesses(subprocesses) {
  const entries = Object.entries(subprocesses);
  const others = entries.filter(([, v]) => v === 'Others' || v.toLowerCase().includes('other'));
  const major = entries.filter(([, v]) => v !== 'Others' && !v.toLowerCase().includes('other'));
  return Object.fromEntries([...major.slice(0, 5), ...others]);
}

export default function ChatSupport() {
  const { user } = useAuth();
  const [searchParams] = useSearchParams();

  // ── Init-phase state ──
  const [initPhase, setInitPhase] = useState('loading'); // loading | feedback-gate | resume-prompt | chat
  const [pendingFeedback, setPendingFeedback] = useState([]);
  const [currentFbIdx, setCurrentFbIdx] = useState(0);
  const [activeSessionData, setActiveSessionData] = useState(null);
  const [activeSessionMsgs, setActiveSessionMsgs] = useState([]);
  const [fbRating, setFbRating] = useState(0);
  const [fbComment, setFbComment] = useState('');
  const [fbSubmitting, setFbSubmitting] = useState(false);

  // ── Chat state ──
  const [messages, setMessages] = useState([]);
  const [inputVisible, setInputVisible] = useState(false);
  const [inputPlaceholder, setInputPlaceholder] = useState('Describe your issue...');
  const [inputValue, setInputValue] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [disabledGroups, setDisabledGroups] = useState(new Set());
  const chatAreaRef = useRef(null);
  const inputRef = useRef(null);
  const sessionIdRef = useRef(null);
  const stateRef = useRef({
    step: 'welcome',
    sectorKey: null,
    sectorName: null,
    subprocessKey: null,
    subprocessName: null,
    language: 'English',
    queryText: '',
    resolution: '',
    attempt: 0,
    previousSolutions: [],
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

  // ── Save message to backend session ──
  const saveMessage = useCallback(async (sender, content, meta = {}) => {
    if (!sessionIdRef.current) return;
    try {
      await chatApiCall(`/api/chat/session/${sessionIdRef.current}/message`, {
        sender,
        content,
        ...meta,
      });
    } catch (e) {
      // silently fail
    }
  }, []);

  // ── Create a new session on backend ──
  const createSession = useCallback(async () => {
    try {
      const data = await chatApiCall('/api/chat/session', {});
      if (data.session) {
        sessionIdRef.current = data.session.id;
      }
    } catch (e) {
      // silently fail
    }
  }, []);

  // ── Start Chat (fresh) ──
  const startChat = useCallback(async () => {
    setMessages([]);
    setDisabledGroups(new Set());
    stateRef.current = {
      step: 'greeting', sectorKey: null, sectorName: null,
      subprocessKey: null, subprocessName: null, language: 'English',
      queryText: '', resolution: '',
      attempt: 0, previousSolutions: [],
    };
    sessionIdRef.current = null;
    hideInput();
    setInitPhase('chat');

    setTimeout(() => {
      addMessage({
        type: 'bot',
        html: `<strong>Welcome to TeleBot Support!</strong><br><br>` +
          `We're delighted to have you here. Please say hello to get started — we'd love to hear from you!`,
      });
      showInput('Type your greeting here...');
    }, 500);
  }, [addMessage, hideInput, showInput]);

  // ── Load Sector Menu ──
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

  // ── Select Sector ──
  const selectSector = useCallback(async (key, name, groupId) => {
    disableGroup(groupId);
    stateRef.current.sectorKey = key;
    stateRef.current.sectorName = name;

    addMessage({ type: 'user', text: name });
    addMessage({ type: 'system', text: `Selected: ${name}` });
    saveMessage('user', name, { sector_name: name });

    setIsTyping(true);
    const data = await chatApiCall('/api/subprocesses', {
      sector_key: key, language: stateRef.current.language,
    });
    setIsTyping(false);

    addMessage({
      type: 'bot',
      html: `Great choice! Now please select the <strong>type of issue</strong> you're facing with <strong>${name}</strong>:`,
    });

    const spGroupId = nextId();
    addMessage({ type: 'subprocess-grid', subprocesses: limitSubprocesses(data.subprocesses), groupId: spGroupId });
    stateRef.current.step = 'subprocess';
  }, [addMessage, disableGroup, saveMessage]);

  // ── Fetch a single solution step ──
  const fetchSolution = useCallback(async (userQuery) => {
    const st = stateRef.current;
    st.attempt += 1;

    if (!sessionIdRef.current) {
      await createSession();
    }

    st.queryText = userQuery;
    saveMessage('user', userQuery, {
      query_text: userQuery,
      sector_name: st.sectorName,
      subprocess_name: st.subprocessName,
    });

    setIsTyping(true);
    const resolveData = await chatApiCall('/api/resolve-step', {
      sector_key: st.sectorKey,
      subprocess_key: st.subprocessKey,
      query: userQuery,
      language: st.language,
      previous_solutions: st.previousSolutions,
      attempt: st.attempt,
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
    saveMessage('bot', resolveData.resolution, {
      resolution: resolveData.resolution,
      language: st.language,
    });

    addMessage({
      type: 'resolution',
      html: formatResolution(resolveData.resolution),
    });

    setTimeout(() => {
      addMessage({
        type: 'bot',
        html: `Did this solution resolve your issue? <em>(Attempt ${st.attempt})</em>`,
      });
      const satGroupId = nextId();
      addMessage({ type: 'satisfaction', groupId: satGroupId, attempt: st.attempt });
    }, 800);

    st.step = 'feedback';
  }, [addMessage, saveMessage, showInput, createSession]);

  // ── Select Subprocess ──
  const selectSubprocess = useCallback(async (key, name, groupId) => {
    disableGroup(groupId);
    stateRef.current.subprocessKey = key;
    stateRef.current.subprocessName = name;
    stateRef.current.attempt = 0;
    stateRef.current.previousSolutions = [];

    addMessage({ type: 'user', text: name });
    saveMessage('user', name, { subprocess_name: name });

    addMessage({
      type: 'bot',
      html: `You selected <strong>${name}</strong>. Please <strong>describe your specific issue</strong> so I can provide the best resolution.`,
    });

    showInput('Describe your issue in any language...');
    stateRef.current.step = 'query';
  }, [addMessage, disableGroup, saveMessage, showInput]);

  // ── Send Message (user describes issue for next solution) ──
  const sendMessage = useCallback(async () => {
    const text = inputValue.trim();
    if (!text) return;

    addMessage({ type: 'user', text });
    setInputValue('');
    hideInput();

    // ── Greeting step: semantically verify it's a greeting, then respond ──
    if (stateRef.current.step === 'greeting') {
      setIsTyping(true);
      let isGreeting = true;
      try {
        const greetData = await chatApiCall('/api/detect-greeting', { text });
        isGreeting = greetData.is_greeting !== false;
      } catch {
        isGreeting = true; // fail-open
      }
      setIsTyping(false);

      if (!isGreeting) {
        addMessage({
          type: 'bot',
          html: `That doesn't look like a greeting. Please say hello to get started — we'd love to hear from you! 😊`,
        });
        showInput('Type your greeting here...');
        return;
      }

      const userName = user?.name || 'there';
      setIsTyping(true);
      await new Promise(resolve => setTimeout(resolve, 700));
      setIsTyping(false);
      addMessage({
        type: 'bot',
        html: `<strong>Hello, ${userName}!</strong><br><br>` +
          `What a lovely greeting — thank you so much for reaching out! It's wonderful to have you here.<br><br>` +
          `I'm your AI-powered telecom support assistant, ready to help you with any issues related to mobile, broadband, DTH, landline, and enterprise services.<br><br>` +
          `<em>Feel free to type in any language — I'll respond in your preferred language.</em>`,
      });
      setTimeout(() => {
        addMessage({
          type: 'bot',
          html: `<strong>How can I help you today?</strong><br>Please select your telecom service category below:`,
        });
        setTimeout(() => loadSectorMenu(), 400);
      }, 1000);
      stateRef.current.step = 'sector';
      return;
    }

    if (stateRef.current.language === 'English') {
      try {
        const langData = await chatApiCall('/api/detect-language', { text });
        stateRef.current.language = langData.language || 'English';
      } catch {
        stateRef.current.language = 'English';
      }
      addMessage({ type: 'system', text: `Language detected: ${stateRef.current.language}` });
    }

    await fetchSolution(text);
  }, [inputValue, addMessage, hideInput, fetchSolution, loadSectorMenu, user]);

  // ── Send Summary Email ──
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
      if (resp.ok) {
        addMessage({ type: 'email-sent', message: data.message });
      } else {
        addMessage({ type: 'system', text: data.error || 'Failed to send email.' });
      }
    } catch {
      addMessage({ type: 'system', text: 'Failed to send email. Please try again later.' });
    }
  }, [addMessage, disableGroup]);

  // ── Satisfied → Resolved ──
  const handleSatisfied = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Yes, my issue is resolved' });
    addMessage({ type: 'thankyou' });
    saveMessage('user', 'Yes, my issue is resolved');

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
    }, 1500);
    stateRef.current.step = 'resolved';
  }, [addMessage, disableGroup, saveMessage]);

  // ── Unsatisfied → Ask for more details ──
  const handleUnsatisfied = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'No, my issue is not resolved' });
    saveMessage('user', 'No, my issue is not resolved');

    addMessage({
      type: 'bot',
      html: `I'm sorry that didn't help. Please <strong>describe your specific issue</strong> so I can provide a better solution.`,
    });
    showInput('Describe your issue in detail...');
    stateRef.current.step = 'query';
  }, [addMessage, disableGroup, saveMessage, showInput]);

  // ── Raise Ticket (user-initiated from attempt 2 onwards) ──
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
        if (data.ticket) {
          refNum = data.ticket.reference_number;
          slaHours = data.ticket.sla_hours || null;
        }
        if (data.assigned_agent) {
          assignedAgent = data.assigned_agent;
        }
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
            (slaHours ? `<br>SLA: Resolve within ${slaHours} hours` : '')) +
        `<br>You can track your ticket from the dashboard.`,
    });

    setTimeout(() => {
      const actionGroupId = nextId();
      addMessage({ type: 'post-feedback-actions', groupId: actionGroupId });
    }, 1000);
    stateRef.current.step = 'escalated';
  }, [addMessage, disableGroup, saveMessage]);

  // ── Back to Menu ──
  const handleBackToMenu = useCallback((groupId) => {
    disableGroup(groupId);
    stateRef.current.attempt = 0;
    stateRef.current.previousSolutions = [];
    addMessage({ type: 'user', text: 'Main Menu' });
    addMessage({ type: 'bot', html: `Sure! Please select your <strong>telecom service category</strong>:` });
    setTimeout(() => loadSectorMenu(), 400);
    stateRef.current.step = 'sector';
  }, [addMessage, disableGroup, loadSectorMenu]);

  // ── Exit → Send summary email + goodbye ──
  const handleExit = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Exit' });
    hideInput();

    if (sessionIdRef.current) {
      addMessage({ type: 'system', text: 'Sending chat summary to your email...' });
      try {
        const token = getToken();
        const resp = await fetch(`${API_BASE}/api/chat/session/${sessionIdRef.current}/send-summary-email`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        });
        const data = await resp.json();
        if (resp.ok) {
          addMessage({ type: 'email-sent', message: data.message });
        }
      } catch {}
    }

    addMessage({ type: 'exit-box' });
    stateRef.current.step = 'exited';
  }, [addMessage, disableGroup, hideInput]);

  // ── Retry ──
  const handleRetry = useCallback((groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'I want to describe my issue again' });
    addMessage({
      type: 'bot',
      html: `Sure! Please <strong>describe your issue again</strong> with as much detail as possible.`,
    });
    showInput('Describe your issue in more detail...');
    stateRef.current.step = 'query';
  }, [addMessage, disableGroup, showInput]);

  // ── Human Handoff → Raise Ticket ──
  const handleHumanHandoff = useCallback(async (groupId) => {
    disableGroup(groupId);
    addMessage({ type: 'user', text: 'Connect me to a human agent' });
    saveMessage('user', 'Connect me to a human agent');

    let refNum = 'TC-' + Date.now().toString(36).toUpperCase() + '-' +
      Math.random().toString(36).substring(2, 6).toUpperCase();
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
        if (data.ticket) {
          refNum = data.ticket.reference_number;
          slaHours = data.ticket.sla_hours || null;
        }
        if (data.assigned_agent) {
          assignedAgent = data.assigned_agent;
        }
      } catch {}
    }

    addMessage({
      type: 'handoff',
      sectorName: stateRef.current.sectorName || 'Telecom',
      subprocessName: stateRef.current.subprocessName || 'General',
      queryText: stateRef.current.queryText || 'N/A',
      refNum,
    });

    const agentBlock = assignedAgent
      ? `<br><br>We are connecting you to your dedicated support expert:<br>
           <div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;padding:10px 14px;margin:8px 0;display:inline-block;min-width:220px;">
             <div style="font-size:13px;font-weight:700;color:#1e40af;">${assignedAgent.name}</div>
             ${assignedAgent.phone ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.phone}</div>` : ''}
             ${assignedAgent.email ? `<div style="font-size:12px;color:#0ea5e9;margin-top:4px;">${assignedAgent.email}</div>` : ''}
             ${assignedAgent.employee_id ? `<div style="font-size:11px;color:#64748b;margin-top:2px;">ID: ${assignedAgent.employee_id}</div>` : ''}
             ${slaHours ? `<div style="font-size:12px;color:#16a34a;margin-top:6px;font-weight:600;">Your issue will be resolved within ${slaHours} hour${slaHours !== 1 ? 's' : ''}</div>` : ''}
           </div>`
      : `<br><br>A customer support executive will contact you shortly.`;

    setTimeout(() => {
      addMessage({
        type: 'bot',
        html: `Your request has been submitted. A support ticket has been raised successfully.` +
          `<br>Reference: <strong>${refNum}</strong>` +
          agentBlock +
          `<br><br>You can track your ticket status from the dashboard.<br><br>` +
          `What would you like to do next?`,
      });
      setTimeout(() => {
        const actionGroupId = nextId();
        addMessage({ type: 'post-feedback-actions', groupId: actionGroupId });
      }, 400);
    }, 1500);
    stateRef.current.step = 'human_handoff';
  }, [addMessage, disableGroup, saveMessage]);

  // ── Key handler ──
  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }, [sendMessage]);

  // ── Auto-resize textarea ──
  const handleInputChange = useCallback((e) => {
    setInputValue(e.target.value);
    e.target.style.height = 'auto';
    e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
  }, []);

  // ═══════════════════════════════════════════════════════════════════════
  //  RESUME CHAT — restore an active session
  // ═══════════════════════════════════════════════════════════════════════
  const resumeChat = useCallback(async (session, msgs) => {
    setInitPhase('chat');
    setMessages([]);
    setDisabledGroups(new Set());
    hideInput();

    sessionIdRef.current = session.id;

    // Restore stateRef from session metadata
    stateRef.current.sectorName = session.sector_name || null;
    stateRef.current.subprocessName = session.subprocess_name || null;
    stateRef.current.language = session.language || 'English';
    stateRef.current.queryText = session.query_text || '';
    stateRef.current.resolution = session.resolution || '';

    // Reverse-lookup sector key from menu
    try {
      const token = getToken();
      const headers = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const menuResp = await fetch(`${API_BASE}/api/menu`, { headers });
      const menuData = await menuResp.json();
      for (const [key, sector] of Object.entries(menuData.menu)) {
        if (sector.name === session.sector_name) {
          stateRef.current.sectorKey = key;
          break;
        }
      }

      // Reverse-lookup subprocess key
      if (stateRef.current.sectorKey && session.subprocess_name) {
        const spData = await chatApiCall('/api/subprocesses', {
          sector_key: stateRef.current.sectorKey,
          language: 'English',
        });
        for (const [key, name] of Object.entries(spData.subprocesses)) {
          if (name === session.subprocess_name) {
            stateRef.current.subprocessKey = key;
            break;
          }
        }
      }
    } catch {}

    // Show "resuming" system message
    const resumeMsg = { type: 'system', text: `Resuming your previous chat session #${session.id}` };
    setMessages([{ ...resumeMsg, id: nextId(), groupId: nextId() }]);

    // Render previous messages as history
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

    // Determine current step and show appropriate interactive UI
    setTimeout(async () => {
      if (!session.sector_name) {
        addMessage({ type: 'bot', html: 'Please select your <strong>telecom service category</strong>:' });
        loadSectorMenu();
      } else if (!session.subprocess_name) {
        addMessage({ type: 'bot', html: `Please select the <strong>type of issue</strong> you're facing with <strong>${session.sector_name}</strong>:` });
        const data = await chatApiCall('/api/subprocesses', {
          sector_key: stateRef.current.sectorKey,
          language: stateRef.current.language,
        });
        const spGroupId = nextId();
        addMessage({ type: 'subprocess-grid', subprocesses: limitSubprocesses(data.subprocesses), groupId: spGroupId });
        stateRef.current.step = 'subprocess';
      } else if (session.resolution) {
        addMessage({
          type: 'bot',
          html: `Did this solution resolve your issue? <em>(Attempt ${stateRef.current.attempt} of 5)</em>`,
        });
        const satGroupId = nextId();
        addMessage({ type: 'satisfaction', groupId: satGroupId });
        stateRef.current.step = 'feedback';
      } else {
        addMessage({
          type: 'bot',
          html: 'Please <strong>describe your specific issue</strong> so I can provide the best resolution.',
        });
        showInput('Describe your issue in any language...');
        stateRef.current.step = 'query';
      }
    }, 400);
  }, [addMessage, hideInput, loadSectorMenu, scrollToBottom, showInput]);

  // ═══════════════════════════════════════════════════════════════════════
  //  FEEDBACK GATE — submit feedback for a pending session
  // ═══════════════════════════════════════════════════════════════════════
  const handleFeedbackSubmit = useCallback(async () => {
    if (fbRating === 0) return;
    const session = pendingFeedback[currentFbIdx];
    if (!session) return;

    setFbSubmitting(true);
    await apiPost('/api/feedback', {
      chat_session_id: session.id,
      rating: fbRating,
      comment: fbComment,
    });
    setFbSubmitting(false);
    setFbRating(0);
    setFbComment('');

    // Move to next pending session or proceed
    if (currentFbIdx + 1 < pendingFeedback.length) {
      setCurrentFbIdx(prev => prev + 1);
    } else {
      // All feedback submitted — proceed to check active session
      proceedAfterFeedback();
    }
  }, [fbRating, fbComment, pendingFeedback, currentFbIdx]);

  const proceedAfterFeedback = useCallback(async () => {
    const resumeId = searchParams.get('resume');

    // Check for direct resume from URL param
    if (resumeId) {
      try {
        const data = await apiGet(`/api/chat/session/${resumeId}`);
        if (data?.session?.status === 'active') {
          setActiveSessionData(data.session);
          setActiveSessionMsgs(data.messages || []);
          setInitPhase('resume-prompt');
          return;
        }
      } catch {}
    }

    // Check for any active session
    const activeData = await apiGet('/api/customer/active-session');
    if (activeData?.session) {
      setActiveSessionData(activeData.session);
      setActiveSessionMsgs(activeData.messages || []);
      setInitPhase('resume-prompt');
      return;
    }

    // No active session → fresh chat
    startChat();
  }, [searchParams, startChat]);

  // ═══════════════════════════════════════════════════════════════════════
  //  INITIALIZATION
  // ═══════════════════════════════════════════════════════════════════════
  const initialized = useRef(false);
  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;

    (async () => {
      // 1. Check pending feedback
      try {
        const fbData = await apiGet('/api/customer/pending-feedback');
        if (fbData?.sessions?.length > 0) {
          setPendingFeedback(fbData.sessions);
          setCurrentFbIdx(0);
          setInitPhase('feedback-gate');
          return;
        }
      } catch {}

      // 2. Check for resume from URL or active session
      const resumeId = searchParams.get('resume');
      if (resumeId) {
        try {
          const data = await apiGet(`/api/chat/session/${resumeId}`);
          if (data?.session?.status === 'active') {
            setActiveSessionData(data.session);
            setActiveSessionMsgs(data.messages || []);
            setInitPhase('resume-prompt');
            return;
          }
        } catch {}
      }

      const activeData = await apiGet('/api/customer/active-session');
      if (activeData?.session) {
        setActiveSessionData(activeData.session);
        setActiveSessionMsgs(activeData.messages || []);
        setInitPhase('resume-prompt');
        return;
      }

      // 3. No pending feedback, no active session → start fresh
      startChat();
    })();
  }, []);

  // ═══════════════════════════════════════════════════════════════════════
  //  RENDER — messages
  // ═══════════════════════════════════════════════════════════════════════
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
              return (
                <button key={sk}
                  className={`subprocess-chip${isOthers ? ' others' : ''}${isDisabled ? ' disabled' : ''}`}
                  onClick={() => !isDisabled && selectSubprocess(sk, sname, msg.groupId)}>
                  <div className="chip-num">{isOthers ? '···' : idx + 1}</div>
                  <div className="chip-label">{sname}</div>
                  <div className="chip-arrow">›</div>
                </button>
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
        return (
          <div key={msg.id} className="satisfaction-container">
            <button className={`sat-btn yes${isDisabled ? ' disabled' : ''}`}
              onClick={() => !isDisabled && handleSatisfied(msg.groupId)}>Yes, Resolved</button>
            <button className={`sat-btn no${isDisabled ? ' disabled' : ''}`}
              onClick={() => !isDisabled && handleUnsatisfied(msg.groupId)}>No, Try Again</button>
            {(msg.attempt || 0) >= 2 && (
              <button className={`sat-btn ticket${isDisabled ? ' disabled' : ''}`}
                onClick={() => !isDisabled && handleRaiseTicket(msg.groupId)}>Raise a Ticket</button>
            )}
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
            <button className={`action-btn menu-btn${isDisabled ? ' disabled' : ''}`}
              onClick={() => !isDisabled && handleBackToMenu(msg.groupId)}>Main Menu</button>
            <button className={`action-btn exit-btn${isDisabled ? ' disabled' : ''}`}
              onClick={() => !isDisabled && handleExit(msg.groupId)}>Exit</button>
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
              <button key={i} className={`unsat-btn${isDisabled ? ' disabled' : ''}`}
                onClick={() => !isDisabled && opt.fn()}>
                <div className={`o-icon ${opt.cls}`} dangerouslySetInnerHTML={{ __html: opt.icon }} />
                <div className="o-info">
                  <div className="o-title">{opt.title}</div>
                  <div className="o-desc">{opt.desc}</div>
                </div>
                <div className="o-arrow">&rsaquo;</div>
              </button>
            ))}
          </div>
        );
      }

      case 'email-action':
        return (
          <div key={msg.id} className="email-action-container">
            <button
              className={`email-btn${isDisabled ? ' disabled' : ''}`}
              onClick={() => !isDisabled && handleSendEmail(msg.groupId)}
            >
              <span className="email-btn-icon"></span>
              Send Summary to My Email
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
            <div className="handoff-row"><span className="h-label">Status</span><span className="h-value status-pending">Pending Agent Assignment</span></div>
            <div className="handoff-ref">Reference No: {msg.refNum}</div>
          </div>
        );

      default:
        return null;
    }
  };

  // ═══════════════════════════════════════════════════════════════════════
  //  RENDER — Feedback Gate Screen
  // ═══════════════════════════════════════════════════════════════════════
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
            {pendingFeedback.length > 1 && (
              <span className="gate-counter"> ({currentFbIdx + 1} of {pendingFeedback.length})</span>
            )}
          </p>

          <div className="gate-session-info">
            <div className="gate-session-row">
              <span className="gate-label">Category</span>
              <span className="gate-value">{session.sector_name || 'N/A'}</span>
            </div>
            <div className="gate-session-row">
              <span className="gate-label">Issue</span>
              <span className="gate-value">{session.subprocess_name || 'N/A'}</span>
            </div>
            <div className="gate-session-row">
              <span className="gate-label">Status</span>
              <span className={`badge badge-${session.status}`}>{session.status}</span>
            </div>
            <div className="gate-session-row">
              <span className="gate-label">Date</span>
              <span className="gate-value">
                {session.created_at ? new Date(session.created_at).toLocaleDateString() : 'N/A'}
              </span>
            </div>
            {session.summary && (
              <div className="gate-summary">
                <span className="gate-label">Summary</span>
                <p>{session.summary}</p>
              </div>
            )}
          </div>

          <div className="gate-rating">
            <label>Rate your experience</label>
            <div className="gate-stars">
              {[1, 2, 3, 4, 5].map(n => (
                <button key={n} type="button"
                  className={`gate-star${n <= fbRating ? ' active' : ''}`}
                  onClick={() => setFbRating(n)}>
                  &#9733;
                </button>
              ))}
            </div>
            <span className="gate-rating-label">
              {fbRating === 0 ? 'Click a star to rate' : `${fbRating}/5`}
            </span>
          </div>

          <div className="gate-comment">
            <label>Comments (optional)</label>
            <textarea
              placeholder="Tell us about your experience..."
              value={fbComment}
              onChange={e => setFbComment(e.target.value)}
              rows={3}
            />
          </div>

          <button
            className="gate-submit-btn"
            disabled={fbRating === 0 || fbSubmitting}
            onClick={handleFeedbackSubmit}
          >
            {fbSubmitting ? 'Submitting...' : 'Submit Feedback'}
          </button>
        </div>
      </div>
    );
  };

  // ═══════════════════════════════════════════════════════════════════════
  //  RENDER — Resume Prompt Screen
  // ═══════════════════════════════════════════════════════════════════════
  const renderResumePrompt = () => {
    const session = activeSessionData;
    if (!session) return null;

    const lastMsg = activeSessionMsgs.length > 0
      ? activeSessionMsgs[activeSessionMsgs.length - 1]
      : null;

    return (
      <div className="gate-overlay">
        <div className="gate-card resume-gate">
          <div className="gate-icon">&#128172;</div>
          <h2 className="gate-title">Active Chat Found</h2>
          <p className="gate-subtitle">
            You have an active chat session. Would you like to continue or start a new one?
          </p>

          <div className="gate-session-info">
            <div className="gate-session-row">
              <span className="gate-label">Session</span>
              <span className="gate-value">#{session.id}</span>
            </div>
            {session.sector_name && (
              <div className="gate-session-row">
                <span className="gate-label">Category</span>
                <span className="gate-value">{session.sector_name}</span>
              </div>
            )}
            {session.subprocess_name && (
              <div className="gate-session-row">
                <span className="gate-label">Issue Type</span>
                <span className="gate-value">{session.subprocess_name}</span>
              </div>
            )}
            <div className="gate-session-row">
              <span className="gate-label">Started</span>
              <span className="gate-value">
                {session.created_at ? new Date(session.created_at).toLocaleString() : 'N/A'}
              </span>
            </div>
            {lastMsg && (
              <div className="gate-summary">
                <span className="gate-label">Last message</span>
                <p>{lastMsg.content.length > 120 ? lastMsg.content.slice(0, 120) + '...' : lastMsg.content}</p>
              </div>
            )}
          </div>

          <div className="gate-actions">
            <button
              className="gate-btn gate-btn-primary"
              onClick={() => resumeChat(activeSessionData, activeSessionMsgs)}
            >
              Continue Chat
            </button>
            <button
              className="gate-btn gate-btn-secondary"
              onClick={() => startChat()}
            >
              Start New Chat
            </button>
          </div>
        </div>
      </div>
    );
  };

  // ═══════════════════════════════════════════════════════════════════════
  //  MAIN RENDER
  // ═══════════════════════════════════════════════════════════════════════
  return (
    <div className="chat-support-page">
      <div className="app-container">
        <div className="header">
          <div className="header-icon" style={{ fontSize: 20, fontWeight: 800, color: '#00338D' }}>TeleBot</div>
          <div className="header-info">
            <h1>Customer Handling</h1>
            <p>AI-powered multilingual support</p>
          </div>
          <div className="status-dot" />
          {initPhase === 'chat' && (
            <button className="restart-btn" onClick={startChat}>Restart</button>
          )}
        </div>

        {initPhase === 'loading' && (
          <div className="chat-area" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div className="typing-indicator visible">
              <div className="typing-dot" /><div className="typing-dot" /><div className="typing-dot" />
            </div>
          </div>
        )}

        {initPhase === 'feedback-gate' && (
          <div className="chat-area">
            {renderFeedbackGate()}
          </div>
        )}

        {initPhase === 'resume-prompt' && (
          <div className="chat-area">
            {renderResumePrompt()}
          </div>
        )}

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

            {inputVisible && (
              <div className="input-area">
                <div className="input-row">
                  <textarea ref={inputRef} value={inputValue} onChange={handleInputChange}
                    onKeyDown={handleKeyDown} placeholder={inputPlaceholder} rows={1} />
                  <button className="send-btn" onClick={sendMessage} disabled={!inputValue.trim()}>Send</button>
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
