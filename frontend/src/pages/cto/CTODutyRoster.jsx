import { useState, useEffect, useCallback } from 'react';
import { apiGet } from '../../api';
import { useTheme } from '../../ThemeContext';

export default function CTODutyRoster() {
  const { isDark } = useTheme();
  const [selectedDate, setSelectedDate] = useState(() => new Date());
  const [calendarMonth, setCalendarMonth] = useState(() => new Date());
  const [rosterData, setRosterData] = useState(null);
  const [rosterLoading, setRosterLoading] = useState(false);
  const [rosterError, setRosterError] = useState('');

  const initials = (name) => {
    if (!name) return '?';
    const parts = name.split(' ').filter(Boolean);
    const letters = parts.slice(0, 2).map(p => p[0].toUpperCase()).join('');
    return letters || '?';
  };

  const contactLine = (phone, email) => {
    const phoneText = phone ? phone : 'Phone not set';
    const emailText = email ? email : 'Email not set';
    return `${phoneText} | ${emailText}`;
  };

  const offDaysText = (days) => {
    if (!days || days.length === 0) return '';
    return `Off days: ${days.join(', ')}`;
  };

  const roleLabel = (role) => {
    if (role === 'manager') return 'Manager';
    if (role === 'human_agent') return 'Human Agent';
    return (role || '').replace('_', ' ');
  };

  const StatusPill = ({ isOnline }) => (
    <span style={{
      display: 'inline-flex',
      alignItems: 'center',
      gap: 6,
      fontSize: 9,
      fontWeight: 800,
      color: isOnline ? '#166534' : '#991b1b',
      background: isOnline ? '#dcfce7' : '#fee2e2',
      padding: '2px 6px',
      borderRadius: 999,
      border: `1px solid ${isOnline ? '#bbf7d0' : '#fecaca'}`,
      whiteSpace: 'nowrap',
      flexShrink: 0,
      minWidth: 52,
      justifyContent: 'center'
    }}>
      <span style={{
        width: 6,
        height: 6,
        borderRadius: '50%',
        background: isOnline ? '#22c55e' : '#ef4444',
        boxShadow: isOnline ? '0 0 0 3px rgba(34,197,94,0.2)' : '0 0 0 3px rgba(239,68,68,0.2)',
      }} />
      {isOnline ? 'Online' : 'Offline'}
    </span>
  );

  const shiftMembers = (shiftEntry) => {
    const members = [];
    if (shiftEntry?.team?.manager) members.push(shiftEntry.team.manager);
    if (shiftEntry?.team?.agents) {
      shiftEntry.team.agents.forEach(a => members.push(a));
    }
    return members;
  };

  const toISODate = (d) => {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  };

  const fetchRoster = useCallback(async (dateObj) => {
    setRosterLoading(true);
    setRosterError('');
    try {
      const resp = await apiGet(`/api/cto/duty-roster?date=${toISODate(dateObj)}`);
      if (resp?.error) {
        setRosterError(resp.error);
        setRosterData(null);
      } else {
        setRosterData(resp);
      }
    } catch (err) {
      setRosterError(err.message || 'Failed to load duty roster');
      setRosterData(null);
    } finally {
      setRosterLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRoster(selectedDate);
  }, [selectedDate, fetchRoster]);

  const today = new Date();
  const monthStart = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth(), 1);
  const monthEnd = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth() + 1, 0);
  const startWeekday = monthStart.getDay();
  const daysInMonth = monthEnd.getDate();
  const weeks = [];
  let dayCounter = 1 - startWeekday;
  for (let w = 0; w < 6; w += 1) {
    const week = [];
    for (let i = 0; i < 7; i += 1) {
      const d = new Date(calendarMonth.getFullYear(), calendarMonth.getMonth(), dayCounter);
      const inMonth = d.getMonth() === calendarMonth.getMonth() && d.getDate() >= 1 && d.getDate() <= daysInMonth;
      week.push({ date: d, inMonth });
      dayCounter += 1;
    }
    weeks.push(week);
  }

  return (
    <div>
      <div className="page-header">
        <h1>Duty Roster</h1>
        <p>Automated shift plan using all admin-registered resources.</p>
      </div>

      <div className="section-card">
        <div className="section-card-body" style={{ display: 'grid', gridTemplateColumns: '3fr 2fr', gap: 18 }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
              <div>
                <div style={{ fontSize: 14, fontWeight: 700 }}>Selected Date</div>
                <div style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#64748b' }}>
                  {selectedDate.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}
                </div>
              </div>
              <div style={{ fontSize: 12, color: isDark ? '#e2e8f0' : '#0f172a', fontWeight: 700, background: isDark ? '#334155' : '#e2e8f0', padding: '4px 10px', borderRadius: 999 }}>
                24x7 Coverage
              </div>
            </div>

            {rosterLoading && (
              <div style={{ padding: 16, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, borderRadius: 10, background: isDark ? '#152238' : '#f8fafc', color: isDark ? '#94a3b8' : undefined }}>
                Loading roster...
              </div>
            )}
            {rosterError && (
              <div style={{ padding: 16, border: '1px solid #fecaca', borderRadius: 10, background: '#fef2f2', color: '#991b1b' }}>
                {rosterError}
              </div>
            )}
            {!rosterLoading && !rosterError && rosterData?.meta && (
              <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 12 }}>
                <div style={{ background: isDark ? '#152238' : '#f1f5f9', padding: '6px 10px', borderRadius: 8, fontSize: 12, fontWeight: 700, color: isDark ? '#e2e8f0' : undefined }}>
                  Total Resources: {rosterData.meta.total_resources}
                </div>
                <div style={{ background: isDark ? '#152238' : '#f1f5f9', padding: '6px 10px', borderRadius: 8, fontSize: 12, fontWeight: 700, color: isDark ? '#e2e8f0' : undefined }}>
                  Team Size: {rosterData.meta.team_size}
                </div>
                <div style={{ background: isDark ? '#152238' : '#f1f5f9', padding: '6px 10px', borderRadius: 8, fontSize: 12, fontWeight: 700, color: isDark ? '#e2e8f0' : undefined }}>
                  Managers: {rosterData.meta.managers}
                </div>
                <div style={{ background: isDark ? '#152238' : '#f1f5f9', padding: '6px 10px', borderRadius: 8, fontSize: 12, fontWeight: 700, color: isDark ? '#e2e8f0' : undefined }}>
                  Agents: {rosterData.meta.agents}
                </div>
                <div style={{ background: '#fff7ed', color: '#9a3412', padding: '6px 10px', borderRadius: 8, fontSize: 12, fontWeight: 700 }}>
                  Off Today: {rosterData.meta.off_today_count}
                </div>
              </div>
            )}

            {!rosterLoading && !rosterError && rosterData?.shifts?.length > 0 && (() => {
              const offToday = rosterData.meta?.off_today || [];
              const shiftLists = rosterData.shifts.map(shiftEntry => shiftMembers(shiftEntry));
              const maxRows = Math.max(offToday.length, ...shiftLists.map(s => s.length), 1);
              return (
                <div style={{
                  border: `1px solid ${isDark ? '#334155' : '#d7dee8'}`,
                  borderRadius: 16,
                  overflow: 'hidden',
                  background: isDark ? '#1e293b' : '#ffffff',
                  boxShadow: isDark ? '0 10px 24px rgba(0, 0, 0, 0.2)' : '0 10px 24px rgba(15, 23, 42, 0.08)'
                }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' }}>
                    <thead>
                      <tr style={{ background: isDark ? 'linear-gradient(90deg, #152238 0%, #1e293b 100%)' : 'linear-gradient(90deg, #f8fafc 0%, #eef2ff 100%)' }}>
                        <th style={{ width: '25%', textAlign: 'left', padding: '14px 16px', fontSize: 12, fontWeight: 800, color: isDark ? '#e2e8f0' : '#0f172a', borderRight: `1px solid ${isDark ? '#334155' : '#d7dee8'}`, borderBottom: `1px solid ${isDark ? '#334155' : '#d7dee8'}` }}>
                          Off Today
                        </th>
                        {[0, 1, 2].map(i => (
                          <th key={i} style={{ width: '25%', textAlign: 'left', padding: '12px 16px', borderRight: i < 2 ? `1px solid ${isDark ? '#334155' : '#d7dee8'}` : 'none', borderBottom: `1px solid ${isDark ? '#334155' : '#d7dee8'}` }}>
                            <div style={{ fontSize: 12, fontWeight: 800, color: isDark ? '#e2e8f0' : '#0f172a' }}>
                              {rosterData.shift_times?.[i]?.name || `Shift ${i + 1}`}
                            </div>
                            <div style={{ fontSize: 11, fontWeight: 700, color: isDark ? '#94a3b8' : '#475569' }}>
                              {rosterData.shift_times?.[i]?.time || ''}
                            </div>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {Array.from({ length: maxRows }).map((_, rowIdx) => (
                        <tr key={rowIdx}>
                          <td style={{ width: '25%', verticalAlign: 'top', padding: 8, borderRight: `1px solid ${isDark ? '#334155' : '#d7dee8'}`, borderBottom: rowIdx < maxRows - 1 ? `1px solid ${isDark ? '#1e293b' : '#eef2f7'}` : 'none', background: isDark ? '#1a1708' : '#fff7ed' }}>
                            {offToday[rowIdx] ? (
                              <div style={{ display: 'grid', gridTemplateColumns: '28px 1fr', gridTemplateRows: 'auto auto', alignItems: 'center', columnGap: 8, rowGap: 2, background: isDark ? '#1e293b' : '#fff', padding: '8px 10px', borderRadius: 12, border: '1px solid #fed7aa', minHeight: 80 }}>
                                <div style={{ gridRow: '1 / span 2', width: 28, height: 28, borderRadius: '50%', background: '#fed7aa', color: '#7c2d12', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 800 }}>
                                  {initials(offToday[rowIdx].name)}
                                </div>
                                <div style={{ minWidth: 0 }}>
                                  <div style={{ fontSize: 12, fontWeight: 700, color: '#7c2d12', lineHeight: '16px', wordBreak: 'break-word' }}>
                                    {offToday[rowIdx].name}
                                  </div>
                                  <div style={{ fontSize: 10, color: '#a16207', lineHeight: '14px' }}>
                                    {roleLabel(offToday[rowIdx].role)}
                                  </div>
                                  <div style={{ marginTop: 4 }}>
                                    <StatusPill isOnline={offToday[rowIdx].is_online} />
                                  </div>
                                </div>
                              </div>
                            ) : (
                              <div style={{ minHeight: 80 }} />
                            )}
                          </td>

                          {shiftLists.map((list, idx) => (
                            <td key={idx} style={{ width: '25%', verticalAlign: 'top', padding: 8, borderRight: idx < 2 ? `1px solid ${isDark ? '#334155' : '#d7dee8'}` : 'none', borderBottom: rowIdx < maxRows - 1 ? `1px solid ${isDark ? '#1e293b' : '#eef2f7'}` : 'none', background: isDark ? '#1e293b' : '#ffffff' }}>
                              {list[rowIdx] ? (
                                <div style={{ display: 'grid', gridTemplateColumns: '28px 1fr', gridTemplateRows: 'auto auto', alignItems: 'center', columnGap: 8, rowGap: 2, background: isDark ? '#152238' : '#f8fafc', padding: '8px 10px', borderRadius: 12, border: `1px solid ${isDark ? '#334155' : '#e2e8f0'}`, minHeight: 80 }}>
                                  <div style={{ gridRow: '1 / span 2', width: 28, height: 28, borderRadius: '50%', background: isDark ? '#334155' : '#e2e8f0', color: isDark ? '#e2e8f0' : '#0f172a', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 800 }}>
                                    {initials(list[rowIdx].name)}
                                  </div>
                                  <div style={{ minWidth: 0 }}>
                                    <div style={{ fontSize: 12, fontWeight: 700, color: isDark ? '#e2e8f0' : '#0f172a', lineHeight: '16px', wordBreak: 'break-word' }}>
                                      {list[rowIdx].name}
                                    </div>
                                    <div style={{ fontSize: 10, color: isDark ? '#94a3b8' : '#64748b', lineHeight: '14px' }}>
                                      {roleLabel(list[rowIdx].role)}
                                    </div>
                                    <div style={{ marginTop: 4 }}>
                                      <StatusPill isOnline={list[rowIdx].is_online} />
                                    </div>
                                  </div>
                                </div>
                              ) : (
                                <div style={{ minHeight: 80 }} />
                              )}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
          </div>

          <div style={{ border: `1px solid ${isDark ? '#334155' : '#d7dee8'}`, borderRadius: 16, padding: 18, background: isDark ? '#1e293b' : '#ffffff', boxShadow: isDark ? '0 10px 24px rgba(0, 0, 0, 0.2)' : '0 10px 24px rgba(15, 23, 42, 0.08)' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16, gap: 14 }}>
              <button
                className="btn btn-outline btn-sm"
                onClick={() => setCalendarMonth(m => new Date(m.getFullYear(), m.getMonth() - 1, 1))}
                style={{ minWidth: 72 }}
              >
                Prev
              </button>
              <div style={{ fontSize: 15, fontWeight: 800, textAlign: 'center', flex: 1 }}>
                {calendarMonth.toLocaleDateString('en-US', { month: 'long', year: 'numeric' })}
              </div>
              <button
                className="btn btn-outline btn-sm"
                onClick={() => setCalendarMonth(m => new Date(m.getFullYear(), m.getMonth() + 1, 1))}
                style={{ minWidth: 72 }}
              >
                Next
              </button>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 10, marginBottom: 10 }}>
              {['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].map(d => (
                <div key={d} style={{ textAlign: 'center', fontSize: 12, fontWeight: 700, color: isDark ? '#94a3b8' : '#64748b' }}>{d}</div>
              ))}
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 10 }}>
              {weeks.map((week, wi) => (
                week.map((cell, ci) => {
                  const isToday = cell.date.toDateString() === today.toDateString();
                  const isSelected = cell.date.toDateString() === selectedDate.toDateString();
                  const baseColor = cell.inMonth ? (isDark ? '#1e293b' : '#ffffff') : (isDark ? '#0f172a' : '#e2e8f0');
                  return (
                    <button
                      key={`${wi}-${ci}`}
                      onClick={() => cell.inMonth && setSelectedDate(new Date(cell.date))}
                      style={{
                        padding: '12px 0',
                        borderRadius: 10,
                        border: isSelected ? `2px solid ${isDark ? '#4da3ff' : '#00338D'}` : `1px solid ${isDark ? '#334155' : '#d7dee8'}`,
                        background: isSelected ? (isDark ? '#1e3a5f' : '#e0e7ff') : baseColor,
                        color: cell.inMonth ? (isDark ? '#e2e8f0' : '#0f172a') : (isDark ? '#475569' : '#94a3b8'),
                        fontWeight: isToday ? 800 : 600,
                        fontSize: 14,
                        cursor: cell.inMonth ? 'pointer' : 'default'
                      }}
                    >
                      {cell.date.getDate()}
                    </button>
                  );
                })
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}  
