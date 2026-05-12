import { useState, useEffect, useCallback } from 'react';
import { apiGet, apiPost } from '../api';
import { useAuth } from '../AuthContext';

/**
 * Shared panel that lists Core Parameter Change Requests created from Core
 * Tickets (MME / SGW / PGW / HSS / PCRF). Behaviour adapts per role:
 *   - human_agent: read-only view of their own requests
 *   - manager / cto / admin: can approve / disapprove
 */
const STATUS_COLOR = {
  pending:       { bg: '#fef3c7', color: '#d97706', border: '#fde68a', label: 'Pending Manager' },
  pending_cto:   { bg: '#fff7ed', color: '#ea580c', border: '#fed7aa', label: 'Pending CTO' },
  approved:      { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0', label: 'Approved' },
  cto_approved:  { bg: '#f0fdf4', color: '#16a34a', border: '#bbf7d0', label: 'CTO Approved' },
  disapproved:   { bg: '#fef2f2', color: '#dc2626', border: '#fecaca', label: 'Manager Rejected' },
  cto_rejected:  { bg: '#fef2f2', color: '#dc2626', border: '#fecaca', label: 'CTO Rejected' },
};
const TYPE_COLOR = {
  standard:  { bg: '#eff6ff', color: '#1d4ed8' },
  normal:    { bg: '#f5f3ff', color: '#6d28d9' },
  urgent:    { bg: '#fff7ed', color: '#ea580c' },
  emergency: { bg: '#fef2f2', color: '#dc2626' },
};

export default function CoreParameterChangePanel() {
  const { user } = useAuth();
  const [list, setList] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all');
  const [busyId, setBusyId] = useState(null);
  const [noteEdit, setNoteEdit] = useState({});  // { [id]: 'note' }

  const fetchAll = useCallback(async () => {
    setLoading(true);
    try {
      const d = await apiGet('/api/core/parameter-change');
      setList(d.requests || []);
    } catch (e) { /* ignore */ }
    setLoading(false);
  }, []);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  const decide = async (id, decision) => {
    setBusyId(id);
    try {
      await apiPost(`/api/core/parameter-change/${id}/decision`, {
        decision, note: noteEdit[id] || '',
      });
      await fetchAll();
    } catch (e) { alert(e.message); }
    setBusyId(null);
  };

  const visible = filter === 'all' ? list : list.filter(r => r.status === filter);
  // Manager can act on 'pending'; CTO acts on 'pending_cto'; admin can act on either.
  const canManagerAct = (r) => r.status === 'pending' && (user?.role === 'manager' || user?.role === 'admin');
  const canCtoAct = (r) => r.status === 'pending_cto' && (user?.role === 'cto' || user?.role === 'admin');

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
        <div>
          <h3 style={{ margin: 0, fontSize: 16, fontWeight: 800, color: '#0f172a', display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ width: 4, height: 22, background: '#7c3aed', borderRadius: 2 }} />
            Core Change Parameter Requests
          </h3>
          <p style={{ margin: '4px 0 0', fontSize: 12, color: '#64748b' }}>
            Parameter modifications proposed against MME / SGW / PGW / HSS / PCRF Core Tickets.
          </p>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          {['all', 'pending', 'approved', 'disapproved'].map(f => (
            <button key={f} onClick={() => setFilter(f)} style={{
              padding: '5px 12px', fontSize: 11, fontWeight: 700, border: 'none', cursor: 'pointer', borderRadius: 6,
              background: filter === f ? '#7c3aed' : '#f1f5f9',
              color: filter === f ? '#fff' : '#475569',
              textTransform: 'capitalize',
            }}>{f}</button>
          ))}
        </div>
      </div>

      {loading ? (
        <div style={{ padding: 50, textAlign: 'center', color: '#94a3b8' }}>Loading…</div>
      ) : visible.length === 0 ? (
        <div style={{ padding: 50, textAlign: 'center', color: '#94a3b8', fontSize: 13, background: '#f8fafc', borderRadius: 8 }}>
          No Core Change Parameter Requests.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {visible.map(r => {
            const sc = STATUS_COLOR[r.status] || STATUS_COLOR.pending;
            const tc = TYPE_COLOR[r.change_type] || TYPE_COLOR.standard;
            const showActions = canManagerAct(r) || canCtoAct(r);
            return (
              <div key={r.id} style={{
                background: '#fff', border: `1px solid ${sc.border}`, borderLeft: `4px solid ${sc.color}`,
                borderRadius: 8, padding: 14,
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginBottom: 8 }}>
                  <span style={{ padding: '3px 10px', borderRadius: 12, fontSize: 11, fontWeight: 800, background: sc.bg, color: sc.color, border: `1px solid ${sc.border}` }}>{sc.label}</span>
                  <span style={{ padding: '2px 8px', borderRadius: 10, fontSize: 10, fontWeight: 800, background: tc.bg, color: tc.color }}>
                    {(r.change_type || 'standard').toUpperCase()}
                  </span>
                  {r.cto_required && <span style={{ padding: '2px 8px', borderRadius: 10, fontSize: 10, fontWeight: 800, background: '#fef2f2', color: '#dc2626', border: '1px solid #fecaca' }}>CTO REQUIRED</span>}
                  <span style={{ padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 700, background: '#7c3aed18', color: '#7c3aed' }}>{r.component_type}</span>
                  <span style={{ fontSize: 13, fontWeight: 800, color: '#0f172a' }}>{r.component_id}</span>
                  <span style={{ fontSize: 12, color: '#475569' }}>KPI: <b>{r.ticket_kpi}</b></span>
                  <span style={{ fontFamily: 'monospace', fontSize: 11, color: '#00338D' }}>{r.ticket_reference}</span>
                  <span style={{ marginLeft: 'auto', fontSize: 10, color: '#64748b' }}>By {r.agent_name} · {r.created_at ? new Date(r.created_at).toLocaleString() : '—'}</span>
                </div>
                {/* Routing line — manager and (if applicable) CTO */}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, marginBottom: 8, fontSize: 11, color: '#475569' }}>
                  {r.manager_name && (
                    <span>Manager: <b style={{ color: '#0f172a' }}>{r.manager_name}</b> <span style={{ color: '#94a3b8' }}>({r.manager_email})</span></span>
                  )}
                  {r.cto_required && r.cto_name && (
                    <span>CTO: <b style={{ color: '#0f172a' }}>{r.cto_name}</b> <span style={{ color: '#94a3b8' }}>({r.cto_email})</span></span>
                  )}
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12, fontSize: 12 }}>
                  <Cell label="Group" v={r.parameter_group} />
                  <Cell label="Parameter" v={r.parameter_name} bold />
                  <Cell label="Unit" v={r.unit} />
                  <Cell label="Current Value" v={r.current_value} />
                  <Cell label="Proposed Value" v={r.proposed_value} highlight />
                  <Cell label="Reason" v={r.reason} />
                </div>
                {(r.manager_note || r.cto_note) && (
                  <div style={{ marginTop: 10, padding: 8, background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 6, fontSize: 12, color: '#475569' }}>
                    {r.manager_note && <div><b>Manager:</b> {r.manager_note}</div>}
                    {r.cto_note && <div><b>CTO:</b> {r.cto_note}</div>}
                  </div>
                )}
                {showActions && (
                  <div style={{ marginTop: 10, display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                    <input placeholder="Optional note…" value={noteEdit[r.id] || ''}
                      onChange={e => setNoteEdit(n => ({ ...n, [r.id]: e.target.value }))}
                      style={{ flex: 1, minWidth: 220, padding: '6px 8px', fontSize: 12, border: '1px solid #cbd5e1', borderRadius: 6 }} />
                    <button onClick={() => decide(r.id, 'approve')} disabled={busyId === r.id}
                      style={{ padding: '6px 14px', fontSize: 12, fontWeight: 700, background: '#16a34a', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
                      {canCtoAct(r) ? 'CTO Approve' : 'Approve'}
                    </button>
                    <button onClick={() => decide(r.id, 'disapprove')} disabled={busyId === r.id}
                      style={{ padding: '6px 14px', fontSize: 12, fontWeight: 700, background: '#dc2626', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer' }}>
                      {canCtoAct(r) ? 'CTO Reject' : 'Disapprove'}
                    </button>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function Cell({ label, v, bold, highlight }) {
  return (
    <div>
      <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.5px' }}>{label}</div>
      <div style={{ fontSize: 13, color: highlight ? '#16a34a' : '#0f172a', fontWeight: highlight || bold ? 800 : 600, marginTop: 2 }}>{v || '—'}</div>
    </div>
  );
}
