import { useState, useEffect } from 'react';
import type { SidecarNodeInfo } from '../types';
import { sidecarCreate, sidecarStop } from '../api';

const STATE_COLOR: Record<string, string> = {
  leader:    'var(--leader)',
  follower:  'var(--follower)',
  candidate: 'var(--candidate)',
};

const STATE_DOT: Record<string, string> = {
  leader:    '#3fb950',
  follower:  '#58a6ff',
  candidate: '#d29922',
};

// Preset node IDs and their default ports (from docker-compose).
const PRESET: Record<string, { httpPort: number; rpcPort: number }> = {
  node1: { httpPort: 8081, rpcPort: 7001 },
  node2: { httpPort: 8082, rpcPort: 7002 },
  node3: { httpPort: 8083, rpcPort: 7003 },
  node4: { httpPort: 8084, rpcPort: 7004 },
  node5: { httpPort: 8085, rpcPort: 7005 },
};

function StateDot({ state }: { state: string }) {
  const color = STATE_DOT[state] ?? '#7d8590';
  return (
    <span style={{
      display: 'inline-block', width: 7, height: 7,
      borderRadius: '50%', background: color,
      boxShadow: state === 'leader' ? `0 0 6px ${color}` : undefined,
      flexShrink: 0,
    }} />
  );
}

function RunningRow({ node, onStop }: {
  node: SidecarNodeInfo;
  onStop: (id: string) => Promise<void>;
}) {
  const [busy, setBusy] = useState(false);

  async function handle() {
    setBusy(true);
    try { await onStop(node.id); } finally { setBusy(false); }
  }

  const stateColor  = STATE_COLOR[node.raft_state] ?? 'var(--muted)';
  const isLeader    = node.raft_state === 'leader';
  const isStarting  = node.raft_state === '';
  const isJoining   = !node.in_cluster && !isStarting;

  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      background: 'var(--bg)',
      border: `1px solid ${isLeader ? 'var(--leader)' : 'var(--border)'}`,
      borderRadius: 8, padding: '8px 12px',
      gap: 8,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
        {isStarting
          ? <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'var(--muted)', display: 'inline-block', flexShrink: 0 }} />
          : <StateDot state={node.raft_state} />
        }
        <span style={{ fontWeight: 600, fontSize: 12, fontFamily: 'monospace', flexShrink: 0 }}>
          {node.id}
        </span>
        {node.dynamic && (
          <span style={{
            fontSize: 9, padding: '1px 5px', borderRadius: 3, flexShrink: 0,
            background: 'var(--border)', color: 'var(--muted)', fontWeight: 600,
            textTransform: 'uppercase', letterSpacing: '.4px',
          }}>
            custom
          </span>
        )}
        {isStarting ? (
          <span style={{ fontSize: 11, color: 'var(--muted)' }}>starting…</span>
        ) : (
          <span style={{ fontSize: 11, color: stateColor }}>{node.raft_state}</span>
        )}
        {node.term > 0 && (
          <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'monospace' }}>
            t{node.term}
          </span>
        )}
        {isJoining && (
          <span style={{
            fontSize: 9, color: 'var(--candidate)',
            background: 'rgba(210,153,34,.12)', border: '1px solid rgba(210,153,34,.25)',
            padding: '1px 6px', borderRadius: 4, flexShrink: 0,
          }}>
            joining
          </span>
        )}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
        <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'monospace' }}>
          :{node.http_port}
        </span>
        <button className="btn btn-danger"
          style={{ padding: '3px 10px', fontSize: 11 }}
          disabled={busy} onClick={handle}>
          {busy ? '…' : 'Stop'}
        </button>
      </div>
    </div>
  );
}

function AddNodeForm({ onCreated }: { onCreated: () => void }) {
  const [id, setId]             = useState('');
  const [httpPort, setHttpPort] = useState('');
  const [rpcPort, setRpcPort]   = useState('');
  const [busy, setBusy]         = useState(false);
  const [error, setError]       = useState('');
  const [open, setOpen]         = useState(false);

  // Auto-fill ports for preset IDs; clear when switching to a custom ID.
  useEffect(() => {
    const preset = PRESET[id.trim()];
    setHttpPort(preset ? String(preset.httpPort) : '');
    setRpcPort(preset  ? String(preset.rpcPort)  : '');
  }, [id]);

  const isPreset = !!PRESET[id.trim()];

  async function submit() {
    const nodeId = id.trim();
    if (!nodeId) { setError('Node ID is required'); return; }

    const http = parseInt(httpPort);
    const rpc  = parseInt(rpcPort) || http + 100;

    if (!isPreset && (!http || http < 1024 || http > 65535)) {
      setError('HTTP port must be 1024 – 65535');
      return;
    }

    setBusy(true);
    setError('');
    try {
      await sidecarCreate(nodeId, isPreset ? undefined : http, isPreset ? undefined : rpc);
      setId(''); setHttpPort(''); setRpcPort('');
      setOpen(false);
      onCreated();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ marginTop: 10 }}>
      <button className="btn btn-ghost"
        style={{ width: '100%', justifyContent: 'center', gap: 6, fontSize: 12 }}
        onClick={() => { setOpen(o => !o); setError(''); }}>
        <span style={{ fontSize: 15, lineHeight: 1, fontWeight: 300 }}>{open ? '−' : '+'}</span>
        Add Node
      </button>

      {open && (
        <div style={{
          marginTop: 8, padding: '14px 14px 12px',
          background: 'var(--bg)', border: '1px solid var(--border)',
          borderRadius: 8, display: 'flex', flexDirection: 'column', gap: 10,
        }}>
          <div>
            <label style={{
              fontSize: 10, color: 'var(--muted)', textTransform: 'uppercase',
              letterSpacing: .6, display: 'block', marginBottom: 5, fontWeight: 500,
            }}>
              Node ID
            </label>
            <input type="text" value={id}
              placeholder="node1 – node5, or a custom ID"
              onChange={e => setId(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && submit()}
              style={{ width: '100%' }}
            />
            {isPreset && (
              <div style={{ fontSize: 10, color: 'var(--follower)', marginTop: 4 }}>
                Preset — ports auto-filled from docker-compose
              </div>
            )}
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
            <div>
              <label style={{
                fontSize: 10, color: 'var(--muted)', textTransform: 'uppercase',
                letterSpacing: .6, display: 'block', marginBottom: 5, fontWeight: 500,
              }}>
                HTTP Port{!isPreset && <span style={{ color: 'var(--offline)', marginLeft: 3 }}>*</span>}
              </label>
              <input type="text" value={httpPort}
                placeholder={isPreset ? '(preset)' : '8086'}
                readOnly={isPreset}
                onChange={e => setHttpPort(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && submit()}
                style={{ width: '100%' }}
              />
            </div>

            <div>
              <label style={{
                fontSize: 10, color: 'var(--muted)', textTransform: 'uppercase',
                letterSpacing: .6, display: 'block', marginBottom: 5, fontWeight: 500,
              }}>
                RPC Port
                <span style={{ fontSize: 9, fontWeight: 400, marginLeft: 4, textTransform: 'none', letterSpacing: 0 }}>
                  {isPreset ? '' : '(opt)'}
                </span>
              </label>
              <input type="text" value={rpcPort}
                placeholder={isPreset ? '(preset)' : httpPort ? String(parseInt(httpPort) + 100) : '7006'}
                readOnly={isPreset}
                onChange={e => setRpcPort(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && submit()}
                style={{ width: '100%' }}
              />
            </div>
          </div>

          {error && (
            <div style={{ fontSize: 11, color: 'var(--offline)', lineHeight: 1.4 }}>{error}</div>
          )}

          <button className="btn btn-primary"
            style={{ width: '100%', justifyContent: 'center' }}
            disabled={busy || !id.trim()}
            onClick={submit}>
            {busy ? 'Starting…' : 'Start Node'}
          </button>

          {!isPreset && (
            <div style={{ fontSize: 10, color: 'var(--muted)', lineHeight: 1.6 }}>
              Custom nodes run via <code style={{ fontFamily: 'monospace' }}>docker run</code> on the
              compose network. At least one bootstrap node must be running first.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

interface Props {
  sidecarNodes: SidecarNodeInfo[];
  onRefresh: () => void;
}

export default function MembershipPanel({ sidecarNodes, onRefresh }: Props) {
  const [flash, setFlash] = useState<{ ok: boolean; msg: string } | null>(null);

  const sidecarOk = sidecarNodes.length > 0;

  function showFlash(ok: boolean, msg: string) {
    setFlash({ ok, msg });
    setTimeout(() => setFlash(null), 4000);
  }

  async function handleStop(nodeId: string) {
    try {
      await sidecarStop(nodeId);
      onRefresh();
      showFlash(true, `${nodeId} stopped`);
    } catch (e: unknown) {
      showFlash(false, e instanceof Error ? e.message : String(e));
    }
  }

  const running  = sidecarNodes.filter(n => n.running);
  const quorumOk = running.filter(n => n.in_cluster).length >= 2;

  if (!sidecarOk) {
    return (
      <div className="panel">
        <div className="section-title">Cluster Control</div>
        <div style={{ color: 'var(--muted)', fontSize: 12, marginTop: 6, marginBottom: 10 }}>
          Sidecar not running — start it to manage nodes:
        </div>
        <div style={{
          padding: '8px 12px', background: 'var(--bg)', borderRadius: 6,
          fontFamily: 'monospace', fontSize: 12, border: '1px solid var(--border)',
          color: 'var(--text)',
        }}>
          go run ./cmd/sidecar
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="section-title" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        Cluster Control
        {running.length > 0 && (
          <span style={{
            fontSize: 9, fontWeight: 500, letterSpacing: .4,
            textTransform: 'none',
            color: quorumOk ? 'var(--leader)' : 'var(--candidate)',
            background: quorumOk ? 'rgba(63,185,80,.1)' : 'rgba(210,153,34,.1)',
            border: `1px solid ${quorumOk ? 'rgba(63,185,80,.25)' : 'rgba(210,153,34,.25)'}`,
            padding: '2px 7px', borderRadius: 4,
          }}>
            {quorumOk ? 'quorum OK' : 'no quorum'}
          </span>
        )}
      </div>

      {flash && (
        <div className={`result-box ${flash.ok ? 'ok' : 'err'}`}
          style={{ marginBottom: 10, fontSize: 11 }}>
          {flash.ok ? '✓ ' : '✗ '}{flash.msg}
        </div>
      )}

      {running.length === 0 ? (
        <div style={{
          padding: '24px 0', textAlign: 'center',
          color: 'var(--muted)', fontSize: 12, lineHeight: 1.7,
        }}>
          No nodes running.
          <br />
          Use the form below to start one.
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: 4 }}>
          {running.map(n => (
            <RunningRow key={n.id} node={n} onStop={handleStop} />
          ))}
        </div>
      )}

      <AddNodeForm onCreated={onRefresh} />
    </div>
  );
}
