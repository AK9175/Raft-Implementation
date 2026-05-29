import { useState, useEffect } from 'react';
import type { SidecarNodeInfo } from '../types';
import { sidecarCreate, sidecarStop, sidecarPause, sidecarUnpause, sidecarRestart } from '../api';

const STATE_COLOR: Record<string, string> = {
  leader:    'var(--leader)',
  follower:  'var(--follower)',
  candidate: 'var(--candidate)',
};

const STATE_DOT: Record<string, string> = {
  leader:    '#3fb950',
  follower:  '#58a6ff',
  candidate: '#d29922',
  paused:    '#4b5563',
};

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
      boxShadow: state === 'leader' ? `0 0 5px ${color}` : undefined,
      flexShrink: 0,
    }} />
  );
}

function RunningRow({ node, onStop, onPause, onUnpause, onRestart }: {
  node: SidecarNodeInfo;
  onStop:    (id: string) => Promise<void>;
  onPause:   (id: string) => Promise<void>;
  onUnpause: (id: string) => Promise<void>;
  onRestart: (id: string) => Promise<void>;
}) {
  const [busy, setBusy] = useState(false);

  async function handle(fn: (id: string) => Promise<void>) {
    setBusy(true);
    try { await fn(node.id); } finally { setBusy(false); }
  }

  const isPaused   = !!node.paused;
  const stateColor = isPaused ? 'var(--muted)' : (STATE_COLOR[node.raft_state] ?? 'var(--muted)');
  const isLeader   = node.raft_state === 'leader';
  const isStarting = !isPaused && node.raft_state === '';
  const isJoining  = !node.in_cluster && !isStarting && !isPaused;

  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      background: 'var(--bg)',
      border: `1px solid ${isLeader ? 'var(--leader)' : isPaused ? 'var(--border)' : 'var(--border)'}`,
      borderRadius: 6, padding: '7px 10px', gap: 8,
      opacity: isPaused ? 0.7 : 1,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 7, minWidth: 0 }}>
        {isStarting
          ? <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'var(--muted)', display: 'inline-block', flexShrink: 0 }} />
          : <StateDot state={isPaused ? 'paused' : node.raft_state} />
        }
        <span style={{ fontWeight: 600, fontSize: 12, fontFamily: 'monospace', flexShrink: 0 }}>
          {node.id}
        </span>
        {node.dynamic && (
          <span style={{
            fontSize: 9, padding: '1px 5px', borderRadius: 3, flexShrink: 0,
            background: 'var(--border)', color: 'var(--muted)', fontWeight: 600,
            textTransform: 'uppercase', letterSpacing: '.4px',
          }}>custom</span>
        )}
        {isPaused
          ? <span style={{ fontSize: 11, color: 'var(--muted)' }}>paused</span>
          : isStarting
            ? <span style={{ fontSize: 11, color: 'var(--muted)' }}>starting…</span>
            : <span style={{ fontSize: 11, color: stateColor }}>{node.raft_state}</span>
        }
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
          }}>joining</span>
        )}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 }}>
        <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'monospace' }}>
          :{node.http_port}
        </span>
        <button className="btn btn-ghost"
          style={{
            padding: '3px 10px', fontSize: 11,
            // Highlight Restart for orphaned nodes — it's the clean rejoin path.
            color: isJoining ? 'var(--follower)' : undefined,
            borderColor: isJoining ? 'rgba(88,166,255,.4)' : undefined,
          }}
          disabled={busy} onClick={() => handle(onRestart)}
          title="Stop and start cleanly — re-joins the current cluster">
          {busy ? '…' : 'Restart'}
        </button>
        {isPaused ? (
          <button className="btn btn-ghost"
            style={{ padding: '3px 10px', fontSize: 11 }}
            disabled={busy} onClick={() => handle(onUnpause)}>
            {busy ? '…' : 'Resume'}
          </button>
        ) : (
          <button className="btn btn-ghost"
            style={{ padding: '3px 10px', fontSize: 11, color: 'var(--candidate)', borderColor: 'rgba(210,153,34,.3)' }}
            disabled={busy || isStarting} onClick={() => handle(onPause)}>
            {busy ? '…' : 'Pause'}
          </button>
        )}
        <button className="btn btn-danger"
          style={{ padding: '3px 10px', fontSize: 11 }}
          disabled={busy} onClick={() => handle(onStop)}>
          {busy ? '…' : 'Stop'}
        </button>
      </div>
    </div>
  );
}

function nextAvailable(existingIds: string[]): number {
  const used = new Set(
    existingIds.flatMap(id => { const m = id.match(/^node(\d+)$/); return m ? [parseInt(m[1])] : []; })
  );
  let n = 1;
  while (used.has(n)) n++;
  return n;
}

function AddNodeForm({ existingIds, onCreated }: { existingIds: string[]; onCreated: () => void }) {
  const [counter, setCounter] = useState(() => nextAvailable(existingIds));
  const [busy, setBusy]       = useState(false);
  const [error, setError]     = useState('');

  useEffect(() => { setCounter(nextAvailable(existingIds)); }, [existingIds.length]);

  const nodeId    = `node${counter}`;
  const isPreset  = !!PRESET[nodeId];
  const httpPort  = PRESET[nodeId]?.httpPort ?? 8080 + counter;
  const rpcPort   = PRESET[nodeId]?.rpcPort  ?? 7000 + counter;
  const alreadyUp = existingIds.includes(nodeId);

  async function submit() {
    if (alreadyUp) return;
    setBusy(true); setError('');
    try {
      await sidecarCreate(nodeId, isPreset ? undefined : httpPort, isPreset ? undefined : rpcPort);
      onCreated();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ marginTop: 12, borderTop: '1px solid var(--border-2)', paddingTop: 12 }}>
      <div style={{ fontSize: 10, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '.6px', fontWeight: 500, marginBottom: 8 }}>
        Add Node
      </div>

      <div style={{ display: 'flex', gap: 6, alignItems: 'stretch' }}>
        <div style={{
          display: 'flex', alignItems: 'center', flex: 1,
          background: 'var(--bg)',
          border: `1px solid ${alreadyUp ? 'var(--candidate)' : 'var(--border)'}`,
          borderRadius: 6, overflow: 'hidden', transition: 'border-color .15s',
        }}>
          <span style={{
            padding: '0 8px 0 10px', fontSize: 13, fontFamily: 'monospace',
            color: 'var(--muted)', userSelect: 'none',
            borderRight: '1px solid var(--border)',
            background: 'var(--surface-2)',
            alignSelf: 'stretch', display: 'flex', alignItems: 'center', flexShrink: 0,
          }}>
            node-
          </span>
          <input
            type="number" min={1} max={99}
            value={counter}
            onChange={e => { const v = parseInt(e.target.value); if (!isNaN(v) && v >= 1) setCounter(v); }}
            onKeyDown={e => e.key === 'Enter' && submit()}
            style={{
              background: 'transparent', border: 'none', outline: 'none',
              color: 'var(--text)', fontSize: 13, fontFamily: 'monospace',
              width: '100%', padding: '7px 10px',
            }}
          />
        </div>
        <button
          className="btn btn-primary"
          style={{ padding: '0 18px', flexShrink: 0 }}
          disabled={busy || alreadyUp}
          onClick={submit}
        >
          {busy ? '…' : 'Start'}
        </button>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 5 }}>
        <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'monospace' }}>
          {isPreset ? 'preset' : 'dynamic'} · http :{httpPort} · rpc :{rpcPort}
        </span>
        {alreadyUp && (
          <span style={{ fontSize: 10, color: 'var(--candidate)' }}>already running</span>
        )}
      </div>

      {error && <div style={{ fontSize: 11, color: 'var(--offline)', marginTop: 6 }}>{error}</div>}
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

  async function handlePause(nodeId: string) {
    try {
      await sidecarPause(nodeId);
      onRefresh();
      showFlash(true, `${nodeId} paused`);
    } catch (e: unknown) {
      showFlash(false, e instanceof Error ? e.message : String(e));
    }
  }

  async function handleUnpause(nodeId: string) {
    try {
      await sidecarUnpause(nodeId);
      onRefresh();
      showFlash(true, `${nodeId} resumed`);
    } catch (e: unknown) {
      showFlash(false, e instanceof Error ? e.message : String(e));
    }
  }

  async function handleRestart(nodeId: string) {
    try {
      await sidecarRestart(nodeId);
      onRefresh();
      showFlash(true, `${nodeId} restarted`);
    } catch (e: unknown) {
      showFlash(false, e instanceof Error ? e.message : String(e));
    }
  }

  const running       = sidecarNodes.filter(n => n.running);
  const clusterNodes  = running.filter(n => n.in_cluster);
  const orphaned      = running.filter(n => !n.in_cluster && !n.paused);
  const hasLeader     = running.some(n => n.raft_state === 'leader');
  const quorumOk      = hasLeader && clusterNodes.length >= 2;
  const electing      = !hasLeader && clusterNodes.length >= 2;

  if (!sidecarOk) {
    return (
      <div className="panel">
        <div className="section-title">Cluster Control</div>
        <div style={{ color: 'var(--muted)', fontSize: 12, marginTop: 6, marginBottom: 10 }}>
          Sidecar not running — start it to manage nodes:
        </div>
        <div style={{
          padding: '7px 11px', background: 'var(--bg)', borderRadius: 6,
          fontFamily: 'monospace', fontSize: 12, border: '1px solid var(--border)',
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
            fontSize: 9, fontWeight: 500, letterSpacing: .4, textTransform: 'none',
            color:       quorumOk ? 'var(--leader)'    : electing ? 'var(--candidate)' : 'var(--offline)',
            background:  quorumOk ? 'rgba(63,185,80,.1)' : electing ? 'rgba(210,153,34,.1)' : 'rgba(248,81,73,.08)',
            border: `1px solid ${quorumOk ? 'rgba(63,185,80,.25)' : electing ? 'rgba(210,153,34,.25)' : 'rgba(248,81,73,.25)'}`,
            padding: '2px 7px', borderRadius: 4,
          }}>
            {quorumOk ? 'quorum OK' : electing ? 'electing…' : 'no quorum'}
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
          padding: '20px 0', textAlign: 'center',
          color: 'var(--muted)', fontSize: 12, lineHeight: 1.7,
        }}>
          No nodes running.<br />Use the form below to start one.
        </div>
      ) : (
        <>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 5, marginBottom: 4 }}>
            {clusterNodes.map(n => (
              <RunningRow key={n.id} node={n} onStop={handleStop} onPause={handlePause} onUnpause={handleUnpause} onRestart={handleRestart} />
            ))}
          </div>

          {orphaned.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <div style={{
                display: 'flex', alignItems: 'center', gap: 6,
                fontSize: 10, color: 'var(--offline)', marginBottom: 6,
                padding: '5px 8px', borderRadius: 5,
                background: 'rgba(248,81,73,.06)', border: '1px solid rgba(248,81,73,.18)',
              }}>
                <span>⚠</span>
                <span>
                  Not in cluster — stale term may disrupt leader if added.
                  Stop and restart via the form below to rejoin cleanly.
                </span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 5, opacity: 0.75 }}>
                {orphaned.map(n => (
                  <RunningRow key={n.id} node={n} onStop={handleStop} onPause={handlePause} onUnpause={handleUnpause} onRestart={handleRestart} />
                ))}
              </div>
            </div>
          )}
        </>
      )}

      <AddNodeForm existingIds={running.map(n => n.id)} onCreated={onRefresh} />
    </div>
  );
}
