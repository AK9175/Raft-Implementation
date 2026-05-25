import { useState } from 'react';
import type { NodeStatus } from '../types';
import { kvGet, kvPut, kvDelete, leaderPort } from '../api';

interface Props {
  nodes: NodeStatus[];
}

type Op = 'GET' | 'PUT' | 'DELETE';

const STATE_COLOR: Record<string, string> = {
  leader:    'var(--leader)',
  follower:  'var(--follower)',
  candidate: 'var(--candidate)',
};

export default function KVPanel({ nodes }: Props) {
  const [op, setOp]         = useState<Op>('GET');
  const [key, setKey]       = useState('');
  const [value, setValue]   = useState('');
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null);
  const [loading, setLoading] = useState(false);
  const [getTargetPort, setGetTargetPort] = useState<number | null>(null);

  const onlineNodes = nodes.filter(n => n.online);

  const effectivePort = (() => {
    if (op === 'GET') {
      if (getTargetPort !== null && onlineNodes.some(n => n.httpPort === getTargetPort)) {
        return getTargetPort;
      }
      return onlineNodes[0]?.httpPort ?? 8081;
    }
    return leaderPort(nodes);
  })();

  const targetNode = onlineNodes.find(n => n.httpPort === effectivePort);

  async function run() {
    if (!key.trim()) return;
    setLoading(true);
    setResult(null);
    try {
      if (op === 'GET') {
        const val = await kvGet(effectivePort, key.trim());
        setResult({ ok: true, message: val });
      } else if (op === 'PUT') {
        await kvPut(effectivePort, key.trim(), value);
        setResult({ ok: true, message: 'committed to cluster' });
      } else {
        await kvDelete(effectivePort, key.trim());
        setResult({ ok: true, message: 'key deleted' });
      }
    } catch (e: unknown) {
      setResult({ ok: false, message: e instanceof Error ? e.message : String(e) });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="panel">
      <div className="section-title">KV Store</div>

      {/* Operation tabs */}
      <div className="btn-row">
        {(['GET', 'PUT', 'DELETE'] as Op[]).map(o => (
          <button
            key={o}
            className={`btn ${op === o ? 'btn-primary' : 'btn-ghost'}`}
            style={{ minWidth: 72 }}
            onClick={() => { setOp(o); setResult(null); }}
          >
            {o}
          </button>
        ))}
      </div>

      {/* Node picker — GET only */}
      {op === 'GET' && onlineNodes.length > 0 && (
        <div className="form-group">
          <label>Read from</label>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {onlineNodes.map(n => {
              const isSelected = n.httpPort === effectivePort;
              const col = STATE_COLOR[n.state] ?? 'var(--muted)';
              return (
                <button
                  key={n.node_id}
                  onClick={() => setGetTargetPort(n.httpPort)}
                  style={{
                    padding: '4px 11px', borderRadius: 6, cursor: 'pointer',
                    border: `1px solid ${isSelected ? col : 'var(--border)'}`,
                    background: isSelected ? `color-mix(in srgb, ${col} 10%, var(--surface))` : 'var(--bg)',
                    color: isSelected ? col : 'var(--muted)',
                    fontSize: 12, fontFamily: 'monospace',
                    fontWeight: isSelected ? 600 : 400,
                    transition: 'all .15s',
                  }}
                >
                  {n.node_id}
                  <span style={{ fontSize: 10, marginLeft: 5, opacity: .7 }}>
                    {n.state}
                  </span>
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Key */}
      <div className="form-group">
        <label>Key</label>
        <input
          type="text"
          placeholder="e.g. username"
          value={key}
          onChange={e => setKey(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && run()}
        />
      </div>

      {/* Value — PUT only */}
      {op === 'PUT' && (
        <div className="form-group">
          <label>Value</label>
          <input
            type="text"
            placeholder="e.g. alice"
            value={value}
            onChange={e => setValue(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && run()}
          />
        </div>
      )}

      <button
        className="btn btn-primary"
        onClick={run}
        disabled={loading || !key.trim() || onlineNodes.length === 0}
        style={{ width: '100%', marginBottom: 12 }}
      >
        {loading ? 'Sending…' : `Run ${op}`}
      </button>

      {result && (
        <div className={`result-box ${result.ok ? 'ok' : 'err'}`}>
          {result.ok ? '✓ ' : '✗ '}{result.message}
        </div>
      )}

      {onlineNodes.length > 0 && (
        <div style={{ marginTop: 10, fontSize: 11, color: 'var(--muted)', lineHeight: 1.6 }}>
          {op === 'GET' ? (
            <>
              Reading from{' '}
              <span style={{ color: 'var(--text)', fontFamily: 'monospace' }}>
                {targetNode?.node_id ?? '…'}
              </span>
              {' '}— locally served, may lag leader
            </>
          ) : (
            <>
              Sending to{' '}
              <span style={{ color: 'var(--text)', fontFamily: 'monospace' }}>
                {targetNode?.node_id ?? '…'}
              </span>
              {targetNode?.state !== 'leader' && ' — will redirect to leader'}
            </>
          )}
        </div>
      )}
    </div>
  );
}
