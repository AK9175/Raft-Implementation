import { useState, useEffect } from 'react';
import type { ScenarioMeta, ScenarioResult } from '../api';
import { fetchLiveScenarios, runLiveScenario } from '../api';

type Status = 'idle' | 'running' | 'passed' | 'failed';

interface ScenarioState {
  meta:     ScenarioMeta;
  status:   Status;
  result:   ScenarioResult | null;
  logsOpen: boolean;
}

function StatusBadge({ status }: { status: Status }) {
  const map: Record<Status, { bg: string; color: string; label: string }> = {
    idle:    { bg: 'rgba(125,133,144,.1)',   color: 'var(--muted)',   label: 'idle'    },
    running: { bg: 'rgba(110,64,201,.12)',   color: 'var(--accent)',  label: 'running' },
    passed:  { bg: 'rgba(63,185,80,.12)',    color: 'var(--leader)',  label: 'passed'  },
    failed:  { bg: 'rgba(248,81,73,.12)',    color: 'var(--offline)', label: 'failed'  },
  };
  const s = map[status];
  return (
    <span style={{
      fontSize: 9, fontWeight: 700, letterSpacing: .4, textTransform: 'uppercase',
      padding: '2px 7px', borderRadius: 4, background: s.bg, color: s.color,
      display: 'inline-flex', alignItems: 'center', gap: 4,
    }}>
      {status === 'running' && (
        <span style={{
          width: 5, height: 5, borderRadius: '50%', background: 'var(--accent)',
          animation: 'live-pulse 1s ease-in-out infinite', flexShrink: 0,
        }} />
      )}
      {s.label}
    </span>
  );
}

function ScenarioCard({ state, activeNodeCount, onRun, onToggleLogs }: {
  state:           ScenarioState;
  activeNodeCount: number;
  onRun:           () => void;
  onToggleLogs:    () => void;
}) {
  const { meta, status, result, logsOpen } = state;
  const isRunning   = status === 'running';
  const notEnough   = activeNodeCount < (meta.min_nodes ?? 1);
  const canRun      = !isRunning && !notEnough;

  const border = status === 'passed' ? 'var(--leader)'
               : status === 'failed'  ? 'var(--offline)'
               : status === 'running' ? 'var(--accent)'
               : 'var(--border)';

  return (
    <div style={{
      background: 'var(--bg)', border: `1px solid ${border}`, borderRadius: 8,
      padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 8,
      transition: 'border-color .2s',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 8 }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 3, flexWrap: 'wrap' }}>
            <span style={{ fontWeight: 600, fontSize: 12, fontFamily: 'monospace' }}>{meta.label}</span>
            <StatusBadge status={status} />
          </div>
          <div style={{ fontSize: 11, color: 'var(--muted)', lineHeight: 1.5 }}>{meta.desc}</div>
          {notEnough && (
            <div style={{ fontSize: 10, color: 'var(--candidate)', marginTop: 4 }}>
              Needs {meta.min_nodes} running nodes — {activeNodeCount} active
            </div>
          )}
        </div>
        <button
          className="btn btn-primary"
          style={{ padding: '4px 14px', fontSize: 11, flexShrink: 0 }}
          disabled={!canRun}
          onClick={onRun}
          title={notEnough ? `Start at least ${meta.min_nodes} nodes first` : undefined}
        >
          {isRunning ? 'Running…' : 'Run'}
        </button>
      </div>

      {result && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 11, color: result.passed ? 'var(--leader)' : 'var(--offline)' }}>
              {result.passed ? '✓ Passed' : `✗ ${result.error ?? 'Failed'}`}
            </span>
            <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'monospace' }}>
              {result.duration_ms}ms
            </span>
            {result.logs.length > 0 && (
              <button
                style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 10, color: 'var(--muted)', padding: 0, textDecoration: 'underline' }}
                onClick={onToggleLogs}
              >
                {logsOpen ? 'hide log' : `show ${result.logs.length} steps`}
              </button>
            )}
          </div>

          {logsOpen && result.logs.length > 0 && (
            <div style={{
              background: 'var(--surface-2)', border: '1px solid var(--border)',
              borderRadius: 6, padding: '8px 10px', fontFamily: 'monospace', fontSize: 10,
              lineHeight: 1.7, maxHeight: 200, overflowY: 'auto',
              whiteSpace: 'pre-wrap', wordBreak: 'break-all',
            }}>
              {result.logs.map((l, i) => (
                <div key={i} style={{
                  color: l.includes('✓') ? 'var(--leader)'
                       : l.includes('✗') || l.toUpperCase().includes('FAIL') ? 'var(--offline)'
                       : l.toLowerCase().includes('waiting') || l.toLowerCase().includes('holding') ? 'var(--candidate)'
                       : 'var(--muted)',
                }}>
                  {l}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default function ChaosPanel({ activeNodeCount }: { activeNodeCount: number }) {
  const [scenarios, setScenarios] = useState<ScenarioState[]>([]);
  const [serverUp, setServerUp]   = useState<boolean | null>(null);

  useEffect(() => {
    fetchLiveScenarios().then(metas => {
      if (metas.length === 0) { setServerUp(false); return; }
      setServerUp(true);
      setScenarios(metas.map(meta => ({ meta, status: 'idle', result: null, logsOpen: false })));
    });
  }, []);

  async function runScenario(idx: number) {
    const id = scenarios[idx].meta.id;
    setScenarios(prev => prev.map((s, i) =>
      i === idx ? { ...s, status: 'running', result: null, logsOpen: false } : s
    ));
    try {
      const result = await runLiveScenario(id);
      setScenarios(prev => prev.map((s, i) =>
        i === idx ? { ...s, status: result.passed ? 'passed' : 'failed', result, logsOpen: true } : s
      ));
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setScenarios(prev => prev.map((s, i) =>
        i === idx ? { ...s, status: 'failed', result: { name: id, passed: false, error: msg, duration_ms: 0, logs: [] }, logsOpen: true } : s
      ));
    }
  }

  function toggleLogs(idx: number) {
    setScenarios(prev => prev.map((s, i) => i === idx ? { ...s, logsOpen: !s.logsOpen } : s));
  }

  if (serverUp === false) {
    return (
      <div className="panel">
        <div className="section-title">Chaos Lab</div>
        <div style={{ color: 'var(--muted)', fontSize: 12, marginTop: 6, marginBottom: 10 }}>
          Sidecar not running — chaos scenarios require the sidecar:
        </div>
        <div style={{ padding: '7px 11px', background: 'var(--bg)', borderRadius: 6, fontFamily: 'monospace', fontSize: 12, border: '1px solid var(--border)' }}>
          go run ./cmd/sidecar
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="section-title" style={{ marginBottom: 4 }}>Chaos Lab</div>
      <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 12, lineHeight: 1.5 }}>
        Injects faults into the <strong style={{ color: 'var(--text)' }}>live Docker cluster</strong> — watch the topology update in real time.
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {scenarios.map((s, i) => (
          <ScenarioCard
            key={s.meta.id}
            state={s}
            activeNodeCount={activeNodeCount}
            onRun={() => runScenario(i)}
            onToggleLogs={() => toggleLogs(i)}
          />
        ))}
      </div>

      {scenarios.length === 0 && serverUp === null && (
        <div style={{ color: 'var(--muted)', fontSize: 12, textAlign: 'center', padding: '20px 0' }}>
          Connecting…
        </div>
      )}
    </div>
  );
}
