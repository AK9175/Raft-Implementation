import { useState, useEffect, useCallback } from 'react';
import type { NodeStatus, LogEntry, SidecarNodeInfo } from './types';
import { KNOWN_NODES } from './types';
import { fetchSidecarNodes, fetchLog } from './api';
import ClusterGraph from './components/ClusterGraph';
import KVPanel from './components/KVPanel';
import MembershipPanel from './components/MembershipPanel';
import LogViewer from './components/LogViewer';
import ChaosPanel from './components/ChaosPanel';
import DocsPanel from './components/DocsPanel';
import './index.css';

const POLL_MS = 2000;

type Tab = 'kv' | 'chaos' | 'cluster' | 'log' | 'docs';

const STATE_DOT: Record<string, string> = {
  leader:    '#3fb950',
  follower:  '#58a6ff',
  candidate: '#d29922',
  paused:    '#4b5563',
};

function SidebarNodeCard({ node }: { node: NodeStatus }) {
  const color    = STATE_DOT[node.state] ?? '#7d8590';
  const isLeader = node.state === 'leader';
  return (
    <div className={`sb-node${isLeader ? ' leader' : ''}`}>
      <span style={{
        width: 6, height: 6, borderRadius: '50%', background: color,
        flexShrink: 0, display: 'inline-block',
        boxShadow: isLeader ? `0 0 4px ${color}` : undefined,
      }} />
      <span style={{ fontFamily: 'monospace', fontWeight: 600, fontSize: 11, flexShrink: 0 }}>
        {node.node_id}
      </span>
      <span className={`node-card__badge badge-${node.state}`}
        style={{ fontSize: 7, padding: '1px 5px', flexShrink: 0 }}>
        {node.state}
      </span>
      <span style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center', flexShrink: 0 }}>
        <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'monospace' }}>t{node.term}</span>
        <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'monospace' }}>ci:{node.commit_index}</span>
      </span>
    </div>
  );
}

function toNodeStatus(n: SidecarNodeInfo): NodeStatus {
  return {
    node_id:             n.id,
    state:               n.paused ? 'paused' : (n.raft_state || 'follower') as NodeStatus['state'],
    term:                n.term,
    leader:              n.leader,
    commit_index:        n.commit_index        ?? 0,
    last_applied:        n.last_applied        ?? 0,
    heartbeats_sent:     n.heartbeats_sent     ?? 0,
    heartbeats_received: n.heartbeats_received ?? 0,
    peers:               n.peers               ?? [],
    snapshot_index:      n.snapshot_index      ?? 0,
    online:              n.running || !!n.paused,
    httpPort:            n.http_port,
  };
}

const TABS: { id: Tab; label: string }[] = [
  { id: 'kv',      label: 'KV Store'  },
  { id: 'chaos',   label: 'Chaos Lab' },
  { id: 'cluster', label: 'Cluster'   },
  { id: 'log',     label: 'Log'       },
  { id: 'docs',    label: 'Docs'      },
];

export default function App() {
  const [sidecarNodes, setSidecarNodes] = useState<SidecarNodeInfo[]>([]);
  const [logEntries, setLogEntries]     = useState<LogEntry[]>([]);
  const [logSource, setLogSource]       = useState<number>(KNOWN_NODES[0].httpPort);
  const [lastTick, setLastTick]         = useState('');
  const [activeTab, setActiveTab]       = useState<Tab>('kv');

  const refresh = useCallback(async () => {
    const raw = await fetchSidecarNodes();
    setSidecarNodes(raw);
    setLastTick(new Date().toLocaleTimeString());

    const online = raw.filter(n => n.running);
    if (online.length === 0) { setLogEntries([]); return; }

    const currentOnline = online.find(n => n.http_port === logSource);
    const sourcePort = currentOnline ? logSource : online[0].http_port;
    if (sourcePort !== logSource) setLogSource(sourcePort);

    const entries = await fetchLog(sourcePort);
    setLogEntries(entries ?? []);
  }, [logSource]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const nodes       = sidecarNodes.map(toNodeStatus);
  const onlineNodes = nodes.filter(n => n.online);
  const leader      = onlineNodes.find(n => n.state === 'leader');
  const hasNodes    = onlineNodes.length > 0;
  const healthOk = hasNodes && !!leader;

  return (
    <div className="app">
      <header className="header">
        <div className="header-brand">
          <h1>Raft <span>Cluster</span></h1>
        </div>

        <div className="status-chips">
          {!hasNodes ? (
            <span className="status-chip chip-neutral">No nodes running</span>
          ) : (
            <>
              <span className={`status-chip ${healthOk ? 'chip-ok' : 'chip-warn'}`}>
                {healthOk ? '● Healthy' : '⚠ No Leader'}
              </span>
              {leader && (
                <span className="status-chip chip-neutral">
                  Leader:&nbsp;
                  <span style={{ color: 'var(--leader)', fontFamily: 'monospace' }}>{leader.node_id}</span>
                </span>
              )}
              {leader && (
                <span className="status-chip chip-neutral">Term {leader.term}</span>
              )}
              <span className="status-chip chip-neutral">
                {onlineNodes.length} node{onlineNodes.length !== 1 ? 's' : ''}
              </span>
            </>
          )}
        </div>

        <div className="header-right">
          <div className="live-dot">{lastTick || 'connecting…'}</div>
        </div>
      </header>

      <div className="content-area">
        <aside className="sidebar">
          <div className="sidebar-section">
            <div className="sidebar-label">Topology</div>
            {hasNodes
              ? <ClusterGraph nodes={onlineNodes} compact />
              : <div style={{ fontSize: 11, color: 'var(--muted)', padding: '16px 0', textAlign: 'center' }}>
                  No nodes running
                </div>
            }
          </div>

          {hasNodes && (
            <div className="sidebar-section">
              <div className="sidebar-label">Nodes</div>
              {onlineNodes.map(n => <SidebarNodeCard key={n.node_id} node={n} />)}
            </div>
          )}
        </aside>

        <div className="tab-area">
          <div className="tab-bar">
            {TABS.map(t => (
              <button
                key={t.id}
                className={`tab-btn${activeTab === t.id ? ' active' : ''}`}
                onClick={() => setActiveTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>

          <div className="tab-content">
            {activeTab === 'kv'      && <KVPanel nodes={onlineNodes} />}
            {activeTab === 'chaos'   && (
              <ChaosPanel activeNodeCount={sidecarNodes.filter(n => n.running && !n.paused).length} />
            )}
            {activeTab === 'cluster' && (
              <MembershipPanel sidecarNodes={sidecarNodes} onRefresh={refresh} />
            )}
            {activeTab === 'log' && hasNodes && (
              <LogViewer
                entries={logEntries}
                nodes={onlineNodes}
                sourcePort={logSource}
                onSourceChange={setLogSource}
              />
            )}
            {activeTab === 'log' && !hasNodes && (
              <div style={{ color: 'var(--muted)', fontSize: 13, padding: '40px 0', textAlign: 'center' }}>
                No nodes running
              </div>
            )}
            {activeTab === 'docs' && <DocsPanel />}
          </div>
        </div>
      </div>
    </div>
  );
}
