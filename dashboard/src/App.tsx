import { useState, useEffect, useCallback } from 'react';
import type { NodeStatus, LogEntry, SidecarNodeInfo } from './types';
import { KNOWN_NODES } from './types';
import { fetchSidecarNodes, fetchLog } from './api';
import NodeCard from './components/NodeCard';
import ClusterGraph from './components/ClusterGraph';
import KVPanel from './components/KVPanel';
import MembershipPanel from './components/MembershipPanel';
import LogViewer from './components/LogViewer';
import './index.css';

const POLL_MS = 2000;

function toNodeStatus(n: SidecarNodeInfo): NodeStatus {
  return {
    node_id:             n.id,
    state:               (n.raft_state || 'follower') as NodeStatus['state'],
    term:                n.term,
    leader:              n.leader,
    commit_index:        n.commit_index        ?? 0,
    last_applied:        n.last_applied        ?? 0,
    heartbeats_sent:     n.heartbeats_sent     ?? 0,
    heartbeats_received: n.heartbeats_received ?? 0,
    peers:               n.peers               ?? [],
    snapshot_index:      n.snapshot_index      ?? 0,
    online:              n.running,
    httpPort:            n.http_port,
  };
}

export default function App() {
  const [sidecarNodes, setSidecarNodes] = useState<SidecarNodeInfo[]>([]);
  const [logEntries, setLogEntries]     = useState<LogEntry[]>([]);
  const [logSource, setLogSource]       = useState<number>(KNOWN_NODES[0].httpPort);
  const [lastTick, setLastTick]         = useState('');

  const refresh = useCallback(async () => {
    const raw = await fetchSidecarNodes();
    setSidecarNodes(raw);
    setLastTick(new Date().toLocaleTimeString());

    const online = raw.filter(n => n.running);
    if (online.length === 0) {
      setLogEntries([]);
      return;
    }

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

  return (
    <div className="app">
      <header className="header">
        <h1>Raft <span>Cluster</span></h1>
        <div className="header-meta">
          {leader && (
            <span style={{ fontSize: 12, color: 'var(--muted)' }}>
              Leader:{' '}
              <span style={{ color: 'var(--leader)', fontFamily: 'monospace' }}>
                {leader.node_id}
              </span>
              <span style={{ margin: '0 6px', opacity: .4 }}>·</span>
              term {leader.term}
            </span>
          )}
          <span style={{ fontSize: 12, color: 'var(--muted)' }}>
            {onlineNodes.length
              ? `${onlineNodes.length} node${onlineNodes.length !== 1 ? 's' : ''} online`
              : 'no nodes running'}
          </span>
          <div className="live-dot">
            {lastTick ? `${lastTick}` : 'connecting…'}
          </div>
        </div>
      </header>

      <main className="main">
        {onlineNodes.length > 0 && (
          <div className="topology-grid">
            <ClusterGraph nodes={onlineNodes} />
            <div>
              <div className="section-title">Node Details</div>
              <div className="node-grid">
                {onlineNodes.map(n => <NodeCard key={n.node_id} node={n} />)}
              </div>
            </div>
          </div>
        )}

        <div className="panels">
          <KVPanel nodes={onlineNodes} />
          <MembershipPanel sidecarNodes={sidecarNodes} onRefresh={refresh} />
        </div>

        {onlineNodes.length > 0 && (
          <LogViewer
            entries={logEntries}
            nodes={onlineNodes}
            sourcePort={logSource}
            onSourceChange={setLogSource}
          />
        )}
      </main>
    </div>
  );
}
