import type { NodeStatus, LogEntry, SidecarNodeInfo } from './types';
import { KNOWN_NODES } from './types';

const SIDECAR = 'http://localhost:9090';
const base    = (port: number) => `http://localhost:${port}`;

// ── Sidecar API ───────────────────────────────────────────────────────────────

export async function fetchSidecarNodes(): Promise<SidecarNodeInfo[]> {
  try {
    const res = await fetch(`${SIDECAR}/nodes`, { signal: AbortSignal.timeout(3000) });
    if (!res.ok) return [];
    return await res.json();
  } catch {
    return [];
  }
}

// Create (and start) a node. For preset nodes just pass the id.
// For new dynamic nodes, http_port is required and rpc_port defaults to http_port+100.
export async function sidecarCreate(id: string, httpPort?: number, rpcPort?: number): Promise<void> {
  const body: Record<string, unknown> = { id };
  if (httpPort !== undefined) body.http_port = httpPort;
  if (rpcPort  !== undefined) body.rpc_port  = rpcPort;
  const res = await fetch(`${SIDECAR}/nodes/create`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60000), // --build can take a while
  });
  if (!res.ok) throw new Error(await res.text());
}

export async function sidecarStop(nodeId: string): Promise<void> {
  const res = await fetch(`${SIDECAR}/nodes/${nodeId}/stop`, {
    method: 'POST',
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(await res.text());
}

// ── Node status — sidecar is the primary source ───────────────────────────────
// Routing through the sidecar means the browser never talks to node HTTP ports
// directly, so CORS is never an issue regardless of which image the node uses.

function sidecarToNodeStatus(n: SidecarNodeInfo): NodeStatus {
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

// Fallback used only when the sidecar is not running.
async function fetchStatusDirect(port: number): Promise<NodeStatus | null> {
  try {
    const res = await fetch(`${base(port)}/status`, { signal: AbortSignal.timeout(1500) });
    if (!res.ok) return null;
    const data = await res.json();
    return { ...data, online: true, httpPort: port };
  } catch {
    return null;
  }
}

export async function fetchAllNodes(): Promise<NodeStatus[]> {
  // Try sidecar first — it has all status fields and no CORS restrictions.
  const sidecarData = await fetchSidecarNodes();
  if (sidecarData.length > 0) {
    return sidecarData.map(sidecarToNodeStatus);
  }

  // Sidecar not running — fall back to direct polling (limited to nodes with CORS).
  return Promise.all(
    KNOWN_NODES.map(async (n) => {
      const status = await fetchStatusDirect(n.httpPort);
      return status ?? {
        node_id: n.id, state: 'follower' as const, term: 0, leader: '',
        commit_index: 0, last_applied: 0, heartbeats_received: 0,
        heartbeats_sent: 0, peers: [], snapshot_index: 0,
        online: false, httpPort: n.httpPort,
      };
    })
  );
}

// ── Log ───────────────────────────────────────────────────────────────────────

export async function fetchLog(port: number): Promise<LogEntry[]> {
  try {
    const res = await fetch(`${base(port)}/log`, { signal: AbortSignal.timeout(1500) });
    if (!res.ok) return [];
    return await res.json();
  } catch {
    return [];
  }
}

// ── KV Store ──────────────────────────────────────────────────────────────────

export async function kvGet(port: number, key: string): Promise<string> {
  const res = await fetch(`${base(port)}/keys/${encodeURIComponent(key)}`,
    { signal: AbortSignal.timeout(3000) });
  if (res.status === 404) throw new Error('key not found');
  if (!res.ok) throw new Error(await res.text());
  return (await res.text()).trim();
}

export async function kvPut(port: number, key: string, value: string): Promise<void> {
  const res = await fetch(
    `${base(port)}/keys/${encodeURIComponent(key)}?value=${encodeURIComponent(value)}`,
    { method: 'PUT', signal: AbortSignal.timeout(6000) });
  if (!res.ok) throw new Error(await res.text());
}

export async function kvDelete(port: number, key: string): Promise<void> {
  const res = await fetch(`${base(port)}/keys/${encodeURIComponent(key)}`,
    { method: 'DELETE', signal: AbortSignal.timeout(6000) });
  if (!res.ok) throw new Error(await res.text());
}

// ── Helpers ───────────────────────────────────────────────────────────────────

export function leaderPort(nodes: NodeStatus[]): number {
  const leader = nodes.find((n) => n.online && n.state === 'leader');
  if (leader) return leader.httpPort;
  const any = nodes.find((n) => n.online);
  return any ? any.httpPort : 8081;
}
