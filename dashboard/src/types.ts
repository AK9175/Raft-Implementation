export interface NodeStatus {
  node_id: string;
  state: 'leader' | 'follower' | 'candidate' | 'paused';
  term: number;
  leader: string;
  commit_index: number;
  last_applied: number;
  heartbeats_received: number;
  heartbeats_sent: number;
  peers: string[];
  snapshot_index: number;
  // synthetic fields added by the dashboard
  online: boolean;
  httpPort: number;
}

export interface LogEntry {
  index: number;
  term: number;
  is_config: boolean;
  config_op?: string;
  config_peer?: string;
  command?: string;
}

export interface KVResult {
  ok: boolean;
  value?: string;
  error?: string;
}

// Sidecar node info — returned by GET http://localhost:9090/nodes
export interface SidecarNodeInfo {
  id: string;
  rpc_addr: string;
  http_port: number;
  rpc_port: number;
  bootstrap: boolean;
  dynamic: boolean;
  running: boolean;
  in_cluster: boolean;
  paused?: boolean;
  raft_state: string;
  term: number;
  leader: string;
  peers: string[];
  commit_index: number;
  last_applied: number;
  heartbeats_sent: number;
  heartbeats_received: number;
  snapshot_index: number;
}

// Known nodes the dashboard will try to contact
export const KNOWN_NODES: { id: string; httpPort: number }[] = [
  { id: 'node1', httpPort: 8081 },
  { id: 'node2', httpPort: 8082 },
  { id: 'node3', httpPort: 8083 },
  { id: 'node4', httpPort: 8084 },
  { id: 'node5', httpPort: 8085 },
];
