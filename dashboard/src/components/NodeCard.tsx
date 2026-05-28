import type { NodeStatus } from '../types';

interface Props {
  node: NodeStatus;
}

const STATE_COLOR: Record<string, string> = {
  leader:    'var(--leader)',
  follower:  'var(--follower)',
  candidate: 'var(--candidate)',
  paused:    'var(--muted)',
};

export default function NodeCard({ node }: Props) {
  return (
    <div className={`node-card ${node.state}`}>
      <div className="node-card__header">
        <span className="node-card__id">{node.node_id}</span>
        <span className={`node-card__badge badge-${node.state}`}>{node.state}</span>
      </div>

      <div className="node-card__rows">
        <div className="node-card__row">
          <span className="label">Term</span>
          <span className="value" style={{ color: STATE_COLOR[node.state] }}>{node.term}</span>
        </div>
        <div className="node-card__row">
          <span className="label">Leader</span>
          <span className="value">{node.leader || '—'}</span>
        </div>
        <div className="node-card__row">
          <span className="label">Commit</span>
          <span className="value">{node.commit_index}</span>
        </div>
        <div className="node-card__row">
          <span className="label">Applied</span>
          <span className="value">{node.last_applied}</span>
        </div>
        {node.snapshot_index > 0 && (
          <div className="node-card__row">
            <span className="label">Snapshot</span>
            <span className="value">{node.snapshot_index}</span>
          </div>
        )}
        <hr className="node-card__divider" />
        <div className="node-card__row">
          <span className="label">Peers</span>
          <span className="value">{node.peers?.length ?? 0}</span>
        </div>
        <div className="node-card__row">
          <span className="label">Port</span>
          <span className="value">:{node.httpPort}</span>
        </div>
      </div>
    </div>
  );
}
