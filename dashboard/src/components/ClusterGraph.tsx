import type { NodeStatus } from '../types';

interface Props {
  nodes: NodeStatus[];
  compact?: boolean;
}

const FULL  = { W: 480, H: 320, CX: 240, CY: 160, RING_R: 110, NODE_R: 26 };
const SMALL = { W: 290, H: 210, CX: 145, CY: 105, RING_R:  82, NODE_R: 27 };

function pos(index: number, total: number, cx: number, cy: number, ringR: number) {
  const angle = (2 * Math.PI * index) / total - Math.PI / 2;
  return { x: cx + ringR * Math.cos(angle), y: cy + ringR * Math.sin(angle) };
}

const STATE: Record<string, { fill: string; stroke: string; text: string; dash?: boolean }> = {
  leader:    { fill: 'rgba(63,185,80,0.13)',  stroke: '#3fb950', text: '#86efac' },
  follower:  { fill: 'rgba(88,166,255,0.10)', stroke: '#58a6ff', text: '#bfdbfe' },
  candidate: { fill: 'rgba(210,153,34,0.13)', stroke: '#d29922', text: '#fde68a' },
  paused:    { fill: 'rgba(125,133,144,0.06)', stroke: '#4b5563', text: '#6b7280', dash: true },
};
const DEFAULT = { fill: 'rgba(125,133,144,0.08)', stroke: '#374151', text: '#9ca3af' };

function truncate(s: string, max = 8) {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

export default function ClusterGraph({ nodes, compact }: Props) {
  const total = nodes.length;
  if (total === 0) return null;

  const { W, H, CX, CY, RING_R, NODE_R } = compact ? SMALL : FULL;

  const positions = nodes.map((_, i) => pos(i, total, CX, CY, RING_R));
  const leaderIdx = nodes.findIndex(n => n.state === 'leader');

  const edgeSet = new Set<string>();
  const edges: { a: number; b: number; isLeaderEdge: boolean }[] = [];

  nodes.forEach((node, aIdx) => {
    node.peers?.forEach((peer) => {
      const peerId = peer.split(':')[0];
      const bIdx   = nodes.findIndex(n => n.node_id === peerId);
      if (bIdx === -1) return;
      const key = [Math.min(aIdx, bIdx), Math.max(aIdx, bIdx)].join('-');
      if (!edgeSet.has(key)) {
        edgeSet.add(key);
        edges.push({ a: aIdx, b: bIdx, isLeaderEdge: aIdx === leaderIdx || bIdx === leaderIdx });
      }
    });
  });

  return (
    <div className={`graph-wrap${compact ? ' compact' : ''}`}>
      {!compact && <div className="section-title">Cluster Topology</div>}
      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', display: 'block' }}>
        {/* Edges */}
        {edges.map(({ a, b, isLeaderEdge }) => {
          const pa = positions[a];
          const pb = positions[b];
          return (
            <line
              key={`${a}-${b}`}
              x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
              stroke={isLeaderEdge ? '#3fb950' : '#30363d'}
              strokeWidth={isLeaderEdge ? 1.2 : 0.8}
              strokeDasharray={isLeaderEdge ? '5 4' : undefined}
              className={isLeaderEdge ? 'edge-flow' : undefined}
              opacity={isLeaderEdge ? 0.5 : 0.3}
            />
          );
        })}

        {/* Nodes */}
        {nodes.map((node, i) => {
          const p = positions[i];
          const s = STATE[node.state] ?? DEFAULT;
          const isLeader = node.state === 'leader';

          return (
            <g key={node.node_id}>
              {isLeader && (
                <circle
                  cx={p.x} cy={p.y} r={NODE_R + 10}
                  fill="none" stroke="#3fb950"
                  strokeWidth={1} className="leader-ring"
                />
              )}
              <circle
                cx={p.x} cy={p.y} r={NODE_R}
                fill={s.fill} stroke={s.stroke} strokeWidth={1.5}
                strokeDasharray={s.dash ? '4 3' : undefined}
                opacity={node.state === 'paused' ? 0.55 : 1}
              />
              <text
                x={p.x} y={p.y - 4}
                textAnchor="middle" dominantBaseline="auto"
                fill={s.text} fontSize={compact ? 11 : 10} fontWeight="600"
                fontFamily="'JetBrains Mono', monospace"
              >
                {truncate(node.node_id)}
              </text>
              <text
                x={p.x} y={p.y + 9}
                textAnchor="middle" dominantBaseline="auto"
                fill={s.text} fontSize={compact ? 9 : 8} opacity={0.6}
                fontFamily="'JetBrains Mono', monospace"
              >
                t{node.term}
              </text>
            </g>
          );
        })}

        {/* Legend — full view only */}
        {!compact && ([
          { color: '#3fb950', label: 'Leader'    },
          { color: '#58a6ff', label: 'Follower'  },
          { color: '#d29922', label: 'Candidate' },
          { color: '#4b5563', label: 'Paused'    },
        ] as const).map(({ color, label }, i) => (
          <g key={label} transform={`translate(${12 + i * 88}, ${H - 14})`}>
            <circle r={3.5} fill={color} opacity={0.75} />
            <text x={9} y={4} fill="#7d8590" fontSize={9}
              fontFamily="Inter, system-ui, sans-serif">{label}</text>
          </g>
        ))}
      </svg>
    </div>
  );
}
