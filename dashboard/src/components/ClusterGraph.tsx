import type { NodeStatus } from '../types';

interface Props {
  nodes: NodeStatus[]; // only online nodes
}

const W = 560;
const H = 380;
const CX = W / 2;
const CY = H / 2;
const RING_R = 138;
const NODE_R  = 40;

function pos(index: number, total: number) {
  const angle = (2 * Math.PI * index) / total - Math.PI / 2;
  return { x: CX + RING_R * Math.cos(angle), y: CY + RING_R * Math.sin(angle) };
}

const STATE: Record<string, { fill: string; stroke: string; text: string; grad: string }> = {
  leader:    { fill: '#14532d', stroke: '#3fb950', text: '#86efac', grad: 'grad-leader'    },
  follower:  { fill: '#1e3a5f', stroke: '#58a6ff', text: '#bfdbfe', grad: 'grad-follower'  },
  candidate: { fill: '#78350f', stroke: '#d29922', text: '#fde68a', grad: 'grad-candidate' },
};
const DEFAULT_STATE = { fill: '#1e2130', stroke: '#374151', text: '#9ca3af', grad: 'grad-default' };

function truncate(s: string, max = 9) {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

export default function ClusterGraph({ nodes }: Props) {
  const total = nodes.length;
  if (total === 0) return null;

  const positions  = nodes.map((_, i) => pos(i, total));
  const leaderIdx  = nodes.findIndex(n => n.state === 'leader');

  const edgeSet = new Set<string>();
  const edges: { a: number; b: number; fromLeader: boolean }[] = [];

  nodes.forEach((node, aIdx) => {
    node.peers?.forEach((peer) => {
      const peerId = peer.split(':')[0];
      const bIdx   = nodes.findIndex(n => n.node_id === peerId);
      if (bIdx === -1) return;
      const key = [Math.min(aIdx, bIdx), Math.max(aIdx, bIdx)].join('-');
      if (!edgeSet.has(key)) {
        edgeSet.add(key);
        edges.push({ a: aIdx, b: bIdx, fromLeader: aIdx === leaderIdx || bIdx === leaderIdx });
      }
    });
  });

  return (
    <div className="graph-wrap">
      <div className="section-title">Cluster Topology</div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        style={{ width: '100%', display: 'block', margin: '0 auto' }}
      >
        <defs>
          <radialGradient id="grad-leader" cx="38%" cy="32%" r="65%">
            <stop offset="0%" stopColor="#22c55e" stopOpacity=".25" />
            <stop offset="100%" stopColor="#14532d" stopOpacity="1" />
          </radialGradient>
          <radialGradient id="grad-follower" cx="38%" cy="32%" r="65%">
            <stop offset="0%" stopColor="#60a5fa" stopOpacity=".25" />
            <stop offset="100%" stopColor="#1e3a5f" stopOpacity="1" />
          </radialGradient>
          <radialGradient id="grad-candidate" cx="38%" cy="32%" r="65%">
            <stop offset="0%" stopColor="#f59e0b" stopOpacity=".25" />
            <stop offset="100%" stopColor="#78350f" stopOpacity="1" />
          </radialGradient>
          <radialGradient id="grad-default" cx="38%" cy="32%" r="65%">
            <stop offset="0%" stopColor="#9ca3af" stopOpacity=".15" />
            <stop offset="100%" stopColor="#1e2130" stopOpacity="1" />
          </radialGradient>
          <filter id="node-glow" x="-30%" y="-30%" width="160%" height="160%">
            <feGaussianBlur stdDeviation="4" result="blur" />
            <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
        </defs>

        {/* Edges */}
        {edges.map(({ a, b, fromLeader }) => {
          const pa = positions[a];
          const pb = positions[b];
          return (
            <line
              key={`${a}-${b}`}
              x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
              stroke={fromLeader ? '#3fb950' : '#30363d'}
              strokeWidth={fromLeader ? 1.5 : 1}
              strokeDasharray={fromLeader ? '6 5' : undefined}
              className={fromLeader ? 'edge-flow' : undefined}
              opacity={fromLeader ? 0.65 : 0.35}
            />
          );
        })}

        {/* Nodes */}
        {nodes.map((node, i) => {
          const p   = positions[i];
          const s   = STATE[node.state] ?? DEFAULT_STATE;
          const isLeader = node.state === 'leader';

          return (
            <g key={node.node_id}>
              {/* Leader outer glow ring */}
              {isLeader && (
                <circle
                  cx={p.x} cy={p.y} r={NODE_R + 14}
                  fill="none" stroke="#3fb950"
                  strokeWidth={1.5} className="leader-ring"
                />
              )}
              {/* Main circle */}
              <circle
                cx={p.x} cy={p.y} r={NODE_R}
                fill={`url(#${s.grad})`}
                stroke={s.stroke} strokeWidth={2}
              />
              {/* Node ID */}
              <text
                x={p.x} y={p.y - 7}
                textAnchor="middle" dominantBaseline="auto"
                fill={s.text} fontSize={12} fontWeight="700"
                fontFamily="'JetBrains Mono', 'Fira Code', monospace"
                letterSpacing="-0.3"
              >
                {truncate(node.node_id)}
              </text>
              {/* State */}
              <text
                x={p.x} y={p.y + 8}
                textAnchor="middle" dominantBaseline="auto"
                fill={s.text} fontSize={9.5} opacity={0.85}
                fontFamily="Inter, system-ui, sans-serif"
              >
                {node.state}
              </text>
              {/* Term */}
              <text
                x={p.x} y={p.y + 21}
                textAnchor="middle" dominantBaseline="auto"
                fill={s.text} fontSize={9} opacity={0.55}
                fontFamily="'JetBrains Mono', monospace"
              >
                t{node.term}
              </text>
            </g>
          );
        })}

        {/* Legend */}
        {([
          { color: '#3fb950', label: 'Leader'    },
          { color: '#58a6ff', label: 'Follower'  },
          { color: '#d29922', label: 'Candidate' },
        ] as const).map(({ color, label }, i) => (
          <g key={label} transform={`translate(${14 + i * 100}, ${H - 18})`}>
            <circle r={4} fill={color} opacity={0.8} />
            <text x={11} y={4} fill="#7d8590" fontSize={10}
              fontFamily="Inter, system-ui, sans-serif">{label}</text>
          </g>
        ))}
      </svg>
    </div>
  );
}
