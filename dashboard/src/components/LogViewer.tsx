import type { NodeStatus, LogEntry } from '../types';

interface Props {
  entries: LogEntry[];
  nodes: NodeStatus[];   // only online nodes — source selector built from this
  sourcePort: number;
  onSourceChange: (port: number) => void;
}

function parseCommand(raw: string): { type: string; key: string; value: string } {
  try {
    const obj = JSON.parse(raw);
    return {
      type:  obj.Type  ?? obj.type  ?? '?',
      key:   obj.Key   ?? obj.key   ?? '',
      value: obj.Value ?? obj.value ?? '',
    };
  } catch {
    return { type: 'raw', key: raw.slice(0, 60), value: '' };
  }
}

function EntryTag({ entry }: { entry: LogEntry }) {
  if (entry.is_config) return <span className="tag tag-config">CONFIG</span>;
  if (!entry.command)  return <span className="tag tag-noop">NOOP</span>;
  const { type } = parseCommand(entry.command);
  if (type === 'DEL' || type === 'DELETE') return <span className="tag tag-del">DEL</span>;
  return <span className="tag tag-set">SET</span>;
}

function EntryCommand({ entry }: { entry: LogEntry }) {
  if (entry.is_config) return <>{entry.config_op?.toUpperCase()} {entry.config_peer}</>;
  if (!entry.command)  return <>—</>;
  const { type, key, value } = parseCommand(entry.command);
  if (type === 'raw') return <>{entry.command.slice(0, 80)}</>;
  if (value) return <>{key} = {value}</>;
  return <>{key}</>;
}

export default function LogViewer({ entries, nodes, sourcePort, onSourceChange }: Props) {
  const sorted = [...entries].sort((a, b) => b.index - a.index);

  return (
    <div className="log-wrap">
      <div className="log-header">
        <div className="section-title" style={{ marginBottom: 0 }}>Replication Log</div>
        <div className="log-source">
          Source:
          <select
            value={sourcePort}
            onChange={e => onSourceChange(Number(e.target.value))}
          >
            {nodes.map(n => (
              <option key={n.node_id} value={n.httpPort}>
                {n.node_id}
                {n.state === 'leader' ? ' ★' : ''}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="log-body">
        {sorted.length === 0 ? (
          <div className="empty-state">
            No log entries yet — write a key to see replication in action.
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th style={{ width: 70 }}>Index</th>
                <th style={{ width: 70 }}>Term</th>
                <th style={{ width: 90 }}>Type</th>
                <th>Command</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map(e => (
                <tr key={e.index}>
                  <td style={{ color: 'var(--muted)' }}>{e.index}</td>
                  <td style={{ color: 'var(--muted)' }}>{e.term}</td>
                  <td><EntryTag entry={e} /></td>
                  <td><EntryCommand entry={e} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
