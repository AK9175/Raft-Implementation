package raft

// LogEntry is a single entry in the replicated log.
// term: which leader's tenure this entry was created in.
// index: 1-based position in the log (0 means "no entry").
// Command: the opaque bytes the state machine will interpret.
//
// Raft §5.3: "Each log entry stores a state machine command along with
// the term number when the entry was received by the leader."
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte

	// Membership change fields — only populated when IsConfig is true.
	// Raft §6: single-server changes are safe with overlapping majorities.
	IsConfig   bool   // true for membership change entries
	ConfigOp   string // "add" or "remove"
	ConfigPeer string // RPC address of the peer being added or removed
}

// Log holds the replicated log for a single Raft node.
//
// entries[0] is always the sentinel. For a fresh log it is {0,0,nil}.
// After compaction through index N it becomes {N, term, nil}, representing
// the last entry included in the snapshot. All methods use entries[0].Index
// as the offset so callers always work in logical (1-based) indices.
type Log struct {
	entries []LogEntry
}

// newLog creates a Log with a sentinel entry at index 0 so real entries
// start at index 1. This simplifies off-by-one arithmetic throughout.
func newLog() *Log {
	return &Log{
		entries: []LogEntry{{Term: 0, Index: 0}}, // sentinel
	}
}

// snapshotIndex returns the index of the last compacted entry (0 for a fresh log).
func (l *Log) snapshotIndex() uint64 { return l.entries[0].Index }

// snapshotTerm returns the term of the last compacted entry (0 for a fresh log).
func (l *Log) snapshotTerm() uint64 { return l.entries[0].Term }

// append adds entries to the log. Callers must ensure entries are contiguous.
func (l *Log) append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// get returns the entry at the given 1-based logical index.
// Returns a zero-value LogEntry if the index is out of range or compacted away.
func (l *Log) get(index uint64) LogEntry {
	offset := l.entries[0].Index
	if index < offset || index-offset >= uint64(len(l.entries)) {
		return LogEntry{}
	}
	return l.entries[index-offset]
}

// lastIndex returns the logical index of the last entry (snapshotIndex if log is empty).
func (l *Log) lastIndex() uint64 {
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// lastTerm returns the term of the last entry.
func (l *Log) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// truncateFrom removes all entries from logical index onward (inclusive).
// Safe to call with any index — will not remove the sentinel.
func (l *Log) truncateFrom(index uint64) {
	offset := l.entries[0].Index
	if index <= offset {
		return // cannot truncate at or before the snapshot boundary
	}
	physical := index - offset
	if physical < uint64(len(l.entries)) {
		l.entries = l.entries[:physical]
	}
}

// compactTo discards all entries up through index, making index the new sentinel.
// This is called after a snapshot is taken — entries before the snapshot are
// no longer needed for log replication.
// No-op if index is already at or before the current snapshot boundary.
func (l *Log) compactTo(index, term uint64) {
	offset := l.entries[0].Index
	if index <= offset {
		return
	}
	physical := index - offset
	if physical >= uint64(len(l.entries)) {
		// Snapshot is past our log end — discard everything.
		l.entries = []LogEntry{{Index: index, Term: term}}
		return
	}
	// Build a new slice so the old backing array can be GC'd.
	//Old part from 0 to physical can be garbage collected
	remaining := make([]LogEntry, 0, uint64(len(l.entries))-physical)
	remaining = append(remaining, LogEntry{Index: index, Term: term})
	remaining = append(remaining, l.entries[physical+1:]...)
	l.entries = remaining
}

// firstIndexForTerm returns the first index in the log that has the given term.
// Returns 0 if no entry with that term exists.
// Used by followers to populate ConflictIndex in AppendEntriesReply.
func (l *Log) firstIndexForTerm(term uint64) uint64 {
	for _, e := range l.entries {
		if e.Term == term {
			return e.Index
		}
	}
	return 0
}

// slice returns entries in the half-open logical range [from, to).
// Automatically clamps from to the first available entry after the snapshot.
func (l *Log) slice(from, to uint64) []LogEntry {
	offset := l.entries[0].Index
	if from <= offset {
		from = offset + 1 // skip compacted entries
	}
	if from >= to {
		return nil
	}
	physFrom := from - offset
	physTo := to - offset
	if physFrom >= uint64(len(l.entries)) {
		return nil
	}
	if physTo > uint64(len(l.entries)) {
		physTo = uint64(len(l.entries))
	}
	return l.entries[physFrom:physTo]
}
