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
}

// Log holds the replicated log for a single Raft node.
// Indexes are 1-based; index 0 is a sentinel (zero value LogEntry).
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

// append adds entries to the log. Callers must ensure entries are contiguous.
func (l *Log) append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// get returns the entry at the given 1-based index.
// Returns the zero-value sentinel if index is out of range.
func (l *Log) get(index uint64) LogEntry {
	if index >= uint64(len(l.entries)) {
		return LogEntry{}
	}
	return l.entries[index]
}

// lastIndex returns the index of the last entry (0 if log is empty).
func (l *Log) lastIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

// lastTerm returns the term of the last entry (0 if log is empty).
func (l *Log) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// truncateFrom removes all entries from index onward (inclusive).
// Used when a follower detects a conflict and must overwrite.
func (l *Log) truncateFrom(index uint64) {
	if index < uint64(len(l.entries)) {
		l.entries = l.entries[:index]
	}
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

// slice returns entries in the half-open range [from, to).
func (l *Log) slice(from, to uint64) []LogEntry {
	if from >= uint64(len(l.entries)) {
		return nil
	}
	if to > uint64(len(l.entries)) {
		to = uint64(len(l.entries))
	}
	return l.entries[from:to]
}
