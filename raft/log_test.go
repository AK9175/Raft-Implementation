package raft

import (
	"bytes"
	"testing"
)

// makeEntry is a helper to build a LogEntry with less boilerplate.
func makeEntry(index, term uint64, cmd string) LogEntry {
	return LogEntry{Index: index, Term: term, Command: []byte(cmd)}
}

// TestSentinel verifies the sentinel at index 0 is always present and
// returns zero values. This is the foundation of 1-based indexing.
func TestSentinel(t *testing.T) {
	l := newLog()

	sentinel := l.get(0)
	if sentinel.Term != 0 || sentinel.Index != 0 || sentinel.Command != nil {
		t.Fatalf("sentinel should be zero value, got %+v", sentinel)
	}

	// fresh log: lastIndex=0 (only the sentinel exists)
	if l.lastIndex() != 0 {
		t.Fatalf("fresh log lastIndex should be 0, got %d", l.lastIndex())
	}

	// fresh log: lastTerm=0
	if l.lastTerm() != 0 {
		t.Fatalf("fresh log lastTerm should be 0, got %d", l.lastTerm())
	}
}

// TestAppendAndGet verifies entries are appended in order and retrieved correctly.
func TestAppendAndGet(t *testing.T) {
	l := newLog()

	l.append(makeEntry(1, 1, "SET foo bar"))
	l.append(makeEntry(2, 1, "SET baz qux"))
	l.append(makeEntry(3, 2, "DEL foo"))

	if l.lastIndex() != 3 {
		t.Fatalf("expected lastIndex=3, got %d", l.lastIndex())
	}
	if l.lastTerm() != 2 {
		t.Fatalf("expected lastTerm=2, got %d", l.lastTerm())
	}

	e := l.get(2)
	if e.Index != 2 || e.Term != 1 {
		t.Fatalf("get(2) wrong: %+v", e)
	}
	if !bytes.Equal(e.Command, []byte("SET baz qux")) {
		t.Fatalf("get(2) wrong command: %s", e.Command)
	}
}

// TestGetOutOfRange verifies get() returns zero value for missing indices.
// This is important — callers rely on Term==0 to mean "no entry".
func TestGetOutOfRange(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "SET foo bar"))

	e := l.get(99)
	if e.Term != 0 || e.Index != 0 {
		t.Fatalf("out of range get should return zero value, got %+v", e)
	}
}

// TestTruncateFrom verifies that truncation removes entries from the given
// index onward and leaves earlier entries intact.
func TestTruncateFrom(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))
	l.append(makeEntry(3, 2, "c"))
	l.append(makeEntry(4, 2, "d"))

	// truncate from index 3 — removes entries 3 and 4
	l.truncateFrom(3)

	if l.lastIndex() != 2 {
		t.Fatalf("expected lastIndex=2 after truncate, got %d", l.lastIndex())
	}

	// entries 1 and 2 must be intact
	if l.get(1).Command == nil || string(l.get(1).Command) != "a" {
		t.Fatalf("entry 1 should survive truncation")
	}
	if l.get(2).Command == nil || string(l.get(2).Command) != "b" {
		t.Fatalf("entry 2 should survive truncation")
	}

	// entries 3 and 4 must be gone
	if l.get(3).Term != 0 {
		t.Fatalf("entry 3 should be gone after truncation")
	}
}

// TestTruncateThenAppend simulates the conflict resolution flow:
// follower detects conflict → truncateFrom → append leader's entries.
// This is the exact sequence HandleAppendEntries will use in Checkpoint 6.
func TestTruncateThenAppend(t *testing.T) {
	l := newLog()

	// follower had stale entries from old leader (term 3)
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))
	l.append(makeEntry(3, 2, "c"))
	l.append(makeEntry(4, 3, "stale"))
	l.append(makeEntry(5, 3, "stale"))

	// new leader sends correct entries starting at index 4 (term 2)
	l.truncateFrom(4)
	l.append(makeEntry(4, 2, "correct"))
	l.append(makeEntry(5, 2, "correct2"))

	if l.lastIndex() != 5 {
		t.Fatalf("expected lastIndex=5, got %d", l.lastIndex())
	}
	if l.get(4).Term != 2 {
		t.Fatalf("entry 4 should be term 2, got %d", l.get(4).Term)
	}
	if string(l.get(4).Command) != "correct" {
		t.Fatalf("entry 4 should be 'correct', got %s", l.get(4).Command)
	}
}

// TestSlice verifies the half-open range [from, to) returns correct entries.
// The leader uses slice(nextIndex[peer], lastIndex+1) to build AppendEntries.
func TestSlice(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))
	l.append(makeEntry(3, 2, "c"))
	l.append(makeEntry(4, 2, "d"))
	l.append(makeEntry(5, 2, "e"))

	// get entries 2, 3, 4
	entries := l.slice(2, 5)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Index != 2 || entries[2].Index != 4 {
		t.Fatalf("wrong entries returned: %+v", entries)
	}

	// slice beyond end should clamp to last entry
	entries = l.slice(4, 100)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries when to>lastIndex, got %d", len(entries))
	}

	// slice from beyond end should return nil
	entries = l.slice(99, 100)
	if entries != nil {
		t.Fatalf("expected nil for out of range slice, got %v", entries)
	}
}

// TestFirstIndexForTerm verifies the conflict optimization helper.
// The follower sends ConflictIndex = firstIndexForTerm(conflictTerm)
// so the leader can skip back an entire term in one round trip.
func TestFirstIndexForTerm(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))
	l.append(makeEntry(3, 2, "c"))
	l.append(makeEntry(4, 2, "d"))
	l.append(makeEntry(5, 3, "e"))

	if l.firstIndexForTerm(1) != 1 {
		t.Fatalf("firstIndexForTerm(1) should be 1")
	}
	if l.firstIndexForTerm(2) != 3 {
		t.Fatalf("firstIndexForTerm(2) should be 3")
	}
	if l.firstIndexForTerm(3) != 5 {
		t.Fatalf("firstIndexForTerm(3) should be 5")
	}
	if l.firstIndexForTerm(99) != 0 {
		t.Fatalf("firstIndexForTerm for missing term should return 0")
	}
}

// TestLogMatchingProperty verifies the core Raft safety guarantee:
// if two logs agree on index+term for some entry, they are identical
// in all entries up to that index. Raft §5.3.
//
// We simulate this by building two logs that diverge after index 3,
// then verifying entries 1-3 are identical in both.
func TestLogMatchingProperty(t *testing.T) {
	// log A — leader's log
	a := newLog()
	a.append(makeEntry(1, 1, "SET foo bar"))
	a.append(makeEntry(2, 1, "SET baz qux"))
	a.append(makeEntry(3, 2, "DEL foo"))
	a.append(makeEntry(4, 2, "SET x 1"))
	a.append(makeEntry(5, 2, "SET y 2"))

	// log B — follower's log, diverges after index 3
	b := newLog()
	b.append(makeEntry(1, 1, "SET foo bar"))
	b.append(makeEntry(2, 1, "SET baz qux"))
	b.append(makeEntry(3, 2, "DEL foo"))
	b.append(makeEntry(4, 3, "stale from old leader"))
	b.append(makeEntry(5, 3, "stale from old leader"))

	// logs agree at index 3, term 2 → entries 1-3 must be identical
	for i := uint64(1); i <= 3; i++ {
		ea := a.get(i)
		eb := b.get(i)
		if ea.Term != eb.Term || ea.Index != eb.Index {
			t.Fatalf("index %d: logs disagree on term: a=%d b=%d", i, ea.Term, eb.Term)
		}
		if !bytes.Equal(ea.Command, eb.Command) {
			t.Fatalf("index %d: logs disagree on command", i)
		}
	}

	// logs disagree at index 4 — different terms
	if a.get(4).Term == b.get(4).Term {
		t.Fatal("logs should disagree at index 4")
	}
}

// TestBatchAppend verifies appending multiple entries at once works correctly.
// The leader uses this when sending a batch of entries to a lagging follower.
func TestBatchAppend(t *testing.T) {
	l := newLog()

	batch := []LogEntry{
		makeEntry(1, 1, "a"),
		makeEntry(2, 1, "b"),
		makeEntry(3, 1, "c"),
	}
	l.append(batch...)

	if l.lastIndex() != 3 {
		t.Fatalf("expected lastIndex=3 after batch append, got %d", l.lastIndex())
	}
	for i := uint64(1); i <= 3; i++ {
		if l.get(i).Index != i {
			t.Fatalf("entry %d has wrong index %d", i, l.get(i).Index)
		}
	}
}

// TestCompactTo verifies that compactTo discards entries up through index,
// makes index the new sentinel, and leaves later entries intact.
func TestCompactTo(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))
	l.append(makeEntry(3, 2, "c"))
	l.append(makeEntry(4, 2, "d"))
	l.append(makeEntry(5, 2, "e"))

	l.compactTo(3, 2)

	if l.snapshotIndex() != 3 {
		t.Fatalf("expected snapshotIndex=3, got %d", l.snapshotIndex())
	}
	if l.snapshotTerm() != 2 {
		t.Fatalf("expected snapshotTerm=2, got %d", l.snapshotTerm())
	}
	if l.lastIndex() != 5 {
		t.Fatalf("expected lastIndex=5 (entries 4,5 survive), got %d", l.lastIndex())
	}

	// Entries 1, 2 must be gone.
	if l.get(1).Term != 0 || l.get(2).Term != 0 {
		t.Fatal("entries 1 and 2 should be compacted away")
	}

	// Entry at snapshotIndex returns the sentinel with the correct term.
	e3 := l.get(3)
	if e3.Index != 3 || e3.Term != 2 {
		t.Fatalf("get(snapshotIndex) should return sentinel {3,2}, got %+v", e3)
	}

	// Entries 4 and 5 must be intact.
	if l.get(4).Term != 2 || string(l.get(4).Command) != "d" {
		t.Fatalf("entry 4 should survive compaction, got %+v", l.get(4))
	}
	if l.get(5).Term != 2 || string(l.get(5).Command) != "e" {
		t.Fatalf("entry 5 should survive compaction, got %+v", l.get(5))
	}
}

// TestCompactToPastEnd verifies compactTo handles the case where the snapshot
// index is beyond the last log entry — all entries are discarded.
func TestCompactToPastEnd(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))

	l.compactTo(10, 3)

	if l.snapshotIndex() != 10 {
		t.Fatalf("expected snapshotIndex=10, got %d", l.snapshotIndex())
	}
	if l.lastIndex() != 10 {
		t.Fatalf("expected lastIndex=10 (only sentinel remains), got %d", l.lastIndex())
	}
	if l.get(10).Term != 3 {
		t.Fatalf("sentinel at 10 should have term 3, got %+v", l.get(10))
	}
}

// TestCompactedSlice verifies that slice() works correctly after compaction:
// requests that span the compaction boundary start from the first available entry.
func TestCompactedSlice(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))
	l.append(makeEntry(3, 2, "c"))
	l.append(makeEntry(4, 2, "d"))
	l.append(makeEntry(5, 2, "e"))

	l.compactTo(3, 2)

	// Slice entirely within remaining entries.
	entries := l.slice(4, 6)
	if len(entries) != 2 || entries[0].Index != 4 || entries[1].Index != 5 {
		t.Fatalf("expected entries [4,5], got %+v", entries)
	}

	// Slice spanning the compaction boundary — must start from first available (4).
	entries = l.slice(1, 5)
	if len(entries) != 1 || entries[0].Index != 4 {
		t.Fatalf("expected only entry 4 when request spans boundary, got %+v", entries)
	}

	// Slice entirely before the boundary — must return nil.
	entries = l.slice(1, 3)
	if entries != nil {
		t.Fatalf("expected nil for slice entirely before snapshot, got %+v", entries)
	}
}

// TestCompactToNoOp verifies compactTo is a no-op when index <= snapshotIndex.
func TestCompactToNoOp(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))
	l.append(makeEntry(2, 1, "b"))
	l.append(makeEntry(3, 1, "c"))

	l.compactTo(2, 1)
	l.compactTo(1, 1) // must not regress

	if l.snapshotIndex() != 2 {
		t.Fatalf("compactTo with lower index should be no-op, got snapshotIndex=%d", l.snapshotIndex())
	}
	if l.lastIndex() != 3 {
		t.Fatalf("entry 3 should still exist, got lastIndex=%d", l.lastIndex())
	}
}

// TestTruncateSentinelSafe verifies truncateFrom never removes the sentinel.
// The sentinel at index 0 must always be present.
func TestTruncateSentinelSafe(t *testing.T) {
	l := newLog()
	l.append(makeEntry(1, 1, "a"))

	// truncate everything including index 1
	l.truncateFrom(1)

	// sentinel must still be there
	if l.lastIndex() != 0 {
		t.Fatalf("expected lastIndex=0 after full truncate, got %d", l.lastIndex())
	}
	if l.get(0).Term != 0 {
		t.Fatalf("sentinel must survive truncation")
	}
}
