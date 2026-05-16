package raft

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// noopTransport satisfies the Transport interface without doing anything.
// Used in tests that don't need real network communication.
type noopTransport struct{}

func (noopTransport) RequestVote(_ context.Context, _ string, _ RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, nil
}
func (noopTransport) AppendEntries(_ context.Context, _ string, _ AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, nil
}
func (noopTransport) Close() error { return nil }

// noopStateMachine satisfies the StateMachine interface without doing anything.
type noopStateMachine struct{}

func (noopStateMachine) Apply(_ []byte) interface{}    { return nil }
func (noopStateMachine) Snapshot() ([]byte, error)     { return nil, nil }
func (noopStateMachine) Restore(_ []byte) error        { return nil }

func newTestNode(t *testing.T, id string) (*RaftNode, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		ID:                   id,
		Peers:                []string{"node2", "node3"},
		Transport:            noopTransport{},
		StateMachine:         noopStateMachine{},
		ElectionTimeoutMinMs: 150,
		ElectionTimeoutMaxMs: 300,
		HeartbeatIntervalMs:  50,
		DataDir:              dir,
	}
	return NewRaftNode(cfg), dir
}

// TestInitialState verifies a new node starts as follower in term 0.
func TestInitialState(t *testing.T) {
	n, _ := newTestNode(t, "node1")

	if n.State() != Follower {
		t.Fatalf("expected Follower, got %s", n.State())
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("expected term 0, got %d", n.CurrentTerm())
	}
}

// TestBecomeCandidate verifies term increment and self-vote.
func TestBecomeCandidate(t *testing.T) {
	n, _ := newTestNode(t, "node1")

	n.mu.Lock()
	n.becomeCandidate()
	term := n.currentTerm
	votedFor := n.votedFor
	state := n.state
	n.mu.Unlock()

	if state != Candidate {
		t.Fatalf("expected Candidate, got %s", state)
	}
	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}
	if votedFor != "node1" {
		t.Fatalf("expected votedFor=node1, got %q", votedFor)
	}
}

// TestBecomeLeader verifies nextIndex and matchIndex are initialized correctly.
func TestBecomeLeader(t *testing.T) {
	n, _ := newTestNode(t, "node1")

	n.mu.Lock()
	n.becomeCandidate()
	n.becomeLeader()
	state := n.state
	next2 := n.nextIndex["node2"]
	next3 := n.nextIndex["node3"]
	match2 := n.matchIndex["node2"]
	n.mu.Unlock()

	if state != Leader {
		t.Fatalf("expected Leader, got %s", state)
	}
	// fresh log: lastIndex=0, so nextIndex should be 1
	if next2 != 1 || next3 != 1 {
		t.Fatalf("expected nextIndex=1, got node2=%d node3=%d", next2, next3)
	}
	if match2 != 0 {
		t.Fatalf("expected matchIndex=0, got %d", match2)
	}
}

// TestBecomeFollower verifies stepping down clears votedFor and updates term.
func TestBecomeFollower(t *testing.T) {
	n, _ := newTestNode(t, "node1")

	n.mu.Lock()
	n.becomeCandidate() // term=1, votedFor=node1
	n.becomeFollower(5) // higher term seen
	term := n.currentTerm
	votedFor := n.votedFor
	state := n.state
	n.mu.Unlock()

	if state != Follower {
		t.Fatalf("expected Follower, got %s", state)
	}
	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}
	if votedFor != "" {
		t.Fatalf("expected empty votedFor, got %q", votedFor)
	}
}

// TestPersistAndReload verifies that currentTerm, votedFor, and log survive a restart.
func TestPersistAndReload(t *testing.T) {
	n, dir := newTestNode(t, "node1")

	// advance state
	n.mu.Lock()
	n.becomeCandidate() // term=1, votedFor=node1
	n.log.append(LogEntry{Term: 1, Index: 1, Command: []byte("SET foo bar")})
	n.persist()
	n.mu.Unlock()

	// simulate restart: create a fresh node with the same DataDir
	cfg := Config{
		ID:                   "node1",
		Peers:                []string{"node2", "node3"},
		Transport:            noopTransport{},
		StateMachine:         noopStateMachine{},
		ElectionTimeoutMinMs: 150,
		ElectionTimeoutMaxMs: 300,
		HeartbeatIntervalMs:  50,
		DataDir:              dir,
	}
	n2 := NewRaftNode(cfg)
	if err := n2.loadState(); err != nil {
		t.Fatalf("loadState failed: %v", err)
	}

	n2.mu.Lock()
	term := n2.currentTerm
	votedFor := n2.votedFor
	lastIdx := n2.log.lastIndex()
	n2.mu.Unlock()

	if term != 1 {
		t.Fatalf("expected term 1 after reload, got %d", term)
	}
	if votedFor != "node1" {
		t.Fatalf("expected votedFor=node1 after reload, got %q", votedFor)
	}
	if lastIdx != 1 {
		t.Fatalf("expected lastIndex=1 after reload, got %d", lastIdx)
	}
}

// TestHigherTermStepsDown verifies the golden rule: any higher term causes
// immediate step-down regardless of current state.
func TestHigherTermStepsDown(t *testing.T) {
	n, _ := newTestNode(t, "node1")

	n.mu.Lock()
	n.becomeCandidate()
	n.becomeLeader()
	n.mu.Unlock()

	// simulate receiving an RPC from a node with a higher term
	args := AppendEntriesArgs{Term: 10, LeaderID: "node2"}
	reply := n.HandleAppendEntries(args)

	if n.State() != Follower {
		t.Fatalf("expected Follower after higher term, got %s", n.State())
	}
	if n.CurrentTerm() != 10 {
		t.Fatalf("expected term 10, got %d", n.CurrentTerm())
	}
	if reply.Term != 10 {
		t.Fatalf("expected reply.Term=10, got %d", reply.Term)
	}
}

// TestElectionTimerFires verifies that a node becomes a candidate
// when no heartbeat is received before the election timeout.
func TestElectionTimerFires(t *testing.T) {
	n, _ := newTestNode(t, "node1")

	// use a very short timeout so the test doesn't take long
	n.config.ElectionTimeoutMinMs = 50
	n.config.ElectionTimeoutMaxMs = 75

	go n.Run()
	defer n.Stop()

	// wait longer than the max election timeout
	time.Sleep(200 * time.Millisecond)

	if n.State() == Follower {
		t.Fatal("expected node to have left Follower state after election timeout")
	}
}

// TestFreshNodeNoStateFile verifies loadState is a no-op when no file exists.
func TestFreshNodeNoStateFile(t *testing.T) {
	dir := t.TempDir()
	// remove any file that might exist
	os.Remove(filepath.Join(dir, "raft-state.bin"))

	cfg := Config{
		ID: "node1", DataDir: dir,
		Transport: noopTransport{}, StateMachine: noopStateMachine{},
		ElectionTimeoutMinMs: 150, ElectionTimeoutMaxMs: 300, HeartbeatIntervalMs: 50,
	}
	n := NewRaftNode(cfg)
	if err := n.loadState(); err != nil {
		t.Fatalf("loadState on fresh node should return nil, got %v", err)
	}
	if n.CurrentTerm() != 0 {
		t.Fatalf("expected term 0 on fresh node, got %d", n.CurrentTerm())
	}
}
