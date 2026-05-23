package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
func (noopTransport) InstallSnapshot(_ context.Context, _ string, _ InstallSnapshotArgs) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, nil
}
func (noopTransport) Close() error { return nil }

// noopStateMachine satisfies the StateMachine interface without doing anything.
type noopStateMachine struct{}

func (noopStateMachine) Apply(_ []byte) interface{}    { return nil }
func (noopStateMachine) Snapshot() ([]byte, error)     { return nil, nil }
func (noopStateMachine) Restore(_ []byte) error        { return nil }

// recordingStateMachine records every command applied to it.
// Used to verify the apply loop delivers entries in order.
type recordingStateMachine struct {
	mu      sync.Mutex
	applied []string
}

func (r *recordingStateMachine) Apply(cmd []byte) interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, string(cmd))
	return len(r.applied) // return the count as the result
}
func (r *recordingStateMachine) Snapshot() ([]byte, error) { return nil, nil }
func (r *recordingStateMachine) Restore(_ []byte) error    { return nil }

func (r *recordingStateMachine) Applied() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.applied))
	copy(out, r.applied)
	return out
}

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

// newCluster creates n nodes wired together via MemoryNetwork and starts them.
// Returns nodes and a shutdown function.
func newCluster(t *testing.T, count int) ([]*RaftNode, func()) {
	t.Helper()
	net := NewMemoryNetwork()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	nodes := make([]*RaftNode, count)
	for i, id := range ids {
		peers := make([]string, 0, count-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		cfg := Config{
			ID:                   id,
			Peers:                peers,
			Transport:            net.Transport(id),
			StateMachine:         noopStateMachine{},
			ElectionTimeoutMinMs: 150,
			ElectionTimeoutMaxMs: 300,
			HeartbeatIntervalMs:  50,
			DataDir:              t.TempDir(),
		}
		nodes[i] = NewRaftNode(cfg)
		net.Register(id, nodes[i])
	}

	for _, node := range nodes {
		go node.Run()
	}

	shutdown := func() {
		for _, node := range nodes {
			node.Stop()
		}
	}
	return nodes, shutdown
}

// waitForLeader polls until exactly one leader exists or the deadline passes.
func waitForLeader(t *testing.T, nodes []*RaftNode, timeout time.Duration) *RaftNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *RaftNode
		for _, n := range nodes {
			if n.State() == Leader {
				if leader != nil {
					t.Fatal("two leaders elected simultaneously")
				}
				leader = n
			}
		}
		if leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// TestLeaderElection verifies that a 3-node cluster elects exactly one leader.
func TestLeaderElection(t *testing.T) {
	nodes, shutdown := newCluster(t, 3)
	defer shutdown()

	waitForLeader(t, nodes, 2*time.Second)

	// Let heartbeats propagate so all nodes converge on the same term
	// before we snapshot state for assertions.
	time.Sleep(200 * time.Millisecond)

	// Re-find the leader at assertion time — the original winner may have
	// stepped down if a stale election bumped the term during startup.
	leader := waitForLeader(t, nodes, time.Second)

	// All nodes must agree on the same term.
	leaderTerm := leader.CurrentTerm()
	for _, n := range nodes {
		if term := n.CurrentTerm(); term != leaderTerm {
			t.Errorf("term mismatch: leader term=%d, node term=%d", leaderTerm, term)
		}
	}

	// Exactly one leader.
	leaders := 0
	for _, n := range nodes {
		if n.State() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected 1 leader, got %d", leaders)
	}
}

// TestLogReplication verifies that commands submitted to the leader are
// replicated to all followers and that all nodes converge on the same log.
func TestLogReplication(t *testing.T) {
	nodes, shutdown := newCluster(t, 3)
	defer shutdown()

	leader := waitForLeader(t, nodes, 2*time.Second)

	commands := []string{"SET a 1", "SET b 2", "SET c 3", "DEL a", "SET d 4"}
	for _, cmd := range commands {
		idx, _, err := leader.Submit([]byte(cmd))
		if err != nil {
			t.Fatalf("Submit(%q) failed: %v", cmd, err)
		}
		if idx == 0 {
			t.Fatalf("Submit(%q) returned index 0", cmd)
		}
	}

	// Give replication time to propagate to all nodes.
	time.Sleep(300 * time.Millisecond)

	// Every node must have all 5 entries and agree on the same log contents.
	for _, n := range nodes {
		n.mu.Lock()
		last := n.log.lastIndex()
		entries := make([]LogEntry, last)
		for i := uint64(1); i <= last; i++ {
			entries[i-1] = n.log.get(i)
		}
		n.mu.Unlock()

		if last != uint64(len(commands)) {
			t.Errorf("node %s: expected lastIndex=%d, got %d", n.id, len(commands), last)
			continue
		}
		for i, cmd := range commands {
			if string(entries[i].Command) != cmd {
				t.Errorf("node %s entry %d: expected %q, got %q", n.id, i+1, cmd, entries[i].Command)
			}
		}
	}

	// commitIndex must have advanced on the leader.
	leader.mu.Lock()
	ci := leader.commitIndex
	leader.mu.Unlock()
	if ci != uint64(len(commands)) {
		t.Errorf("leader commitIndex: expected %d, got %d", len(commands), ci)
	}
}

// TestSubmitOnNonLeader verifies Submit returns ErrNotLeader on followers.
func TestSubmitOnNonLeader(t *testing.T) {
	n, _ := newTestNode(t, "node1")
	// node1 starts as follower
	_, _, err := n.Submit([]byte("SET x 1"))
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

// TestApplyLoop verifies that committed entries are delivered on ApplyCh in
// index order and that the state machine receives every command exactly once.
func TestApplyLoop(t *testing.T) {
	net := NewMemoryNetwork()
	ids := []string{"node1", "node2", "node3"}

	sms := make([]*recordingStateMachine, len(ids))
	nodes := make([]*RaftNode, len(ids))

	for i, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		sms[i] = &recordingStateMachine{}
		cfg := Config{
			ID: id, Peers: peers,
			Transport:            net.Transport(id),
			StateMachine:         sms[i],
			ElectionTimeoutMinMs: 150, ElectionTimeoutMaxMs: 300,
			HeartbeatIntervalMs: 50,
			DataDir:             t.TempDir(),
		}
		nodes[i] = NewRaftNode(cfg)
		net.Register(id, nodes[i])
	}

	for _, node := range nodes {
		go node.Run()
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 2*time.Second)

	commands := []string{"SET a 1", "SET b 2", "SET c 3", "DEL a", "SET d 4"}
	for _, cmd := range commands {
		if _, _, err := leader.Submit([]byte(cmd)); err != nil {
			t.Fatalf("Submit(%q) failed: %v", cmd, err)
		}
	}

	// Drain ApplyCh on the leader — must receive all 5 in order.
	applyCh := leader.ApplyCh()
	received := make([]ApplyMsg, 0, len(commands))
	timeout := time.After(2 * time.Second)
	for len(received) < len(commands) {
		select {
		case msg := <-applyCh:
			received = append(received, msg)
		case <-timeout:
			t.Fatalf("timed out waiting for apply messages, got %d/%d", len(received), len(commands))
		}
	}

	// Verify index, term, and command for each message.
	for i, msg := range received {
		if msg.Index != uint64(i+1) {
			t.Errorf("msg[%d]: expected index %d, got %d", i, i+1, msg.Index)
		}
		if string(msg.Command) != commands[i] {
			t.Errorf("msg[%d]: expected command %q, got %q", i, commands[i], msg.Command)
		}
		if msg.Result.(int) != i+1 {
			t.Errorf("msg[%d]: expected result %d, got %v", i, i+1, msg.Result)
		}
	}

	// Give followers time to apply via their own apply loops.
	time.Sleep(300 * time.Millisecond)

	// Every node's state machine must have the same commands applied in the same order.
	for i, sm := range sms {
		got := sm.Applied()
		if len(got) != len(commands) {
			t.Errorf("node %s: state machine applied %d commands, want %d", ids[i], len(got), len(commands))
			continue
		}
		for j, cmd := range commands {
			if got[j] != cmd {
				t.Errorf("node %s apply[%d]: expected %q, got %q", ids[i], j, cmd, got[j])
			}
		}
	}
}

// TestTakeSnapshot verifies that TakeSnapshot compacts the log through lastApplied,
// updates the snapshot index, and persists the snapshot file to disk.
func TestTakeSnapshot(t *testing.T) {
	n, dir := newTestNode(t, "node1")

	// Seed the log and set lastApplied as if three entries have been applied.
	n.mu.Lock()
	n.log.append(LogEntry{Term: 1, Index: 1, Command: []byte("a")})
	n.log.append(LogEntry{Term: 1, Index: 2, Command: []byte("b")})
	n.log.append(LogEntry{Term: 1, Index: 3, Command: []byte("c")})
	n.log.append(LogEntry{Term: 1, Index: 4, Command: []byte("d")})
	n.log.append(LogEntry{Term: 1, Index: 5, Command: []byte("e")})
	n.lastApplied = 3
	n.mu.Unlock()

	if err := n.TakeSnapshot([]byte("snap-data")); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	n.mu.Lock()
	snapIdx := n.log.snapshotIndex()
	snapTerm := n.log.snapshotTerm()
	lastIdx := n.log.lastIndex()
	e2 := n.log.get(2) // compacted
	e4 := n.log.get(4) // still present
	n.mu.Unlock()

	if snapIdx != 3 {
		t.Fatalf("expected snapshotIndex=3, got %d", snapIdx)
	}
	if snapTerm != 1 {
		t.Fatalf("expected snapshotTerm=1, got %d", snapTerm)
	}
	if lastIdx != 5 {
		t.Fatalf("expected lastIndex=5 (entries 4,5 survive compaction), got %d", lastIdx)
	}
	if e2.Term != 0 {
		t.Fatal("entry 2 should be compacted away")
	}
	if e4.Term != 1 {
		t.Fatalf("entry 4 should still exist after compaction, got %+v", e4)
	}

	// Snapshot file must exist on disk.
	if _, err := os.Stat(filepath.Join(dir, "raft-snapshot.bin")); err != nil {
		t.Fatalf("snapshot file should exist: %v", err)
	}

	// Second TakeSnapshot at the same lastApplied must be a no-op.
	if err := n.TakeSnapshot([]byte("snap-data-2")); err != nil {
		t.Fatalf("second TakeSnapshot failed: %v", err)
	}
	n.mu.Lock()
	snapIdx2 := n.log.snapshotIndex()
	n.mu.Unlock()
	if snapIdx2 != 3 {
		t.Fatalf("second TakeSnapshot should be a no-op, snapshotIndex changed to %d", snapIdx2)
	}
}

// TestInstallSnapshot verifies that HandleInstallSnapshot updates the node's
// log boundary, applied/commit indices, and queues a restore notification.
func TestInstallSnapshot(t *testing.T) {
	n, _ := newTestNode(t, "node1")

	// Put the node in term 1 so the incoming snapshot term is valid.
	n.mu.Lock()
	n.currentTerm = 1
	n.mu.Unlock()

	args := InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot-bytes"),
	}
	reply := n.HandleInstallSnapshot(args)

	if reply.Term != 1 {
		t.Fatalf("expected reply.Term=1, got %d", reply.Term)
	}

	n.mu.Lock()
	snapIdx := n.log.snapshotIndex()
	snapTerm := n.log.snapshotTerm()
	lastApplied := n.lastApplied
	commitIdx := n.commitIndex
	n.mu.Unlock()

	if snapIdx != 5 {
		t.Fatalf("expected snapshotIndex=5, got %d", snapIdx)
	}
	if snapTerm != 1 {
		t.Fatalf("expected snapshotTerm=1, got %d", snapTerm)
	}
	if lastApplied != 5 {
		t.Fatalf("expected lastApplied=5, got %d", lastApplied)
	}
	if commitIdx != 5 {
		t.Fatalf("expected commitIndex=5, got %d", commitIdx)
	}

	// The apply loop must have a pending restore notification.
	select {
	case snap := <-n.snapshotNotifyC:
		if snap.index != 5 || snap.term != 1 {
			t.Fatalf("wrong snapshot in channel: %+v", snap)
		}
		if string(snap.data) != "snapshot-bytes" {
			t.Fatalf("wrong snapshot data: %q", snap.data)
		}
	default:
		t.Fatal("expected a pending snapshot notification on snapshotNotifyC")
	}

	// Stale snapshot (lower index) must be rejected.
	stale := InstallSnapshotArgs{Term: 1, LeaderID: "node2", LastIncludedIndex: 3, LastIncludedTerm: 1}
	n.HandleInstallSnapshot(stale)
	n.mu.Lock()
	snapIdxAfter := n.log.snapshotIndex()
	n.mu.Unlock()
	if snapIdxAfter != 5 {
		t.Fatalf("stale snapshot should not regress snapshotIndex, got %d", snapIdxAfter)
	}
}

// TestSnapshotAndRestart verifies that a node restores its state from a snapshot
// on restart: the state machine is restored and lastApplied/commitIndex are set.
func TestSnapshotAndRestart(t *testing.T) {
	n, dir := newTestNode(t, "node1")

	// Simulate having applied three entries and taking a snapshot.
	n.mu.Lock()
	n.log.append(LogEntry{Term: 1, Index: 1, Command: []byte("a")})
	n.log.append(LogEntry{Term: 1, Index: 2, Command: []byte("b")})
	n.log.append(LogEntry{Term: 1, Index: 3, Command: []byte("c")})
	n.lastApplied = 3
	n.mu.Unlock()

	if err := n.TakeSnapshot([]byte("state")); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Simulate restart: fresh node, same DataDir.
	sm := &recordingStateMachine{}
	cfg := Config{
		ID: "node1", Peers: []string{"node2", "node3"},
		Transport: noopTransport{}, StateMachine: sm,
		ElectionTimeoutMinMs: 150, ElectionTimeoutMaxMs: 300,
		HeartbeatIntervalMs: 50, DataDir: dir,
	}
	n2 := NewRaftNode(cfg)
	if err := n2.loadState(); err != nil {
		t.Fatalf("loadState: %v", err)
	}

	// restoreFromSnapshot is called by Run(); invoke it directly here to avoid
	// starting the full event loop in a unit test.
	n2.restoreFromSnapshot()

	n2.mu.Lock()
	snapIdx := n2.log.snapshotIndex()
	lastApplied := n2.lastApplied
	commitIdx := n2.commitIndex
	n2.mu.Unlock()

	if snapIdx != 3 {
		t.Fatalf("restarted node should have snapshotIndex=3, got %d", snapIdx)
	}
	if lastApplied != 3 {
		t.Fatalf("restarted node should have lastApplied=3, got %d", lastApplied)
	}
	if commitIdx != 3 {
		t.Fatalf("restarted node should have commitIndex=3, got %d", commitIdx)
	}
}

// TestLeaderStaysLeader verifies the leader does not get displaced when the
// cluster is healthy — its heartbeats must suppress follower election timeouts.
// Requires Checkpoint 4 (heartbeat sending) to pass.
func TestLeaderStaysLeader(t *testing.T) {
	nodes, shutdown := newCluster(t, 3)
	defer shutdown()

	first := waitForLeader(t, nodes, 2*time.Second)

	// Wait several heartbeat intervals and confirm the same node is still leader.
	time.Sleep(300 * time.Millisecond)

	if first.State() != Leader {
		t.Fatal("leader was displaced without any failure")
	}
}
