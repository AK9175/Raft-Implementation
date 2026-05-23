package kvstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/atharva/raft/raft"
)

// newTestCluster creates count KVStore+RaftNode pairs wired together via MemoryNetwork.
func newTestCluster(t *testing.T, count int) ([]*KVStore, []*raft.RaftNode, func()) {
	t.Helper()
	net := raft.NewMemoryNetwork()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	stores := make([]*KVStore, count)
	nodes := make([]*raft.RaftNode, count)

	for i, id := range ids {
		peers := make([]string, 0, count-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		cfg := raft.Config{
			ID:                   id,
			Peers:                peers,
			Transport:            net.Transport(id),
			ElectionTimeoutMinMs: 150,
			ElectionTimeoutMaxMs: 300,
			HeartbeatIntervalMs:  50,
			DataDir:              t.TempDir(),
		}
		stores[i], nodes[i] = New(cfg)
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
	return stores, nodes, shutdown
}

// waitForLeaderStore polls until one of the nodes is a leader, then returns
// the matching KVStore.
func waitForLeaderStore(t *testing.T, stores []*KVStore, nodes []*raft.RaftNode, timeout time.Duration) *KVStore {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if node.State() == raft.Leader {
				return stores[i]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// TestKVStoreSetGet verifies basic Set then Get on the leader.
func TestKVStoreSetGet(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	leader := waitForLeaderStore(t, stores, nodes, 2*time.Second)

	if err := leader.Set("hello", "world"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	val, err := leader.Get("hello")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "world" {
		t.Fatalf("expected %q, got %q", "world", val)
	}
}

// TestKVStoreOverwrite verifies that Set on an existing key replaces the value.
func TestKVStoreOverwrite(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	leader := waitForLeaderStore(t, stores, nodes, 2*time.Second)

	if err := leader.Set("k", "first"); err != nil {
		t.Fatalf("first Set failed: %v", err)
	}
	if err := leader.Set("k", "second"); err != nil {
		t.Fatalf("second Set failed: %v", err)
	}
	val, err := leader.Get("k")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "second" {
		t.Fatalf("expected %q, got %q", "second", val)
	}
}

// TestKVStoreDelete verifies that Delete removes the key so Get returns ErrKeyNotFound.
func TestKVStoreDelete(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	leader := waitForLeaderStore(t, stores, nodes, 2*time.Second)

	if err := leader.Set("key", "value"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := leader.Delete("key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err := leader.Get("key")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after Delete, got %v", err)
	}
}

// TestKVStoreKeyNotFound verifies Get on a missing key returns ErrKeyNotFound.
func TestKVStoreKeyNotFound(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	leader := waitForLeaderStore(t, stores, nodes, 2*time.Second)

	_, err := leader.Get("no-such-key")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// TestKVStoreNotLeader verifies that Set on a follower returns ErrNotLeader.
func TestKVStoreNotLeader(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	waitForLeaderStore(t, stores, nodes, 2*time.Second)
	// Let the cluster stabilize so followers don't start new elections.
	time.Sleep(200 * time.Millisecond)

	var follower *KVStore
	for i, node := range nodes {
		if node.State() != raft.Leader {
			follower = stores[i]
			break
		}
	}
	if follower == nil {
		t.Skip("no follower found — single-node cluster?")
	}

	err := follower.Set("x", "1")
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader from follower, got %v", err)
	}
}

// TestKVStoreReplication verifies that commands committed on the leader are
// applied to every follower's state machine.
func TestKVStoreReplication(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	leader := waitForLeaderStore(t, stores, nodes, 2*time.Second)

	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range want {
		if err := leader.Set(k, v); err != nil {
			t.Fatalf("Set(%q, %q) failed: %v", k, v, err)
		}
	}

	// Give followers time to apply the entries.
	time.Sleep(300 * time.Millisecond)

	for i, store := range stores {
		store.mu.RLock()
		for k, wantV := range want {
			if got := store.data[k]; got != wantV {
				t.Errorf("store[%d] key=%q: want %q, got %q", i, k, wantV, got)
			}
		}
		store.mu.RUnlock()
		_ = nodes[i]
	}
}

// TestKVStoreOrderPreserved verifies commands are applied in submission order.
func TestKVStoreOrderPreserved(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	leader := waitForLeaderStore(t, stores, nodes, 2*time.Second)

	// SET x=1, SET x=2, SET x=3 — last write wins
	for i := 1; i <= 3; i++ {
		if err := leader.Set("x", fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("Set iteration %d failed: %v", i, err)
		}
	}
	val, err := leader.Get("x")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "3" {
		t.Fatalf("expected last write to win, got %q", val)
	}
}

// TestKVStoreSnapshotRoundTrip verifies Snapshot/Restore serialization.
// No Raft cluster needed — tests the encoding layer directly.
func TestKVStoreSnapshotRoundTrip(t *testing.T) {
	kv := &KVStore{
		data:    map[string]string{"alpha": "1", "beta": "2", "gamma": "3"},
		pending: make(map[uint64]pendingCall),
	}

	data, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Snapshot returned empty bytes")
	}

	kv2 := &KVStore{
		data:    make(map[string]string),
		pending: make(map[uint64]pendingCall),
	}
	if err := kv2.Restore(data); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	kv2.mu.RLock()
	defer kv2.mu.RUnlock()
	for k, want := range kv.data {
		if got := kv2.data[k]; got != want {
			t.Errorf("key %q: want %q, got %q", k, want, got)
		}
	}
	if len(kv2.data) != len(kv.data) {
		t.Errorf("restored map has %d keys, want %d", len(kv2.data), len(kv.data))
	}
}

// TestKVStoreSnapshotThenGet verifies that after a snapshot is taken and the
// log is compacted, the state machine still returns the correct values.
func TestKVStoreSnapshotThenGet(t *testing.T) {
	stores, nodes, shutdown := newTestCluster(t, 3)
	defer shutdown()

	leader := waitForLeaderStore(t, stores, nodes, 2*time.Second)
	// Disable auto-snapshot so we control when it happens.
	leader.snapshotThreshold = 0

	// Find leader node for TakeSnapshot.
	var leaderNode *raft.RaftNode
	for i, node := range nodes {
		if node.State() == raft.Leader {
			leaderNode = nodes[i]
			break
		}
	}

	for i := 0; i < 5; i++ {
		if err := leader.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)); err != nil {
			t.Fatalf("Set[%d] failed: %v", i, err)
		}
	}

	// Take a manual snapshot.
	snapData, err := leader.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if err := leaderNode.TakeSnapshot(snapData); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Reads after compaction must still work.
	for i := 0; i < 5; i++ {
		val, err := leader.Get(fmt.Sprintf("key%d", i))
		if err != nil {
			t.Fatalf("Get(key%d) after snapshot failed: %v", i, err)
		}
		if want := fmt.Sprintf("val%d", i); val != want {
			t.Errorf("key%d: want %q, got %q", i, want, val)
		}
	}
}
