package kvstore

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
	"time"

	"github.com/atharva/raft/raft"
)

var (
	ErrNotLeader   = errors.New("kvstore: not the leader")
	ErrKeyNotFound = errors.New("kvstore: key not found")
	ErrTimeout     = errors.New("kvstore: timed out waiting for commit")
)

// op is a single KV operation encoded into the Raft log entry.
// All three operation types share the same wire format — Value is empty for GET/DEL.
type op struct {
	Type  string // "SET", "GET", "DEL"
	Key   string
	Value string
}

// applyResult is what Apply() returns for each committed entry.
// It flows through ApplyMsg.Result back to the waiting client.
type applyResult struct {
	Value string
	Err   error
}

// pendingCall represents a client goroutine blocked on call(), waiting for a
// specific log index to be committed and applied.
type pendingCall struct {
	term uint64          // term at Submit time — detects leader changes
	ch   chan applyResult // buffered(1): writer never blocks
}

// KVStore is a linearizable key-value store backed by a Raft cluster.
// It implements raft.StateMachine so the Raft node can drive it.
//
// Every write (Set/Delete) and read (Get) goes through the Raft log so all
// nodes see commands in the same order, giving strong consistency.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string

	node *raft.RaftNode

	pendingMu sync.Mutex
	pending   map[uint64]pendingCall // log index → blocked caller

	snapshotThreshold uint64 // take snapshot every N applied entries (0 = disabled)
}

// SetSnapshotThreshold changes how many applied entries trigger an auto-snapshot.
// 0 disables auto-snapshotting. Safe to call before node.Run().
func (kv *KVStore) SetSnapshotThreshold(n uint64) { kv.snapshotThreshold = n }

// New creates a KVStore and a RaftNode wired together.
// It sets cfg.StateMachine to the new store, so callers must not set it themselves.
// The caller is responsible for starting the node: go node.Run().
func New(cfg raft.Config) (*KVStore, *raft.RaftNode) {
	kv := &KVStore{
		data:              make(map[string]string),
		pending:           make(map[uint64]pendingCall),
		snapshotThreshold: 100,
	}
	cfg.StateMachine = kv
	node := raft.NewRaftNode(cfg)
	kv.node = node
	go kv.readApplyCh() // drain ApplyCh and route results to callers
	return kv, node
}

// Set replicates a SET key=value command and blocks until it is committed
// on a majority of nodes and applied to the state machine.
func (kv *KVStore) Set(key, value string) error {
	_, err := kv.call(op{Type: "SET", Key: key, Value: value})
	return err
}

// Get reads directly from the local state machine.
// Any node (leader or follower) can serve reads without redirecting.
// A follower that is slightly behind may return a value that is a few
// entries stale, but replication lag is typically under one heartbeat (50ms).
func (kv *KVStore) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return v, nil
}

// Delete replicates a DEL command and blocks until committed.
func (kv *KVStore) Delete(key string) error {
	_, err := kv.call(op{Type: "DEL", Key: key})
	return err
}

// call encodes the op, submits it to Raft, and blocks until the entry is
// applied or a timeout/error occurs.
func (kv *KVStore) call(o op) (string, error) {
	data, err := encodeOp(o)
	if err != nil {
		return "", err
	}

	index, term, err := kv.node.Submit(data)
	if err != nil {
		return "", ErrNotLeader
	}

	ch := make(chan applyResult, 1)
	kv.pendingMu.Lock()
	kv.pending[index] = pendingCall{term: term, ch: ch}
	kv.pendingMu.Unlock()

	select {
	case r := <-ch:
		return r.Value, r.Err
	case <-time.After(5 * time.Second):
		kv.pendingMu.Lock()
		delete(kv.pending, index)
		kv.pendingMu.Unlock()
		return "", ErrTimeout
	}
}

// readApplyCh drains node.ApplyCh() forever.
// On regular entries: routes ApplyMsg.Result to the waiting caller (if any).
// On snapshot entries: fails all pending calls, since their commands may be lost.
// Also triggers periodic snapshots to bound log growth.
func (kv *KVStore) readApplyCh() {
	var applied uint64
	for msg := range kv.node.ApplyCh() {
		if msg.IsSnapshot {
			// Raft already called Restore() before sending this message.
			// Fail all pending calls — entries between old and new snapshot
			// may have been skipped on this node.
			kv.failAllPending()
			applied = msg.SnapshotIndex
			continue
		}

		r, _ := msg.Result.(applyResult)
		applied++

		// Route result to the caller that submitted this index.
		// Followers have no pending calls — their result is simply discarded.
		kv.pendingMu.Lock()
		call, ok := kv.pending[msg.Index]
		if ok {
			delete(kv.pending, msg.Index)
		}
		kv.pendingMu.Unlock()

		if ok {
			if call.term != msg.Term {
				// Another leader won this index with a different command.
				// Our submit was effectively lost — tell the caller to retry.
				call.ch <- applyResult{Err: ErrNotLeader}
			} else {
				call.ch <- r
			}
		}

		// Trigger a snapshot periodically to keep the log from growing forever.
		if kv.snapshotThreshold > 0 && applied%kv.snapshotThreshold == 0 {
			if data, err := kv.Snapshot(); err == nil {
				kv.node.TakeSnapshot(data)
			}
		}
	}
}

// failAllPending cancels every in-flight client call with ErrNotLeader.
func (kv *KVStore) failAllPending() {
	kv.pendingMu.Lock()
	defer kv.pendingMu.Unlock()
	for idx, call := range kv.pending {
		call.ch <- applyResult{Err: ErrNotLeader}
		delete(kv.pending, idx)
	}
}

// Apply implements raft.StateMachine.
// Called by the Raft apply loop for every committed entry on every node.
// Executes the operation against the map and returns the result, which Raft
// places in ApplyMsg.Result for readApplyCh to forward to the waiting caller.
func (kv *KVStore) Apply(command []byte) interface{} {
	var o op
	if err := decodeOp(command, &o); err != nil {
		return applyResult{Err: err}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch o.Type {
	case "SET":
		kv.data[o.Key] = o.Value
		return applyResult{}
	case "DEL":
		delete(kv.data, o.Key)
		return applyResult{}
	default:
		return applyResult{Err: errors.New("unknown op: " + o.Type)}
	}
}

// Snapshot implements raft.StateMachine.
// Serializes the entire map to bytes for log compaction.
func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore implements raft.StateMachine.
// Replaces the entire map with the snapshot data.
// Called by Raft when installing a snapshot from the leader.
func (kv *KVStore) Restore(snapshot []byte) error {
	var data map[string]string
	if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&data); err != nil {
		return err
	}

	kv.mu.Lock()
	kv.data = data
	kv.mu.Unlock()
	return nil
}

func encodeOp(o op) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(o)
	return buf.Bytes(), err
}

func decodeOp(data []byte, o *op) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(o)
}
