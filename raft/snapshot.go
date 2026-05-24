package raft

import (
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"time"
)

// snapshotFile is what gets written to raft-snapshot.bin.
type snapshotFile struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// TakeSnapshot is called by the application after applying log entries up through
// lastApplied. data is the full serialized state machine state at that point.
// The log is compacted through lastApplied — entries before it are discarded.
//
// Why the application drives snapshots?
//   Only the application knows when it has a consistent, complete snapshot ready.
//   Raft controls *which index* to compact to (lastApplied); the application
//   controls *what bytes* to save (the state machine encoding).
func (n *RaftNode) TakeSnapshot(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	index := n.lastApplied
	if index == 0 || index <= n.log.snapshotIndex() {
		return nil // nothing new to compact
	}

	term := n.log.get(index).Term
	if err := n.saveSnapshot(index, term, data); err != nil {
		return err
	}
	n.snapshotData = data
	n.log.compactTo(index, term)
	n.persist() // persist the compacted log (sentinel now at index)
	return nil
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC from the leader.
// Called when a follower's nextIndex has fallen behind the leader's snapshot boundary —
// the entries it needs have already been compacted away. Raft §7.
func (n *RaftNode) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := InstallSnapshotReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}

	n.leaderID = args.LeaderID
	n.notifyHeartbeat()

	// Already have this snapshot or a newer one — nothing to do.
	if args.LastIncludedIndex <= n.log.snapshotIndex() {
		return reply
	}

	if err := n.saveSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data); err != nil {
		return reply // don't install a snapshot we failed to persist
	}

	n.snapshotData = args.Data
	n.log.compactTo(args.LastIncludedIndex, args.LastIncludedTerm)

	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}
	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}

	n.persist()

	// Hand the snapshot off to the apply loop for out-of-lock state machine restoration.
	// Drain any stale pending notification so the newest snapshot always wins.
	select {
	case <-n.snapshotNotifyC:
	default:
	}
	n.snapshotNotifyC <- snapshotToApply{
		index: args.LastIncludedIndex,
		term:  args.LastIncludedTerm,
		data:  args.Data,
	}

	return reply
}

// sendSnapshotToPeer sends an InstallSnapshot RPC to peer and processes the reply.
// Called when nextIndex[peer] <= snapshotIndex (entries the peer needs are compacted).
// Runs in its own goroutine. term is the leader term at dispatch time.
func (n *RaftNode) sendSnapshotToPeer(peer string, term uint64, args InstallSnapshotArgs) {
	ctx, cancel := context.WithTimeout(context.Background(),
		10*time.Duration(n.config.HeartbeatIntervalMs)*time.Millisecond)
	defer cancel()

	reply, err := n.transport.InstallSnapshot(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	// Ignore stale replies.
	if n.state != Leader || n.currentTerm != term {
		return
	}

	// Peer accepted the snapshot — advance its tracking indices.
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.nextIndex[peer] = n.matchIndex[peer] + 1
}

// buildSnapshotArgsLocked constructs InstallSnapshot args from in-memory state.
// Must be called with n.mu held.
func (n *RaftNode) buildSnapshotArgsLocked() InstallSnapshotArgs {
	return InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: n.log.snapshotIndex(),
		LastIncludedTerm:  n.log.snapshotTerm(),
		Data:              n.snapshotData,
	}
}

// restoreFromSnapshot loads the snapshot file (if any) and restores the state machine.
// Called at the start of Run(), before applyLoop is spawned, so no lock is needed.
func (n *RaftNode) restoreFromSnapshot() {
	index, term, data, err := n.loadSnapshotFile()
	if err != nil || index == 0 {
		return
	}

	// Restore state machine directly — no other goroutines are running yet.
	n.stateMachine.Restore(data)
	n.snapshotData = data

	// compactTo may already match the sentinel loadState() restored — safe no-op.
	n.log.compactTo(index, term)

	if n.lastApplied < index {
		n.lastApplied = index
	}
	if n.commitIndex < index {
		n.commitIndex = index
	}
}

// saveSnapshot writes a snapshot atomically to raft-snapshot.bin using the same
// write-to-tmp → fsync → rename pattern as persist().
// Must be called with n.mu held.
func (n *RaftNode) saveSnapshot(index, term uint64, data []byte) error {
	sf := snapshotFile{Index: index, Term: term, Data: data}
	path := filepath.Join(n.config.DataDir, "raft-snapshot.bin")
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := gob.NewEncoder(f).Encode(sf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	return os.Rename(tmp, path)
}

// loadSnapshotFile reads the snapshot from disk.
// Returns index=0 and nil error if no snapshot file exists (fresh node).
func (n *RaftNode) loadSnapshotFile() (index, term uint64, data []byte, err error) {
	path := filepath.Join(n.config.DataDir, "raft-snapshot.bin")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, nil
		}
		return 0, 0, nil, err
	}
	defer f.Close()

	var sf snapshotFile
	if err := gob.NewDecoder(f).Decode(&sf); err != nil {
		return 0, 0, nil, err
	}
	return sf.Index, sf.Term, sf.Data, nil
}
