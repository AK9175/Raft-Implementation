package raft

// ApplyMsg is sent on ApplyCh() for every committed log entry or installed
// snapshot. Callers inspect IsSnapshot to tell the two apart.
type ApplyMsg struct {
	// Log entry fields (valid when IsSnapshot == false)
	Index   uint64      // log index of the committed entry
	Term    uint64      // term in which the entry was originally written
	Command []byte      // the raw command bytes passed to Submit
	Result  interface{} // return value from StateMachine.Apply

	// Snapshot fields (valid when IsSnapshot == true)
	IsSnapshot    bool
	SnapshotIndex uint64 // last index covered by the snapshot
	SnapshotTerm  uint64 // term at SnapshotIndex
	SnapshotData  []byte // raw bytes from StateMachine.Snapshot()
}

// snapshotToApply carries a snapshot through the snapshotNotifyC channel
// to the apply loop for out-of-lock state machine restoration.
type snapshotToApply struct {
	index uint64
	term  uint64
	data  []byte
}

// applyLoop runs in its own goroutine (started by Run) and applies every
// committed log entry to the state machine in index order.
//
// Why a separate goroutine?
//
//	stateMachine.Apply() is application code — it might be slow (disk I/O,
//	network calls). Calling it inside the event loop or under the mutex would
//	stall heartbeats and RPCs. The apply loop decouples the two: the event
//	loop advances commitIndex, and this goroutine drains entries at its own pace.
//
// Why release the lock before Apply()?
//
//	Same reason: Apply() must not hold n.mu or it blocks all RPC handlers.
//	We snapshot the entry under the lock, release, apply, send to applyCh,
//	then re-lock to check for more entries.
func (n *RaftNode) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return

		case snap := <-n.snapshotNotifyC:
			// A snapshot was installed (via InstallSnapshot RPC or startup restore).
			// Restore the state machine outside the lock — Restore() is application code.
			n.stateMachine.Restore(snap.data)
			select {
			case n.applyCh <- ApplyMsg{
				IsSnapshot:    true,
				SnapshotIndex: snap.index,
				SnapshotTerm:  snap.term,
				SnapshotData:  snap.data,
			}:
			case <-n.stopCh:
				return
			}

		case <-n.commitNotifyC:
			// commitIndex advanced — apply any unapplied entries.
			n.mu.Lock()
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				entry := n.log.get(n.lastApplied) // copy entry under lock
				n.mu.Unlock()

				// Apply to state machine without holding the lock.
				result := n.stateMachine.Apply(entry.Command)

				// Deliver to the caller. Blocks if the caller is slow — the buffered
				// channel (cap 64) absorbs short bursts.
				select {
				case n.applyCh <- ApplyMsg{
					Index:   entry.Index,
					Term:    entry.Term,
					Command: entry.Command,
					Result:  result,
				}:
				case <-n.stopCh:
					return
				}

				n.mu.Lock()
			}
			n.mu.Unlock()
		}
	}
}
