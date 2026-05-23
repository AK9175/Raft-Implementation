package raft

// ApplyMsg is sent on ApplyCh() for every log entry the node commits and applies
// to the state machine. Callers read this channel to learn about committed commands
// and the result each command produced.
type ApplyMsg struct {
	Index   uint64      // log index of the committed entry
	Term    uint64      // term in which the entry was originally written
	Command []byte      // the raw command bytes passed to Submit
	Result  interface{} // return value from StateMachine.Apply
}

// applyLoop runs in its own goroutine (started by Run) and applies every
// committed log entry to the state machine in index order.
//
// Why a separate goroutine?
//   stateMachine.Apply() is application code — it might be slow (disk I/O,
//   network calls). Calling it inside the event loop or under the mutex would
//   stall heartbeats and RPCs. The apply loop decouples the two: the event
//   loop advances commitIndex, and this goroutine drains entries at its own pace.
//
// Why release the lock before Apply()?
//   Same reason: Apply() must not hold n.mu or it blocks all RPC handlers.
//   We snapshot the entry under the lock, release, apply, send to applyCh,
//   then re-lock to check for more entries.
func (n *RaftNode) applyLoop() {
	for {
		// Wait until commitIndex advances or the node stops.
		select {
		case <-n.stopCh:
			return
		case <-n.commitNotifyC:
		}

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
