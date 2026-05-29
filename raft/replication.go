package raft

import (
	"context"
	"sort"
	"time"
)

// --- heartbeats and replication ---

// sendHeartbeats sends AppendEntries (with any pending entries) to every peer
// in parallel. Called by Run() on every heartbeatTicker tick when leader.
// Must be called with n.mu held. Releases the lock before RPCs, reacquires
// before returning so the event loop's lock invariant is preserved.
func (n *RaftNode) sendHeartbeats() {
	term := n.currentTerm
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	snapshotIdx := n.log.snapshotIndex()

	// Build per-peer args under the lock so nextIndex/log are consistent.
	type peerMsg struct {
		useSnap  bool
		aeArgs   AppendEntriesArgs
		snapArgs InstallSnapshotArgs
	}
	msgs := make(map[string]peerMsg, len(peers))
	for _, peer := range peers {
		if n.nextIndex[peer] <= snapshotIdx {
			msgs[peer] = peerMsg{useSnap: true, snapArgs: n.buildSnapshotArgsLocked()}
		} else {
			msgs[peer] = peerMsg{useSnap: false, aeArgs: n.buildArgsLocked(peer)}
		}
	}
	n.heartbeatsSent.Add(1) // one round = one tick, regardless of peer count
	n.mu.Unlock()           // release before RPCs — don't block the event loop

	for _, peer := range peers {
		m := msgs[peer]
		if m.useSnap {
			go n.sendSnapshotToPeer(peer, term, m.snapArgs)
		} else {
			go n.sendToPeer(peer, term, m.aeArgs)
		}
	}

	n.mu.Lock() // reacquire — caller (Run) expects lock held on return
}

// buildArgsLocked constructs the AppendEntries args for peer using the current
// log state. Must be called with n.mu held.
func (n *RaftNode) buildArgsLocked(peer string) AppendEntriesArgs {
	nextIdx := n.nextIndex[peer]
	prevIdx := nextIdx - 1
	prevTerm := n.log.get(prevIdx).Term
	raw := n.log.slice(nextIdx, n.log.lastIndex()+1)
	entries := make([]LogEntry, len(raw))
	copy(entries, raw)
	return AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

// sendToPeer sends one AppendEntries RPC to peer and processes the reply.
// Runs in its own goroutine. term is the leader term when the RPC was built —
// used to detect stale replies after a term change.
func (n *RaftNode) sendToPeer(peer string, term uint64, args AppendEntriesArgs) {
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(n.config.HeartbeatIntervalMs)*time.Millisecond)
	defer cancel()

	reply, err := n.transport.AppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Golden rule: step down on any higher term.
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	// Ignore stale replies (we stepped down or moved to a new term).
	if n.state != Leader || n.currentTerm != term {
		return
	}

	// Any reply means the peer is reachable — record it for CheckQuorum
	// regardless of whether it accepted or rejected the entries.
	if n.lastHeardFrom == nil {
		n.lastHeardFrom = make(map[string]time.Time)
	}
	n.lastHeardFrom[peer] = time.Now()

	if reply.Success {
		// Peer accepted the entries — advance its tracking indices.
		newMatch := args.PrevLogIndex + uint64(len(args.Entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeCommit()
		return
	}

	// Peer rejected — use ConflictTerm/ConflictIndex to skip back fast.
	// Raft §5.3 optimization: jump back an entire term per round trip.
	if reply.ConflictTerm == 0 {
		// Follower log is shorter than PrevLogIndex — jump to its end.
		n.nextIndex[peer] = reply.ConflictIndex
	} else {
		// Find the last entry in our log that has ConflictTerm.
		found := uint64(0)
		for i := n.log.lastIndex(); i >= 1; i-- {
			if n.log.get(i).Term == reply.ConflictTerm {
				found = i
				break
			}
		}
		if found > 0 {
			// We have that term — start just after our last entry for it.
			n.nextIndex[peer] = found + 1
		} else {
			// We don't have that term — use follower's hint.
			n.nextIndex[peer] = reply.ConflictIndex
		}
	}
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
}

// maybeCommit advances commitIndex if a majority of nodes have replicated an
// entry from the current term. Must be called with n.mu held.
// Raft §5.4.2: a leader may only commit entries from its own term by counting
// replicas. Entries from prior terms are committed implicitly.
func (n *RaftNode) maybeCommit() {
	// Gather replication progress: leader has everything up to lastIndex.
	indices := make([]uint64, 0, len(n.peers)+1)
	indices = append(indices, n.log.lastIndex())
	for _, peer := range n.peers {
		indices = append(indices, n.matchIndex[peer])
	}

	// Sort descending; the majority-th element is replicated by a quorum.
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })
	majority := (len(n.peers)+1)/2 + 1
	quorumIdx := indices[majority-1]

	// Only commit if the entry belongs to the current term (safety rule).
	if quorumIdx > n.commitIndex && n.log.get(quorumIdx).Term == n.currentTerm {
		n.commitIndex = quorumIdx
		n.notifyCommit()
	}
}

// --- RPC handler ---

// HandleAppendEntries processes an incoming AppendEntries RPC.
// Implements the full Raft §5.3 log consistency check and entry appending.
func (n *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	} else if args.Term == n.currentTerm && n.state == Candidate {
		// Raft §5.2: a candidate that receives AppendEntries from a legitimate
		// leader in the same term must revert to follower.
		n.state = Follower
	}

	reply := AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}

	// Valid message from the current leader — reset our election timer and record who it is.
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now() // used by pre-vote to reject spurious elections
	n.notifyHeartbeat()

	// --- Log consistency check (Raft §5.3) ---
	// The leader ensures its log matches ours at PrevLogIndex before we accept
	// any new entries. If PrevLogIndex == 0, the leader is sending from the
	// very beginning so there is nothing to check.
	if args.PrevLogIndex > 0 {
		prev := n.log.get(args.PrevLogIndex)
		if prev.Term == 0 {
			// We don't have an entry at PrevLogIndex — our log is too short.
			// Tell the leader to back up to just past our last entry.
			reply.ConflictIndex = n.log.lastIndex() + 1
			reply.ConflictTerm = 0
			return reply
		}
		if prev.Term != args.PrevLogTerm {
			// Term mismatch at PrevLogIndex — report the conflicting term and
			// the first index where that term appears so the leader can skip
			// the whole term in one round trip.
			reply.ConflictTerm = prev.Term
			reply.ConflictIndex = n.log.firstIndexForTerm(prev.Term)
			return reply
		}
	}

	// --- Append entries, resolving conflicts ---
	// Walk through the incoming entries. For each one:
	//   • If the entry is at or before the snapshot boundary, skip it — it's
	//     already applied and compacted; we can't (and don't need to) re-check it.
	//   • If we already have a matching entry at that index, skip it.
	//   • If we have a conflicting entry (same index, different term), truncate
	//     everything from that point and append the rest of the batch.
	//   • If the index is past our log end, append from here onward.
	for i, entry := range args.Entries {
		if entry.Index <= n.log.snapshotIndex() {
			continue // already compacted — skip without touching the log
		}
		existing := n.log.get(entry.Index)
		if existing.Term == 0 {
			// Past our log end — append this and all remaining entries.
			n.log.append(args.Entries[i:]...)
			break
		}
		if existing.Term != entry.Term {
			// Conflict — truncate stale suffix and replace with leader's entries.
			n.log.truncateFrom(entry.Index)
			n.log.append(args.Entries[i:]...)
			break
		}
		// existing.Term == entry.Term — entry already matches, keep scanning.
	}

	// --- Advance commitIndex ---
	// Raft §5.3: if the leader's commitIndex is ahead of ours, catch up — but
	// never commit past the last entry we actually have.
	if args.LeaderCommit > n.commitIndex {
		last := n.log.lastIndex()
		if args.LeaderCommit < last {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = last
		}
		n.notifyCommit()
	}

	n.persist()
	reply.Success = true
	if len(args.Entries) == 0 {
		n.heartbeatsRecv.Add(1) // pure heartbeat (no new entries)
	}
	return reply
}
