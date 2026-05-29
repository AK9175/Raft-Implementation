package raft

import (
	"context"
	"time"
)

// --- state transitions ---
// All three must be called with n.mu held.

// becomeFollower steps down to follower and updates the term.
// Called whenever a higher term is observed — from any RPC, any state.
// Raft §5.1: "If a server receives a request with a stale term number, it rejects the request."
func (n *RaftNode) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = "" // unknown until we hear an AppendEntries from the new leader
	n.persist()
}

// becomeCandidate increments the term, votes for self, and transitions to candidate.
// Called when the election timer fires in follower or candidate state.
// Raft §5.2
func (n *RaftNode) becomeCandidate() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = "" // no leader while election is in progress
	n.persist()
	// RequestVote RPCs are sent in Checkpoint 3 (startElection)
}

// becomeLeader transitions to leader and initializes per-peer tracking state.
// Called after receiving votes from a majority of peers.
// Raft §5.2: nextIndex initialized optimistically to lastIndex+1; matchIndex to 0.
func (n *RaftNode) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id // we are the leader
	lastIndex := n.log.lastIndex()
	now := time.Now()
	if n.lastHeardFrom == nil {
		n.lastHeardFrom = make(map[string]time.Time, len(n.peers))
	}
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
		n.lastHeardFrom[peer] = now // seed so CheckQuorum doesn't fire immediately
	}
}

// --- election ---

// startPreVote runs a pre-vote round before committing to a real election.
// Raft §9.6: the candidate probes peers with its *next* term (term+1) without
// actually incrementing its own term or clearing votedFor. Peers grant a
// pre-vote only if they haven't heard from a leader recently AND the candidate's
// log is at least as up-to-date. If a majority grants, we proceed to a real
// election; otherwise we stay follower and avoid accumulating a high term.
//
// Returns true if the node should proceed to a real election.
// Must be called WITHOUT n.mu held; acquires it internally.
func (n *RaftNode) startPreVote() bool {
	n.mu.Lock()
	nextTerm := n.currentTerm + 1
	args := RequestVoteArgs{
		Term:         nextTerm, // the term we WOULD use
		CandidateID:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
		IsPreVote:    true,
	}
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.Unlock()

	if len(peers) == 0 {
		return false
	}

	majority := (len(peers)+1)/2 + 1
	votes := 1 // count self

	type result struct{ granted bool }
	ch := make(chan result, len(peers))

	for _, peer := range peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(n.config.ElectionTimeoutMinMs)*time.Millisecond)
			defer cancel()
			reply, err := n.transport.RequestVote(ctx, peer, args)
			if err != nil {
				ch <- result{false}
				return
			}
			ch <- result{reply.VoteGranted}
		}(peer)
	}

	for range peers {
		r := <-ch
		if r.granted {
			votes++
			if votes >= majority {
				return true
			}
		}
	}
	return false
}

// startElection runs in its own goroutine after becomeCandidate().
// It sends RequestVote to all peers in parallel and calls becomeLeader()
// if a majority grants their votes. Raft §5.2.
func (n *RaftNode) startElection() {
	// Snapshot the state we need to build the RPC args.
	// We release the lock before sending RPCs so we don't block the event loop.
	n.mu.Lock()
	term := n.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.Unlock()

	// votes starts at 1 — we already voted for ourselves in becomeCandidate.
	votes := 1
	majority := (len(peers)+1)/2 + 1

	for _, peer := range peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(n.config.ElectionTimeoutMinMs)*time.Millisecond)
			defer cancel()

			reply, err := n.transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Golden rule: step down immediately on any higher term.
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			// Ignore stale replies — we may have moved to a new term or already won.
			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if !reply.VoteGranted {
				return
			}

			votes++
			if votes >= majority {
				n.becomeLeader()
			}
		}(peer)
	}
}

// --- RPC handlers ---

// HandleRequestVote processes an incoming RequestVote RPC.
// So this is a method expicitly used by follower, when it receives a leaders request
// to vote it checks all the leader info (like its term, with current term)
// We use this method on all (Candidate, Leader, Follower)
// Full implementation in Checkpoint 3.
func (n *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// ── Pre-vote path ────────────────────────────────────────────────────────
	// Pre-vote is a stateless probe: we neither update our term nor record a
	// vote. We grant only if we haven't heard from a leader recently (i.e. we
	// would time out and start an election ourselves) AND the candidate's log
	// is at least as up-to-date as ours.
	if args.IsPreVote {
		reply := RequestVoteReply{Term: n.currentTerm}
		// Reject if we have heard from a valid leader recently (within one
		// election-timeout window). This is the core pre-vote rule from Raft
		// §9.6: a node that is receiving heartbeats would not time out and
		// start an election, so it should not help the candidate either.
		// Using a timestamp rather than leaderID avoids the race where leaderID
		// is "" for a brief window right after a new leader is elected.
		threshold := time.Duration(n.config.ElectionTimeoutMinMs) * time.Millisecond
		if time.Since(n.lastLeaderContact) < threshold {
			return reply // healthy leader exists — reject pre-vote
		}
		candidateUpToDate := args.LastLogTerm > n.log.lastTerm() ||
			(args.LastLogTerm == n.log.lastTerm() && args.LastLogIndex >= n.log.lastIndex())
		reply.VoteGranted = candidateUpToDate
		return reply
	}

	// ── Real vote path ───────────────────────────────────────────────────────
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := RequestVoteReply{Term: n.currentTerm}

	// Reject if candidate is behind us in term.
	if args.Term < n.currentTerm {
		return reply
	}

	// Condition 1: haven't voted for someone else this term.
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID

	// Condition 2: candidate's log is at least as up-to-date as ours.
	// Raft §5.4.1
	candidateUpToDate := args.LastLogTerm > n.log.lastTerm() ||
		(args.LastLogTerm == n.log.lastTerm() && args.LastLogIndex >= n.log.lastIndex())

	if canVote && candidateUpToDate {
		n.votedFor = args.CandidateID
		n.persist() // must persist before replying — crash safety
		reply.VoteGranted = true
		n.notifyHeartbeat() // reset our election timer — a legitimate candidate is out there
	}

	return reply
}
