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
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
	// Heartbeats are sent in Checkpoint 4 (sendHeartbeats)
}

// --- election ---

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
	// Accessed only under n.mu, so no separate mutex needed.
	votes := 1
	majority := (len(peers)+1)/2 + 1 // (total nodes) / 2 + 1

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
			//So basically this is running inside a goroutine, so each RPC vote reply
			//Will run this code, and as soon as we get the majority vote, the node becomes
			//Leader and changes its state from Candidate to Leader, so any new vote will
			//result in return (line no 321).
			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if !reply.VoteGranted {
				return
			}

			votes++
			if votes >= majority {
				// n.state == Candidate check above ensures we only call this once:
				// after becomeLeader sets state=Leader, subsequent goroutines return early.
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
	//n -> Current node instance (Follower side which has received request to vote)
	//args -> Args sent by candidate node
	// Step down first so reply.Term reflects the updated term.
	// We do this so that if a node which is a stale leader can be changed to follower
	// So such stale leaders, and stale candidates can change their state to follower
	// A node which was leader, but crashed and meanwhile a diff node started election,
	// and when this node came back up it got the RequestVote by that candidate node with a
	//larger term, so it demotes itself to follower
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := RequestVoteReply{Term: n.currentTerm}

	// Reject if candidate is behind us in term.
	//So this means no vote was granted to the candidate requesting vote
	if args.Term < n.currentTerm {
		return reply
	}

	// Condition 1: haven't voted for someone else this term.
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID

	// Condition 2: candidate's log is at least as up-to-date as ours.
	// Higher last term wins outright — a newer term means a more recent leader,
	// whose entries are more authoritative regardless of log length.
	// Only when last terms are equal does log length break the tie.
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
