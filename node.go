package raft

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ErrNotLeader is returned by Submit when this node is not the current leader.
var ErrNotLeader = errors.New("raft: not the leader")

// NodeState represents the three possible states of a Raft node.
// At any moment, every node is exactly one of these.
// Raft §5.1
type NodeState uint8

const (
	Follower  NodeState = 0 // default state; waits for leader heartbeats
	Candidate NodeState = 1 // seeking election; waiting for votes
	Leader    NodeState = 2 // drives log replication and heartbeats
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Config holds the parameters for a RaftNode. Separating config from the node
// makes construction explicit and keeps the library easy to embed.
type Config struct {
	// ID uniquely identifies this node within the cluster. Must be stable across restarts.
	ID string

	// Peers is the list of peer node addresses (not including self).
	Peers []string

	// Transport handles sending RPCs to peers.
	Transport Transport

	// StateMachine is the application logic that runs on top of the replicated log.
	StateMachine StateMachine

	// ElectionTimeoutMin/Max control the randomized election timeout range.
	// Raft §5.2: must be much larger than heartbeat interval.
	// Defaults (if zero): 150ms–300ms.
	ElectionTimeoutMinMs int
	ElectionTimeoutMaxMs int

	// HeartbeatIntervalMs is how often the leader sends heartbeats.
	// Must be less than ElectionTimeoutMin.
	// Default (if zero): 50ms.
	HeartbeatIntervalMs int

	// DataDir is the directory where persistent state is written.
	// Must exist before the node starts.
	DataDir string
}

// RaftNode is a single participant in a Raft cluster.
//
// The node runs a main event loop (run()) that drives all state transitions.
// External callers interact through Submit() (write a command) and Stop().
//
// Raft §5: all state transitions are deterministic given the inputs.
type RaftNode struct {
	mu sync.Mutex

	// --- identity ---
	id    string
	peers []string

	// --- persistent state (must survive crashes — Raft §5.4) ---
	currentTerm uint64 // latest term this node has seen
	votedFor    string // candidateID this node voted for in currentTerm ("" if none)
	log         *Log

	// --- volatile state (can be recomputed after crash) ---
	state       NodeState
	commitIndex uint64 // index of highest log entry known to be committed
	lastApplied uint64 // index of highest log entry applied to state machine

	// --- leader-only volatile state (reinitialized after each election) ---
	nextIndex  map[string]uint64 // for each peer: next log index to send
	matchIndex map[string]uint64 // for each peer: highest log index known to be replicated

	// --- dependencies ---
	transport    Transport
	stateMachine StateMachine
	config       Config

	// --- lifecycle ---
	stopCh     chan struct{}
	done       chan struct{}
	heartbeatC chan struct{} // RPC handlers send here when a valid heartbeat or vote is received
}

// NewRaftNode creates a new RaftNode from the given config.
// The node starts as a follower in term 0 with no votes cast.
func NewRaftNode(cfg Config) *RaftNode {
	if cfg.ElectionTimeoutMinMs == 0 {
		cfg.ElectionTimeoutMinMs = 150
	}
	if cfg.ElectionTimeoutMaxMs == 0 {
		cfg.ElectionTimeoutMaxMs = 300
	}
	if cfg.HeartbeatIntervalMs == 0 {
		cfg.HeartbeatIntervalMs = 50
	}

	n := &RaftNode{
		id:           cfg.ID,
		peers:        cfg.Peers,
		state:        Follower,
		log:          newLog(),
		nextIndex:    make(map[string]uint64),
		matchIndex:   make(map[string]uint64),
		transport:    cfg.Transport,
		stateMachine: cfg.StateMachine,
		config:       cfg,
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
		heartbeatC:   make(chan struct{}, 1),
	}
	return n
}

// State returns the current node state. Safe to call concurrently.
func (n *RaftNode) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// CurrentTerm returns the current term. Safe to call concurrently.
func (n *RaftNode) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// Stop signals the node to shut down and waits for it to finish.
func (n *RaftNode) Stop() {
	close(n.stopCh)
	<-n.done
}

// --- state transitions ---
// All three must be called with n.mu held.

// becomeFollower steps down to follower and updates the term.
// Called whenever a higher term is observed — from any RPC, any state.
// Raft §5.1: "If a server receives a request with a stale term number, it rejects the request."
func (n *RaftNode) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.persist()
}

// becomeCandidate increments the term, votes for self, and transitions to candidate.
// Called when the election timer fires in follower or candidate state.
// Raft §5.2
func (n *RaftNode) becomeCandidate() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.persist()
	// RequestVote RPCs are sent in Checkpoint 3 (startElection)
}

// becomeLeader transitions to leader and initializes per-peer tracking state.
// Called after receiving votes from a majority of peers.
// Raft §5.2: nextIndex initialized optimistically to lastIndex+1; matchIndex to 0.
func (n *RaftNode) becomeLeader() {
	n.state = Leader
	lastIndex := n.log.lastIndex()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
	// Heartbeats are sent in Checkpoint 4 (sendHeartbeats)
}

// --- election timer ---

// randomElectionTimeout returns a random duration in [min, max).
// Randomization prevents all followers from timing out simultaneously and
// splitting votes indefinitely. Raft §5.2.
// This is like assigning random timeout to any node(that timeout number will be node specific for making it timeout of Election)
func (n *RaftNode) randomElectionTimeout() time.Duration {
	spread := n.config.ElectionTimeoutMaxMs - n.config.ElectionTimeoutMinMs
	ms := n.config.ElectionTimeoutMinMs + rand.Intn(spread)
	return time.Duration(ms) * time.Millisecond
}

// resetTimer drains a fired timer and resets it to d.
// Required because time.Timer.Reset is only safe on a stopped, drained timer.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// notifyHeartbeat signals the main loop that a valid heartbeat or granted vote
// was received, so the election timer should be reset.
// Non-blocking: if the channel already has a pending signal, we skip — one is enough.
// Notify heartbeat acts like a signalling mechanism, it sends signal to the main event loop saying that
// Valid leader was found, so we need to reset the timer (resetTimer), and assign new
// countdown number once again to the node (randomElectionTimeout)
func (n *RaftNode) notifyHeartbeat() {
	select {
	case n.heartbeatC <- struct{}{}:
	default:
	}
}

// --- main event loop ---

// Run starts the node's event loop. Must be called in its own goroutine.
// It drives all state transitions and coordinates timers.
func (n *RaftNode) Run() {
	defer close(n.done)

	electionTimer := time.NewTimer(n.randomElectionTimeout())
	defer electionTimer.Stop()

	heartbeatTicker := time.NewTicker(time.Duration(n.config.HeartbeatIntervalMs) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-electionTimer.C:
			// No heartbeat received before timeout — start an election.
			n.mu.Lock()
			if n.state != Leader {
				n.becomeCandidate()
				go n.startElection()
			}
			n.mu.Unlock()
			resetTimer(electionTimer, n.randomElectionTimeout())

		case <-n.heartbeatC:
			// Valid heartbeat received or vote granted — reset election timer.
			resetTimer(electionTimer, n.randomElectionTimeout())

		case <-heartbeatTicker.C:
			// Send heartbeats if we are the leader.
			n.mu.Lock()
			if n.state == Leader {
				n.sendHeartbeats()
			}
			n.mu.Unlock()
		}
	}
}

// --- heartbeats ---

// sendHeartbeats sends AppendEntries (with any pending entries) to every peer
// in parallel. Called by Run() on every heartbeatTicker tick when leader.
// Must be called with n.mu held. Releases the lock before RPCs, reacquires
// before returning so the event loop's lock invariant is preserved.
func (n *RaftNode) sendHeartbeats() {
	term := n.currentTerm
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)

	// Build per-peer args under the lock so nextIndex/log are consistent.
	peerArgs := make(map[string]AppendEntriesArgs, len(peers))
	for _, peer := range peers {
		peerArgs[peer] = n.buildArgsLocked(peer)
	}
	n.mu.Unlock() // release before RPCs — don't block the event loop

	for _, peer := range peers {
		go n.sendToPeer(peer, term, peerArgs[peer])
	}

	n.mu.Lock() // reacquire — caller (Run) expects lock held on return
}

// buildArgsLocked constructs the AppendEntries args for peer using the current
// log state. Must be called with n.mu held.
func (n *RaftNode) buildArgsLocked(peer string) AppendEntriesArgs {
	nextIdx := n.nextIndex[peer]
	prevIdx := nextIdx - 1
	prevTerm := n.log.get(prevIdx).Term
	entries := n.log.slice(nextIdx, n.log.lastIndex()+1)
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

	if reply.Success {
		// Peer accepted the entries — advance its tracking indices.
		//PrevlogIndex + no of new entries that are appended.
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
	}
}

// --- client interface ---

// Submit appends a command to the replicated log and triggers replication.
// Returns the log index, term, and nil on success.
// Returns ErrNotLeader if this node is not the current leader.
// The caller must watch for the entry at (index, term) to be committed via
// an external notification mechanism (added in Checkpoint 8).
func (n *RaftNode) Submit(command []byte) (index uint64, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, 0, ErrNotLeader
	}
	index = n.log.lastIndex() + 1
	term = n.currentTerm
	n.log.append(LogEntry{Term: term, Index: index, Command: command})
	n.persist()

	// Trigger immediate replication — don't wait for the heartbeat tick.
	for _, peer := range n.peers {
		args := n.buildArgsLocked(peer)
		go n.sendToPeer(peer, term, args)
	}
	return index, term, nil
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

// --- RPC handlers (called by the transport when a peer sends us an RPC) ---

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

// HandleAppendEntries processes an incoming AppendEntries RPC.
// Implements the full Raft §5.3 log consistency check and entry appending.
func (n *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}

	// Valid message from the current leader — reset our election timer.
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
	//   • If we already have a matching entry at that index, skip it.
	//   • If we have a conflicting entry (same index, different term), truncate
	//     everything from that point and append the rest of the batch.
	//   • If the index is past our log end, append from here onward.
	for i, entry := range args.Entries {
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
	}

	n.persist()
	reply.Success = true
	return reply
}
