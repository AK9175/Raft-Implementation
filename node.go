package raft

import (
	"math/rand"
	"sync"
	"time"
)

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
				// go n.startElection() — added in Checkpoint 3
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
				// n.sendHeartbeats() — added in Checkpoint 4
			}
			n.mu.Unlock()
		}
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

	// Vote granting logic (log up-to-date check) added in Checkpoint 3.

	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
// Full implementation in Checkpoint 6.
func (n *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Step down if we see a higher term — do this before building the reply
	// so reply.Term reflects the updated term, not the stale one.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := AppendEntriesReply{Term: n.currentTerm}

	// Reject if message is from a stale leader.
	if args.Term < n.currentTerm {
		return reply
	}

	// Valid message from current leader — reset election timer.
	n.notifyHeartbeat()

	// Log consistency check and entry appending added in Checkpoint 6.

	return reply
}
