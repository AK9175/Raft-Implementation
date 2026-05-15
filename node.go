package raft

import "sync"

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
	stopCh chan struct{}
	done   chan struct{}
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
