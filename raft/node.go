package raft

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
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
// The node runs a main event loop (Run()) that drives all state transitions.
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

	// --- cluster ---
	leaderID string // ID of the node we last heard from as leader ("" if unknown)

	// --- snapshot ---
	snapshotData    []byte               // latest snapshot bytes (nil until first snapshot)
	snapshotNotifyC chan snapshotToApply // apply loop reads this to restore after InstallSnapshot

	// --- lifecycle ---
	stopCh        chan struct{}
	done          chan struct{}
	heartbeatC    chan struct{} // signals valid heartbeat/vote → reset election timer
	commitNotifyC chan struct{} // signals commitIndex advanced → wake apply loop
	applyCh       chan ApplyMsg // committed entries flow out to the application here

	// --- diagnostics (atomic — readable without holding mu) ---
	heartbeatsRecv atomic.Uint64 // total heartbeats received as follower
	heartbeatsSent atomic.Uint64 // total heartbeat rounds sent as leader
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
		id:              cfg.ID,
		peers:           cfg.Peers,
		state:           Follower,
		log:             newLog(),
		nextIndex:       make(map[string]uint64),
		matchIndex:      make(map[string]uint64),
		transport:       cfg.Transport,
		stateMachine:    cfg.StateMachine,
		config:          cfg,
		snapshotNotifyC: make(chan snapshotToApply, 1),
		stopCh:          make(chan struct{}),
		done:            make(chan struct{}),
		heartbeatC:      make(chan struct{}, 1),
		commitNotifyC:   make(chan struct{}, 1),
		applyCh:         make(chan ApplyMsg, 64),
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

// LeaderID returns the ID of the node this node believes is the current leader.
// Returns "" if no leader is known (e.g. right after startup or a partition).
// Safe to call concurrently.
func (n *RaftNode) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// HeartbeatsReceived returns the total number of valid heartbeats this node
// has accepted as a follower. Increments every ~50ms when a leader is present.
func (n *RaftNode) HeartbeatsReceived() uint64 { return n.heartbeatsRecv.Load() }

// HeartbeatsSent returns the total number of heartbeat rounds this node has
// sent as leader (one round = one AppendEntries per peer). Increments every 50ms.
func (n *RaftNode) HeartbeatsSent() uint64 { return n.heartbeatsSent.Load() }

// CommitIndex returns the highest log index known to be committed.
func (n *RaftNode) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LastApplied returns the highest log index applied to the state machine.
func (n *RaftNode) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// Stop signals the node to shut down and waits for it to finish.
func (n *RaftNode) Stop() {
	close(n.stopCh)
	<-n.done
}

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
		if n.nextIndex[peer] <= n.log.snapshotIndex() {
			snapArgs := n.buildSnapshotArgsLocked()
			go n.sendSnapshotToPeer(peer, term, snapArgs)
		} else {
			args := n.buildArgsLocked(peer)
			go n.sendToPeer(peer, term, args)
		}
	}
	return index, term, nil
}

// ApplyCh returns the channel on which committed log entries are delivered after
// being applied to the state machine. Callers should read this continuously;
// the channel is buffered but a slow reader will eventually stall the apply loop.
func (n *RaftNode) ApplyCh() <-chan ApplyMsg {
	return n.applyCh
}

// --- timers ---

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

// --- signals ---

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

// notifyCommit wakes the apply loop whenever commitIndex advances.
// Same non-blocking pattern as notifyHeartbeat — one pending signal is enough.
func (n *RaftNode) notifyCommit() {
	select {
	case n.commitNotifyC <- struct{}{}:
	default:
	}
}

// --- main event loop ---

// Run starts the node's event loop. Must be called in its own goroutine.
// It drives all state transitions and coordinates timers.
func (n *RaftNode) Run() {
	defer close(n.done)

	n.loadState()          // restore term, votedFor, and log entries from disk
	n.restoreFromSnapshot() // must run before applyLoop so the SM is ready before any Apply calls

	go n.applyLoop() // apply committed entries to the state machine in the background

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
