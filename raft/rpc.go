package raft

// RequestVoteArgs is sent by a candidate to gather votes.
// Raft §5.2
type RequestVoteArgs struct {
	Term         uint64 // candidate's current term
	CandidateID  string // so voters know who is asking
	LastLogIndex uint64 // index of candidate's last log entry
	LastLogTerm  uint64 // term of candidate's last log entry
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64 // responder's current term (so candidate can update itself)
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries and as a heartbeat.
// Raft §5.3
type AppendEntriesArgs struct {
	Term         uint64     // leader's current term
	LeaderID     string     // so followers can redirect clients
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit uint64     // leader's commitIndex
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    uint64 // responder's current term (so leader can update itself)
	Success bool   // true if follower contained entry matching PrevLogIndex/PrevLogTerm

	// Conflict info — only meaningful when Success==false.
	// Allows the leader to skip back an entire term instead of one index at a time.
	// Raft §5.3 (last paragraph).
	ConflictTerm  uint64 // term of the conflicting entry; 0 if follower log is too short
	ConflictIndex uint64 // first index in the log that has ConflictTerm;
	//                      if ConflictTerm==0, this is len(followerLog)+1
}

// InstallSnapshotArgs is sent by the leader to a lagging follower whose
// nextIndex has fallen behind the leader's snapshot boundary. Instead of
// replaying thousands of compacted entries, the leader ships the full
// snapshot in one RPC. Raft §7.
type InstallSnapshotArgs struct {
	Term              uint64 // leader's current term
	LeaderID          string // so follower can redirect clients
	LastIncludedIndex uint64 // snapshot covers all entries up through this index
	LastIncludedTerm  uint64 // term of the entry at LastIncludedIndex
	Data              []byte // raw snapshot bytes from StateMachine.Snapshot()
}

// InstallSnapshotReply is the follower's response to InstallSnapshot.
type InstallSnapshotReply struct {
	Term uint64 // follower's current term, for the leader to step down if stale
}
