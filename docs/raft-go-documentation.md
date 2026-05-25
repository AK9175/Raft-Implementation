# Raft Consensus Algorithm & Go Implementation
## Comprehensive Technical Documentation

**Course:** Distributed Systems — MS Software Engineering, SJSU  
**Implementation:** Raft KV Store in Go with gRPC/Protobuf Transport and Docker  

---

# Part 1: Distributed Systems Foundations

## 1.1 Why Distributed Systems Are Hard

A distributed system is a group of computers working together to appear as a single system to end users. The core difficulty comes from three unavoidable realities:

**Partial Failures** — In a single machine, either everything works or nothing does. In a distributed system, some components can fail while others continue. A machine may crash, a network link may drop packets, or a process may slow down without dying. The system must decide how to proceed without knowing which parts are broken.

**No Shared Clock** — Two machines cannot agree on what "now" means. Network delays mean message A can arrive before message B even if A was sent after B. Without a shared clock, ordering events across machines requires careful protocol design.

**No Shared Memory** — Machines cannot read each other's RAM. Every piece of state must be explicitly communicated over the network. Any communication can fail or be delayed.

## 1.2 The Consensus Problem

**Consensus** is the act of getting a group of machines to agree on a single value, even when some machines fail.

This is the foundation of every fault-tolerant distributed system. Examples:
- Agreeing on which machine is the leader
- Agreeing on the order of writes to a database
- Agreeing on whether a transaction should commit or abort

**Requirements for a correct consensus algorithm:**

| Property | Meaning |
|---|---|
| **Agreement** | All non-faulty nodes decide on the same value |
| **Validity** | The decided value was proposed by some node (not invented) |
| **Termination** | All non-faulty nodes eventually decide |

**The FLP Impossibility Result (1985):** Fisher, Lynch, and Paterson proved that in an asynchronous network (where messages can be arbitrarily delayed), no deterministic consensus algorithm can guarantee termination if even one node can fail. Raft works around this by using randomized timeouts and making timing assumptions — if the network is "eventually synchronous," Raft will elect a leader.

## 1.3 The CAP Theorem

Eric Brewer's CAP theorem states that a distributed system can guarantee at most two of three properties simultaneously:

- **C — Consistency:** Every read sees the most recent write
- **A — Availability:** Every request receives a response (not necessarily the most recent)
- **P — Partition Tolerance:** The system continues operating despite network partitions

Raft chooses **CP**: it prefers consistency over availability. During a network partition, the minority partition stops accepting writes rather than risk divergence. When the network heals, consistency is automatically restored.

---

# Part 2: The Raft Algorithm

## 2.1 What is Raft?

Raft is a **consensus algorithm** designed to be understandable. It was introduced in 2014 by Diego Ongaro and John Ousterhout at Stanford in the paper *"In Search of an Understandable Consensus Algorithm."*

The central design goal was understandability: Raft was explicitly designed as an alternative to Paxos, which was known to be correct but notoriously difficult to understand and implement correctly.

**What Raft provides:**
- A cluster of N servers maintains an identical ordered log of commands
- The log is replicated across all servers
- Even if up to `floor((N-1)/2)` servers fail, the log is never lost and the cluster continues to operate

## 2.2 Raft vs Paxos

| Dimension | Raft | Paxos |
|---|---|---|
| Designed for | Understandability | Theoretical correctness |
| Leader model | Strong single leader | Multi-leader variants |
| Log structure | Explicit, ordered | Implicit |
| Membership changes | Defined in paper | Left to implementer |
| Real implementations | etcd, CockroachDB, Consul, TiKV | Chubby, Zookeeper |
| Learning curve | Moderate | Very high |

## 2.3 The Three Guarantees

**Safety — Nothing bad ever happens:**
- At most one leader per term (no split brain)
- A committed entry is never lost or overwritten
- All nodes apply the same commands in the same order

**Liveness — Something good eventually happens:**
- A leader will always be elected (given enough time without network failures)
- Committed entries will eventually be applied on all nodes

**Fault Tolerance — The system survives failures:**
- N nodes tolerate `floor((N-1)/2)` simultaneous failures
- 3 nodes → tolerate 1 failure
- 5 nodes → tolerate 2 failures
- 7 nodes → tolerate 3 failures

## 2.4 Core Concepts

### Terms (Logical Clock)

Every action in Raft happens within a **term** — a monotonically increasing integer that acts as a logical clock for the cluster.

```
Term 1            Term 2            Term 3
┌──────────┐     ┌──────────┐      ┌──────────┐
│ node1    │     │ node2    │      │ node1    │
│ (leader) │     │ (leader) │      │ (leader) │
└──────────┘     └──────────┘      └──────────┘
  Election          Election
  timeout           timeout
```

Rules:
- Every server stores `currentTerm` on disk — it survives crashes
- Terms begin when an election starts and end when the next election starts
- If a server sees a message with a **higher term**, it immediately updates its term and steps down to follower
- Messages with **lower terms** are rejected as stale

### Node States

Every node is in exactly one of three states at all times:

```
              election timeout
    ┌──────────────────────────────────────┐
    │                                      ▼
┌───────────┐   votes from    ┌─────────────┐  majority  ┌──────────┐
│ Follower  │◄── higher term  │  Candidate  │ ──────────►│  Leader  │
└───────────┘                 └─────────────┘            └──────────┘
    ▲                                                          │
    └──────────────────── higher term seen ───────────────────┘
```

**Follower:**
- Default state on startup
- Passively receives log entries and heartbeats from leader
- Resets election timer on each valid heartbeat
- If timer expires without heartbeat → becomes Candidate

**Candidate:**
- Starts election by incrementing term and requesting votes
- Returns to Follower if it sees a higher term
- Becomes Leader if it wins majority
- Restarts election if timeout fires again (split vote)

**Leader:**
- Only one per term (guaranteed by majority voting)
- Sends heartbeats every 100ms to suppress follower elections
- Handles all client writes
- Replicates log entries to all followers

### The Replicated Log

The replicated log is the core data structure of Raft. Every write the application makes becomes an entry in this log.

```
Index:  1       2       3       4       5
       ┌──────┬───────┬───────┬───────┬──────────────┐
Term:  │ T:1  │  T:1  │  T:1  │  T:2  │    T:2       │
       │SET   │SET    │DEL    │SET    │SET            │
       │a=1   │b=2    │a      │c=3    │d=4            │
       └──────┴───────┴───────┴───────┴──────────────┘
                                        ↑
                                   commitIndex=5

```

Properties:
- 1-based indexing (index 0 is reserved as a sentinel)
- Each entry carries: term, index, and command bytes
- Entries are **immutable once committed** — never overwritten
- Committed means a majority of nodes have the entry in their logs

---

# Part 3: Raft Protocol Deep Dive

## 3.1 Leader Election

### Election Timer

Every follower runs an **election timer** — a countdown that resets every time a valid heartbeat is received. If the timer reaches zero before a heartbeat arrives, the follower concludes the leader is dead and starts a new election.

The timer uses a **random timeout in a range** (e.g., 500–1000ms). Randomization is critical: if all nodes used the same timeout, they would all start elections simultaneously, split votes, and never elect a leader. With randomization, one node almost always fires first and wins before others even start.

### The RequestVote RPC

When a candidate starts an election:

```
RequestVoteArgs {
    Term         uint64  // candidate's current term
    CandidateID  string  // so voters know who is asking
    LastLogIndex uint64  // index of candidate's last log entry
    LastLogTerm  uint64  // term of candidate's last log entry
}

RequestVoteReply {
    Term        uint64  // voter's term (so candidate can update itself)
    VoteGranted bool
}
```

### Voting Rules

A voter grants its vote only if **both** conditions are satisfied:

**Condition 1 — One vote per term:**
```
canVote = (votedFor == "") || (votedFor == candidateID)
```
A node votes for at most one candidate per term. `votedFor` is persisted on disk so this holds across crashes.

**Condition 2 — Log up-to-date check:**
```
candidateUpToDate = (candidateLastTerm > myLastTerm)
                 || (candidateLastTerm == myLastTerm 
                     && candidateLastIndex >= myLastIndex)
```
This is the **Leader Completeness** safety condition. A node will not vote for a candidate whose log is less complete than its own. This ensures the elected leader always has all committed entries.

Why? If an entry is committed, a majority has it. The new leader must have received votes from a majority. At least one voter in that majority had the committed entry. If the candidate's log wasn't at least as up-to-date as that voter's, it wouldn't have gotten the vote.

### Golden Rule

**If any node sees a message with a higher term, it immediately steps down to Follower.**

This applies to every RPC — RequestVote, AppendEntries, InstallSnapshot. It's the simplest and most important rule in Raft. It ensures no "zombie leader" can persist after a new term begins.

## 3.2 Log Replication

### The AppendEntries RPC

```
AppendEntriesArgs {
    Term         uint64     // leader's current term
    LeaderID     string     // so followers can redirect clients
    PrevLogIndex uint64     // index of entry immediately before new ones
    PrevLogTerm  uint64     // term of PrevLogIndex entry
    Entries      []LogEntry // new entries to append (empty = heartbeat)
    LeaderCommit uint64     // leader's commitIndex
}

AppendEntriesReply {
    Term          uint64  // follower's term
    Success       bool    // true if consistency check passed
    ConflictTerm  uint64  // term of conflicting entry (for fast backtrack)
    ConflictIndex uint64  // first index of conflicting term
}
```

### Log Consistency Check

Before appending new entries, a follower verifies that its log matches the leader's at `PrevLogIndex`:

```
if args.PrevLogIndex > 0 {
    prev := log.get(args.PrevLogIndex)
    if prev.Term == 0 {
        // don't have this entry — log too short
        reply.ConflictIndex = lastIndex + 1
        reply.ConflictTerm  = 0
        return
    }
    if prev.Term != args.PrevLogTerm {
        // term mismatch at this index
        reply.ConflictTerm  = prev.Term
        reply.ConflictIndex = firstIndexForTerm(prev.Term)
        return
    }
}
```

If the check fails, the follower returns conflict information so the leader can **skip back an entire term** instead of one entry at a time. This is a key optimization that keeps log repair fast even with large log divergences.

### Commit Rule (Raft §5.4.2)

**A leader only commits entries from its own current term.**

```
if quorumIndex > commitIndex && log.get(quorumIndex).Term == currentTerm {
    commitIndex = quorumIndex
}
```

Entries from previous terms are committed **implicitly** when a current-term entry commits after them. This prevents a subtle data loss scenario described in Raft §5.4 (Figure 8 in the paper).

**Why?** Consider a leader L1 that replicates entry E to some (but not all) nodes, then crashes. L2 becomes leader, overwrites E on some nodes, then L1 restarts and becomes leader again (it has E, which is more up-to-date). If L1 commits E by counting replicas, but L2 had already overwritten it on some nodes — those nodes have a different entry at the same index. Safety violation.

The fix: L1 must commit a new entry in its own term first. The new entry's replication guarantees that L1's log is fully propagated to a majority — including E. Only then can E be considered safe.

### Heartbeats

Heartbeats are AppendEntries RPCs with empty `Entries`. The leader sends them every `HeartbeatIntervalMs` (100ms) to:
1. Suppress follower election timers
2. Advance follower `commitIndex` (via `LeaderCommit` field)
3. Detect failed followers

## 3.3 Persistence

Raft requires three fields to survive crashes:

| Field | Why |
|---|---|
| `currentTerm` | Prevents voting twice in the same term after restart |
| `votedFor` | Prevents voting for two different candidates after restart |
| `log entries` | Committed entries must never be lost |

All other state is volatile and can be recomputed:
- `commitIndex` — recomputed from leader's `LeaderCommit` on first heartbeat
- `lastApplied` — recomputed by replaying log entries
- `nextIndex`, `matchIndex` — reinitialized on each leader election

### Crash-Safe Write Pattern

A naive write to disk is dangerous:

```
os.WriteFile("raft-state.bin", data)  // WRONG: crash mid-write = corrupt file
```

The safe pattern used in this implementation:

```
1. Write to raft-state.bin.tmp
2. f.Sync()   ← force OS to flush page cache to physical disk
3. os.Rename("raft-state.bin.tmp", "raft-state.bin")  ← atomic on POSIX
```

`Rename` is atomic on all POSIX systems (Linux, macOS). The OS either completes it or doesn't — the on-disk file is always either the complete old version or the complete new version, never corrupt.

`Sync()` before rename is critical: without it, the OS might acknowledge the rename but not yet flush the data from its page cache to disk. A power cut after rename (but before flush) would leave a renamed-but-empty file.

## 3.4 Log Compaction and Snapshots

Without compaction, the log grows forever. Once a node has applied entries 1..N to its state machine, it no longer needs those raw entries — the state machine already reflects them. It can replace entries 1..N with a single **snapshot**.

```
Before snapshot:
log: [1][2][3][4][5][6][7][8][9][10]

After snapshot through index 9:
log: [snapshot@9][10]
      ↑ sentinel entry (Index:9, Term:T)
      entries 1–9 discarded
```

The sentinel stores the index and term of the last compacted entry. This is needed for the log consistency check when `PrevLogIndex` falls at the snapshot boundary.

### InstallSnapshot RPC

If a follower's `nextIndex` falls before the leader's snapshot boundary, the leader ships the full snapshot instead of replaying log entries:

```
InstallSnapshotArgs {
    Term              uint64
    LeaderID          string
    LastIncludedIndex uint64  // snapshot covers all entries through this
    LastIncludedTerm  uint64
    Data              []byte  // raw state machine bytes
}
```

The follower:
1. Saves the snapshot to disk (atomic write)
2. Calls `log.compactTo(index, term)` — fast-forwards log
3. Signals `applyLoop` to call `stateMachine.Restore(data)`

## 3.5 Dynamic Membership (Raft §6)

### Single-Server Changes

Adding or removing one node at a time is safe because any two majorities in the old and new configurations must overlap by at least one server:

```
3-node cluster adding node4:
  Old majority: 2 of {node1, node2, node3}
  New majority: 3 of {node1, node2, node3, node4}
  Overlap guaranteed: at least 1 node in common
```

If majorities always overlap, it's impossible for two leaders to be elected simultaneously — one in the old config and one in the new config — because a node in the overlap would have to vote for both.

### Config Log Entries

Membership changes are treated as special log entries:

```go
LogEntry{
    IsConfig:   true,
    ConfigOp:   "add",     // or "remove"
    ConfigPeer: "node4:7001",
}
```

They are replicated and committed like any other entry. When applied:
- `add`: new peer added to `n.peers`, `nextIndex`/`matchIndex` initialized
- `remove`: peer removed from `n.peers`, tracking state cleaned up
- `remove self`: `n.peers = nil`, node steps down if leader

### Leader Self-Removal — A Special Case

The leader is never in its own `n.peers` list (a node doesn't track itself as a peer). When the leader removes itself:

1. Special path in `RemovePeer` detects `rpcAddr == SelfAddr`
2. Config entry is replicated to all followers
3. Majority commits the entry
4. `applyConfigEntry` clears `n.peers`, sets `n.state = Follower`
5. Heartbeats stop; followers elect a new leader

**The cascading commit limitation (Raft §5.4.2):**
The removed leader never sends a final heartbeat with `LeaderCommit=N` (it stepped down first). The new leader has the remove entry in its log but can't commit it until a new same-term entry commits. This resolves as soon as the next write is submitted.

---

# Part 4: Go Concurrency in the Implementation

## 4.1 Goroutines

A goroutine is a lightweight thread managed by the Go runtime. Goroutines are much cheaper than OS threads — you can run millions simultaneously. They are multiplexed onto OS threads by the Go scheduler.

**In this implementation, goroutines are used for:**

| Goroutine | Purpose |
|---|---|
| `go node.Run()` | Main event loop — drives all state transitions |
| `go n.applyLoop()` | Background loop — applies committed entries to state machine |
| `go n.startElection()` | Runs an election concurrently without blocking the event loop |
| `go n.sendToPeer(peer, ...)` | Sends one AppendEntries RPC without blocking other peers |
| `go n.sendSnapshotToPeer(...)` | Ships snapshot to lagging follower |
| `go transport.Serve()` | Listens for incoming Raft gRPC requests (protobuf over HTTP/2) |

### Why spawn goroutines for RPCs?

```go
// WRONG: sequential — if node2 is slow, node3 waits
reply2 := sendToPeer(node2, args)
reply3 := sendToPeer(node3, args)

// RIGHT: parallel — all RPCs in-flight simultaneously
go sendToPeer(node2, args)
go sendToPeer(node3, args)
```

If the leader sent AppendEntries sequentially, one slow follower would delay all others. The heartbeat interval would be violated. Goroutines make parallel fan-out trivial.

## 4.2 Mutexes (sync.Mutex)

A mutex (mutual exclusion lock) ensures that only one goroutine accesses shared state at a time.

```go
type RaftNode struct {
    mu sync.Mutex
    // all fields below protected by mu
    currentTerm uint64
    votedFor    string
    state       NodeState
    // ...
}
```

**Lock discipline in this implementation:**

```go
// Pattern 1: defer unlock (most common)
func (n *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
    n.mu.Lock()
    defer n.mu.Unlock()
    // safe to read/write all fields
}

// Pattern 2: release before blocking operations (RPCs)
func (n *RaftNode) sendHeartbeats() {
    // called with n.mu held
    n.mu.Unlock()      // release before network calls
    for _, peer := range peers {
        go n.sendToPeer(peer, args)  // non-blocking
    }
    n.mu.Lock()        // reacquire — caller expects lock held
}
```

**Why release before RPCs?** If the lock is held during an HTTP request (which can take 50–200ms), no other goroutine can process incoming RPCs. This would cause deadlocks and heartbeat failures. The pattern is: take lock → read state → release lock → do network I/O → re-take lock → update state.

### Lock Ordering

This implementation uses a **single mutex** for all Raft state. With a single lock, there is no risk of deadlock from incorrect lock ordering. Deadlocks from `sync.Mutex` only occur when two goroutines each hold one lock and wait for the other.

## 4.3 Channels

Channels are Go's primitive for communicating between goroutines. They implement the Go philosophy: *"Don't communicate by sharing memory; share memory by communicating."*

### Buffered vs Unbuffered Channels

```go
unbuffered := make(chan struct{})    // send blocks until receiver is ready
buffered   := make(chan struct{}, 1) // send doesn't block if < 1 item pending
```

**Channels used in this implementation:**

```go
heartbeatC    chan struct{}     // cap:1  — signals "valid heartbeat received"
commitNotifyC chan struct{}     // cap:1  — signals "commitIndex advanced"
snapshotNotifyC chan snapshotToApply // cap:1 — signals "install this snapshot"
applyCh       chan ApplyMsg    // cap:64 — committed entries flow to application
stopCh        chan struct{}    // closed on Stop() — signals all loops to exit
done          chan struct{}    // closed when Run() returns — Stop() waits on this
```

### Non-Blocking Send Pattern

```go
func (n *RaftNode) notifyHeartbeat() {
    select {
    case n.heartbeatC <- struct{}{}:  // send if space available
    default:                           // drop if already pending
    }
}
```

The `default` case makes the send non-blocking. If the channel already has a pending signal, we don't need another — one is enough to reset the timer. This pattern prevents goroutines from blocking on signal delivery.

### Channel for Shutdown

```go
// In Stop():
close(n.stopCh)   // broadcast to all goroutines: time to exit
<-n.done          // wait until Run() has fully exited

// In Run() and applyLoop():
select {
case <-n.stopCh:
    return        // clean exit
case <-other:
    // normal work
}
```

Closing a channel is a **broadcast** — every goroutine blocked on `<-stopCh` will immediately unblock. This is cleaner than sending N signals to N goroutines.

## 4.4 Atomic Operations (sync/atomic)

For counters that are read and written from multiple goroutines without needing the main mutex:

```go
type RaftNode struct {
    heartbeatsRecv atomic.Uint64  // total heartbeats received
    heartbeatsSent atomic.Uint64  // total heartbeat rounds sent
}

// Writer (no lock needed):
n.heartbeatsSent.Add(1)

// Reader (no lock needed):
func (n *RaftNode) HeartbeatsSent() uint64 {
    return n.heartbeatsSent.Load()
}
```

Atomic operations use CPU-level instructions (like `LOCK XADD` on x86) that are inherently thread-safe without a mutex. They're faster than mutexes for simple counters.

## 4.5 Select Statement

`select` lets a goroutine wait on multiple channels simultaneously:

```go
// Main event loop
for {
    select {
    case <-n.stopCh:
        return

    case <-electionTimer.C:
        // election timer fired — start election
        n.mu.Lock()
        if n.state != Leader && len(n.peers) > 0 {
            n.becomeCandidate()
            go n.startElection()
        }
        n.mu.Unlock()
        resetTimer(electionTimer, n.randomElectionTimeout())

    case <-n.heartbeatC:
        // valid heartbeat received — reset timer
        resetTimer(electionTimer, n.randomElectionTimeout())

    case <-heartbeatTicker.C:
        // send heartbeats if leader
        n.mu.Lock()
        if n.state == Leader {
            n.sendHeartbeats()
        }
        n.mu.Unlock()
    }
}
```

**Key property:** If multiple cases are ready simultaneously, Go picks one at random. This is intentional — it prevents starvation and makes the system fair.

## 4.6 Timer and Ticker

```go
// Timer fires once after a duration
electionTimer := time.NewTimer(n.randomElectionTimeout())

// Ticker fires repeatedly at fixed intervals
heartbeatTicker := time.NewTicker(100 * time.Millisecond)
```

### Safe Timer Reset

Resetting a timer is tricky — if the timer already fired (channel has an item), you must drain it before resetting, or you'll get a spurious fire:

```go
func resetTimer(t *time.Timer, d time.Duration) {
    if !t.Stop() {
        select {
        case <-t.C:   // drain if already fired
        default:
        }
    }
    t.Reset(d)
}
```

## 4.7 Interfaces

Interfaces enable **dependency inversion** — the core Raft logic depends on abstractions, not concrete implementations.

```go
type Transport interface {
    RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error)
    AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error)
    InstallSnapshot(ctx context.Context, addr string, args InstallSnapshotArgs) (InstallSnapshotReply, error)
    Close() error
}
```

The same Raft node runs with:
- `GRPCTransport` in Docker (protobuf messages over HTTP/2 gRPC)
- `MemoryTransport` in tests (direct function calls, no network)

The node doesn't know or care which transport it has — it only calls the `Transport` interface. Tests run 1000x faster because there's no actual network overhead. Swapping HTTP/JSON for gRPC required zero changes to the Raft core.

```go
type StateMachine interface {
    Apply(command []byte) interface{}
    Snapshot() ([]byte, error)
    Restore(data []byte) error
}
```

The KVStore implements `StateMachine`. Swapping it for a different application (a distributed counter, a job queue) requires zero changes to the Raft layer.

## 4.8 Context and Timeouts

```go
ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()

reply, err := n.transport.AppendEntries(ctx, peer, args)
```

`context.WithTimeout` creates a context that automatically cancels after the deadline. The gRPC client respects this — if the RPC takes longer than the deadline, `AppendEntries` returns with an error. The goroutine then exits cleanly rather than blocking indefinitely.

`defer cancel()` is always called to release resources even if the context expires naturally.

## 4.9 Encoding: Gob, Protobuf, and JSON

**Gob** (Go Binary) — for persistent state on disk:

```go
// Encode to file
gob.NewEncoder(f).Encode(PersistedState{...})

// Decode from file
gob.NewDecoder(f).Decode(&state)
```

Gob is Go-specific, compact, and efficient. It's used for `raft-state.bin` and `raft-snapshot.bin` because only Go processes read them and performance matters.

**Protobuf** — for Raft RPCs between nodes (gRPC transport):

```go
// Defined once in proto/raft.proto
message AppendEntriesArgs {
    uint64 term    = 1;
    string leader_id = 2;
    ...
}

// Generated code handles serialization automatically
// In transport_grpc.go — client side:
pb.NewRaftClient(conn).AppendEntries(ctx, &pb.AppendEntriesArgs{...})

// In transport_grpc.go — server side (grpcHandler):
func (h *grpcHandler) AppendEntries(_ context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error)
```

Protobuf is used for inter-node RPCs because it produces compact binary messages (~3x smaller than JSON), serializes/deserializes faster, and travels over HTTP/2 (multiplexed, persistent connections). The schema is defined once in `raft.proto` and `protoc` generates all the boilerplate.

**JSON** — for the KVStore HTTP API (clients only):

```go
// Status endpoint
json.NewEncoder(w).Encode(map[string]interface{}{
    "node_id": nodeID, "state": node.State().String(), ...
})

// Admin endpoints
json.NewDecoder(r.Body).Decode(&req)
```

JSON is used for the client-facing HTTP API because it's human-readable, works with `curl`, and browsers can consume it. It is **not** used for Raft inter-node RPCs.

---

# Part 5: Architecture and Code Organization

## 5.1 Package Structure

```
raft/                       — core consensus layer
  node.go                   — RaftNode struct, Config, Submit, AddPeer, RemovePeer
  election.go               — becomeFollower/Candidate/Leader, startElection, HandleRequestVote
  replication.go            — sendHeartbeats, sendToPeer, maybeCommit, HandleAppendEntries
  apply.go                  — applyLoop, ApplyMsg, snapshotToApply
  log.go                    — Log struct, LogEntry, append/get/slice/compactTo
  persist.go                — persist(), loadState(), PersistedState
  snapshot.go               — TakeSnapshot, HandleInstallSnapshot, sendSnapshotToPeer
  rpc.go                    — RequestVoteArgs/Reply, AppendEntriesArgs/Reply, InstallSnapshotArgs
  transport.go              — Transport interface
  transport_grpc.go         — GRPCTransport — protobuf over HTTP/2 (production)
  transport_http.go         — HTTPTransport — JSON over HTTP (legacy, kept for reference)
  transport_memory.go       — MemoryTransport — in-process, for tests
  state_machine.go          — StateMachine interface

proto/
  raft.proto                — gRPC service + message definitions (source of truth)
  raft.pb.go                — generated protobuf message structs
  raft_grpc.pb.go           — generated RaftClient + RaftServer interfaces

kvstore/
  kvstore.go                — KVStore implementing StateMachine

cmd/kvstore/
  main.go                   — binary entrypoint: wires Raft + KVStore + HTTP server

infra/
  Dockerfile                — multi-stage build
  docker-compose.yml        — 4-node cluster definition
```

## 5.2 RaftNode Data Model

```go
type RaftNode struct {
    mu sync.Mutex

    // Identity
    id    string
    peers []string

    // Persistent state (Raft Figure 2)
    currentTerm uint64
    votedFor    string
    log         *Log

    // Volatile state
    state       NodeState
    commitIndex uint64
    lastApplied uint64

    // Leader-only volatile state (reinitialized each election)
    nextIndex  map[string]uint64  // next entry index to send to each peer
    matchIndex map[string]uint64  // highest index known replicated on each peer

    // Infrastructure
    transport    Transport
    stateMachine StateMachine
    config       Config

    // Cluster
    leaderID string

    // Snapshot
    snapshotData    []byte
    snapshotNotifyC chan snapshotToApply

    // Lifecycle
    stopCh        chan struct{}
    done          chan struct{}
    heartbeatC    chan struct{}
    commitNotifyC chan struct{}
    applyCh       chan ApplyMsg

    // Observability
    heartbeatsRecv atomic.Uint64
    heartbeatsSent atomic.Uint64
}
```

## 5.3 Log Data Model

```go
type Log struct {
    entries []LogEntry  // entries[0] is always the sentinel
}

type LogEntry struct {
    Term    uint64
    Index   uint64
    Command []byte

    // Membership change fields
    IsConfig   bool
    ConfigOp   string  // "add" or "remove"
    ConfigPeer string  // RPC address
}
```

The sentinel at `entries[0]` simplifies boundary arithmetic:
- Fresh log: `{Index:0, Term:0}` — no entries yet
- After compaction: `{Index:N, Term:T}` — represents the snapshot boundary

All log methods work in **logical 1-based indices**. The physical position in the slice is `logical_index - entries[0].Index`.

## 5.4 The Event Loop (Run())

The main event loop is a single goroutine that owns all state transitions. All state reads and writes happen under `n.mu`.

```
┌─────────────────────────────────────────────────────────┐
│                        Run()                             │
│                                                          │
│  loadState()     restore from disk                       │
│  restoreFromSnapshot()  rebuild state machine            │
│  go applyLoop()  start background applicator             │
│                                                          │
│  ┌─────────────────────────────────────┐                │
│  │              select {}               │                │
│  │                                      │                │
│  │  stopCh      → exit cleanly          │                │
│  │  electionTimer.C → start election   │                │
│  │  heartbeatC  → reset election timer  │                │
│  │  heartbeatTicker.C → send heartbeats │                │
│  └─────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────┘
```

**Why a single event loop?** It eliminates races on state transitions. If `becomeFollower`, `becomeCandidate`, and `becomeLeader` could all be called from different goroutines simultaneously, state could be corrupted. The event loop serializes all transitions.

## 5.5 The Apply Loop

The apply loop runs in a **separate goroutine** to decouple log commitment from state machine application:

```go
func (n *RaftNode) applyLoop() {
    for {
        select {
        case <-n.stopCh:
            return
        case snap := <-n.snapshotNotifyC:
            n.stateMachine.Restore(snap.data)
            // send snapshot notification on applyCh
        case <-n.commitNotifyC:
            n.mu.Lock()
            for n.lastApplied < n.commitIndex {
                n.lastApplied++
                entry := n.log.get(n.lastApplied)
                if entry.IsConfig {
                    n.applyConfigEntry(entry)  // fast, under lock
                    n.mu.Unlock()
                } else {
                    n.mu.Unlock()
                    result := n.stateMachine.Apply(entry.Command)  // may be slow
                    // send result on applyCh
                }
                n.mu.Lock()
            }
            n.mu.Unlock()
        }
    }
}
```

**Why separate?** `stateMachine.Apply()` is application code. It might do disk I/O, network calls, or complex computation. If called inside the event loop, it would stall heartbeats and all incoming RPCs. The apply loop decouples the two: the event loop advances `commitIndex`, and the apply loop drains entries at its own pace.

---

# Part 6: The KVStore Application

## 6.1 Design

The KVStore is a linearizable key-value store built on top of the Raft log. It implements the `StateMachine` interface.

```go
type KVStore struct {
    mu      sync.RWMutex
    data    map[string]string
    pending map[uint64]pendingCall   // log index → waiting client call

    node *raft.RaftNode
}
```

**Linearizability** means every operation appears to execute atomically at a single point in time between its invocation and response, and the order respects real time. Reads always see the result of the most recently completed write.

## 6.2 Write Path (Set/Delete)

```
client.Set("name", "alice")
  → encode operation: gob.Encode({Type:"SET", Key:"name", Value:"alice"})
  → node.Submit(encoded bytes)
     → raft log entry created, majority replication
  → applyLoop: stateMachine.Apply(encoded bytes)
     → decode operation, kv.data["name"] = "alice"
     → pending[index].ch ← result
  → client.Set returns nil
```

The key insight: the client **blocks** until the Raft log entry is committed and applied. It doesn't return until the write is durable on a majority.

## 6.3 Read Path (Get)

```
client.Get("name")
  → kv.mu.RLock()
  → return kv.data["name"]
  → kv.mu.RUnlock()
```

Reads are served locally from the in-memory map. No Raft log entry. No network call. This is fast but has a trade-off: the value may be up to one heartbeat interval (100ms) stale.

## 6.4 State Machine Interface

```go
type StateMachine interface {
    Apply(command []byte) interface{}  // execute command, return result
    Snapshot() ([]byte, error)         // serialize full state to bytes
    Restore(data []byte) error         // rebuild state from snapshot bytes
}
```

The KVStore implements all three:
- `Apply`: decodes a SET/DEL/ADD operation, updates the map
- `Snapshot`: gob-encodes the entire `data` map
- `Restore`: gob-decodes the map, replacing all in-memory state

## 6.5 HTTP API

| Method | Path | Who handles it |
|---|---|---|
| `PUT` | `/keys/{key}?value={val}` | Leader (followers redirect) |
| `GET` | `/keys/{key}` | Any node (local read) |
| `DELETE` | `/keys/{key}` | Leader (followers redirect) |
| `POST` | `/admin/add-node` | Leader (followers 307-redirect) |
| `POST` | `/admin/remove-node` | Leader (followers 307-redirect) |
| `GET` | `/status` | Any node |

**Redirect codes matter:**
- `302 Found` → browsers and curl **change** the method to GET on redirect
- `307 Temporary Redirect` → browsers and curl **preserve** the original method

Admin endpoints use 307 to preserve the POST method. Write endpoints use 302 (a known limitation — clients must use `?value=` query param and `-L` flag).

---

# Part 7: Deployment

## 7.1 Docker Architecture

Each node runs in its own Docker container with its own network namespace:

```
Host machine                Docker bridge network
┌────────────────────────────────────────────────────┐
│                                                    │
│  ┌──────────────┐  ┌──────────────┐               │
│  │   node1      │  │   node2      │               │
│  │ :7001 RPC    │  │ :7001 RPC    │               │
│  │ :8081 HTTP   │  │ :8082 HTTP   │               │
│  └──────────────┘  └──────────────┘               │
│                                                    │
│  Port bindings (host → container):                 │
│  localhost:7001 → node1:7001                       │
│  localhost:8081 → node1:8081                       │
│  localhost:8082 → node2:8082                       │
└────────────────────────────────────────────────────┘
```

**Why all nodes use `:7001` for Raft RPC:** Each container has its own network namespace — they don't share ports. Docker DNS resolves `node2:7001` to node2's container IP. The host machine maps them to different ports (`7001`, `7002`, `7003`) for external debugging, but containers talk to each other directly using DNS names.

**Persistent state:** Docker named volumes survive container restarts. `/data/raft-state.bin` and `/data/raft-snapshot.bin` persist across `docker stop/start`. Only `docker compose down -v` wipes the volumes.

## 7.2 Environment Variables

| Variable | Example | Description |
|---|---|---|
| `NODE_ID` | `node1` | Unique identifier |
| `RAFT_RPC_ADDR` | `:7001` | Raft RPC listen address |
| `RAFT_PEERS` | `node2:7001,node3:7001` | Peer RPC addresses |
| `HTTP_ADDR` | `:8081` | Client HTTP API address |
| `PEER_HTTP_ADDRS` | `node1=localhost:8081,...` | For client redirects |
| `DATA_DIR` | `/data` | Persistent state directory |
| `ELECTION_TIMEOUT_MIN_MS` | `500` | Min election timeout |
| `ELECTION_TIMEOUT_MAX_MS` | `1000` | Max election timeout |
| `HEARTBEAT_INTERVAL_MS` | `100` | Heartbeat interval |

## 7.3 Temporary Stop vs Permanent Removal

This is a common point of confusion:

| Intent | Docker | Admin API | Re-join |
|---|---|---|---|
| Temporary maintenance | `docker stop node3` | (nothing) | `docker start node3` — automatic |
| Permanent removal | `docker stop node3` | `remove-node` | `docker start node3` + `add-node` |

If you call `remove-node` and then just `docker start` without `add-node`, the node's container runs but it is not in the cluster. It receives no heartbeats. With peers configured in `RAFT_PEERS`, it will start elections and disrupt the cluster with high-term RPCs.

---

# Part 8: Safety Properties and Interview Preparation

## 8.1 The Five Safety Properties

| Property | Statement |
|---|---|
| **Election Safety** | At most one leader per term |
| **Leader Append-Only** | A leader never overwrites or deletes entries — only appends |
| **Log Matching** | If two logs have the same index and term, they are identical up to that index |
| **Leader Completeness** | If an entry is committed in term T, it will be present in all leaders with term > T |
| **State Machine Safety** | If a node has applied entry N, no other node will ever apply a different entry at index N |

## 8.2 Common Interview Questions

**Q: What is the Raft algorithm?**  
A consensus algorithm that allows a cluster of servers to maintain a replicated log. One server is elected leader and handles all writes. Writes are committed once a majority of servers have the entry. Raft guarantees that committed entries are never lost even if servers crash.

**Q: How does leader election work?**  
When a follower doesn't receive heartbeats within a randomized election timeout (500–1000ms), it becomes a candidate, increments its term, and requests votes. A candidate wins if it gets votes from a majority. Voting conditions: (1) voter hasn't voted in this term, (2) candidate's log is at least as up-to-date. Randomized timeouts prevent simultaneous elections.

**Q: What does "at least as up-to-date" mean for log comparison?**  
Compare last log terms first — higher wins. If equal, compare log lengths — longer wins. This ensures the elected leader always has all committed entries.

**Q: How does Raft prevent two leaders?**  
Majority voting. In a cluster of N, a candidate needs `floor(N/2) + 1` votes. Since two disjoint majorities cannot exist (they would need more than N total votes), two leaders cannot be elected in the same term.

**Q: What happens when a leader crashes?**  
Followers stop receiving heartbeats. Election timers fire (randomized to avoid ties). One follower becomes candidate, wins election, becomes new leader. The new leader reinitializes `nextIndex` and `matchIndex`, sends heartbeats, and resumes replication. Any uncommitted entries from the old leader are either replicated or overwritten (if the new leader doesn't have them).

**Q: Why can't a leader commit entries from previous terms directly?**  
Consider a leader that replicates entry E (from term 1) to some nodes, crashes, a new leader overwrites it on those nodes, the old leader restarts and becomes leader again. It has E (more up-to-date log). If it could commit E by counting replicas, it might count nodes that no longer have E. Safety violation. The fix: commit only current-term entries by counting; previous entries are committed implicitly once a current-term entry commits after them.

**Q: What is a network partition and how does Raft handle it?**  
A partition splits the cluster into groups that can't communicate. In a 4-node cluster split 2+2, neither side has a majority (3 needed). No leader can be elected on either side. Writes are blocked. When the partition heals, the cluster elects a new leader and resumes. This is CP behavior — availability is sacrificed to preserve consistency.

**Q: What state must a Raft node persist on disk and why?**  
Three fields: `currentTerm` (prevents voting twice in the same term after restart), `votedFor` (prevents voting for two candidates after restart), and `log entries` (committed entries must survive crashes). All other state can be recomputed.

**Q: How does Raft handle a slow follower that has fallen far behind?**  
If the leader's log has been compacted past the point the follower needs, the leader sends an `InstallSnapshot` RPC instead of `AppendEntries`. The follower restores its state machine from the snapshot and then resumes normal replication from the snapshot boundary.

**Q: What is the difference between `commitIndex` and `lastApplied`?**  
`commitIndex` is the highest log index known to be committed (majority has it). `lastApplied` is the highest index actually applied to the state machine. `lastApplied <= commitIndex` always. The gap represents entries committed but not yet applied. The apply loop drains this gap asynchronously.

**Q: How does single-server membership change work?**  
Adding or removing one node at a time ensures that any two majorities (old and new config) overlap by at least one node. The change is submitted as a special log entry, replicated and committed like any other entry. The new peer is added immediately (before commit) to start replication early. Followers update their peer list only when the config entry is applied.

**Q: Why does adding a node happen before the config entry commits?**  
To let the new node start catching up on missed entries immediately. Waiting until commit would delay replication unnecessarily. It's safe because a single-server change maintains the majority overlap property even with the new peer counted.

## 8.3 Key Numbers to Remember

| Parameter | Value | Why |
|---|---|---|
| Heartbeat interval | 100ms | Fast enough to suppress election timeouts |
| Election timeout min | 500ms | 5× heartbeat — tolerates several missed heartbeats |
| Election timeout max | 1000ms | Spread prevents simultaneous elections |
| Cluster sizes | 3, 5, 7 | Odd numbers maximize fault tolerance |
| Max failures for N nodes | `floor((N-1)/2)` | 3→1, 5→2, 7→3 |
| Majority quorum | `floor(N/2) + 1` | 3→2, 5→3, 7→4 |

## 8.4 Common Bugs to Be Aware Of

| Bug | Symptom | Fix |
|---|---|---|
| Voting twice in same term | Split brain possible | Persist `votedFor` before replying |
| Not clearing `votedFor` on term update | Stale vote persists | `becomeFollower` always resets `votedFor=""` |
| Committing entries from previous terms | Safety violation | Only commit entries where `entry.Term == currentTerm` |
| Leader sends heartbeats to self | No-op but wastes CPU | `peers` never contains self |
| Election timer not reset on vote grant | Competing election during legitimate candidate | `notifyHeartbeat()` on vote grant |
| RPC timeout equals heartbeat interval | Tight: 1 slow RPC = missed heartbeat | RPC timeout < heartbeat interval |
| Snapshot install missing `leaderID` update | Client redirect fails after snapshot | Set `n.leaderID = args.LeaderID` in `HandleInstallSnapshot` |
| Removed node keeps electing (self not in peers) | High term accumulation | Check `SelfAddr` in remove path, clear `n.peers` |

---

# Part 9: Known Limitations and Production Gaps

| Limitation | Description | Production Solution |
|---|---|---|
| Stale reads | GET served locally — up to 1 heartbeat behind | Read index or leader leases (etcd uses this) |
| No no-op on election | Previous-term entries stall until next write | Append no-op entry immediately on becoming leader |
| No leader transfer | Leader must step down gracefully by removing itself | `TransferLeadership` RPC to hand off to specific node |
| No pre-vote | Partitioned nodes accumulate high terms and disrupt cluster on reconnect | Pre-vote phase: check if you'd win before incrementing term |
| No learner state | New node counts toward quorum immediately | Non-voting learner until caught up, then promote |
| Single machine | All nodes on one host = no real fault tolerance | Deploy across machines or availability zones |
| No TLS | gRPC Raft RPCs and KVStore HTTP traffic are plaintext | Mutual TLS on gRPC (`grpc.WithTransportCredentials`), TLS on HTTP API |
| Silent persist errors | Disk write failure ignored | Crash the node — running with corrupt state is worse |

---

*This documentation covers the Raft consensus algorithm and its Go implementation including leader election, log replication, persistence, snapshotting, and dynamic cluster membership. All concepts were validated through a working 4-node Docker cluster.*
