# Raft Consensus — Distributed KV Store

A complete implementation of the Raft consensus algorithm in Go, with a linearizable key-value store built on top, HTTP/JSON transport, Docker containerization, and live cluster membership changes.

---

## Table of Contents

- [What is Raft?](#what-is-raft)
- [What Problem Does It Solve?](#what-problem-does-it-solve)
- [Raft vs Paxos](#raft-vs-paxos)
- [The Three Guarantees](#the-three-guarantees)
- [Real World Usage](#real-world-usage)
- [Architecture](#architecture)
- [Package Structure](#package-structure)
- [Checkpoints Implemented](#checkpoints-implemented)
- [Raft Concepts and Conditions](#raft-concepts-and-conditions)
- [Code Flow](#code-flow)
- [Use Cases with Diagrams](#use-cases-with-diagrams)
- [Deployment](#deployment)
- [API Reference](#api-reference)
- [Observability](#observability)
- [Debug Tools](#debug-tools)
- [Known Limitations](#known-limitations)

---

## What is Raft?

Raft is a **distributed consensus algorithm** — a protocol that allows a cluster of servers to agree on a sequence of values even when some servers crash or become unreachable.

It was introduced in the 2014 paper *"In Search of an Understandable Consensus Algorithm"* by Diego Ongaro and John Ousterhout at Stanford. The core design goal was understandability: Raft was explicitly designed to be easier to teach, implement, and reason about than Paxos.

At its heart, Raft answers one question: **how do N machines maintain an identical, ordered log of commands, even when machines fail?**

---

## What Problem Does It Solve?

### The Split-Brain Problem

In a distributed system with a single primary server, if the primary crashes:
- Clients cannot write new data
- If a new primary is elected without coordination, two primaries might both accept writes — leading to **split-brain**: two servers with diverging state, no way to reconcile

Raft solves this by requiring a **majority quorum** for every decision. In a 3-node cluster, at least 2 nodes must agree before any write is committed. This makes it mathematically impossible to have two leaders simultaneously — there is only one majority.

### The Data Loss Problem

Without a replicated log, a crashed server loses its data. Raft maintains a **replicated log** — every write is copied to a majority of nodes before being acknowledged. If the leader crashes, any node that received a quorum of writes can become the new leader with no data loss.

### The Consistency Problem

Without coordination, two clients reading from two different servers might get different values. Raft provides **linearizability**: every read and write appears to happen at a single point in time, on a single logical machine, even though it runs on many physical machines.

---

## Raft vs Paxos

| | Raft | Paxos |
|---|---|---|
| Designed for | Understandability | Theoretical correctness |
| Leader | Strong single leader | Multi-leader variants exist |
| Log structure | Explicit, ordered | Implicit |
| Membership changes | Defined in paper | Left to implementer |
| Real implementations | etcd, CockroachDB, Consul | Chubby (Google), Zookeeper |
| Learning curve | Lower | Very high |

Raft decomposes consensus into three largely independent subproblems — leader election, log replication, and safety — and solves each one explicitly.

---

## The Three Guarantees

**Safety** — Nothing bad ever happens:
- At most one leader per term
- A committed entry is never lost
- All nodes apply the same commands in the same order

**Liveness** — Something good eventually happens:
- A leader will always be elected (given enough time without network failures)
- Committed entries will eventually be applied on all nodes

**Fault Tolerance** — The system survives failures:
- A cluster of N nodes tolerates `floor((N-1)/2)` simultaneous failures
- 3 nodes → tolerate 1 failure
- 5 nodes → tolerate 2 failures

---

## Real World Usage

| System | Uses Raft For |
|---|---|
| **etcd** | Kubernetes cluster state, service discovery |
| **CockroachDB** | Distributed SQL transactions per range |
| **Consul** | Service mesh, distributed KV, leader election |
| **TiKV** | Distributed storage layer for TiDB |
| **YugabyteDB** | Distributed ACID transactions |
| **InfluxDB** | Cluster metadata coordination |

---

## Architecture

### Cluster Overview

```
                        ┌─────────────────────────────────────┐
                        │           Raft Cluster               │
                        │                                      │
   ┌──────────────┐     │  ┌──────────┐      ┌──────────┐    │
   │              │     │  │  node1   │◄────►│  node2   │    │
   │   Client     │────►│  │ (leader) │      │(follower)│    │
   │ (curl/HTTP)  │     │  └──────────┘      └──────────┘    │
   │              │     │        │▲                 │         │
   └──────────────┘     │        ││                 │         │
                        │        ▼│           ┌──────────┐    │
                        │  ┌──────────┐       │  node3   │    │
                        │  │  node4   │◄─────►│(follower)│    │
                        │  │(follower)│       └──────────┘    │
                        │  └──────────┘                       │
                        └─────────────────────────────────────┘

   Ports (per node):
     :7001  Raft RPC  (AppendEntries, RequestVote, InstallSnapshot)
     :808x  KVStore HTTP API  (GET /keys/, PUT /keys/, DELETE /keys/)
```

### Two Address Spaces

```
  Inside Docker (container DNS):          Outside Docker (host machine):
  ┌─────────────────────────┐             ┌───────────────────────────┐
  │  node1:7001  Raft RPC   │             │  localhost:8081  HTTP API  │
  │  node2:7001  Raft RPC   │             │  localhost:8082  HTTP API  │
  │  node3:7001  Raft RPC   │             │  localhost:8083  HTTP API  │
  │  node4:7001  Raft RPC   │             │  localhost:8084  HTTP API  │
  └─────────────────────────┘             └───────────────────────────┘
        used by Raft nodes                   used by clients (curl)
        to replicate entries                 to read/write data
```

### Package Dependency Diagram

```
  ┌─────────────────────────────────────────────┐
  │              cmd/kvstore                     │
  │   main.go — wires everything together        │
  │   HTTP server, env config, admin endpoints   │
  └─────────────┬───────────────┬───────────────┘
                │               │
                ▼               ▼
  ┌─────────────────┐  ┌────────────────────────┐
  │    kvstore/     │  │        raft/            │
  │  KVStore        │  │  RaftNode (core)        │
  │  Apply()        │  │  Log, Transport         │
  │  Snapshot()     │  │  Election, Replication  │
  │  Restore()      │  │  Persistence, Snapshot  │
  └─────────────────┘  └────────────────────────┘
         │                        │
         └──────────┬─────────────┘
                    ▼
         ┌────────────────────┐
         │      infra/        │
         │  Dockerfile        │
         │  docker-compose    │
         └────────────────────┘
```

---

## Package Structure

```
.
├── raft/
│   ├── node.go           # RaftNode struct, Config, Submit, AddPeer, RemovePeer
│   ├── election.go       # becomeFollower/Candidate/Leader, startElection, HandleRequestVote
│   ├── replication.go    # sendHeartbeats, sendToPeer, maybeCommit, HandleAppendEntries
│   ├── apply.go          # applyLoop — applies committed entries to state machine
│   ├── log.go            # Log struct, LogEntry, append/get/slice/compactTo
│   ├── persist.go        # persist(), loadState() — crash-safe atomic writes to disk
│   ├── snapshot.go       # TakeSnapshot, HandleInstallSnapshot, sendSnapshotToPeer
│   ├── rpc.go            # RequestVoteArgs/Reply, AppendEntriesArgs/Reply, InstallSnapshotArgs
│   ├── transport.go      # Transport interface
│   ├── transport_http.go # HTTPTransport — JSON over HTTP, used in Docker
│   ├── transport_memory.go # MemoryTransport — in-process, used in tests
│   └── state_machine.go  # StateMachine interface (Apply, Snapshot, Restore)
│
├── kvstore/
│   └── kvstore.go        # KVStore — implements StateMachine, linearizable Set/Get/Delete
│
├── cmd/
│   ├── kvstore/main.go   # binary entrypoint — wires Raft + KVStore + HTTP server
│   └── readstate/main.go # debug tool — decodes raft-state.bin to human-readable JSON
│
└── infra/
    ├── Dockerfile        # multi-stage build — Go builder + minimal alpine runtime
    └── docker-compose.yml # 4-node cluster definition with named volumes
```

---

## Checkpoints Implemented

| Checkpoint | Description |
|---|---|
| 1 | Project scaffold — module, core types, interfaces |
| 2 | Node state transitions, persistence, event loop |
| 3 | Leader election — RequestVote RPC, randomized timeouts |
| 4 | Log replication — AppendEntries, consistency check, commit |
| 5 | Persistence — crash-safe atomic writes (fsync + rename) |
| 6 | Log compaction — snapshots, compactTo, InstallSnapshot |
| 7 | Snapshot install — lagging follower catch-up via snapshot RPC |
| 8 | Apply loop — committed entries applied to state machine asynchronously |
| 9 | KVStore state machine — linearizable SET/GET/DEL over Raft log |
| 10 | HTTP transport + Docker — real network RPC, containerized cluster |
| 11 | Dynamic membership — live add/remove nodes via config log entries |

---

## Raft Concepts and Conditions

### Terms

Every action in Raft happens within a **term** — a monotonically increasing integer that acts as a logical clock. A new term begins every time an election starts.

```
Term 1          Term 2          Term 3
┌──────────┐   ┌──────────┐   ┌──────────┐
│ node1    │   │ node2    │   │ node1    │
│ (leader) │   │ (leader) │   │ (leader) │
└──────────┘   └──────────┘   └──────────┘
  election       election       election
  timeout        timeout
```

Rules:
- Each server stores its `currentTerm` on disk — it survives crashes
- If a server sees a message with a higher term, it immediately updates its term and steps down to follower
- Stale messages (lower term) are rejected

### Leader Election

```
All nodes start as Followers
         │
         │ election timer fires (150–300ms random)
         ▼
    Candidate
    - increment currentTerm
    - vote for self
    - send RequestVote to all peers
         │
    ┌────┴────────────────────────────┐
    │                                 │
    ▼ majority votes received         ▼ sees higher term or loses
  Leader                           Follower
  - send heartbeats every 50ms     - reset election timer
  - replicate log entries          - wait for next heartbeat
```

**Voting conditions** (both must be true to grant a vote):
1. Node has not already voted in this term
2. Candidate's log is at least as up-to-date (higher last term, or same term and longer log)

**Why randomized timeouts?** If all followers had the same timeout, they would all start elections simultaneously, split votes forever, and never elect a leader. Randomization (150–300ms) ensures one node almost always times out first and wins before the others even start.

### Log Replication

```
Client write: SET key=value
       │
       ▼
  Leader appends entry to its log (index N, term T)
       │
       ├──► AppendEntries ──► node2
       ├──► AppendEntries ──► node3    ← all in parallel
       └──► AppendEntries ──► node4
                │
          majority acks received (e.g. node2 + node3)
                │
                ▼
         commitIndex = N
                │
                ▼
         apply to state machine
                │
                ▼
         reply OK to client
```

**Log Consistency Check** — before accepting entries, a follower verifies that its log matches the leader's at `PrevLogIndex`. If not, it rejects with conflict info so the leader can back up efficiently (skip an entire term per round trip).

**Commit rule** — a leader only commits entries from its **own term**. Entries from previous terms are committed implicitly when a current-term entry is committed. This prevents a subtle data loss scenario described in Raft §5.4.

### Safety Conditions

| Property | Guarantee |
|---|---|
| **Election Safety** | At most one leader per term |
| **Leader Append-Only** | A leader never overwrites its log, only appends |
| **Log Matching** | If two logs have an entry with the same index and term, they are identical up to that index |
| **Leader Completeness** | If an entry is committed in term T, it will be in the log of all leaders with term > T |
| **State Machine Safety** | If a node applies entry N, no other node will apply a different entry at index N |

### Persistence (Raft Figure 2)

Only three fields **must** survive crashes — the rest is recomputed:

| Field | Why it must persist |
|---|---|
| `currentTerm` | Prevents voting twice in the same term after restart |
| `votedFor` | Prevents voting for two different candidates after restart |
| `log entries` | Committed entries must never be lost |

**Crash-safe write pattern:**

```
WRONG:  write → crash → corrupt file on disk

RIGHT:  write to raft-state.bin.tmp
        fsync (flush OS page cache to physical disk)
        rename .tmp → raft-state.bin   ← atomic on POSIX systems
```

Rename is atomic: the OS either completes it or doesn't — the on-disk file is always either the old complete version or the new complete version, never corrupt.

### Snapshotting (Log Compaction)

Without compaction, the log grows forever. Once enough entries are applied, the state machine takes a snapshot:

```
Before snapshot:
  log: [1][2][3][4][5][6][7][8][9][10]
                              ↑ commitIndex=9, lastApplied=9

After snapshot through index 9:
  log: [snapshot@9][10]
       ↑ sentinel

  raft-snapshot.bin: full KVStore state at index 9
```

A lagging follower that needs entries before the snapshot boundary receives an `InstallSnapshot` RPC instead of `AppendEntries` — the leader ships the full snapshot in one call.

### Membership Changes (Raft §6)

Single-server changes (add/remove one node at a time) are safe because any two majorities in the old and new configurations must overlap by at least one node:

```
3-node cluster adding node4:
  Old majority: 2 of {node1, node2, node3}
  New majority: 3 of {node1, node2, node3, node4}
  Overlap: always at least 1 node in common → no split brain possible
```

Config changes are replicated as special log entries (`IsConfig=true`). When committed, all nodes update their peer list.

---

## Code Flow

### Startup Sequence

```
main.go
  │
  ├─ NewHTTPTransport(":7001")    — create RPC server
  ├─ kvstore.New(cfg)             — create KVStore + RaftNode
  │     └─ go kv.readApplyCh()   — drain committed entries
  ├─ transport.Register(node)     — wire /raft/* HTTP routes
  ├─ go transport.Serve()         — start Raft RPC listener
  ├─ go node.Run()                — start Raft event loop
  │     ├─ n.loadState()          — restore term/votedFor/log from disk
  │     ├─ n.restoreFromSnapshot()— rebuild state machine from snapshot
  │     └─ go n.applyLoop()       — background: apply committed entries
  └─ http.ListenAndServe(":808x") — start KVStore HTTP API
```

### Write Flow (PUT)

```
curl -L -X PUT http://localhost:8082/keys/name?value=raft
         │
         ▼
  node2 HTTP handler
  node2 is follower → 302 redirect to leader (node1)
         │
         ▼ (curl follows redirect with -L)
  node1 HTTP handler
  node1 is leader → store.Set("name", "raft")
         │
         ▼
  kvstore.call()
  encodeOp({Type:"SET", Key:"name", Value:"raft"})
  node.Submit(encodedBytes)
         │
         ▼
  RaftNode.Submit() [under lock]
  append LogEntry{Term:T, Index:N, Command:bytes} to log
  persist() → write to raft-state.bin.tmp → fsync → rename
  trigger replication to all peers
         │
    ┌────┴──────────────────────┐
    ▼                           ▼
  sendToPeer(node2)         sendToPeer(node3)
  AppendEntries RPC         AppendEntries RPC
    │                           │
    ▼ Success                   ▼ Success
  matchIndex[node2]=N       matchIndex[node3]=N
  maybeCommit()
         │
         ▼
  majority reached → commitIndex = N
  notifyCommit() → wake applyLoop
         │
         ▼
  applyLoop: stateMachine.Apply(command)
  kvstore: kv.data["name"] = "raft"
  applyCh ← ApplyMsg{Index:N, Term:T, Result:...}
         │
         ▼
  kvstore.readApplyCh: route result to pending caller
  pendingCall.ch ← applyResult{}
         │
         ▼
  kvstore.Set() returns nil → HTTP 200 OK
```

### Read Flow (GET)

```
curl http://localhost:8083/keys/name
         │
         ▼
  node3 HTTP handler
  r.Method == GET → serve locally (no redirect)
         │
         ▼
  store.Get("name")
  kv.mu.RLock()
  return kv.data["name"]   ← direct map lookup, no Raft log
         │
         ▼
  200 OK: "raft"
```

### Leader Election Flow

```
node2 election timer fires (no heartbeat for 150–300ms)
         │
         ▼
  becomeCandidate()
  currentTerm++, votedFor = self, persist()
         │
         ├──► RequestVote(term=T) ──► node1 (granted)
         ├──► RequestVote(term=T) ──► node3 (granted)
         │
  votes = 3 >= majority = 2
         │
         ▼
  becomeLeader()
  nextIndex[peer] = lastIndex+1, matchIndex[peer] = 0
         │
         ▼
  immediate heartbeat to all peers
  peers reset election timers, acknowledge node2 as leader
```

### Crash Recovery Flow

```
node crashes (power cut, kill, docker stop)
         │
         ▼
node restarts (docker start)
         │
         ▼
Run()
  loadState()
  ├─ open raft-state.bin
  └─ restore: currentTerm, votedFor, log entries
         │
  restoreFromSnapshot()
  ├─ open raft-snapshot.bin (if exists)
  ├─ stateMachine.Restore(data) — rebuild KVStore map
  └─ advance lastApplied/commitIndex to snapshot index
         │
  go applyLoop()
  leader sends heartbeat with LeaderCommit=N
  node: commitIndex = N → applyLoop replays entries
         │
         ▼
node fully recovered, rejoins as follower, no data loss
```

### Snapshot Flow

```
kvstore applied 100 entries (snapshotThreshold = 100)
         │
         ▼
kv.Snapshot() → gob.Encode(kv.data) → []byte
         │
         ▼
node.TakeSnapshot(data)
  saveSnapshot() → raft-snapshot.bin.tmp → fsync → rename
  log.compactTo(lastApplied) → discard entries 1–100
  persist() → raft-state.bin updated with compacted log
         │
  Before: [1][2]...[100][101][102]
  After:  [snap@100][101][102]
```

### Node Join Flow (AddPeer)

```
POST /admin/add-node {"addr":"node4:7001"}
         │ (redirects to leader if needed)
         ▼
node.AddPeer("node4:7001")
  submitConfigLocked("add", "node4:7001")
  ├─ append LogEntry{IsConfig:true, ConfigOp:"add", ConfigPeer:"node4:7001"}
  ├─ persist()
  ├─ n.peers = append(n.peers, "node4:7001")  ← immediate replication start
  ├─ nextIndex["node4:7001"] = lastIndex+1
  └─ trigger replication to all peers including node4
         │
  node4 receives entries, catches up:
  ├─ conflict resolution backs nextIndex to 1
  ├─ leader resends all entries from index 1
  └─ if compacted: InstallSnapshot first, then entries
         │
  majority acks config entry → commits
         │
  applyLoop on each node:
  entry.IsConfig → applyConfigEntry()
  node1, node2, node3: add "node4:7001" to n.peers
  node4: skip (own address)
         │
         ▼
  all 4 nodes exchange heartbeats — cluster is fully 4-node
```

---

## Use Cases with Diagrams

### Use Case 1 — Normal Write (PUT)

**Scenario:** Client writes `name=alice` to a follower node.

```
┌────────┐  PUT /keys/name?value=alice   ┌──────────┐
│ Client │ ────────────────────────────► │  node2   │
└────────┘                               │(follower)│
    ▲                                    └────┬─────┘
    │        302 → leader (node1)             │
    │ ◄───────────────────────────────────────┘
    │
    │  PUT /keys/name?value=alice (follows redirect)
    └───────────────────────────────► ┌──────────┐
                                      │  node1   │
                                      │ (leader) │
                                      └────┬─────┘
                                           │ AppendEntries (parallel)
                                     ┌─────┴──────┐
                                     ▼            ▼
                                ┌────────┐   ┌────────┐
                                │ node2  │   │ node3  │
                                └────┬───┘   └────┬───┘
                                     │ ack         │ ack
                                     └──────┬──────┘
                                            │ majority reached
                                            ▼
                                     commit + apply
                                     200 OK → client
```

**Step-by-step:**

```
Step 1 — Client hits node2 (a follower)
  node2: r.Method == PUT and state != Leader
  node2: 302 redirect → http://localhost:8081/keys/name?value=alice

Step 2 — Client follows redirect to node1 (leader)
  node1: store.Set("name", "alice")
  node1: encodes command → node.Submit(bytes)

Step 3 — Leader appends to its OWN log first (does not wait for followers)
  node1 log: [..., entry{term:1, index:5, cmd:"SET name alice"}]
  node1: persist() → write raft-state.bin.tmp → fsync → rename
  node1: commitIndex stays at 4 (not committed yet)

Step 4 — Leader sends AppendEntries to ALL followers in parallel
  node1 → node2: AppendEntries{PrevLogIndex:4, Entries:[entry5], LeaderCommit:4}
  node1 → node3: AppendEntries{PrevLogIndex:4, Entries:[entry5], LeaderCommit:4}
  node1 → node4: AppendEntries{PrevLogIndex:4, Entries:[entry5], LeaderCommit:4}

Step 5 — Each follower validates and appends
  follower checks: do I have an entry at PrevLogIndex(4) with matching term?
  YES → append entry5 to local log → persist → reply Success

Step 6 — Leader counts acknowledgements (majority check)
  node1 gets Success from node2: matchIndex[node2]=5
  node1 gets Success from node3: matchIndex[node3]=5
  maybeCommit: quorum of {node1=5, node2=5, node3=5, node4=?} → quorumIdx=5
  entry5.Term(1) == currentTerm(1) → commitIndex = 5

Step 7 — Leader applies to state machine
  applyLoop: stateMachine.Apply("SET name alice") → kv.data["name"]="alice"
  applyCh ← ApplyMsg{Index:5, ...}
  kvstore routes result → HTTP 200 OK → client

Step 8 — Followers learn commit on next heartbeat
  node1 → all: AppendEntries{LeaderCommit:5}
  followers: commitIndex = 5 → applyLoop → kv.data["name"]="alice"

Key points:
  - Leader writes locally FIRST, then replicates
  - Client gets OK after majority (not all) nodes have the entry
  - Followers apply slightly after — reads can be ~1 heartbeat stale
  - If leader crashes after step 6, entry is safe (majority have it)
```

---

### Use Case 2 — Read (GET) from Any Node

**Scenario:** Client reads `name` from a follower node.

```
┌────────┐  GET /keys/name   ┌───────────────────────────┐
│ Client │ ────────────────► │ node3 (follower)           │
└────────┘                   │ kv.mu.RLock()              │
    ▲                        │ return kv.data["name"]     │
    │   200 OK: alice        └───────────────────────────┘
    └────────────────
```

**Step-by-step:**

```
Step 1 — Client hits any node (doesn't matter which)
  node3: r.Method == GET → serve locally, no redirect

Step 2 — KVStore local read
  store.Get("name")
  kv.mu.RLock()           ← shared read lock
  val = kv.data["name"]   ← direct map lookup, no Raft involved
  kv.mu.RUnlock()
  return "alice"

Step 3 — Response to client
  HTTP 200 OK: "alice"

Key points:
  - Reads NEVER go through Raft log — instant, no network hop to leader
  - Trade-off: may return data up to 1 heartbeat (100ms) stale
  - Example: leader committed a new value but follower's applyLoop
    hasn't applied it yet — follower returns the old value
  - This is called "stale read" — acceptable in most use cases
  - For strict consistency, reads must go to leader (not implemented here)
```

---

### Use Case 3 — Leader Election

**Scenario:** The current leader crashes; the cluster elects a new one.

```
  node1 (leader, term 1) — CRASHES
         │
  node2, node3, node4 stop receiving heartbeats
         │
  each node's election timer counts down (500–1000ms random)
  node3 fires first
         │
  node3: becomeCandidate()        node2: still counting down
  term++ → term=2                 node4: still counting down
  votedFor = "node3"
  persist()
         │
  node3 → RequestVote(term=2) ──► node2 (votes YES)
  node3 → RequestVote(term=2) ──► node4 (votes YES)
         │
  votes = 3 (self + node2 + node4) >= majority(2 of 3) = 2
         │
  node3: becomeLeader()
  nextIndex[node2]=lastIndex+1, nextIndex[node4]=lastIndex+1
  matchIndex[*]=0
         │
  node3 → heartbeat ──► node2, node4
  node2, node4: reset election timers, leaderID="node3"
```

**Step-by-step:**

```
Step 1 — Heartbeat timeout
  node1 was sending heartbeats every 100ms
  node1 crashes → no more heartbeats
  node3's election timer reaches 0 (fired at ~600ms)

Step 2 — node3 becomes Candidate
  n.state = Candidate
  n.currentTerm++ (1 → 2)
  n.votedFor = "node3"
  persist() → term and vote written to disk (crash-safe)

Step 3 — node3 sends RequestVote to all peers in parallel
  RequestVoteArgs{Term:2, CandidateID:"node3",
                  LastLogIndex:4, LastLogTerm:1}

Step 4 — Each peer decides whether to vote
  Voter checks TWO conditions:
    a) Has it already voted in term 2? NO → can vote
    b) Is node3's log at least as up-to-date?
       node3.LastLogTerm(1) vs voter.lastTerm(1) → equal
       node3.LastLogIndex(4) vs voter.lastIndex(4) → equal
       → UP-TO-DATE → grant vote

Step 5 — node3 counts votes
  node3 gets vote from node2: votes=2
  node3 gets vote from node4: votes=3
  votes(3) >= majority(2) → becomeLeader()

Step 6 — node3 becomes Leader
  n.state = Leader
  n.leaderID = "node3"
  Initialize: nextIndex[node2]=5, nextIndex[node4]=5
              matchIndex[node2]=0, matchIndex[node4]=0

Step 7 — node3 sends first heartbeat
  node2, node4 receive AppendEntries{Term:2, LeaderID:"node3"}
  → reset election timers → leaderID = "node3"

Key points:
  - Randomized timeouts (500–1000ms) ensure only ONE node fires first
  - Two conditions for vote: haven't voted + log is up-to-date
  - Log up-to-date check prevents a stale node from becoming leader
  - Higher last term wins outright; equal terms use log length
  - node1 eventually restarts: sees term=2 > its term=1 → becomes follower
```

---

### Use Case 4 — Follower Crash and Recovery

**Scenario:** node3 crashes while cluster is running; writes happen during downtime; node3 restarts.

```
  node3 crashes (docker stop / power cut)
         │
  node1 (leader) keeps replicating to node2, node4
  writes during downtime: SET a=1, SET b=2, SET c=3
         │
  node3 restarts (docker start)
         │
  Run() begins:
  loadState()         restore term, votedFor, log from disk
  restoreSnapshot()   rebuild KVStore from last snapshot (if any)
  go applyLoop()      ready to apply committed entries
         │
  node1 sends heartbeat to node3:
  AppendEntries{PrevLogIndex:7, PrevLogTerm:1, LeaderCommit:7}
         │
  node3 log consistency check:
  "do I have index 7?" → NO → ConflictIndex = lastIndex+1
         │
  node1: backs up nextIndex[node3] → resends from index 5
  node3: appends entries 5,6,7 → replies Success
         │
  node3: commitIndex=7 → applyLoop applies entries 5,6,7
  kv.data["a"]="1", kv.data["b"]="2", kv.data["c"]="3"
         │
  node3 fully recovered, all data present
```

**Step-by-step:**

```
Step 1 — node3 crashes
  raft-state.bin on disk: term=1, log=[1..4] (last checkpoint)
  raft-snapshot.bin: snapshot@3 (if taken)
  volatile state (commitIndex, peers) lost

Step 2 — Cluster continues without node3
  node1+node2 = 2 out of 3 = majority → writes still accepted
  entries 5,6,7 committed on node1+node2

Step 3 — node3 restarts
  loadState(): read raft-state.bin
    currentTerm=1, votedFor="", log=[1..4]
  restoreSnapshot(): read raft-snapshot.bin
    rebuild kv.data from snapshot
    lastApplied=3, commitIndex=3
  go applyLoop(): background loop started, waits for commitNotify

Step 4 — node3 receives first heartbeat from leader (node1)
  AppendEntries{Term:1, PrevLogIndex:4, Entries:[5,6,7], LeaderCommit:7}
  Log consistency check: log.get(4).Term == 1 → matches
  Append entries 5,6,7 to local log
  commitIndex = min(LeaderCommit=7, lastIndex=7) = 7
  notifyCommit()

Step 5 — applyLoop catches up
  lastApplied=3, commitIndex=7
  Apply entry 4 → kv.data updated
  Apply entry 5 → kv.data["a"]="1"
  Apply entry 6 → kv.data["b"]="2"
  Apply entry 7 → kv.data["c"]="3"

Step 6 — node3 fully recovered
  state=follower, leader="node1", commitIndex=7, lastApplied=7
  No manual intervention needed
  All data present — nothing lost

Key points:
  - Disk persistence (raft-state.bin) survives crashes
  - On restart, volatile state is 0/nil; only persisted state restored
  - Leader sends missing entries on first heartbeat
  - If node3 missed 1000+ entries, leader may send snapshot instead
    (InstallSnapshot RPC) to avoid replaying the full log
```

---

### Use Case 5 — Snapshot and Log Compaction

**Scenario:** After 100 entries applied, the KVStore takes a snapshot to reclaim disk space.

```
  log: [1][2][3]...[98][99][100][101][102]
       ↑                    ↑
       old entries       lastApplied=100

  kv.Snapshot() → gob.Encode(kv.data) → []byte{...}
  node.TakeSnapshot(bytes)

  log: [snap@100][101][102]
        ↑ sentinel
        (entries 1–100 discarded from memory and disk)
```

**Step-by-step:**

```
Step 1 — Application decides to snapshot
  kvstore: if len(applied) >= snapshotThreshold(100)
  kv.Snapshot() → gob.Encode(kv.data) → snapshot bytes

Step 2 — Raft compacts the log
  node.TakeSnapshot(data):
    index = lastApplied = 100
    term  = log.get(100).Term
    saveSnapshot(index, term, data)  ← atomic write to raft-snapshot.bin
    snapshotData = data
    log.compactTo(100, term)
      entries[0] = {Index:100, Term:1}  ← new sentinel
      entries 1–100 discarded (GC'd)
    persist()  ← raft-state.bin updated (log now starts at sentinel)

Step 3 — A new follower joins and needs entry 50 (compacted)
  leader: nextIndex[newNode] = 51 (backed up during consistency check)
  leader: nextIndex[newNode](51) <= snapshotIndex(100)
  → send InstallSnapshot instead of AppendEntries
  InstallSnapshotArgs{LastIncludedIndex:100, LastIncludedTerm:1, Data:bytes}

Step 4 — Follower installs snapshot
  HandleInstallSnapshot:
    log.compactTo(100, 1)   ← fast-forward log to snapshot boundary
    lastApplied = 100
    commitIndex = 100
    snapshotNotifyC ← snap  ← signal applyLoop to restore

Step 5 — applyLoop restores state machine
  stateMachine.Restore(data) → rebuild kv.data from snapshot bytes
  follower now has full state at index 100
  leader resumes AppendEntries from index 101

Key points:
  - Application drives when to snapshot (it knows when state is consistent)
  - Raft controls which index to compact to (lastApplied)
  - Atomic write pattern: tmp → fsync → rename (crash-safe)
  - InstallSnapshot is used when log entries are compacted away
  - After snapshot, new nodes catch up in one RPC instead of replaying log
```

---

### Use Case 6 — Adding a Node Live (Dynamic Membership)

**Scenario:** Add node4 to a running 3-node cluster without stopping it.

```
  3-node cluster: node1(leader), node2, node3
  Quorum = 2 of 3

         │
  docker compose up node4 -d
  node4 starts with RAFT_PEERS="" → no elections (empty-peers guard)
         │
  POST /admin/add-node {"addr":"node4:7001"}
         │
  node1: submitConfigLocked("add", "node4:7001")
  node1: peers = [node2, node3, node4]  ← immediate (before commit)
  node1: nextIndex[node4]=lastIndex+1
         │
  node1 → AppendEntries ──► node2, node3, node4 (config entry + backfill)
         │
  node4 catches up from index 1
  (or via InstallSnapshot if log compacted)
         │
  majority acks (node1+node2+node3 = 3) → config entry commits
         │
  applyConfigEntry on each node:
    node2, node3: add "node4:7001" to n.peers
    node4: skip (own SelfAddr)
         │
  4-node cluster: quorum = 3 of 4
```

**Step-by-step:**

```
Step 1 — Start node4 container
  node4 starts with RAFT_PEERS="" → len(peers)=0
  Empty-peers guard: election timer fires but no election started
  node4 waits silently

Step 2 — Leader receives add-node request
  POST /admin/add-node {"addr":"node4:7001"} → node1 (leader)
  node1.AddPeer("node4:7001"):
    append LogEntry{IsConfig:true, ConfigOp:"add", ConfigPeer:"node4:7001"}
    persist()
    n.peers = append(n.peers, "node4:7001")  ← add IMMEDIATELY
    nextIndex["node4:7001"] = lastIndex+1
    matchIndex["node4:7001"] = 0
    trigger replication to ALL peers including node4

Step 3 — node4 receives first AppendEntries
  node4 has empty log → fails consistency check
  ConflictIndex = 1 (log too short)
  leader backs up: nextIndex[node4] = 1
  leader resends ALL entries from index 1

Step 4 — node4 catches up
  node4 appends all entries 1..N including the add-node config entry
  node4 applies the config entry: ConfigPeer("node4:7001") == SelfAddr → skip

Step 5 — Config entry commits
  node1+node2+node3 acknowledge → majority of new 4-node cluster
  commitIndex advances past config entry
  applyLoop on node2, node3: applyConfigEntry("add","node4:7001")
    → n.peers = append(n.peers, "node4:7001")

Step 6 — 4-node cluster fully operational
  All nodes: peers = [node1, node2, node3, node4]
  Leader now needs 3 of 4 for quorum (was 2 of 3)

Key points:
  - Leader adds node4 to peers BEFORE commit so replication starts early
  - Single-server change: only one node added at a time — safe because
    any two majorities (old 2-of-3 and new 3-of-4) overlap by 1 node
  - node4 can catch up via log replay or InstallSnapshot
  - Quorum increases from 2→3 as soon as node4 is in peers
```

---

### Use Case 7 — Removing a Follower Safely

**Scenario:** Permanently remove node3 (a follower) from the cluster.

```
  4-node cluster: node1(leader), node2, node3, node4
  Quorum = 3 of 4
         │
  Step 1: docker stop node3   ← container stopped (optional but safe)
         │
  POST /admin/remove-node {"addr":"node3:7001"}
         │
  node1: submitConfigLocked("remove", "node3:7001")
  config entry replicated and committed on node1+node2+node4
         │
  applyConfigEntry on each node:
    node1, node2, node4: remove "node3:7001" from n.peers
    node3 (if running): ConfigPeer==SelfAddr → clear all peers → stop elections
         │
  3-node cluster: node1, node2, node4
  Quorum = 2 of 3
```

**Step-by-step:**

```
Step 1 — (Optional) Stop node3 container
  docker stop node3
  node1 tries heartbeats to node3 → timeout → retry next tick
  node1+node2+node4 = 3 of 4 → still majority → writes continue

Step 2 — Submit remove-node to leader
  POST /admin/remove-node {"addr":"node3:7001"}
  node1.RemovePeer("node3:7001"):
    "node3:7001" found in n.peers → submitConfigLocked("remove","node3:7001")
    append LogEntry{IsConfig:true, ConfigOp:"remove", ConfigPeer:"node3:7001"}
    persist()
    trigger replication (node3 still in peers for now, keeps receiving)

Step 3 — Config entry replicates and commits
  node1+node2+node4 acknowledge → 3 of 4 = majority → commit

Step 4 — applyConfigEntry on each node
  node1: remove "node3:7001" from n.peers, delete nextIndex/matchIndex
  node2: same
  node4: same
  node3 (if running): ConfigPeer("node3:7001") == SelfAddr
    → n.peers = nil
    → nextIndex/matchIndex cleared
    → node3 now isolated: empty peers, no elections, no disruption

Step 5 — 3-node cluster
  peers = [node1, node2, node4], quorum = 2 of 3
  node3 is silent (no peers to start elections with)

Key points:
  - Config entry is replicated to node3 too (while it's still in peers)
  - node3 applies "remove self" → clears peers → empty-peers guard silences it
  - Clean removal: no disruption to cluster during or after
  - If you restart node3 later, it will still be silent (peers=nil in memory)
    but on restart, RAFT_PEERS env var restores peers → need add-node again
```

---

### Use Case 8 — Removing the Leader (Special Case)

**Scenario:** node1 IS the leader. Client calls remove-node for node1.

```
  node1 (leader), node2, node3, node4
         │
  POST /admin/remove-node {"addr":"node1:7001"}
  → node1 (leader) receives it directly
         │
  Step 1: node1 submits config entry for own removal
  (special case: self is never in n.peers, checked via SelfAddr)
         │
  Step 2: config entry replicates to node2, node3, node4
  majority (node1 + node2 + node3) acks → commit
         │
  Step 3: applyConfigEntry on node1
  ConfigPeer == SelfAddr:
    n.peers = nil
    n.state = Follower   ← STEP DOWN
    n.leaderID = ""
    heartbeats stop going out
         │
  Step 4: node2, node3, node4 election timers fire (500–1000ms)
  new leader elected among them
         │
  LIMITATION: The remove-node entry (index N) was committed by node1
  but node1 stepped down before sending LeaderCommit=N to followers.
  New leader has the entry in its log but can't commit it until
  it commits a NEW entry in its own term (Raft §5.4.2).
         │
  Step 5: Do ANY write → new leader commits term-2 entry
  entry N (remove-node) cascades through → applied on new leader
  node1 removed from all peers permanently
```

**Step-by-step:**

```
Step 1 — remove-node submitted to leader (self)
  RemovePeer("node1:7001"):
    rpcAddr == n.config.SelfAddr → special path (not in n.peers)
    → submitConfigLocked("remove","node1:7001") → append config entry

Step 2 — Config entry commits
  node2, node3, node4 acknowledge → majority with node1 = commit
  node1's applyLoop applies the entry

Step 3 — node1 steps down immediately
  applyConfigEntry: ConfigPeer=="node1:7001" == SelfAddr
    n.peers = nil
    n.state = Follower
    n.leaderID = ""
  heartbeatTicker.C fires: n.state != Leader → sendHeartbeats skipped
  No more heartbeats reach node2, node3, node4

Step 4 — New election
  node2/3/4 election timers fire after 500–1000ms
  one wins, becomes new leader (term+1)

Step 5 — The cascading commit problem
  New leader (say node3) has the remove entry in its log at index N
  but commitIndex=N-1 (never learned from node1 that N was committed)
  Raft §5.4.2: cannot commit previous-term entries by replica counting
  → stuck until a new same-term entry commits

Step 6 — Trigger resolution
  Submit any write → new leader appends entry at index N+1, term=2
  majority acks → commitIndex = N+1
  applyLoop: applies entry N (remove-node) then N+1 (the write)
  node1 removed from node3's peers → heartbeats to node1 stop

Key points:
  - The "cascading commit" delay is unique to leader self-removal
  - For follower removal, leader sends LeaderCommit immediately → clean
  - Production fix: no-op entry appended right after becoming leader
    (automatically commits all previous-term entries)
  - Alternative: leader transfer (hand off leadership before removing self)
  - In THIS implementation: just do any write after remove-node to resolve
```

---

### Use Case 9 — Network Partition (Split Brain Prevention)

**Scenario:** Network splits into {node1, node2} and {node3, node4}. node1 is leader.

```
  Before partition:
  node1(leader) ←──► node2
       │                │
       ▼                ▼
  node3 ◄─────────► node4

  PARTITION:
  Side A: node1(leader), node2   ← minority (2 of 4)
  Side B: node3, node4           ← minority (2 of 4)
         │
  Side A: node1 tries to commit writes
  node1 + node2 = 2 of 4 → NOT majority → writes BLOCKED
         │
  Side B: node3 or node4 election timer fires
  RequestVote → only gets 2 votes (self + 1) → NOT majority → no leader
         │
  BOTH sides are blocked — no split brain
         │
  Partition heals:
  node1+node2+node3+node4 reconnected
  One leader elected, cluster resumes
```

**Step-by-step:**

```
Step 1 — Partition occurs
  Side A (node1, node2): can talk to each other
  Side B (node3, node4): can talk to each other
  No cross-partition communication

Step 2 — Side A (node1 as leader) tries to commit
  node1: append entry → send AppendEntries to node2, node3, node4
  node2 replies → matchIndex[node2]=N
  node3, node4 → timeout (unreachable)
  maybeCommit: quorum needs 3 of 4
    {node1=N, node2=N, node3=0, node4=0} → quorumIdx=0
    0 > commitIndex? NO → write STALLED
  Client gets no response (timeout)

Step 3 — Side B tries to elect a leader
  node3's election timer fires → becomeCandidate(term=2)
  RequestVote → node4 only → 2 votes of 4 → NOT majority
  node3 can't win → back to follower at higher term
  Same for node4

Step 4 — Both sides deadlocked
  No writes on Side A (can't reach majority)
  No leader on Side B (can't reach majority)
  This is CORRECT behavior — better to stop than to diverge

Step 5 — Partition heals
  node1, node2 regain connectivity to node3, node4
  node3/node4 have term=2 (from failed elections)
  node1 receives message with term=2 → becomeFollower(term=2)
  Election happens → one leader elected at term=2 or higher
  Cluster resumes, all nodes converge on same log

Key points:
  - Majority quorum PREVENTS split brain: 2 minority groups can't both commit
  - 4-node cluster needs 3 for quorum — one partition of 2 can never proceed
  - 3-node cluster needs 2 for quorum — one partition of 2 CAN proceed
    (safe because only one side of 2-1 has majority)
  - A node in the minority simply stalls — it doesn't corrupt state
  - When partition heals, higher-term messages automatically reconcile state
```

---

## Deployment

### Prerequisites

- Go 1.21+
- Docker Desktop
- Docker Compose v2

### Quick Start (Docker)

```bash
# Clone
git clone https://github.com/AK9175/Raft-Implementation.git
cd Raft-Implementation

# Start 3-node cluster
docker compose -f infra/docker-compose.yml up --build node1 node2 node3 -d

# Verify
curl http://localhost:8081/status
curl http://localhost:8082/status
curl http://localhost:8083/status
```

### Local Run (without Docker)

```bash
# Terminal 1 — node1
NODE_ID=node1 RAFT_RPC_ADDR=:7001 HTTP_ADDR=:8081 DATA_DIR=/tmp/raft/node1 \
RAFT_PEERS=localhost:7002,localhost:7003 \
PEER_HTTP_ADDRS="node1=localhost:8081,node2=localhost:8082,node3=localhost:8083" \
go run ./cmd/kvstore

# Terminal 2 — node2
NODE_ID=node2 RAFT_RPC_ADDR=:7002 HTTP_ADDR=:8082 DATA_DIR=/tmp/raft/node2 \
RAFT_PEERS=localhost:7001,localhost:7003 \
PEER_HTTP_ADDRS="node1=localhost:8081,node2=localhost:8082,node3=localhost:8083" \
go run ./cmd/kvstore

# Terminal 3 — node3
NODE_ID=node3 RAFT_RPC_ADDR=:7003 HTTP_ADDR=:8083 DATA_DIR=/tmp/raft/node3 \
RAFT_PEERS=localhost:7001,localhost:7002 \
PEER_HTTP_ADDRS="node1=localhost:8081,node2=localhost:8082,node3=localhost:8083" \
go run ./cmd/kvstore
```

### Docker Internals

**Why all nodes use the same port `:7001` for Raft RPC:**
Each container has its own network namespace — they don't share ports. `node1`, `node2`, `node3` all listen on `:7001` inside their own container. The host maps them to different ports (`7001`, `7002`, `7003`) only for external access.

**How containers resolve each other:**
Docker Compose creates a bridge network. Each service name becomes a DNS entry. `node2:7001` inside the Docker network resolves to node2's container IP on port 7001.

**Why `PEER_HTTP_ADDRS` uses `localhost` not container names:**
The 302 redirect URL must be resolvable by the **client** (curl on the host machine), not by containers. `node2:8082` doesn't resolve on the host, but `localhost:8082` does (via Docker port binding).

**Persistent state:**
```
Docker named volume:  raft-implementation_node1-data
Mounted at:           /data inside container
Files:
  /data/raft-state.bin      — term, votedFor, log entries
  /data/raft-snapshot.bin   — state machine snapshot
```

### Environment Variables

| Variable | Required | Example | Description |
|---|---|---|---|
| `NODE_ID` | Yes | `node1` | Unique node identifier |
| `RAFT_RPC_ADDR` | Yes | `:7001` | Address to listen for Raft RPCs |
| `HTTP_ADDR` | Yes | `:8081` | Address for KVStore HTTP API |
| `DATA_DIR` | Yes | `/data` | Directory for persistent state |
| `RAFT_PEERS` | No | `node2:7001,node3:7001` | Peer Raft RPC addresses |
| `PEER_HTTP_ADDRS` | No | `node1=localhost:8081,...` | Peer HTTP addresses for client redirects |
| `NODE_RPC_ADDR` | No | `node1:7001` | Override self RPC address for membership changes |

### Cluster Operations

**Start cluster:**
```bash
docker compose -f infra/docker-compose.yml up --build node1 node2 node3 -d
```

**Add node4:**
```bash
docker compose -f infra/docker-compose.yml up node4 -d
curl -L -X POST http://localhost:8081/admin/add-node \
     -H "Content-Type: application/json" \
     -d '{"addr":"node4:7001"}'
```

**Remove a node safely (stop first, then remove):**
```bash
docker compose -f infra/docker-compose.yml stop node3
curl -L -X POST http://localhost:8081/admin/remove-node \
     -H "Content-Type: application/json" \
     -d '{"addr":"node3:7001"}'
```

**Temporary stop vs permanent removal — critical distinction:**

Docker manages containers. The admin API manages cluster membership. They are independent layers.

| Intent | Docker | Admin API | Bring back |
|---|---|---|---|
| Temporary maintenance | `docker stop node3` | do NOT call remove-node | `docker start node3` — rejoins automatically |
| Permanent removal | `docker stop node3` | call `remove-node` | `docker start node3` + call `add-node` |

```
WRONG — starts container after remove-node without add-node:
  docker stop node3
  POST /admin/remove-node          ← node3 removed from cluster peers
  docker start node3               ← node3 gets no heartbeats, starts elections
                                      → disrupts cluster with high-term RPCs

RIGHT — temporary stop (no membership change):
  docker stop node3                ← still in peers, leader keeps trying to replicate
  docker start node3               ← receives heartbeat, rejoins as follower automatically

RIGHT — permanent removal then re-add:
  docker stop node3
  POST /admin/remove-node          ← officially removed from cluster
  docker start node3               ← container running but not in cluster
  POST /admin/add-node             ← officially re-added, leader starts heartbeats
                                      node3 receives heartbeat, becomes follower
```

**Simulate leader failure:**
```bash
# Find leader
curl http://localhost:8081/status | grep leader

# Stop it
docker compose -f infra/docker-compose.yml stop node1

# Observe new election (~300ms)
curl http://localhost:8082/status
```

**Restart a crashed node:**
```bash
docker compose -f infra/docker-compose.yml start node1
# recovers automatically: loadState → catch up → rejoin as follower
```

**Full teardown:**
```bash
docker compose -f infra/docker-compose.yml down -v --rmi all
```

---

## Testing

A complete set of scenarios to verify every feature of the cluster.

---

### Test 1 — Basic CRUD

```bash
# Write to any node (redirects to leader automatically)
curl -L -X PUT "http://localhost:8081/keys/name?value=raft"
curl -L -X PUT "http://localhost:8082/keys/version?value=1.0"
curl -L -X PUT "http://localhost:8083/keys/status?value=running"

# Read from any node (served locally, no redirect)
curl http://localhost:8081/keys/name
curl http://localhost:8082/keys/version
curl http://localhost:8083/keys/status

# Delete
curl -L -X DELETE "http://localhost:8081/keys/version"

# Verify deleted
curl http://localhost:8082/keys/version   # → 404 not found
```

---

### Test 2 — Redirect Behaviour

```bash
# Write to a follower — should get 302 redirect to leader
# Without -L: shows redirect, no data written
curl -v -X PUT "http://localhost:8082/keys/test?value=hello" 2>&1 | grep "< HTTP"
# → HTTP/1.1 302 Found

# With -L: follows redirect, data written
curl -L -X PUT "http://localhost:8082/keys/test?value=hello"
# → OK

# GET never redirects — always served locally
curl -v "http://localhost:8082/keys/test" 2>&1 | grep "< HTTP"
# → HTTP/1.1 200 OK  (no redirect)
```

---

### Test 3 — Replication Across All Nodes

```bash
# Write on leader
curl -L -X PUT "http://localhost:8081/keys/replicated?value=yes"

# Read from every node — all should return the same value
curl http://localhost:8081/keys/replicated
curl http://localhost:8082/keys/replicated
curl http://localhost:8083/keys/replicated
curl http://localhost:8084/keys/replicated
```

---

### Test 4 — Leader Election

```bash
# Find current leader
curl http://localhost:8081/status | grep leader

# Stop the leader (say node1)
docker compose -f infra/docker-compose.yml stop node1

# Wait ~300ms for new election, then check
curl http://localhost:8082/status
curl http://localhost:8083/status
# → new leader elected, term incremented by 1

# Writes still work through new leader
curl -L -X PUT "http://localhost:8082/keys/after-election?value=cluster-alive"

# Bring node1 back — rejoins as follower automatically
docker compose -f infra/docker-compose.yml start node1
curl http://localhost:8081/status
# → state: follower, same leader as node2/node3
```

---

### Test 5 — Data Durability After Crash

```bash
# Write before crash
curl -L -X PUT "http://localhost:8081/keys/before-crash?value=persisted"

# Stop a node
docker compose -f infra/docker-compose.yml stop node3

# Write while node3 is down
curl -L -X PUT "http://localhost:8081/keys/during-crash?value=also-persisted"

# Restart node3
docker compose -f infra/docker-compose.yml start node3

# Wait a second for catch-up
sleep 1

# Both keys should be on node3
curl http://localhost:8083/keys/before-crash    # → persisted
curl http://localhost:8083/keys/during-crash    # → also-persisted
```

---

### Test 6 — Node Failure Does Not Lose Data

```bash
# Write to cluster
curl -L -X PUT "http://localhost:8081/keys/durable?value=safe"

# Verify all 4 nodes have it
curl http://localhost:8081/keys/durable
curl http://localhost:8082/keys/durable
curl http://localhost:8083/keys/durable
curl http://localhost:8084/keys/durable

# Stop 1 node (cluster still has majority)
docker compose -f infra/docker-compose.yml stop node4

# Writes still work (3 of 4 nodes up = majority)
curl -L -X PUT "http://localhost:8081/keys/while-down?value=still-works"

# Bring node4 back
docker compose -f infra/docker-compose.yml start node4
sleep 1

# node4 caught up — has both keys
curl http://localhost:8084/keys/durable
curl http://localhost:8084/keys/while-down
```

---

### Test 7 — Heartbeat Monitoring

```bash
# Check heartbeat counters growing in real time
curl http://localhost:8081/status   # leader: heartbeats_sent growing
curl http://localhost:8082/status   # follower: heartbeats_received growing

# Run twice with a gap and compare counts
curl -s http://localhost:8081/status | grep heartbeats_sent
sleep 5
curl -s http://localhost:8081/status | grep heartbeats_sent
# → should have increased by ~100 (20 heartbeats/sec × 5 sec)
```

---

### Test 8 — Dynamic Membership (Add Node)

```bash
# Start with 3-node cluster (node1, node2, node3)
# Write some data
curl -L -X PUT "http://localhost:8081/keys/before-join?value=existed"

# Start node4 container
docker compose -f infra/docker-compose.yml up node4 -d

# Add node4 to cluster
curl -L -X POST http://localhost:8081/admin/add-node \
     -H "Content-Type: application/json" \
     -d '{"addr":"node4:7001"}'

# Verify node4 is follower and has old data
curl http://localhost:8084/status              # state: follower
curl http://localhost:8084/keys/before-join    # → existed (replicated)

# Write after join — node4 should get it too
curl -L -X PUT "http://localhost:8081/keys/after-join?value=new-data"
curl http://localhost:8084/keys/after-join     # → new-data
```

---

### Test 9 — Dynamic Membership (Remove Node)

```bash
# Stop target node first
docker compose -f infra/docker-compose.yml stop node3

# Remove from cluster config
curl -L -X POST http://localhost:8081/admin/remove-node \
     -H "Content-Type: application/json" \
     -d '{"addr":"node3:7001"}'

# Cluster now has node1, node2, node4
curl http://localhost:8081/status   # commit_index advanced
curl http://localhost:8082/status
curl http://localhost:8084/status

# Writes still work (3-node cluster, quorum = 2)
curl -L -X PUT "http://localhost:8081/keys/after-remove?value=3-nodes"
curl http://localhost:8084/keys/after-remove   # → 3-nodes
```

---

### Test 10 — Re-add a Removed Node

```bash
# After Test 9 (node3 removed), bring it back
docker compose -f infra/docker-compose.yml up node3 -d

# Add it back to cluster
curl -L -X POST http://localhost:8081/admin/add-node \
     -H "Content-Type: application/json" \
     -d '{"addr":"node3:7001"}'

# node3 should be follower with all data caught up
curl http://localhost:8083/status
curl http://localhost:8083/keys/after-remove   # → 3-nodes
```

---

### Test 11 — Majority Loss (Cluster Unavailable)

```bash
# Stop 2 nodes in a 3-node cluster (loses majority)
docker compose -f infra/docker-compose.yml stop node2
docker compose -f infra/docker-compose.yml stop node3

# Writes fail — no majority to commit
curl -L -X PUT "http://localhost:8081/keys/no-majority?value=blocked"
# → 503 timed out waiting for commit (after 5 seconds)

# Reads still work from surviving node (local state machine)
curl http://localhost:8081/keys/before-join    # → existed

# Restore majority
docker compose -f infra/docker-compose.yml start node2
docker compose -f infra/docker-compose.yml start node3

# Writes work again
curl -L -X PUT "http://localhost:8081/keys/restored?value=back"
```

---

## API Reference

### KVStore HTTP API

| Method | Path | Description |
|---|---|---|
| `PUT` | `/keys/{key}?value={val}` | Write a key-value pair |
| `GET` | `/keys/{key}` | Read a value (served locally) |
| `DELETE` | `/keys/{key}` | Delete a key |
| `GET` | `/status` | Node health and Raft state |
| `POST` | `/admin/add-node` | Add a node to the cluster |
| `POST` | `/admin/remove-node` | Remove a node from the cluster |

```bash
# Write (redirects to leader automatically with -L)
curl -L -X PUT "http://localhost:8081/keys/name?value=raft"

# Read from any node
curl "http://localhost:8083/keys/name"

# Delete
curl -L -X DELETE "http://localhost:8082/keys/name"

# Add node
curl -L -X POST http://localhost:8081/admin/add-node \
     -H "Content-Type: application/json" \
     -d '{"addr":"node4:7001"}'

# Remove node
curl -L -X POST http://localhost:8081/admin/remove-node \
     -H "Content-Type: application/json" \
     -d '{"addr":"node3:7001"}'
```

**Redirect behaviour:**
- `PUT` / `DELETE` → 302 to leader if called on follower (use `-L`)
- `GET` → served locally, no redirect
- `/admin/*` → 307 to leader (307 preserves POST method, unlike 302)

---

## Observability

### `/status` Endpoint

```bash
curl http://localhost:8081/status
```

```json
{
  "node_id":             "node1",
  "state":               "leader",
  "term":                5,
  "leader":              "node1",
  "commit_index":        42,
  "last_applied":        42,
  "heartbeats_received": 0,
  "heartbeats_sent":     1204
}
```

| Field | Description |
|---|---|
| `state` | `follower`, `candidate`, or `leader` |
| `term` | Current term — logical clock, increments on every election |
| `leader` | Node ID of the known leader (`""` during election) |
| `commit_index` | Highest log index committed by majority |
| `last_applied` | Highest log index applied to state machine |
| `heartbeats_received` | Total heartbeats received as follower (~20/sec) |
| `heartbeats_sent` | Total heartbeat rounds sent as leader (~20/sec) |

**Reading the counters:**
- Leader: `heartbeats_sent` grows, `heartbeats_received` = 0
- Follower: `heartbeats_received` grows, `heartbeats_sent` = 0
- Both = 0, state = follower: election in progress, no leader yet
- `commit_index` > `last_applied`: apply loop still catching up

---

## Debug Tools

### Inspect Persistent State

```bash
# Copy from container
docker cp infra-node1-1:/data/raft-state.bin /tmp/node1-state.bin

# Decode to JSON
go run ./cmd/readstate /tmp/node1-state.bin
```

```json
{
  "CurrentTerm": 5,
  "VotedFor": "node2",
  "Entries": [
    {"Term": 1, "Index": 1, "Command": "..."},
    {"Term": 2, "Index": 2, "Command": "..."}
  ]
}
```

### Inspect a Snapshot

```bash
docker cp infra-node1-1:/data/raft-snapshot.bin /tmp/node1-snap.bin
go run ./cmd/readstate /tmp/node1-snap.bin
```

### Watch Status Live

```bash
watch -n 1 'curl -s http://localhost:8081/status | python3 -m json.tool'
```

---

## Known Limitations

| Limitation | Description | Production Solution |
|---|---|---|
| **Stale reads** | GET served locally — may be ~50ms behind leader | Read index or leader leases |
| **Leader removal** | Must stop the leader first, then call remove-node | Leader transfer before removal |
| **Laptop sleep** | Goroutines freeze, timers expire, burst of elections on wake | Always-on servers |
| **No learner state** | New node counts toward quorum immediately on AddPeer | Non-voting learner until caught up, then promote |
| **Removed node re-election** | A removed node that missed its own removal can win elections | Check cluster membership in RequestVote |
| **Single machine** | All nodes on one host = no real fault tolerance | Deploy across machines or availability zones |
| **No TLS** | RPC and HTTP traffic is plaintext | Mutual TLS on all Raft RPC channels |
