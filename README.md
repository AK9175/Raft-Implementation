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

```
┌────────┐  PUT /keys/x?value=hello   ┌──────────┐
│ Client │ ─────────────────────────► │  node2   │
└────────┘                            │(follower)│
    ▲                                 └────┬─────┘
    │           302 → leader               │
    │ ◄────────────────────────────────────┘
    │
    │  PUT /keys/x?value=hello (follows redirect)
    └──────────────────────────────► ┌──────────┐
                                     │  node1   │
                                     │ (leader) │
                                     └────┬─────┘
                                          │ AppendEntries
                                    ┌─────┴──────┐
                                    ▼            ▼
                               ┌────────┐   ┌────────┐
                               │ node2  │   │ node3  │
                               └────┬───┘   └────┬───┘
                                    │ ack         │ ack
                                    └──────┬──────┘
                                           │ majority
                                           ▼
                                    commit + apply
                                    200 OK → client
```

### Use Case 2 — Read (GET) from Any Node

```
┌────────┐  GET /keys/x   ┌──────────────────────────┐
│ Client │ ─────────────► │ any node  (no redirect)   │
└────────┘                │ kv.data["x"] → hello      │
    ▲                     └──────────────────────────┘
    │     200 OK: hello
    └──────────────────

  No leader contact. Served from local state machine.
  Trade-off: may be up to one heartbeat (~50ms) stale.
```

### Use Case 3 — Leader Failure

```
  node1 (leader) crashes
         │
  node2, node3 stop receiving heartbeats
         │
  election timers fire (random 150–300ms)
  node2 fires first → becomeCandidate (term++)
         │
  RequestVote ──► node3 (granted)
  votes = 2 >= majority = 2
         │
  node2 → becomeLeader
  cluster resumes — no data loss
         │
  node1 restarts later:
  ├─ loadState() → restore term from disk
  ├─ receives heartbeat from node2 (higher term)
  ├─ becomeFollower, catch up via AppendEntries
  └─ rejoins as follower
```

### Use Case 4 — Node Restart / Crash Recovery

```
  node3 crashes
  node1, node2 continue (still majority, writes accepted)
         │
  node3 restarts
         │
  loadState()       → term, votedFor, log restored from .bin
  restoreSnapshot() → KVStore map rebuilt
  applyLoop()       → replay entries up to commitIndex
         │
  leader sends missing entries via AppendEntries
         │
  node3 fully caught up, rejoins as follower
  no manual intervention required
```

### Use Case 5 — Snapshot and Log Compaction

```
  100 entries applied
         │
  kv.Snapshot() → serialize map → bytes
  node.TakeSnapshot(bytes)
  ├─ write raft-snapshot.bin (atomic)
  └─ log.compactTo(100) → discard entries 1–100

  Before: [1][2][3]...[100][101][102]
  After:  [snap@100][101][102]
         │
  new node joins, needs entry 50 (compacted):
  leader → InstallSnapshot (full state at 100)
  new node: restore snapshot → receive 101, 102
```

### Use Case 6 — Adding a Node Live

```
  3-node cluster: node1(leader), node2, node3
         │
  docker compose up node4 -d
  POST /admin/add-node {"addr":"node4:7001"}
         │
  leader appends config entry (index N)
  leader adds node4 to peers immediately → replication starts
         │
  node4 catches up from index 1
         │
  config entry commits (node1+node2+node3 = majority of 4)
         │
  applyConfigEntry on node2, node3 → add node4 to their peers
         │
  4-node cluster, new quorum = 3 of 4
```

### Use Case 7 — Removing a Node Safely

```
  4-node cluster: node1(leader), node2, node3, node4
         │
  Step 1: docker compose stop node3
  node3 gone, remaining 3 still majority
         │
  Step 2: POST /admin/remove-node {"addr":"node3:7001"}
         │
  leader appends config entry (remove node3)
  commits on node1 + node2 + node4
         │
  applyConfigEntry on all:
  → delete "node3:7001" from peers
  → delete nextIndex/matchIndex for node3
         │
  3-node cluster: node1, node2, node4
  new quorum = 2 of 3
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
