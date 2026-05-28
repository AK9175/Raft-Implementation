# Raft — Visual Reference

Key concepts illustrated with Mermaid diagrams.

---

## Node Roles & State Transitions

Every node starts as a **Follower**. When no heartbeat arrives before its randomised election
timeout (500–1000 ms), it becomes a **Candidate** and solicits votes. Winning a majority
makes it **Leader**; seeing a higher term at any point resets it to Follower.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Follower : startup

    Follower --> Candidate : election timeout fires
    Candidate --> Leader : majority votes received
    Candidate --> Follower : discovers higher term
    Candidate --> Candidate : timeout — split vote, retry
    Leader --> Follower : discovers higher term
```

---

## Log Replication

The leader appends the client command, fans out `AppendEntries` to all followers in
parallel, and commits once a **majority** acknowledge. The commit result is applied to
the state machine before replying to the client.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 2
    participant F2 as Follower 3

    C->>+L: PUT /keys/name?value=alice
    L->>L: append(idx N, term T) · persist
    par replicate in parallel
        L->>F1: AppendEntries(prevIdx, entries)
        L->>F2: AppendEntries(prevIdx, entries)
    end
    F1-->>L: success
    F2-->>L: success
    Note over L: majority acks → commitIndex = N
    L->>L: apply to state machine
    L-->>-C: 200 OK
```

---

## Leader Election

When a leader goes offline, the first follower whose timer fires initiates an election.
Randomised timeouts ensure only one candidate wins in most cases.

```mermaid
sequenceDiagram
    participant N1 as node1 (leader)
    participant N2 as node2
    participant N3 as node3 (candidate)
    participant N4 as node4

    Note over N1: crashes — no more heartbeats
    Note over N3: election timer fires first
    N3->>N3: term++ · votedFor=self · persist
    N3->>N2: RequestVote(term=T+1)
    N3->>N4: RequestVote(term=T+1)
    N2-->>N3: VoteGranted
    N4-->>N3: VoteGranted
    Note over N3: 3 votes ≥ majority → becomeLeader
    N3->>N2: Heartbeat (AppendEntries, empty)
    N3->>N4: Heartbeat (AppendEntries, empty)
    Note over N2,N4: reset election timers
```

---

## Write Path — Client PUT via Follower

A `PUT` to a follower is automatically redirected to the leader (HTTP 302).
The leader replicates and commits before responding.

```mermaid
sequenceDiagram
    participant C as Client
    participant F as node2 (follower)
    participant L as node1 (leader)
    participant F2 as node3

    C->>F: PUT /keys/name?value=alice
    F-->>C: 302 Redirect → node1
    C->>+L: PUT /keys/name?value=alice (follows redirect)
    L->>L: append to log
    par
        L->>F: AppendEntries
        L->>F2: AppendEntries
    end
    F-->>L: ack
    F2-->>L: ack
    Note over L: majority → commit
    L-->>-C: 200 OK
```

---

## Read Path — Client GET (Stale Read)

Reads are served **locally** by whichever node receives the request — no Raft round-trip,
but the result may be up to one heartbeat interval (~100 ms) stale.

```mermaid
sequenceDiagram
    participant C as Client
    participant F as node3 (follower)

    C->>+F: GET /keys/name
    F->>F: kv.mu.RLock() · return kv.data["name"]
    F-->>-C: 200 OK — "alice"
    Note over C,F: no Raft involvement — instant, possibly stale
```

---

## System Architecture

```mermaid
graph TD
    Browser["Browser\nReact Dashboard :5173"]
    Sidecar["cmd/sidecar\n:9090"]
    N1["node1\n:8081 HTTP / :7001 gRPC"]
    N2["node2\n:8082 HTTP / :7002 gRPC"]
    N3["node3\n:8083 HTTP / :7003 gRPC"]
    Docker["Docker\ninfra_default network"]

    Browser -->|"REST poll + control"| Sidecar
    Sidecar -->|"docker compose up/stop"| Docker
    Docker -->|start/stop| N1
    Docker -->|start/stop| N2
    Docker -->|start/stop| N3
    Sidecar -->|"GET /status · POST /admin"| N1
    Sidecar -->|"GET /status · POST /admin"| N2
    Sidecar -->|"GET /status · POST /admin"| N3
    N1 <-->|"Raft gRPC"| N2
    N1 <-->|"Raft gRPC"| N3
    N2 <-->|"Raft gRPC"| N3
```

---

## Node Join — Live Membership Change

Adding a new node to a running cluster: sidecar wipes stale state, starts the container,
then calls `add-node` on the leader. The leader backfills the log immediately.

```mermaid
sequenceDiagram
    participant UI as Dashboard
    participant S as Sidecar :9090
    participant D as Docker
    participant L as Leader
    participant N as new node

    UI->>S: POST /nodes/create {id: node4}
    S->>S: clearNodeData(node4)
    S->>D: docker compose up node4 --build
    D-->>N: container started
    S->>S: waitForHTTP(:8084, 30s)
    S->>L: POST /admin/add-node {addr: node4:7001}
    L->>N: AppendEntries (full backfill from idx 1)
    N-->>L: caught up
    L-->>S: 200 OK
    S-->>UI: {ok: true, id: node4}
```

---

## Network Partition — Split Brain Prevention

With 4 nodes, a 2–2 split leaves **neither** side with a majority. The old leader
cannot commit, and the minority side cannot elect a new one. Both sides deadlock
safely until the partition heals.

```mermaid
graph LR
    subgraph A ["Side A — 2 of 4 — writes blocked"]
        N1["node1 (leader)"]
        N2["node2"]
    end
    subgraph B ["Side B — 2 of 4 — cannot elect"]
        N3["node3"]
        N4["node4"]
    end
    A -. "partition" .- B
```

---

## Crash Recovery

A restarted node replays from its persisted log and snapshot, then catches up
via heartbeats from the leader — no data loss.

```mermaid
flowchart TD
    Crash["node crashes"] --> Restart["docker start"]
    Restart --> Load["Run() · loadState()\nrestore term, votedFor, log"]
    Load --> Snap{snapshot\nexists?}
    Snap -->|yes| Restore["stateMachine.Restore()\nrebuild KV map"]
    Snap -->|no| Loop
    Restore --> Loop["go applyLoop()"]
    Loop --> Hb["leader heartbeat\nAppendEntries(leaderCommit=N)"]
    Hb --> Replay["replay missing entries"]
    Replay --> Done["fully recovered — rejoins as follower"]
```

---

## Package Dependency Map

```mermaid
graph TD
    CMD["cmd/kvstore\nmain.go"]
    KV["kvstore/\nKVStore · StateMachine"]
    RAFT["raft/\nRaftNode · Log · Election\nReplication · Snapshot"]
    PROTO["proto/\nraft.proto → gRPC stubs"]
    GRPC["transport_grpc.go\nHTTP/2 + protobuf"]
    MEM["transport_memory.go\nin-process transport (tests)"]
    SIDECAR["cmd/sidecar\n:9090 — Docker bridge"]
    DASH["dashboard/\nReact + Vite :5173"]

    CMD --> KV
    CMD --> RAFT
    RAFT --> PROTO
    RAFT --> GRPC
    RAFT --> MEM
    DASH -->|"HTTP REST"| SIDECAR
    SIDECAR -->|"docker + /admin API"| CMD
```
