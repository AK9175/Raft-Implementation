// Sidecar server — bridges Docker container lifecycle and Raft cluster membership.
//
// Two kinds of nodes:
//
//   Preset (node1-5 in docker-compose.yml):
//     Started via "docker compose up --build".
//     node1-3 are bootstrap nodes that self-form the cluster via RAFT_PEERS.
//     node4-5 start isolated and are added via add-node.
//
//   Dynamic (arbitrary, created at runtime via the UI form):
//     Started via "docker run" using the image built from the compose services.
//     Always start with empty peers and are added to the cluster via add-node.
//     Stopped with "docker stop && docker rm".
//
// Endpoints:
//
//	GET  /nodes            — combined docker + raft status for all known nodes
//	POST /nodes/create     — create and start a node (preset or dynamic)
//	POST /nodes/{id}/stop  — remove from cluster (if needed) + stop container
//
// Run from the project root:
//
//	go run ./cmd/sidecar
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ── types ─────────────────────────────────────────────────────────────────────

type raftStatus struct {
	State              string   `json:"state"`
	Term               uint64   `json:"term"`
	Leader             string   `json:"leader"`
	Peers              []string `json:"peers"`
	CommitIndex        uint64   `json:"commit_index"`
	LastApplied        uint64   `json:"last_applied"`
	HeartbeatsSent     uint64   `json:"heartbeats_sent"`
	HeartbeatsReceived uint64   `json:"heartbeats_received"`
	SnapshotIndex      uint64   `json:"snapshot_index"`
}

type NodeDef struct {
	ID        string `json:"id"`
	RPCAddr   string `json:"rpc_addr"`  // Docker-internal: "nodeX:7001"
	HTTPPort  int    `json:"http_port"` // host-mapped HTTP port
	RPCPort   int    `json:"rpc_port"`  // host-mapped gRPC port (0 for compose nodes)
	Bootstrap bool   `json:"bootstrap"` // self-forms cluster via RAFT_PEERS
	Dynamic   bool   `json:"dynamic"`   // created at runtime via docker run
}

type NodeInfo struct {
	NodeDef
	Running            bool     `json:"running"`
	InCluster          bool     `json:"in_cluster"`
	Paused             bool     `json:"paused"`
	RaftState          string   `json:"raft_state"`
	Term               uint64   `json:"term"`
	Leader             string   `json:"leader"`
	Peers              []string `json:"peers"`
	CommitIndex        uint64   `json:"commit_index"`
	LastApplied        uint64   `json:"last_applied"`
	HeartbeatsSent     uint64   `json:"heartbeats_sent"`
	HeartbeatsReceived uint64   `json:"heartbeats_received"`
	SnapshotIndex      uint64   `json:"snapshot_index"`
}

// ── pause tracking ────────────────────────────────────────────────────────────

var (
	pausedMu    sync.RWMutex
	pausedNodes = map[string]bool{}
)

func isPaused(id string) bool {
	pausedMu.RLock()
	defer pausedMu.RUnlock()
	return pausedNodes[id]
}

func setPaused(id string, v bool) {
	pausedMu.Lock()
	defer pausedMu.Unlock()
	if v {
		pausedNodes[id] = true
	} else {
		delete(pausedNodes, id)
	}
}

// ── live scenario types ───────────────────────────────────────────────────────

type LiveScenarioMeta struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Desc     string `json:"desc"`
	MinNodes int    `json:"min_nodes"`
}

type LiveScenarioResult struct {
	Name   string   `json:"name"`
	Passed bool     `json:"passed"`
	Error  string   `json:"error,omitempty"`
	DurMs  int64    `json:"duration_ms"`
	Logs   []string `json:"logs"`
}

var liveScenarioList = []LiveScenarioMeta{
	{
		ID:       "isolate_leader",
		Label:    "Isolate Leader",
		Desc:     "Pauses the current leader container. The cluster elects a new leader. After 3 seconds the old leader is restored and rejoins as follower.",
		MinNodes: 3,
	},
	{
		ID:       "lose_follower",
		Label:    "Lose a Follower",
		Desc:     "Pauses one follower. The remaining nodes still hold quorum and the cluster continues. Follower is restored after 3 seconds.",
		MinNodes: 3,
	},
	{
		ID:       "quorum_loss",
		Label:    "Quorum Loss",
		Desc:     "Pauses the majority of nodes so the cluster drops below quorum. The leader cannot commit new entries. All nodes are restored after 4 seconds.",
		MinNodes: 3,
	},
	{
		ID:       "leader_churn",
		Label:    "Leader Churn",
		Desc:     "Performs 3 rapid leader-failover cycles in a row. Each round pauses the current leader, waits for a new one, then restores. Watch the term counter climb.",
		MinNodes: 3,
	},
}

// ── node registry ─────────────────────────────────────────────────────────────

// presetNodes are defined in docker-compose.yml.
var presetNodes = []NodeDef{
	{ID: "node1", RPCAddr: "node1:7001", HTTPPort: 8081, Bootstrap: true},
	{ID: "node2", RPCAddr: "node2:7001", HTTPPort: 8082, Bootstrap: true},
	{ID: "node3", RPCAddr: "node3:7001", HTTPPort: 8083, Bootstrap: true},
	{ID: "node4", RPCAddr: "node4:7001", HTTPPort: 8084, Bootstrap: false},
	{ID: "node5", RPCAddr: "node5:7001", HTTPPort: 8085, Bootstrap: false},
}

var (
	nodesMu sync.RWMutex
	nodes   []NodeDef // presetNodes + dynamic nodes added at runtime
)

func init() {
	nodes = make([]NodeDef, len(presetNodes))
	copy(nodes, presetNodes)
}

func allNodes() []NodeDef {
	nodesMu.RLock()
	defer nodesMu.RUnlock()
	out := make([]NodeDef, len(nodes))
	copy(out, nodes)
	return out
}

func findNode(id string) (NodeDef, bool) {
	nodesMu.RLock()
	defer nodesMu.RUnlock()
	for _, n := range nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeDef{}, false
}

func addDynamicNode(n NodeDef) {
	nodesMu.Lock()
	defer nodesMu.Unlock()
	nodes = append(nodes, n)
}

func removeDynamicNode(id string) {
	nodesMu.Lock()
	defer nodesMu.Unlock()
	for i, n := range nodes {
		if n.ID == id && n.Dynamic {
			nodes = append(nodes[:i], nodes[i+1:]...)
			return
		}
	}
}

// ── globals ───────────────────────────────────────────────────────────────────

var (
	projectDir  string
	composeFile string
)

func main() {
	projectDir = os.Getenv("PROJECT_DIR")
	if projectDir == "" {
		var err error
		projectDir, err = os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
	}
	composeFile = filepath.Join(projectDir, "infra", "docker-compose.yml")

	addr := os.Getenv("SIDECAR_ADDR")
	if addr == "" {
		addr = ":9090"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", handleNodes)
	mux.HandleFunc("/nodes/create", handleCreateNode)
	mux.HandleFunc("/nodes/", handleNodeAction)
	mux.HandleFunc("/live-scenarios", handleListLiveScenarios)
	mux.HandleFunc("/live-scenarios/", handleRunLiveScenario)

	log.Printf("sidecar listening on %s  (compose: %s)", addr, composeFile)
	if err := http.ListenAndServe(addr, corsMiddleware(mux)); err != nil {
		log.Fatal(err)
	}
}

// ── handlers ──────────────────────────────────────────────────────────────────

// GET /nodes — returns combined docker + raft status for all known nodes.
func handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	all := allNodes()

	type portResult struct {
		port int
		st   *raftStatus
	}
	ch := make(chan portResult, len(all))
	for _, n := range all {
		go func(nd NodeDef) {
			if isPaused(nd.ID) {
				ch <- portResult{nd.HTTPPort, nil} // skip HTTP for paused containers
			} else {
				ch <- portResult{nd.HTTPPort, fetchRaftStatus(nd.HTTPPort)}
			}
		}(n)
	}

	statusByPort := make(map[int]*raftStatus, len(all))
	for range all {
		r := <-ch
		statusByPort[r.port] = r.st
	}

	allPeers := map[string]bool{}
	for _, st := range statusByPort {
		if st == nil {
			continue
		}
		for _, p := range st.Peers {
			allPeers[p] = true
		}
	}

	result := make([]NodeInfo, len(all))
	for i, n := range all {
		st := statusByPort[n.HTTPPort]
		paused := isPaused(n.ID)
		running := st != nil || paused
		inCluster := allPeers[n.RPCAddr] || paused
		if st != nil && st.State == "leader" {
			inCluster = true
		}
		info := NodeInfo{NodeDef: n, Running: running, InCluster: inCluster, Paused: paused}
		if st != nil {
			info.RaftState          = st.State
			info.Term               = st.Term
			info.Leader             = st.Leader
			info.Peers              = st.Peers
			info.CommitIndex        = st.CommitIndex
			info.LastApplied        = st.LastApplied
			info.HeartbeatsSent     = st.HeartbeatsSent
			info.HeartbeatsReceived = st.HeartbeatsReceived
			info.SnapshotIndex      = st.SnapshotIndex
		} else if paused {
			info.RaftState = "paused"
		}
		result[i] = info
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// POST /nodes/create — create (and start) a node.
// Body: {"id":"nodeX","http_port":8086,"rpc_port":7006}
// If id matches a preset node, rpc_port and http_port are ignored (use preset config).
// If id is new, http_port is required and rpc_port defaults to http_port+100.
func handleCreateNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID       string `json:"id"`
		HTTPPort int    `json:"http_port"`
		RPCPort  int    `json:"rpc_port"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, `body must be JSON with at least {"id":"..."}`, http.StatusBadRequest)
		return
	}

	// Check for duplicate.
	if existing, ok := findNode(req.ID); ok {
		if fetchRaftStatus(existing.HTTPPort) != nil {
			http.Error(w, fmt.Sprintf("node %q is already running", req.ID), http.StatusConflict)
			return
		}
		// Preset node that is not currently running → start it.
		if !existing.Dynamic {
			if err := startPresetNode(existing); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"ok":true,"id":%q}`, req.ID)
			return
		}
		// Dynamic node was stopped; remove old entry so we can recreate below.
		removeDynamicNode(req.ID)
	}

	// New dynamic node.
	if req.HTTPPort == 0 {
		http.Error(w, "http_port is required for new nodes", http.StatusBadRequest)
		return
	}
	if req.RPCPort == 0 {
		req.RPCPort = req.HTTPPort + 100
	}

	if err := createDynamicNode(req.ID, req.HTTPPort, req.RPCPort); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"id":%q}`, req.ID)
}

// POST /nodes/{id}/stop|pause|unpause
func handleNodeAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/nodes/"), "/")
	if len(parts) != 2 {
		http.Error(w, "path must be /nodes/{id}/stop|pause|unpause", http.StatusBadRequest)
		return
	}
	nodeID, action := parts[0], parts[1]

	node, ok := findNode(nodeID)
	if !ok {
		http.Error(w, fmt.Sprintf("unknown node: %s", nodeID), http.StatusNotFound)
		return
	}

	var err error
	switch action {
	case "stop":
		err = stopNode(node)
	case "pause":
		err = pauseNode(node)
	case "unpause":
		err = unpauseNode(node)
	default:
		http.Error(w, "action must be stop, pause, or unpause", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"id":%q}`, nodeID)
}

// GET /live-scenarios
func handleListLiveScenarios(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(liveScenarioList)
}

// POST /live-scenarios/{id}
func handleRunLiveScenario(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/live-scenarios/")
	if id == "" {
		http.Error(w, "missing scenario id", http.StatusBadRequest)
		return
	}

	log.Printf("[chaos] running live scenario %q", id)
	var result LiveScenarioResult
	switch id {
	case "isolate_leader":
		result = runScenarioIsolateLeader()
	case "lose_follower":
		result = runScenarioLoseFollower()
	case "quorum_loss":
		result = runScenarioQuorumLoss()
	case "leader_churn":
		result = runScenarioLeaderChurn()
	default:
		http.Error(w, "unknown scenario: "+id, http.StatusNotFound)
		return
	}

	if result.Passed {
		log.Printf("[chaos] %q PASSED in %dms", id, result.DurMs)
	} else {
		log.Printf("[chaos] %q FAILED in %dms: %s", id, result.DurMs, result.Error)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// ── start / stop logic ────────────────────────────────────────────────────────

// startPresetNode starts a docker-compose-defined node.
func startPresetNode(node NodeDef) error {
	log.Printf("[sidecar] starting preset %s (bootstrap=%v)", node.ID, node.Bootstrap)

	// Wipe the persisted state (term, log, vote) before starting.
	// A node restarting with a stale high term will cause the cluster leader to
	// step down the moment it receives a message from it, triggering unnecessary
	// re-elections. A clean start means the node rejoins as a fresh learner and
	// catches up via log replication or snapshot.
	clearNodeData(node.ID)

	if err := compose("up", "-d", "--no-deps", "--build", node.ID); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}

	log.Printf("[sidecar] waiting for %s on :%d…", node.ID, node.HTTPPort)
	if err := waitForHTTP(node.HTTPPort, 30*time.Second); err != nil {
		return fmt.Errorf("%s did not come online: %w", node.ID, err)
	}

	// Find an existing cluster to join.
	port := findOnlinePortExcluding(node.HTTPPort)

	if node.Bootstrap && port == 0 {
		// No other cluster exists yet — rely on RAFT_PEERS to self-bootstrap.
		log.Printf("[sidecar] %s up (no cluster yet — self-bootstrapping via RAFT_PEERS)", node.ID)
		return nil
	}

	if port == 0 {
		return fmt.Errorf("no cluster nodes online; start at least one node first")
	}

	log.Printf("[sidecar] calling add-node for %s via :%d", node.RPCAddr, port)
	if err := raftAddNode(port, node.RPCAddr); err != nil {
		if node.Bootstrap {
			// Non-fatal for bootstrap nodes: already-a-member is OK on first start.
			log.Printf("[sidecar] add-node for bootstrap %s: %v (may already be a member)", node.ID, err)
			return nil
		}
		return fmt.Errorf("add-node: %w", err)
	}
	log.Printf("[sidecar] %s started and joined cluster", node.ID)
	return nil
}

// createDynamicNode starts a brand-new node via docker run.
func createDynamicNode(id string, httpPort, rpcPort int) error {
	log.Printf("[sidecar] creating dynamic node %s (http:%d rpc:%d)", id, httpPort, rpcPort)

	image, err := getBuiltImage()
	if err != nil {
		return err
	}

	rpcAddr      := id + ":7001"
	peerHTTPAddrs := buildPeerHTTPAddrs(id, httpPort)
	containerName := "raft-" + id

	args := []string{
		"run", "-d",
		"--name", containerName,
		"--network", composeNetworkName(),
		"--hostname", id,
		"-p", fmt.Sprintf("%d:%d", httpPort, httpPort),
		"-p", fmt.Sprintf("%d:7001", rpcPort),
		"-e", "NODE_ID=" + id,
		"-e", "RAFT_RPC_ADDR=:7001",
		"-e", fmt.Sprintf("HTTP_ADDR=:%d", httpPort),
		"-e", "RAFT_PEERS=",
		"-e", "DATA_DIR=/data",
		"-e", "PEER_HTTP_ADDRS=" + peerHTTPAddrs,
		"-e", "ELECTION_TIMEOUT_MIN_MS=500",
		"-e", "ELECTION_TIMEOUT_MAX_MS=1000",
		"-e", "HEARTBEAT_INTERVAL_MS=100",
		image,
	}

	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("docker run: %w\n%s", err, string(out))
	}

	// Register in runtime node list.
	addDynamicNode(NodeDef{
		ID:        id,
		RPCAddr:   rpcAddr,
		HTTPPort:  httpPort,
		RPCPort:   rpcPort,
		Bootstrap: false,
		Dynamic:   true,
	})

	log.Printf("[sidecar] waiting for %s on :%d…", id, httpPort)
	if err := waitForHTTP(httpPort, 30*time.Second); err != nil {
		return fmt.Errorf("%s did not come online: %w", id, err)
	}

	port := findOnlinePortExcluding(httpPort)
	if port == 0 {
		return fmt.Errorf("no cluster nodes online to call add-node; start bootstrap nodes first")
	}
	log.Printf("[sidecar] calling add-node for %s via :%d", rpcAddr, port)
	if err := raftAddNode(port, rpcAddr); err != nil {
		return fmt.Errorf("add-node: %w", err)
	}

	log.Printf("[sidecar] dynamic node %s started and joined cluster", id)
	return nil
}

func stopNode(node NodeDef) error {
	log.Printf("[sidecar] stopping %s (dynamic=%v bootstrap=%v)", node.ID, node.Dynamic, node.Bootstrap)

	if !node.Bootstrap {
		port := findOnlinePortExcluding(node.HTTPPort)
		if port != 0 {
			log.Printf("[sidecar] calling remove-node for %s via :%d", node.RPCAddr, port)
			if err := raftRemoveNode(port, node.RPCAddr); err != nil {
				log.Printf("[sidecar] remove-node warning: %v (continuing)", err)
			} else {
				time.Sleep(2 * time.Second)
			}
		}
	}

	if node.Dynamic {
		containerName := "raft-" + node.ID
		exec.Command("docker", "stop", containerName).Run()
		exec.Command("docker", "rm", containerName).Run()
		removeDynamicNode(node.ID)
	} else {
		if err := compose("stop", node.ID); err != nil {
			return fmt.Errorf("docker compose stop: %w", err)
		}
	}

	log.Printf("[sidecar] %s stopped", node.ID)
	return nil
}

// ── pause / unpause ───────────────────────────────────────────────────────────

func pauseNode(node NodeDef) error {
	log.Printf("[sidecar] pausing %s (dynamic=%v)", node.ID, node.Dynamic)
	if node.Dynamic {
		if out, err := exec.Command("docker", "pause", "raft-"+node.ID).CombinedOutput(); err != nil {
			return fmt.Errorf("docker pause: %w\n%s", err, string(out))
		}
	} else {
		if err := compose("pause", node.ID); err != nil {
			return fmt.Errorf("docker compose pause: %w", err)
		}
	}
	setPaused(node.ID, true)
	log.Printf("[sidecar] %s paused", node.ID)
	return nil
}

func unpauseNode(node NodeDef) error {
	log.Printf("[sidecar] unpausing %s (dynamic=%v)", node.ID, node.Dynamic)
	if node.Dynamic {
		if out, err := exec.Command("docker", "unpause", "raft-"+node.ID).CombinedOutput(); err != nil {
			return fmt.Errorf("docker unpause: %w\n%s", err, string(out))
		}
	} else {
		if err := compose("unpause", node.ID); err != nil {
			return fmt.Errorf("docker compose unpause: %w", err)
		}
	}
	setPaused(node.ID, false)
	log.Printf("[sidecar] %s unpaused", node.ID)
	return nil
}

// ── live scenarios ────────────────────────────────────────────────────────────

func runScenarioIsolateLeader() LiveScenarioResult {
	name  := "isolate_leader"
	start := time.Now()
	var logs []string
	ts := func() string { return time.Now().Format("15:04:05.000") }
	logf := func(f string, a ...any) { logs = append(logs, fmt.Sprintf("["+ts()+"] "+f, a...)) }
	fail := func(msg string) LiveScenarioResult {
		return LiveScenarioResult{Name: name, Passed: false, Error: msg, DurMs: time.Since(start).Milliseconds(), Logs: logs}
	}

	// 1. Find current leader.
	all := allNodes()
	var leaderNode *NodeDef
	var leaderTerm uint64
	for _, n := range all {
		if isPaused(n.ID) { continue }
		st := fetchRaftStatus(n.HTTPPort)
		if st != nil && st.State == "leader" {
			cp := n
			leaderNode = &cp
			leaderTerm = st.Term
			break
		}
	}
	if leaderNode == nil {
		return fail("no leader found — is the cluster running?")
	}
	logf("Found leader: %s (term %d)", leaderNode.ID, leaderTerm)

	// 2. Pause the leader.
	if err := pauseNode(*leaderNode); err != nil {
		return fail("pause failed: " + err.Error())
	}
	logf("Paused %s — waiting for the cluster to elect a new leader…", leaderNode.ID)

	// 3. Poll until a different leader appears.
	var newLeaderID string
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range all {
			if n.ID == leaderNode.ID || isPaused(n.ID) { continue }
			if st := fetchRaftStatus(n.HTTPPort); st != nil && st.State == "leader" {
				newLeaderID = n.ID
				break
			}
		}
		if newLeaderID != "" { break }
		time.Sleep(300 * time.Millisecond)
	}
	if newLeaderID == "" {
		unpauseNode(*leaderNode)
		return fail("new leader not elected within timeout — not enough nodes online?")
	}
	logf("New leader elected: %s", newLeaderID)
	logf("Holding partition for 3 s so the topology is visible…")
	time.Sleep(3 * time.Second)

	// 4. Restore.
	if err := unpauseNode(*leaderNode); err != nil {
		return fail("unpause failed: " + err.Error())
	}
	logf("Restored %s — it will receive a higher-term heartbeat and step down", leaderNode.ID)
	time.Sleep(800 * time.Millisecond)
	logf("Scenario complete ✓  old leader rejoined as follower")

	return LiveScenarioResult{Name: name, Passed: true, DurMs: time.Since(start).Milliseconds(), Logs: logs}
}

func runScenarioLoseFollower() LiveScenarioResult {
	name  := "lose_follower"
	start := time.Now()
	var logs []string
	ts := func() string { return time.Now().Format("15:04:05.000") }
	logf := func(f string, a ...any) { logs = append(logs, fmt.Sprintf("["+ts()+"] "+f, a...)) }
	fail := func(msg string) LiveScenarioResult {
		return LiveScenarioResult{Name: name, Passed: false, Error: msg, DurMs: time.Since(start).Milliseconds(), Logs: logs}
	}

	// 1. Find a follower.
	all := allNodes()
	var follower *NodeDef
	var onlineCount int
	for _, n := range all {
		if isPaused(n.ID) { continue }
		st := fetchRaftStatus(n.HTTPPort)
		if st == nil { continue }
		onlineCount++
		if st.State == "follower" && follower == nil {
			cp := n
			follower = &cp
		}
	}
	if follower == nil {
		return fail("no follower found — need at least 2 nodes online")
	}
	if onlineCount < 2 {
		return fail("only 1 node online; need at least 2 to maintain quorum after partition")
	}
	logf("Found follower: %s (%d nodes online)", follower.ID, onlineCount)

	// 2. Pause the follower.
	if err := pauseNode(*follower); err != nil {
		return fail("pause failed: " + err.Error())
	}
	logf("Paused %s — cluster still holds quorum with %d/%d nodes", follower.ID, onlineCount-1, onlineCount)
	logf("Holding partition for 3 s so the topology is visible…")
	time.Sleep(3 * time.Second)

	// 3. Verify the leader is still up.
	hasLeader := false
	for _, n := range all {
		if n.ID == follower.ID || isPaused(n.ID) { continue }
		if st := fetchRaftStatus(n.HTTPPort); st != nil && st.State == "leader" {
			hasLeader = true
			logf("Leader %s is still serving — quorum maintained ✓", n.ID)
			break
		}
	}
	if !hasLeader {
		unpauseNode(*follower)
		return fail("leader was lost after follower partition — unexpected quorum failure")
	}

	// 4. Restore.
	if err := unpauseNode(*follower); err != nil {
		return fail("unpause failed: " + err.Error())
	}
	logf("Restored %s — it will catch up via log replication", follower.ID)
	time.Sleep(600 * time.Millisecond)
	logf("Scenario complete ✓")

	return LiveScenarioResult{Name: name, Passed: true, DurMs: time.Since(start).Milliseconds(), Logs: logs}
}

func runScenarioQuorumLoss() LiveScenarioResult {
	name  := "quorum_loss"
	start := time.Now()
	var logs []string
	ts   := func() string { return time.Now().Format("15:04:05.000") }
	logf := func(f string, a ...any) { logs = append(logs, fmt.Sprintf("["+ts()+"] "+f, a...)) }
	fail := func(msg string) LiveScenarioResult {
		return LiveScenarioResult{Name: name, Passed: false, Error: msg, DurMs: time.Since(start).Milliseconds(), Logs: logs}
	}

	// 1. Collect all online, non-paused nodes.
	all := allNodes()
	var online []NodeDef
	for _, n := range all {
		if isPaused(n.ID) { continue }
		if st := fetchRaftStatus(n.HTTPPort); st != nil {
			online = append(online, n)
		}
	}
	n := len(online)
	if n < 3 {
		return fail(fmt.Sprintf("need at least 3 running nodes, found %d", n))
	}

	// quorum = floor(n/2)+1; to break it we pause ceil(n/2) = (n+1)/2 nodes.
	quorum   := n/2 + 1
	toPause  := (n + 1) / 2
	logf("Cluster: %d nodes, quorum=%d — pausing %d to break quorum", n, quorum, toPause)

	var paused []NodeDef
	for i := 0; i < toPause; i++ {
		nd := online[i]
		if err := pauseNode(nd); err != nil {
			// best-effort: restore whatever we paused so far and bail
			for _, p := range paused { unpauseNode(p) }
			return fail(fmt.Sprintf("pause %s: %v", nd.ID, err))
		}
		paused = append(paused, nd)
		logf("Paused %s (%d/%d)", nd.ID, i+1, toPause)
	}
	logf("Quorum lost — only %d/%d nodes reachable, leader cannot commit new entries", n-toPause, n)
	logf("Holding for 4 s so the topology is visible…")
	time.Sleep(4 * time.Second)

	// 2. Restore all paused nodes.
	for _, nd := range paused {
		if err := unpauseNode(nd); err != nil {
			logf("WARNING: unpause %s failed: %v", nd.ID, err)
		} else {
			logf("Restored %s", nd.ID)
		}
	}
	time.Sleep(800 * time.Millisecond)
	logf("Scenario complete ✓  cluster is recovering quorum")

	return LiveScenarioResult{Name: name, Passed: true, DurMs: time.Since(start).Milliseconds(), Logs: logs}
}

func runScenarioLeaderChurn() LiveScenarioResult {
	name  := "leader_churn"
	start := time.Now()
	var logs []string
	ts   := func() string { return time.Now().Format("15:04:05.000") }
	logf := func(f string, a ...any) { logs = append(logs, fmt.Sprintf("["+ts()+"] "+f, a...)) }
	fail := func(msg string) LiveScenarioResult {
		return LiveScenarioResult{Name: name, Passed: false, Error: msg, DurMs: time.Since(start).Milliseconds(), Logs: logs}
	}

	all := allNodes()
	const rounds = 3

	for round := 1; round <= rounds; round++ {
		logf("── Round %d/%d ──────────────────────────", round, rounds)

		// Find current leader.
		var leaderNode *NodeDef
		var leaderTerm uint64
		for _, n := range all {
			if isPaused(n.ID) { continue }
			if st := fetchRaftStatus(n.HTTPPort); st != nil && st.State == "leader" {
				cp := n
				leaderNode = &cp
				leaderTerm = st.Term
				break
			}
		}
		if leaderNode == nil {
			// Give the cluster a moment to settle and retry once.
			time.Sleep(600 * time.Millisecond)
			for _, n := range all {
				if isPaused(n.ID) { continue }
				if st := fetchRaftStatus(n.HTTPPort); st != nil && st.State == "leader" {
					cp := n
					leaderNode = &cp
					leaderTerm = st.Term
					break
				}
			}
			if leaderNode == nil {
				return fail(fmt.Sprintf("round %d: no leader found", round))
			}
		}
		logf("Current leader: %s (term %d)", leaderNode.ID, leaderTerm)

		// Pause the leader.
		if err := pauseNode(*leaderNode); err != nil {
			return fail(fmt.Sprintf("round %d: pause %s: %v", round, leaderNode.ID, err))
		}
		logf("Paused %s — waiting for new election…", leaderNode.ID)

		// Wait for a different leader.
		var newLeaderID string
		var newTerm uint64
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			for _, n := range all {
				if n.ID == leaderNode.ID || isPaused(n.ID) { continue }
				if st := fetchRaftStatus(n.HTTPPort); st != nil && st.State == "leader" {
					newLeaderID = n.ID
					newTerm = st.Term
					break
				}
			}
			if newLeaderID != "" { break }
			time.Sleep(250 * time.Millisecond)
		}
		if newLeaderID == "" {
			unpauseNode(*leaderNode)
			return fail(fmt.Sprintf("round %d: new leader not elected within timeout", round))
		}
		logf("New leader: %s (term %d → %d, +%d)", newLeaderID, leaderTerm, newTerm, newTerm-leaderTerm)

		// Hold briefly so the topology is visible.
		time.Sleep(1500 * time.Millisecond)

		// Restore old leader.
		if err := unpauseNode(*leaderNode); err != nil {
			return fail(fmt.Sprintf("round %d: unpause %s: %v", round, leaderNode.ID, err))
		}
		logf("Restored %s — stepping down as follower", leaderNode.ID)
		time.Sleep(700 * time.Millisecond)
	}

	logf("Leader churn complete ✓  %d elections triggered", rounds)
	return LiveScenarioResult{Name: name, Passed: true, DurMs: time.Since(start).Milliseconds(), Logs: logs}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// clearNodeData removes the named Docker volume that holds a preset node's
// persisted Raft state (term, vote, log). Called before every restart so the
// node joins as a fresh learner instead of bringing back a stale high term.
//
// docker compose stop leaves the container in "exited" state; it still holds a
// reference to its volume, so "docker volume rm" would fail with "volume is in
// use". We force-remove the stopped container first, then remove the volume.
// docker compose up will create a fresh container + empty volume on next start.
func clearNodeData(nodeID string) {
	projectName := filepath.Base(filepath.Dir(composeFile)) // e.g. "infra"
	volumeName := projectName + "_" + nodeID + "-data"      // e.g. "infra_node4-data"

	// Remove the stopped container so it releases its volume reference.
	if out, err := exec.Command("docker", "compose", "-f", composeFile,
		"rm", "-f", "-s", nodeID).CombinedOutput(); err != nil {
		log.Printf("[sidecar] compose rm %s: %s", nodeID, strings.TrimSpace(string(out)))
	}

	// Now the volume has no references — safe to remove.
	out, err := exec.Command("docker", "volume", "rm", volumeName).CombinedOutput()
	if err != nil {
		log.Printf("[sidecar] clear %s: %s", volumeName, strings.TrimSpace(string(out)))
	} else {
		log.Printf("[sidecar] cleared volume %s", volumeName)
	}
}

// compose runs: docker compose -f <composeFile> <args...>
func compose(args ...string) error {
	full := append([]string{"compose", "-f", composeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Dir = projectDir
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w\n%s", err, string(out))
	}
	return nil
}

// composeNetworkName returns the Docker network name created by docker compose.
// Convention: {directory-of-compose-file}_default.
func composeNetworkName() string {
	return filepath.Base(filepath.Dir(composeFile)) + "_default"
}

// getBuiltImage returns an image name/ID that can be passed to docker run.
// Docker Compose names built images as {project}-{service}, e.g. "infra-node1".
// We check for those images first (works even when no containers are running),
// then fall back to docker compose images -q as a secondary check.
func getBuiltImage() (string, error) {
	projectName := filepath.Base(filepath.Dir(composeFile)) // e.g. "infra"

	// Primary: look for the conventionally-named image (project-service).
	for _, n := range presetNodes {
		imageName := projectName + "-" + n.ID // e.g. "infra-node1"
		if _, err := exec.Command("docker", "image", "inspect", imageName).Output(); err == nil {
			return imageName, nil
		}
	}

	// Fallback: docker compose images -q (can return empty on some Docker versions).
	for _, n := range presetNodes {
		out, err := exec.Command("docker", "compose", "-f", composeFile,
			"images", "-q", n.ID).Output()
		if err == nil {
			if img := strings.TrimSpace(string(out)); img != "" {
				return img, nil
			}
		}
	}

	return "", fmt.Errorf("no built image found — run docker compose build (or start any node first) to build the image")
}

// buildPeerHTTPAddrs constructs PEER_HTTP_ADDRS for a new node, including
// all currently known nodes plus the new node itself.
func buildPeerHTTPAddrs(newID string, newHTTPPort int) string {
	all := allNodes()
	parts := make([]string, 0, len(all)+1)
	seen := map[string]bool{}
	for _, n := range all {
		parts = append(parts, fmt.Sprintf("%s=localhost:%d", n.ID, n.HTTPPort))
		seen[n.ID] = true
	}
	if !seen[newID] {
		parts = append(parts, fmt.Sprintf("%s=localhost:%d", newID, newHTTPPort))
	}
	return strings.Join(parts, ",")
}

func waitForHTTP(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fetchRaftStatus(port) != nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s", timeout)
}

func fetchRaftStatus(port int) *raftStatus {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://localhost:%d/status", port), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var st raftStatus
	if json.NewDecoder(resp.Body).Decode(&st) != nil {
		return nil
	}
	return &st
}

// findOnlinePortExcluding returns the HTTP port of the current Raft leader,
// excluding the given port. Sending add-node / remove-node directly to the
// leader avoids the follower→leader redirect, which fails when the follower's
// PEER_HTTP_ADDRS map does not include the leader (e.g. dynamically-added nodes).
func findOnlinePortExcluding(exclude int) int {
	all := allNodes()

	// Collect live statuses in one pass.
	type result struct {
		node NodeDef
		st   *raftStatus
	}
	var live []result
	for _, n := range all {
		if n.HTTPPort == exclude {
			continue
		}
		if st := fetchRaftStatus(n.HTTPPort); st != nil {
			live = append(live, result{n, st})
		}
	}

	// Pass 1: self-identified leader.
	for _, r := range live {
		if r.st.State == "leader" {
			return r.node.HTTPPort
		}
	}

	// Pass 2: follower that knows the leader — resolve leader's HTTP port from
	// our node registry so we can send directly (skipping an unreliable redirect).
	for _, r := range live {
		if r.st.Leader == "" {
			continue
		}
		// r.st.Leader is the leader's RPC addr, e.g. "node5:7001" or "node5".
		leaderID := strings.Split(r.st.Leader, ":")[0]
		for _, n := range all {
			if n.ID == leaderID && n.HTTPPort != exclude {
				return n.HTTPPort
			}
		}
	}

	// Pass 3: any responding node (caller gets a clearer error from the Raft layer).
	for _, r := range live {
		return r.node.HTTPPort
	}
	return 0
}

func raftAddNode(port int, rpcAddr string) error {
	body, _ := json.Marshal(map[string]string{"addr": rpcAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://localhost:%d/admin/add-node", port),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("add-node returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func raftRemoveNode(port int, rpcAddr string) error {
	body, _ := json.Marshal(map[string]string{"addr": rpcAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://localhost:%d/admin/remove-node", port),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("remove-node returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
