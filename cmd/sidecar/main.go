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
	mux.HandleFunc("/nodes/", handleNodeStop)

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
		go func(port int) {
			ch <- portResult{port, fetchRaftStatus(port)}
		}(n.HTTPPort)
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
		running := st != nil
		inCluster := allPeers[n.RPCAddr]
		if running && st.State == "leader" {
			inCluster = true
		}
		info := NodeInfo{NodeDef: n, Running: running, InCluster: inCluster}
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

// POST /nodes/{id}/stop — remove from cluster (if needed) + stop container.
func handleNodeStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Path: /nodes/{id}/stop
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/nodes/"), "/")
	if len(parts) != 2 || parts[1] != "stop" {
		http.Error(w, "path must be /nodes/{id}/stop or POST /nodes/create", http.StatusBadRequest)
		return
	}
	nodeID := parts[0]

	node, ok := findNode(nodeID)
	if !ok {
		http.Error(w, fmt.Sprintf("unknown node: %s", nodeID), http.StatusNotFound)
		return
	}

	if err := stopNode(node); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"id":%q}`, nodeID)
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
