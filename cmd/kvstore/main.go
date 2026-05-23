package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/atharva/raft/kvstore"
	"github.com/atharva/raft/raft"
)

// Configuration is read entirely from environment variables so each container
// only needs its own env block — no config files, no hardcoded addresses.
//
// Required:
//
//	NODE_ID          unique node identifier, e.g. "node1"
//	RAFT_RPC_ADDR    address this node listens on for Raft RPCs, e.g. ":7001"
//	HTTP_ADDR        address for the KVStore HTTP API, e.g. ":8081"
//	DATA_DIR         directory for persistent state, e.g. "/data"
//
// Optional:
//
//	RAFT_PEERS       comma-separated peer Raft RPC addresses, e.g. "node2:7001,node3:7001"
//	PEER_HTTP_ADDRS  comma-separated id=host:port pairs used to build redirect URLs,
//	                 e.g. "node1=localhost:8081,node2=localhost:8082,node3=localhost:8083"
//
// Usage (local):
//
//	NODE_ID=node1 RAFT_RPC_ADDR=:7001 HTTP_ADDR=:8081 DATA_DIR=/tmp/node1 \
//	  RAFT_PEERS=node2:7001,node3:7001 \
//	  PEER_HTTP_ADDRS=node1=localhost:8081,node2=localhost:8082,node3=localhost:8083 \
//	  go run ./cmd/kvstore
func main() {
	nodeID := mustEnv("NODE_ID")
	raftRPCAddr := mustEnv("RAFT_RPC_ADDR")
	httpAddr := mustEnv("HTTP_ADDR")
	dataDir := mustEnv("DATA_DIR")

	peers := splitNonEmpty(os.Getenv("RAFT_PEERS"), ",")
	peerHTTPAddrs := parsePeerHTTPAddrs(os.Getenv("PEER_HTTP_ADDRS"))

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", dataDir, err)
	}

	transport := raft.NewHTTPTransport(raftRPCAddr)

	cfg := raft.Config{
		ID:                   nodeID,
		Peers:                peers,
		Transport:            transport,
		ElectionTimeoutMinMs: 150,
		ElectionTimeoutMaxMs: 300,
		HeartbeatIntervalMs:  50,
		DataDir:              dataDir,
	}

	store, node := kvstore.New(cfg)
	transport.Register(node) // wire Raft RPC server → node handlers

	// Start Raft RPC server.
	go func() {
		log.Printf("[%s] raft rpc listening on %s", nodeID, raftRPCAddr)
		if err := transport.Serve(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("raft rpc server: %v", err)
		}
	}()

	// Start the Raft node event loop.
	go node.Run()

	// KVStore HTTP API — one mux per node, redirect to leader on writes.
	mux := http.NewServeMux()
	mux.HandleFunc("/keys/", makeHandler(store, node, peerHTTPAddrs))
	mux.HandleFunc("/status", makeStatusHandler(nodeID, node))

	log.Printf("[%s] kvstore http listening on %s", nodeID, httpAddr)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Fatalf("http server: %v", err)
	}
}

// makeHandler returns an HTTP handler for this node's KVStore API.
// Followers redirect clients to the leader using peerHTTPAddrs.
func makeHandler(store *kvstore.KVStore, node *raft.RaftNode, peerHTTPAddrs map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/keys/")
		if key == "" {
			http.Error(w, "missing key in path", http.StatusBadRequest)
			return
		}

		// GETs are served locally — no redirect needed.
		// Only writes (PUT/DELETE) must go to the leader.
		if r.Method != http.MethodGet && node.State() != raft.Leader {
			leaderID := node.LeaderID()
			if leaderID == "" {
				http.Error(w, "no leader elected yet, retry shortly", http.StatusServiceUnavailable)
				return
			}
			leaderAddr, ok := peerHTTPAddrs[leaderID]
			if !ok {
				http.Error(w, "unknown leader address", http.StatusInternalServerError)
				return
			}
			target := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.RequestURI())
			http.Redirect(w, r, target, http.StatusFound)
			return
		}

		switch r.Method {
		case http.MethodGet:
			val, err := store.Get(key)
			if err == kvstore.ErrKeyNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintln(w, val)

		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			value := strings.TrimSpace(string(body))
			if value == "" {
				value = r.URL.Query().Get("value")
			}
			if err := store.Set(key, value); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintln(w, "OK")

		case http.MethodDelete:
			if err := store.Delete(key); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintln(w, "OK")

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// makeStatusHandler returns a handler that reports this node's Raft state as JSON.
// Useful for verifying leader election, term progression, and cluster health.
//
//	curl http://localhost:8081/status
func makeStatusHandler(nodeID string, node *raft.RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id":             nodeID,
			"state":               node.State().String(),
			"term":                node.CurrentTerm(),
			"leader":              node.LeaderID(),
			"commit_index":        node.CommitIndex(),
			"last_applied":        node.LastApplied(),
			"heartbeats_received": node.HeartbeatsReceived(),
			"heartbeats_sent":     node.HeartbeatsSent(),
		})
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func splitNonEmpty(s, sep string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, sep)
}

// parsePeerHTTPAddrs parses "node1=localhost:8081,node2=localhost:8082" into a map.
func parsePeerHTTPAddrs(raw string) map[string]string {
	m := make(map[string]string)
	for _, pair := range splitNonEmpty(raw, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return m
}
