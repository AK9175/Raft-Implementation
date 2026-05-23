package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/atharva/raft"
	"github.com/atharva/raft/kvstore"
)

// peerHTTPAddrs maps node ID → HTTP address.
// In a real deployment this comes from a config file.
// Here it's hardcoded so the in-process demo wires up cleanly.
var peerHTTPAddrs = map[string]string{
	"node1": "localhost:8081",
	"node2": "localhost:8082",
	"node3": "localhost:8083",
}

// main spins up a 3-node in-memory Raft cluster and exposes it over HTTP.
//
// Every node listens on its own port. Client requests to a follower are
// redirected (HTTP 302) to the current leader's port automatically.
//
// Usage:
//
//	go run ./cmd/kvstore
//
// Then in another terminal:
//
//	curl -L -X PUT  "http://localhost:8081/keys/hello?value=world"
//	curl -L         "http://localhost:8082/keys/hello"
//	curl -L -X DELETE "http://localhost:8083/keys/hello"
//
// -L tells curl to follow the 302 redirect to the leader automatically.
func main() {
	net := raft.NewMemoryNetwork()
	ids := []string{"node1", "node2", "node3"}

	stores := make([]*kvstore.KVStore, len(ids))
	nodes := make([]*raft.RaftNode, len(ids))

	for i, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		dir := fmt.Sprintf("/tmp/raft-kvstore/%s", id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("mkdir %s: %v", dir, err)
		}

		cfg := raft.Config{
			ID:                   id,
			Peers:                peers,
			Transport:            net.Transport(id),
			ElectionTimeoutMinMs: 150,
			ElectionTimeoutMaxMs: 300,
			HeartbeatIntervalMs:  50,
			DataDir:              dir,
		}
		stores[i], nodes[i] = kvstore.New(cfg)
		net.Register(id, nodes[i])
	}

	for _, node := range nodes {
		go node.Run()
	}

	// Start one HTTP server per node, each on its own port.
	for i, id := range ids {
		store := stores[i]
		node := nodes[i]
		addr := peerHTTPAddrs[id]

		mux := http.NewServeMux()
		mux.HandleFunc("/keys/", makeHandler(store, node))

		srv := &http.Server{Addr: addr, Handler: mux}
		go func(addr string) {
			log.Printf("node %s listening on %s", id, addr)
			if err := srv.ListenAndServe(); err != nil {
				log.Fatalf("server %s: %v", addr, err)
			}
		}(addr)
	}

	log.Println("waiting for leader election...")
	time.Sleep(500 * time.Millisecond)
	log.Println("cluster ready")

	// Block forever — servers run in goroutines above.
	select {}
}

// makeHandler returns an HTTP handler for one node.
// If this node is not the leader it redirects to the leader's HTTP address.
func makeHandler(store *kvstore.KVStore, node *raft.RaftNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/keys/")
		if key == "" {
			http.Error(w, "missing key in path", http.StatusBadRequest)
			return
		}

		// If we are not the leader, redirect the client to whoever is.
		if node.State() != raft.Leader {
			leaderID := node.LeaderID()
			if leaderID == "" {
				// Election in progress — no leader yet.
				http.Error(w, "no leader elected yet, retry shortly", http.StatusServiceUnavailable)
				return
			}
			leaderAddr, ok := peerHTTPAddrs[leaderID]
			if !ok {
				http.Error(w, "unknown leader address", http.StatusInternalServerError)
				return
			}
			// 302 redirect — client follows it to the leader directly.
			target := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.RequestURI())
			http.Redirect(w, r, target, http.StatusFound)
			return
		}

		// We are the leader — handle the request.
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
