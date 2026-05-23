package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// HTTPTransport implements Transport over HTTP/JSON.
//
// Client side: POST JSON to http://<peer>/<path>
// Server side: HTTP handlers that decode JSON and call the node's RPC handlers.
//
// Usage:
//
//	transport := raft.NewHTTPTransport(":7001")
//	store, node := kvstore.New(cfg) // cfg.Transport = transport
//	transport.Register(node)        // wire server → node handlers
//	go transport.Serve()            // start listening
//	go node.Run()
type HTTPTransport struct {
	listenAddr string
	client     *http.Client
	mux        *http.ServeMux
	server     *http.Server
}

// NewHTTPTransport creates an HTTPTransport that will listen on listenAddr.
// Call Register(node) before Serve() to wire up the server-side handlers.
func NewHTTPTransport(listenAddr string) *HTTPTransport {
	mux := http.NewServeMux()
	t := &HTTPTransport{
		listenAddr: listenAddr,
		client:     &http.Client{},
		mux:        mux,
		server:     &http.Server{Addr: listenAddr, Handler: mux},
	}
	return t
}

// Register wires the HTTP server to route incoming Raft RPCs to node.
// Must be called before Serve().
func (t *HTTPTransport) Register(node *RaftNode) {
	t.mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(node.HandleRequestVote(args))
	})

	t.mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(node.HandleAppendEntries(args))
	})

	t.mux.HandleFunc("/raft/install-snapshot", func(w http.ResponseWriter, r *http.Request) {
		var args InstallSnapshotArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(node.HandleInstallSnapshot(args))
	})
}

// Serve starts the HTTP server. Blocks until Close() is called.
func (t *HTTPTransport) Serve() error {
	return t.server.ListenAndServe()
}

// Close shuts down the HTTP server gracefully.
func (t *HTTPTransport) Close() error {
	return t.server.Close()
}

// RequestVote sends a RequestVote RPC to addr over HTTP.
func (t *HTTPTransport) RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	return reply, t.post(ctx, addr, "/raft/request-vote", args, &reply)
}

// AppendEntries sends an AppendEntries RPC to addr over HTTP.
func (t *HTTPTransport) AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	return reply, t.post(ctx, addr, "/raft/append-entries", args, &reply)
}

// InstallSnapshot sends an InstallSnapshot RPC to addr over HTTP.
func (t *HTTPTransport) InstallSnapshot(ctx context.Context, addr string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	return reply, t.post(ctx, addr, "/raft/install-snapshot", args, &reply)
}

// post marshals body as JSON, POSTs to http://addr/path, and decodes the
// JSON response into reply. The context controls the request deadline.
func (t *HTTPTransport) post(ctx context.Context, addr, path string, body, reply interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s%s", addr, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft rpc %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}
