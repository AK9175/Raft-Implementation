package raft

import (
	"context"
	"fmt"
	"sync"
)

// MemoryNetwork is a shared registry that lets RaftNodes talk to each other
// directly via method calls instead of TCP sockets.
//
// Why this exists: tests need to spin up multiple nodes on one machine and
// control message delivery (drop, delay, partition) without real networking.
// The Transport interface lets us swap this in for the TCP transport at test time.
type MemoryNetwork struct {
	mu    sync.RWMutex
	nodes map[string]*RaftNode
}

func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{nodes: make(map[string]*RaftNode)}
}

// Register adds a node to the network under the given id.
// Must be called before Run() so RPCs can be routed to it.
func (net *MemoryNetwork) Register(id string, node *RaftNode) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = node
}

// Transport returns a MemoryTransport scoped to the given node id.
// Pass the returned transport into Config.Transport when creating the node.
func (net *MemoryNetwork) Transport(id string) *MemoryTransport {
	return &MemoryTransport{network: net, id: id}
}

// MemoryTransport implements Transport using direct method calls on RaftNode.
// Each call is synchronous: it finds the target node in the registry and calls
// its handler directly, simulating an instantaneous RPC.
type MemoryTransport struct {
	network *MemoryNetwork
	id      string
}

func (t *MemoryTransport) RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	node, err := t.lookup(addr)
	if err != nil {
		return RequestVoteReply{}, err
	}
	// honour cancellation before calling the handler
	select {
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	default:
	}
	return node.HandleRequestVote(args), nil
}

func (t *MemoryTransport) AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	node, err := t.lookup(addr)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	select {
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	default:
	}
	return node.HandleAppendEntries(args), nil
}

func (t *MemoryTransport) Close() error { return nil }

func (t *MemoryTransport) lookup(addr string) (*RaftNode, error) {
	t.network.mu.RLock()
	defer t.network.mu.RUnlock()
	node, ok := t.network.nodes[addr]
	if !ok {
		return nil, fmt.Errorf("raft: node %q not found in memory network", addr)
	}
	return node, nil
}
