package raft

import "context"

// Transport abstracts how RPC messages are sent between Raft nodes.
//
// Decoupling transport from the node logic lets us:
//   - Use TCP in production (real network)
//   - Use in-memory channels in tests (controllable, fast, injectable faults)
//
// The node never touches sockets directly — it only calls these methods.
type Transport interface {
	// RequestVote sends a RequestVote RPC to the node at addr and returns the reply.
	RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error)

	// AppendEntries sends an AppendEntries RPC to the node at addr and returns the reply.
	AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error)

	// Close shuts down the transport.
	Close() error
}
