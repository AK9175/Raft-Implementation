package raft

import (
	"context"
	"fmt"
	"net"

	pb "github.com/atharva/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport implements Transport over gRPC + protobuf.
//
// Client side: dials addr on demand, sends a protobuf request, closes connection.
// Server side: a gRPC server that routes incoming RPCs to the RaftNode handlers.
//
// Usage (drop-in replacement for HTTPTransport):
//
//	transport := raft.NewGRPCTransport(":7001")
//	store, node := kvstore.New(cfg) // cfg.Transport = transport
//	transport.Register(node)        // wire gRPC server → node handlers
//	go transport.Serve()            // start listening
//	go node.Run()
type GRPCTransport struct {
	listenAddr string
	server     *grpc.Server
	handler    *grpcHandler
}

// NewGRPCTransport creates a GRPCTransport that will listen on listenAddr.
// Call Register(node) before Serve().
func NewGRPCTransport(listenAddr string) *GRPCTransport {
	srv := grpc.NewServer()
	t := &GRPCTransport{
		listenAddr: listenAddr,
		server:     srv,
	}
	return t
}

// Register wires the gRPC server to route incoming Raft RPCs to node.
// Must be called before Serve().
func (t *GRPCTransport) Register(node *RaftNode) {
	t.handler = &grpcHandler{node: node}
	pb.RegisterRaftServer(t.server, t.handler)
}

// Serve starts the gRPC server. Blocks until Close() is called.
func (t *GRPCTransport) Serve() error {
	lis, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return fmt.Errorf("grpc transport listen %s: %w", t.listenAddr, err)
	}
	return t.server.Serve(lis)
}

// Close shuts down the gRPC server gracefully.
func (t *GRPCTransport) Close() error {
	t.server.GracefulStop()
	return nil
}

// ── client-side Transport methods ─────────────────────────────────────────

// RequestVote sends a RequestVote RPC to addr over gRPC.
func (t *GRPCTransport) RequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	conn, err := dial(ctx, addr)
	if err != nil {
		return RequestVoteReply{}, err
	}
	defer conn.Close()

	resp, err := pb.NewRaftClient(conn).RequestVote(ctx, &pb.RequestVoteArgs{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return RequestVoteReply{}, err
	}
	return RequestVoteReply{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// AppendEntries sends an AppendEntries RPC to addr over gRPC.
func (t *GRPCTransport) AppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	conn, err := dial(ctx, addr)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	defer conn.Close()

	resp, err := pb.NewRaftClient(conn).AppendEntries(ctx, &pb.AppendEntriesArgs{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      toProtoEntries(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

// InstallSnapshot sends an InstallSnapshot RPC to addr over gRPC.
func (t *GRPCTransport) InstallSnapshot(ctx context.Context, addr string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	conn, err := dial(ctx, addr)
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	defer conn.Close()

	resp, err := pb.NewRaftClient(conn).InstallSnapshot(ctx, &pb.InstallSnapshotArgs{
		Term:              args.Term,
		LeaderId:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	return InstallSnapshotReply{Term: resp.Term}, nil
}

// ── server-side gRPC handler ───────────────────────────────────────────────

// grpcHandler bridges the generated gRPC server interface to RaftNode's
// handler methods, translating proto types ↔ internal raft types.
type grpcHandler struct {
	pb.UnimplementedRaftServer
	node *RaftNode
}

func (h *grpcHandler) RequestVote(_ context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	reply := h.node.HandleRequestVote(RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &pb.RequestVoteReply{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (h *grpcHandler) AppendEntries(_ context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	reply := h.node.HandleAppendEntries(AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      fromProtoEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	})
	return &pb.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictTerm:  reply.ConflictTerm,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}

func (h *grpcHandler) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	reply := h.node.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	return &pb.InstallSnapshotReply{Term: reply.Term}, nil
}

// ── helpers ────────────────────────────────────────────────────────────────

// dial opens a gRPC connection to addr. The context controls the dial timeout.
// Connections are short-lived (one per RPC call) — acceptable for a Raft
// implementation; a production system would use a connection pool.
func dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr, //nolint:staticcheck
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

func toProtoEntries(entries []LogEntry) []*pb.LogEntry {
	out := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = &pb.LogEntry{
			Term:       e.Term,
			Index:      e.Index,
			Command:    e.Command,
			IsConfig:   e.IsConfig,
			ConfigOp:   e.ConfigOp,
			ConfigPeer: e.ConfigPeer,
		}
	}
	return out
}

func fromProtoEntries(entries []*pb.LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = LogEntry{
			Term:       e.Term,
			Index:      e.Index,
			Command:    e.Command,
			IsConfig:   e.IsConfig,
			ConfigOp:   e.ConfigOp,
			ConfigPeer: e.ConfigPeer,
		}
	}
	return out
}
