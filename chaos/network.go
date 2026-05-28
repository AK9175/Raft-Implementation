package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/atharva/raft/raft"
)

// action describes what the rule engine does with a matched message.
type action int

const (
	actionDrop  action = iota // silently discard the message
	actionLoss                // drop with probability p
	actionDelay               // sleep d before forwarding
)

// rule is a single entry in the rule engine. All fields participate in
// matching; zero values are wildcards. Rules are checked in insertion order
// and the first match wins.
type rule struct {
	id   string // opaque caller-assigned ID for later removal
	from string // sender node ID; "" = any
	to   string // receiver node ID; "" = any
	act  action
	prob float64       // actionLoss: drop probability [0,1]
	d    time.Duration // actionDelay: sleep duration
}

// ChaosNetwork wraps a *raft.MemoryNetwork and layers a rule engine on top.
// Rules are checked before every RPC. Use the rule helpers on ChaosInjector
// to add/remove rules.
type ChaosNetwork struct {
	base *raft.MemoryNetwork

	mu    sync.RWMutex
	rules []rule
}

// NewChaosNetwork creates a ChaosNetwork backed by a fresh MemoryNetwork.
func NewChaosNetwork() *ChaosNetwork {
	return &ChaosNetwork{base: raft.NewMemoryNetwork()}
}

// Register adds a node to the underlying MemoryNetwork.
func (cn *ChaosNetwork) Register(id string, node *raft.RaftNode) {
	cn.base.Register(id, node)
}

// Transport returns a ChaosTransport for the given node ID.
func (cn *ChaosNetwork) Transport(id string) *ChaosTransport {
	return &ChaosTransport{net: cn, base: cn.base.Transport(id), id: id}
}

// addRule appends a rule.
func (cn *ChaosNetwork) addRule(r rule) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.rules = append(cn.rules, r)
}

// removeRule removes all rules with the given ID.
func (cn *ChaosNetwork) removeRule(id string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	filtered := cn.rules[:0]
	for _, r := range cn.rules {
		if r.id != id {
			filtered = append(filtered, r)
		}
	}
	cn.rules = filtered
}

// clearRules removes every rule.
func (cn *ChaosNetwork) clearRules() {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.rules = cn.rules[:0]
}

// evalResult is what the rule engine returns for a given (from, to) pair.
type evalResult int

const (
	evalForward evalResult = iota
	evalDrop
)

// evaluate checks the rule list for the (from, to) pair.
// It returns (evalForward, 0) if no rule matches or the delay action applies,
// and (evalDrop, 0) if the message should be dropped.
// For delay rules it sleeps before returning evalForward.
func (cn *ChaosNetwork) evaluate(from, to string) evalResult {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	for _, r := range cn.rules {
		if r.from != "" && r.from != from {
			continue
		}
		if r.to != "" && r.to != to {
			continue
		}
		switch r.act {
		case actionDrop:
			return evalDrop
		case actionLoss:
			if rand.Float64() < r.prob {
				return evalDrop
			}
			return evalForward
		case actionDelay:
			time.Sleep(r.d)
			return evalForward
		}
	}
	return evalForward
}

// ChaosTransport implements raft.Transport. It consults the ChaosNetwork rule
// engine before every RPC, dropping or delaying as configured, then delegates
// to the underlying MemoryTransport.
type ChaosTransport struct {
	net  *ChaosNetwork
	base *raft.MemoryTransport
	id   string
}

func (ct *ChaosTransport) RequestVote(ctx context.Context, addr string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	if ct.net.evaluate(ct.id, addr) == evalDrop {
		return raft.RequestVoteReply{}, fmt.Errorf("chaos: packet dropped (%s→%s)", ct.id, addr)
	}
	return ct.base.RequestVote(ctx, addr, args)
}

func (ct *ChaosTransport) AppendEntries(ctx context.Context, addr string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	if ct.net.evaluate(ct.id, addr) == evalDrop {
		return raft.AppendEntriesReply{}, fmt.Errorf("chaos: packet dropped (%s→%s)", ct.id, addr)
	}
	return ct.base.AppendEntries(ctx, addr, args)
}

func (ct *ChaosTransport) InstallSnapshot(ctx context.Context, addr string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	if ct.net.evaluate(ct.id, addr) == evalDrop {
		return raft.InstallSnapshotReply{}, fmt.Errorf("chaos: packet dropped (%s→%s)", ct.id, addr)
	}
	return ct.base.InstallSnapshot(ctx, addr, args)
}

func (ct *ChaosTransport) Close() error { return ct.base.Close() }
