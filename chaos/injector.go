// Package chaos provides fault-injection primitives for Raft testing.
//
// # Architecture
//
// ChaosInjector owns a ChaosNetwork (rule engine) and a set of running
// RaftNodes. Callers describe failure scenarios by composing the injector
// methods: PartitionNode, HealAll, InjectPacketLoss, InjectDelay, CrashNode.
//
// Each rule method returns a cleanup func so tests can restore network health
// at a precise moment without having to track rule IDs manually.
package chaos

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atharva/raft/raft"
)

// Observation records a single notable event during a chaos scenario.
type Observation struct {
	At  time.Time
	Msg string
}

// ChaosInjector drives fault injection on a running cluster.
type ChaosInjector struct {
	net   *ChaosNetwork
	nodes map[string]*raft.RaftNode

	mu   sync.Mutex
	obs  []Observation
	rseq atomic.Uint64 // monotonic counter for rule IDs
}

// NewInjector creates a ChaosInjector connected to the given ChaosNetwork and
// node map. Both must have been populated before calling this.
func NewInjector(net *ChaosNetwork, nodes map[string]*raft.RaftNode) *ChaosInjector {
	return &ChaosInjector{net: net, nodes: nodes}
}

// record appends an observation to the injector's log.
func (ci *ChaosInjector) record(msg string, args ...any) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.obs = append(ci.obs, Observation{
		At:  time.Now(),
		Msg: fmt.Sprintf(msg, args...),
	})
}

// Observations returns a copy of all recorded observations.
func (ci *ChaosInjector) Observations() []Observation {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	out := make([]Observation, len(ci.obs))
	copy(out, ci.obs)
	return out
}

// nextID returns a unique, opaque rule identifier.
func (ci *ChaosInjector) nextID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, ci.rseq.Add(1))
}

// PartitionNode cuts all inbound and outbound traffic for nodeID.
// Returns a heal func that removes the partition rules.
//
// This adds two rules:
//   - drop all messages sent FROM nodeID
//   - drop all messages sent TO nodeID
func (ci *ChaosInjector) PartitionNode(nodeID string) (heal func()) {
	outID := ci.nextID("part-out")
	inID := ci.nextID("part-in")

	ci.net.addRule(rule{id: outID, from: nodeID, act: actionDrop})
	ci.net.addRule(rule{id: inID, to: nodeID, act: actionDrop})
	ci.record("partition: isolated %s", nodeID)

	return func() {
		ci.net.removeRule(outID)
		ci.net.removeRule(inID)
		ci.record("partition: healed %s", nodeID)
	}
}

// PartitionAsymmetric blocks messages from src to dst only (one-way).
// Returns a cleanup func.
func (ci *ChaosInjector) PartitionAsymmetric(src, dst string) (heal func()) {
	id := ci.nextID("part-asym")
	ci.net.addRule(rule{id: id, from: src, to: dst, act: actionDrop})
	ci.record("partition(asymmetric): %s→%s dropped", src, dst)

	return func() {
		ci.net.removeRule(id)
		ci.record("partition(asymmetric): %s→%s healed", src, dst)
	}
}

// HealAll removes every rule in the network, restoring full connectivity.
func (ci *ChaosInjector) HealAll() {
	ci.net.clearRules()
	ci.record("heal-all: full connectivity restored")
}

// InjectPacketLoss installs a probabilistic drop rule for the given node pair.
// prob is the drop probability [0.0, 1.0]. Pass "" for src or dst to match any.
// Returns a cleanup func.
func (ci *ChaosInjector) InjectPacketLoss(src, dst string, prob float64) (restore func()) {
	id := ci.nextID("loss")
	ci.net.addRule(rule{id: id, from: src, to: dst, act: actionLoss, prob: prob})
	ci.record("packet-loss: %s→%s @ %.0f%%", src, dst, prob*100)

	return func() {
		ci.net.removeRule(id)
		ci.record("packet-loss: removed %s→%s rule", src, dst)
	}
}

// InjectDelay installs a latency rule: every message from src to dst sleeps d
// before being forwarded. Pass "" for src or dst to match any.
// Returns a cleanup func.
func (ci *ChaosInjector) InjectDelay(src, dst string, d time.Duration) (restore func()) {
	id := ci.nextID("delay")
	ci.net.addRule(rule{id: id, from: src, to: dst, act: actionDelay, d: d})
	ci.record("delay: %s→%s +%v", src, dst, d)

	return func() {
		ci.net.removeRule(id)
		ci.record("delay: removed %s→%s rule", src, dst)
	}
}

// CrashNode stops the given node and removes it from the network, simulating
// a hard crash. The node cannot be restarted via the injector.
func (ci *ChaosInjector) CrashNode(nodeID string) {
	n, ok := ci.nodes[nodeID]
	if !ok {
		return
	}
	n.Stop()
	ci.net.base.Disconnect(nodeID)
	ci.record("crash: %s stopped", nodeID)
}
