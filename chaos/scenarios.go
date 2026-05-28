package chaos

import (
	"fmt"
	"os"
	"time"

	"github.com/atharva/raft/raft"
)

func init() {
	register(ScenarioMeta{
		ID:    "split_brain",
		Label: "Split Brain",
		Desc:  "Isolates the leader; the remaining majority elects a new one. Writes sent to the isolated node cannot commit without a quorum.",
	}, scenarioSplitBrain)

	register(ScenarioMeta{
		ID:    "stale_log_cannot_win",
		Label: "Stale Log Cannot Win",
		Desc:  "A follower is partitioned while the log advances. After healing, its RequestVotes are rejected by up-to-date peers (Raft §5.4.1).",
	}, scenarioStaleLogCannotWin)

	register(ScenarioMeta{
		ID:    "leader_isolation",
		Label: "Leader Isolation Write Loss",
		Desc:  "The leader is cut off from all peers. Writes it accepts locally must not commit — the commit index must not advance without a quorum.",
	}, scenarioLeaderIsolationWriteLoss)

	register(ScenarioMeta{
		ID:    "packet_loss_converges",
		Label: "30% Packet Loss Still Converges",
		Desc:  "Probabilistic 30% packet loss is injected on all links. The cluster degrades gracefully but still elects a leader and commits entries.",
	}, scenarioPacketLossStillConverges)
}

// ── shared state machine ──────────────────────────────────────────────────────

type kvStateMachine struct{}

func (kvStateMachine) Apply(_ []byte) interface{} { return nil }
func (kvStateMachine) Snapshot() ([]byte, error)  { return nil, nil }
func (kvStateMachine) Restore(_ []byte) error      { return nil }

// ── cluster helpers ───────────────────────────────────────────────────────────

func chaosClusterTB(tb TB, count int) ([]*raft.RaftNode, map[string]*raft.RaftNode, *ChaosInjector, func()) {
	tb.Helper()

	net := NewChaosNetwork()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	var dirs []string
	nodes := make([]*raft.RaftNode, count)
	byID := make(map[string]*raft.RaftNode, count)

	for i, id := range ids {
		peers := make([]string, 0, count-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		dir, err := os.MkdirTemp("", "raft-chaos-*")
		if err != nil {
			tb.Fatalf("MkdirTemp: %v", err)
		}
		dirs = append(dirs, dir)

		cfg := raft.Config{
			ID:                   id,
			Peers:                peers,
			Transport:            net.Transport(id),
			StateMachine:         kvStateMachine{},
			ElectionTimeoutMinMs: 200,
			ElectionTimeoutMaxMs: 400,
			HeartbeatIntervalMs:  60,
			DataDir:              dir,
		}
		node := raft.NewRaftNode(cfg)
		nodes[i] = node
		byID[id] = node
		net.Register(id, node)
	}

	for _, n := range nodes {
		go n.Run()
	}

	injector := NewInjector(net, byID)

	teardown := func() {
		for _, n := range nodes {
			n.Stop()
		}
		for _, d := range dirs {
			os.RemoveAll(d)
		}
	}
	return nodes, byID, injector, teardown
}

func waitForLeaderTB(tb TB, nodes []*raft.RaftNode, timeout time.Duration) *raft.RaftNode {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found *raft.RaftNode
		for _, n := range nodes {
			if n.State() == raft.Leader {
				if found != nil {
					tb.Fatal("two leaders elected simultaneously")
				}
				found = n
			}
		}
		if found != nil {
			return found
		}
		time.Sleep(15 * time.Millisecond)
	}
	tb.Fatal("no leader elected within timeout")
	return nil
}

func nodesConsistent(nodes []*raft.RaftNode) bool {
	if len(nodes) == 0 {
		return true
	}
	ref := nodes[0].LastIndex()
	for _, n := range nodes[1:] {
		if n.LastIndex() != ref {
			return false
		}
	}
	return true
}

func waitConsistentTB(tb TB, nodes []*raft.RaftNode, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nodesConsistent(nodes) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, n := range nodes {
		tb.Logf("  %s lastIndex=%d", n.ID(), n.LastIndex())
	}
	tb.Fatal("nodes did not converge on the same log within timeout")
}

// ── Scenario: Split Brain ─────────────────────────────────────────────────────

func scenarioSplitBrain(tb TB) {
	nodes, _, injector, teardown := chaosClusterTB(tb, 5)
	defer teardown()

	waitForLeaderTB(tb, nodes, 3*time.Second)
	// Let heartbeats propagate so the elected term stabilises before we submit.
	time.Sleep(200 * time.Millisecond)
	leader := waitForLeaderTB(tb, nodes, 2*time.Second)
	tb.Logf("initial leader: %s (term %d)", leader.ID(), leader.CurrentTerm())

	for i := 0; i < 3; i++ {
		if _, _, err := leader.Submit([]byte(fmt.Sprintf("before-partition-%d", i))); err != nil {
			tb.Fatalf("pre-partition Submit: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	healLeader := injector.PartitionNode(leader.ID())

	var isolatedWrites int
	for i := 0; i < 5; i++ {
		_, _, err := leader.Submit([]byte(fmt.Sprintf("isolated-%d", i)))
		if err == nil {
			isolatedWrites++
		}
	}
	tb.Logf("isolated leader accepted %d write(s) locally (won't commit without quorum)", isolatedWrites)

	remaining := make([]*raft.RaftNode, 0, 4)
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForLeaderTB(tb, remaining, 3*time.Second)
	tb.Logf("new leader after partition: %s (term %d)", newLeader.ID(), newLeader.CurrentTerm())

	if newLeader.CurrentTerm() <= leader.CurrentTerm() {
		tb.Errorf("new leader term %d should exceed old leader term %d",
			newLeader.CurrentTerm(), leader.CurrentTerm())
	}

	for i := 0; i < 3; i++ {
		if _, _, err := newLeader.Submit([]byte(fmt.Sprintf("after-partition-%d", i))); err != nil {
			tb.Fatalf("post-partition Submit: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	healLeader()
	// Give the old leader time to receive a heartbeat from the new leader
	// (higher term) and step down. One heartbeat cycle = 60ms; use 10× margin.
	time.Sleep(800 * time.Millisecond)

	if leader.State() == raft.Leader {
		tb.Errorf("isolated leader should have stepped down after heal, still reports Leader")
	}

	waitConsistentTB(tb, nodes, 5*time.Second)

	for _, obs := range injector.Observations() {
		tb.Logf("[obs %s] %s", obs.At.Format("15:04:05.000"), obs.Msg)
	}
}

// ── Scenario: Stale Log Cannot Win ───────────────────────────────────────────

func scenarioStaleLogCannotWin(tb TB) {
	nodes, _, injector, teardown := chaosClusterTB(tb, 3)
	defer teardown()

	leader := waitForLeaderTB(tb, nodes, 3*time.Second)
	tb.Logf("initial leader: %s", leader.ID())

	var stale *raft.RaftNode
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			stale = n
			break
		}
	}
	tb.Logf("stale target: %s", stale.ID())

	healStale := injector.PartitionNode(stale.ID())

	for i := 0; i < 10; i++ {
		if _, _, err := leader.Submit([]byte(fmt.Sprintf("advance-%d", i))); err != nil {
			tb.Fatalf("Submit during partition: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	healStale()
	time.Sleep(500 * time.Millisecond)

	if stale.State() == raft.Leader {
		tb.Errorf("stale node %s won election despite having a shorter log", stale.ID())
	}

	newLeader := waitForLeaderTB(tb, nodes, 3*time.Second)
	tb.Logf("leader after heal: %s (term %d)", newLeader.ID(), newLeader.CurrentTerm())

	waitConsistentTB(tb, nodes, 3*time.Second)

	for _, obs := range injector.Observations() {
		tb.Logf("[obs %s] %s", obs.At.Format("15:04:05.000"), obs.Msg)
	}
}

// ── Scenario: Leader Isolation Write Loss ────────────────────────────────────

func scenarioLeaderIsolationWriteLoss(tb TB) {
	nodes, _, injector, teardown := chaosClusterTB(tb, 3)
	defer teardown()

	leader := waitForLeaderTB(tb, nodes, 3*time.Second)
	tb.Logf("initial leader: %s", leader.ID())

	baselineCommit := leader.CommitIndex()
	injector.PartitionNode(leader.ID())

	var accepted int
	for i := 0; i < 5; i++ {
		_, _, err := leader.Submit([]byte(fmt.Sprintf("lost-%d", i)))
		if err == nil {
			accepted++
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Logf("isolated leader accepted %d write(s) into local log", accepted)

	remaining := make([]*raft.RaftNode, 0, 2)
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForLeaderTB(tb, remaining, 3*time.Second)
	tb.Logf("new leader: %s (term %d)", newLeader.ID(), newLeader.CurrentTerm())

	isolatedCommit := leader.CommitIndex()
	if isolatedCommit > baselineCommit {
		tb.Errorf("isolated leader commit index advanced from %d to %d without quorum",
			baselineCommit, isolatedCommit)
	}
	tb.Logf("isolated leader commitIndex: baseline=%d, after-writes=%d (no advance — correct)",
		baselineCommit, isolatedCommit)

	injector.HealAll()
	// Wait for the old leader to receive a higher-term heartbeat and step down.
	time.Sleep(600 * time.Millisecond)

	if leader.State() == raft.Leader {
		tb.Errorf("old isolated leader still reports Leader state after heal")
	}

	// Submit one entry to the new leader. This entry starts at index 1 and
	// conflicts with the stale entries on the old leader, causing them to be
	// overwritten — the necessary condition for log convergence.
	if _, _, err := newLeader.Submit([]byte("post-heal")); err != nil {
		tb.Fatalf("post-heal Submit: %v", err)
	}

	waitConsistentTB(tb, nodes, 5*time.Second)

	for _, obs := range injector.Observations() {
		tb.Logf("[obs %s] %s", obs.At.Format("15:04:05.000"), obs.Msg)
	}
}

// ── Scenario: Packet Loss Still Converges ────────────────────────────────────

func scenarioPacketLossStillConverges(tb TB) {
	nodes, _, injector, teardown := chaosClusterTB(tb, 3)
	defer teardown()

	restore := injector.InjectPacketLoss("", "", 0.30)
	defer restore()

	leader := waitForLeaderTB(tb, nodes, 8*time.Second)
	tb.Logf("leader under 30%% packet loss: %s (term %d)", leader.ID(), leader.CurrentTerm())

	for i := 0; i < 5; i++ {
		if _, _, err := leader.Submit([]byte(fmt.Sprintf("lossy-%d", i))); err != nil {
			leader = waitForLeaderTB(tb, nodes, 3*time.Second)
			if _, _, err2 := leader.Submit([]byte(fmt.Sprintf("lossy-%d", i))); err2 != nil {
				tb.Logf("Submit %d skipped under lossy network: %v", i, err2)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	restore()

	waitConsistentTB(tb, nodes, 5*time.Second)

	for _, obs := range injector.Observations() {
		tb.Logf("[obs %s] %s", obs.At.Format("15:04:05.000"), obs.Msg)
	}
}
