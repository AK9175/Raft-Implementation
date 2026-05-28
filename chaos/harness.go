package chaos

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// TB is the subset of testing.TB used by scenario helpers.
// *testing.T satisfies it natively; Harness satisfies it for non-test use.
type TB interface {
	Helper()
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Fatal(args ...any)
}

// harnessStop is the sentinel panic value used to unwind on Fatal.
type harnessStop struct{ msg string }

// Harness is a TB that records logs and failures instead of calling testing.T.
type Harness struct {
	mu     sync.Mutex
	logs   []string
	failed bool
}

func (h *Harness) Helper() {}

func (h *Harness) Logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	h.mu.Lock()
	h.logs = append(h.logs, msg)
	h.mu.Unlock()
}

func (h *Harness) Errorf(format string, args ...any) {
	msg := "ERROR: " + fmt.Sprintf(format, args...)
	h.mu.Lock()
	h.logs = append(h.logs, msg)
	h.failed = true
	h.mu.Unlock()
}

func (h *Harness) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	h.mu.Lock()
	h.logs = append(h.logs, "FATAL: "+msg)
	h.failed = true
	h.mu.Unlock()
	panic(harnessStop{msg})
}

func (h *Harness) Fatal(args ...any) {
	h.Fatalf("%s", fmt.Sprint(args...))
}

func (h *Harness) Logs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]string, len(h.logs))
	copy(cp, h.logs)
	return cp
}

func (h *Harness) Failed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failed
}

// ── ScenarioResult ────────────────────────────────────────────────────────────

// ScenarioResult holds the outcome of a single chaos scenario run.
type ScenarioResult struct {
	Name   string   `json:"name"`
	Passed bool     `json:"passed"`
	Error  string   `json:"error,omitempty"`
	DurMs  int64    `json:"duration_ms"`
	Logs   []string `json:"logs"`
}

// ── Registry ──────────────────────────────────────────────────────────────────

// ScenarioMeta describes a registered scenario for the UI.
type ScenarioMeta struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Desc  string `json:"desc"`
}

type scenarioEntry struct {
	meta ScenarioMeta
	fn   func(TB)
}

var registry []scenarioEntry

func register(meta ScenarioMeta, fn func(TB)) {
	registry = append(registry, scenarioEntry{meta: meta, fn: fn})
}

// ListScenarios returns metadata for all registered scenarios in stable order.
func ListScenarios() []ScenarioMeta {
	out := make([]ScenarioMeta, len(registry))
	for i, e := range registry {
		out[i] = e.meta
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// RunScenario executes the named scenario and returns the result.
func RunScenario(id string) ScenarioResult {
	var entry *scenarioEntry
	for i := range registry {
		if registry[i].meta.ID == id {
			entry = &registry[i]
			break
		}
	}
	if entry == nil {
		return ScenarioResult{
			Name:  id,
			Error: "unknown scenario: " + id,
		}
	}

	h := &Harness{}
	start := time.Now()

	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(harnessStop); ok {
					return // already recorded in Fatalf
				}
				panic(r) // unexpected panic — re-raise
			}
		}()
		entry.fn(h)
	}()

	result := ScenarioResult{
		Name:  id,
		Passed: !h.Failed(),
		DurMs: time.Since(start).Milliseconds(),
		Logs:  h.Logs(),
	}
	if !result.Passed {
		for _, l := range result.Logs {
			if strings.HasPrefix(l, "FATAL:") || strings.HasPrefix(l, "ERROR:") {
				result.Error = l
				break
			}
		}
	}
	return result
}
