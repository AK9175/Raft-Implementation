package raft

import (
	"encoding/gob"
	"os"
	"path/filepath"
)

// PersistedState is what gets written to disk.
// Exactly the three fields from Raft Figure 2 that must survive a crash.
type PersistedState struct {
	CurrentTerm uint64
	VotedFor    string
	Entries     []LogEntry
}

// persist writes currentTerm, votedFor, and the log to disk.
// Must be called with n.mu held, and must be called BEFORE sending any reply
// that depends on these values (vote reply, append reply).
//
// WHY NOT write directly to raft-state.bin?
//   os.WriteFile("raft-state.bin", data)
//   Problem: if power cuts out mid-write, the file is left half-written on disk.
//   On restart we read corrupt data — wrong term, wrong votedFor, truncated log.
//   This can cause the node to vote twice in the same term → two leaders → data loss.
//
// SOLUTION: write to a temp file first, then rename.
//   Step 1 — write all data to raft-state.bin.tmp (the real file is untouched)
//   Step 2 — rename raft-state.bin.tmp → raft-state.bin
//   Rename is atomic on all POSIX systems (Linux, macOS): the OS either
//   completes it or doesn't — it can never leave both files half-renamed.
//   So raft-state.bin is always either the old complete file or the new complete
//   file. Never corrupt.
//
// PROBLEM with temp+rename alone: the OS does not write to disk immediately.
//   Writes go into the OS page cache (RAM) first. The OS flushes to disk later,
//   at its own discretion — could be milliseconds, could be seconds.
//   If power cuts out after the rename but before the OS flushes the cache:
//     raft-state.bin exists (rename succeeded) but its contents are empty or
//     partial (cache was never flushed). We still get corrupt data on restart.
//
// SOLUTION: fsync before rename.
//   f.Sync() tells the OS: "flush your page cache to disk RIGHT NOW, block
//   until it's physically written." Only after fsync returns do we rename.
//   Now a power cut after rename is safe — the bytes are already on disk.
//   Crash timeline with fsync:
//     write to .tmp → fsync (blocks until on disk) → rename → done
//   A crash anywhere before fsync returns leaves the old raft-state.bin intact.
//   A crash after fsync leaves the new complete file on disk.
func (n *RaftNode) persist() {
	state := PersistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Entries:     n.log.entries,
	}

	path := filepath.Join(n.config.DataDir, "raft-state.bin")
	tmp := path + ".tmp"

	// Step 1: write to temp file — real file is untouched until rename.
	f, err := os.Create(tmp)
	if err != nil {
		return
	}

	if err := gob.NewEncoder(f).Encode(state); err != nil {
		f.Close()
		os.Remove(tmp)
		return
	}

	// Step 2: fsync — force OS page cache to disk before we rename.
	// Without this, a power cut after rename could leave an empty/partial file.
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return
	}
	f.Close()

	// Step 3: atomic rename — raft-state.bin is now the new complete file.
	os.Rename(tmp, path)
}

// loadState reads persisted state from disk and restores it into the node.
// Called once during startup, before Run().
// Returns nil if no state file exists (fresh node, first boot).
func (n *RaftNode) loadState() error {
	path := filepath.Join(n.config.DataDir, "raft-state.bin")

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh node, start from zero
		}
		return err
	}
	defer f.Close()

	var state PersistedState
	if err := gob.NewDecoder(f).Decode(&state); err != nil {
		return err
	}

	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log.entries = state.Entries
	return nil
}
