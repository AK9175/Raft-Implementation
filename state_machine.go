package raft

// StateMachine is the interface that any application built on top of Raft must implement.
//
// Raft guarantees that Apply is called on every node in the same order with the
// same commands. The state machine defines what those commands mean.
//
// Raft §2: "The state machine processes the same sequence of commands from the
// log, so they produce the same outputs."
type StateMachine interface {
	// Apply executes a committed log entry against the state machine.
	// The return value is sent back to the client that submitted the command.
	Apply(command []byte) interface{}

	// Snapshot serializes the current state machine state to bytes.
	// Called when the Raft log needs to be compacted (Checkpoint 11).
	Snapshot() ([]byte, error)

	// Restore replaces the current state machine state from a snapshot.
	// Called when a node is recovering from a snapshot (Checkpoint 11).
	Restore(snapshot []byte) error
}
