// readstate decodes a raft-state.bin or raft-snapshot.bin file and prints
// its contents as human-readable JSON.
//
// Usage:
//
//	go run ./cmd/readstate /tmp/raft-kvstore/node1/raft-state.bin
//	go run ./cmd/readstate /tmp/raft-kvstore/node1/raft-snapshot.bin
//
// Or from inside a container:
//
//	docker cp infra-node1-1:/data/raft-state.bin /tmp/node1-state.bin
//	go run ./cmd/readstate /tmp/node1-state.bin
package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/atharva/raft/raft"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: readstate <path-to-.bin-file>")
		os.Exit(1)
	}
	path := os.Args[1]

	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer f.Close()

	// Try raft-state.bin first.
	var state raft.PersistedState
	if err := gob.NewDecoder(f).Decode(&state); err != nil {
		log.Fatalf("decode: %v (is this a valid raft-state.bin or raft-snapshot.bin?)", err)
	}

	out, _ := json.MarshalIndent(state, "", "  ")
	fmt.Println(string(out))
}
