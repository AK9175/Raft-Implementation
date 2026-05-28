// Chaos server — runs fault-injection scenarios in-process and exposes the
// results over HTTP so the dashboard can trigger them via button clicks.
//
// Endpoints:
//
//	GET  /scenarios        — list available scenarios (id, label, desc)
//	POST /scenarios/{id}   — run a scenario; blocks until done, returns JSON result
//
// Run from the project root:
//
//	go run ./cmd/chaos
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/atharva/raft/chaos"
)

func main() {
	addr := os.Getenv("CHAOS_ADDR")
	if addr == "" {
		addr = ":9091"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/scenarios", handleList)
	mux.HandleFunc("/scenarios/", handleRun)

	log.Printf("chaos server listening on %s", addr)
	if err := http.ListenAndServe(addr, corsMiddleware(mux)); err != nil {
		log.Fatal(err)
	}
}

func handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chaos.ListScenarios())
}

func handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// path is /scenarios/{id}
	id := strings.TrimPrefix(r.URL.Path, "/scenarios/")
	if id == "" {
		http.Error(w, "missing scenario id", http.StatusBadRequest)
		return
	}

	log.Printf("running scenario %q", id)
	result := chaos.RunScenario(id)
	if result.Passed {
		log.Printf("scenario %q PASSED in %dms", id, result.DurMs)
	} else {
		log.Printf("scenario %q FAILED in %dms: %s", id, result.DurMs, result.Error)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
