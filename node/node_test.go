package node

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNodeGetStatsSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stats" {
			t.Errorf("expected /stats, got %s", r.URL.Path)
		}
		stats := NodeStats{
			MemStats: &MemStats{
				MemTotal:     8 * 1024 * 1024 * 1024,
				MemAvailable: 4 * 1024 * 1024 * 1024,
			},
			DiskStats: &DiskStats{
				All:  100 * 1024 * 1024 * 1024,
				Free: 60 * 1024 * 1024 * 1024,
				Used: 40 * 1024 * 1024 * 1024,
			},
			TaskCount: 2,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	}))
	defer ts.Close()

	n := &Node{
		Name: "test-node-1",
		Api:  strings.TrimPrefix(ts.URL, "http://"),
		Role: "worker",
	}

	if err := n.GetStats(); err != nil {
		t.Fatalf("unexpected error from GetStats: %v", err)
	}

	if !n.Healthy {
		t.Errorf("expected node to be healthy")
	}
	if n.TaskCount != 2 {
		t.Errorf("expected task count 2, got %d", n.TaskCount)
	}
	if n.Memory != 8*1024*1024*1024 {
		t.Errorf("expected memory %d, got %d", 8*1024*1024*1024, n.Memory)
	}
}

func TestNodeGetStatsFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	n := &Node{
		Name: "test-node-fail",
		Api:  ts.URL,
		Role: "worker",
	}

	if err := n.GetStats(); err == nil {
		t.Fatal("expected error from failing node, got nil")
	}
	if n.Healthy {
		t.Errorf("expected node to be unhealthy")
	}
}
