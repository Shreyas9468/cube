package scheduler_test

import (
	"testing"

	"github.com/Shreyas9468/cube/node"
	"github.com/Shreyas9468/cube/scheduler"
	"github.com/Shreyas9468/cube/task"
)

func makeNodes(names ...string) []*node.Node {
	nodes := make([]*node.Node, 0, len(names))
	for _, n := range names {
		nodes = append(nodes, &node.Node{
			Name:    n,
			Healthy: true,
			Disk:    100,
		})
	}
	return nodes
}

// ── RoundRobin ────────────────────────────────────────────────────────────────

func TestRoundRobinSelectsAllCandidates(t *testing.T) {
	rr := &scheduler.RoundRobin{Name: "rr"}
	nodes := makeNodes("a", "b", "c")
	candidates := rr.SelectCandidateNodes(task.Task{}, nodes)
	if len(candidates) != len(nodes) {
		t.Errorf("expected %d candidates, got %d", len(nodes), len(candidates))
	}
}

func TestRoundRobinPicksAllNodes(t *testing.T) {
	rr := &scheduler.RoundRobin{Name: "rr"}
	nodes := makeNodes("alpha", "beta", "gamma")
	seen := map[string]bool{}

	for i := 0; i < len(nodes)*2; i++ {
		candidates := rr.SelectCandidateNodes(task.Task{}, nodes)
		scores := rr.Score(task.Task{}, candidates)
		picked := rr.Pick(scores, candidates)
		if picked == nil {
			t.Fatal("Pick returned nil")
		}
		seen[picked.Name] = true
	}

	for _, n := range nodes {
		if !seen[n.Name] {
			t.Errorf("node %q was never selected across %d rounds", n.Name, len(nodes)*2)
		}
	}
}

func TestRoundRobinSingleNode(t *testing.T) {
	rr := &scheduler.RoundRobin{Name: "rr"}
	nodes := makeNodes("solo")
	for i := 0; i < 3; i++ {
		candidates := rr.SelectCandidateNodes(task.Task{}, nodes)
		scores := rr.Score(task.Task{}, candidates)
		picked := rr.Pick(scores, candidates)
		if picked == nil || picked.Name != "solo" {
			t.Errorf("iteration %d: expected 'solo', got %v", i, picked)
		}
	}
}

// ── Epvm ─────────────────────────────────────────────────────────────────────

func TestEpvmFiltersNodesByDisk(t *testing.T) {
	epvm := &scheduler.Epvm{Name: "epvm"}

	// Node with plenty of disk (100 free) and a node with no free disk.
	plenty := &node.Node{Name: "plenty", Healthy: true, Disk: 100, DiskAllocated: 0}
	cramped := &node.Node{Name: "cramped", Healthy: true, Disk: 5, DiskAllocated: 5}

	t1 := task.Task{Disk: 10} // needs 10 GB
	candidates := epvm.SelectCandidateNodes(t1, []*node.Node{plenty, cramped})

	if len(candidates) != 1 || candidates[0].Name != "plenty" {
		t.Errorf("expected only 'plenty', got %v", candidates)
	}
}

func TestEpvmPickLowestScore(t *testing.T) {
	epvm := &scheduler.Epvm{Name: "epvm"}
	nodes := makeNodes("x", "y")
	scores := map[string]float64{"x": 3.5, "y": 1.2}

	picked := epvm.Pick(scores, nodes)
	if picked == nil || picked.Name != "y" {
		t.Errorf("expected 'y' (lowest score), got %v", picked)
	}
}
