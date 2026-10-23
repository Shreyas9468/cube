package scheduler

import (
	"github.com/Shreyas9468/cube/node"
	"github.com/Shreyas9468/cube/task"
)

// Scheduler defines the interface every scheduling algorithm must satisfy.
type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
	Score(t task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

// RoundRobin implements a simple round-robin Scheduler.
type RoundRobin struct {
	Name       string
	LastWorker int
}

// SelectCandidateNodes returns all available nodes.
func (r *RoundRobin) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

// Score assigns equal scores so Pick uses round-robin index.
func (r *RoundRobin) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	var newWorker int
	if r.LastWorker+1 < len(nodes) {
		newWorker = r.LastWorker + 1
		r.LastWorker++
	} else {
		newWorker = 0
		r.LastWorker = 0
	}
	for i, n := range nodes {
		if i == newWorker {
			nodeScores[n.Name] = 0.1
		} else {
			nodeScores[n.Name] = 1.0
		}
	}
	return nodeScores
}

// Pick chooses the node with the lowest score.
func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var bestNode *node.Node
	var lowestScore float64
	for i, n := range candidates {
		if i == 0 {
			bestNode = n
			lowestScore = scores[n.Name]
			continue
		}
		if scores[n.Name] < lowestScore {
			bestNode = n
			lowestScore = scores[n.Name]
		}
	}
	return bestNode
}
