package scheduler

import (
	"math"
	"time"

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

// Epvm (Enhanced Parallel Virtual Machine) is a resource-aware Scheduler.
// It scores nodes by projected CPU and memory usage after placing the task.
type Epvm struct {
	Name string
}

// SelectCandidateNodes filters out nodes that cannot satisfy task requirements.
func (e *Epvm) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node
	for _, n := range nodes {
		if checkDisk(t, n) {
			candidates = append(candidates, n)
		}
	}
	return candidates
}

// Score calculates a composite resource-availability score for each candidate.
func (e *Epvm) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	maxJobs := 4.0

	for _, n := range nodes {
		cpuUsage, err := calculateCpuPercent(n)
		if err != nil {
			continue
		}
		cpuLoad := calculateLoad(cpuUsage, math.Pow(2, 0.8))
		memAlloc := float64(n.Stats.MemStats.MemTotal-n.Stats.MemStats.MemAvailable) / float64(n.Stats.MemStats.MemTotal)
		memReq := float64(t.Memory) / float64(n.Stats.MemStats.MemTotal)
		memLoad := calculateLoad(memAlloc+memReq, math.Pow(2, 0.8))
		taskLoad := calculateLoad(float64(n.TaskCount)/maxJobs, math.Pow(2, 0.8))

		nodeScores[n.Name] = cpuLoad + memLoad + taskLoad
	}
	return nodeScores
}

// Pick chooses the candidate with the lowest (best) score.
func (e *Epvm) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	minCost := 0.00
	var bestNode *node.Node
	for i, n := range candidates {
		if i == 0 {
			minCost = scores[n.Name]
			bestNode = n
			continue
		}
		if scores[n.Name] < minCost {
			minCost = scores[n.Name]
			bestNode = n
		}
	}
	return bestNode
}

func checkDisk(t task.Task, n *node.Node) bool {
	return t.Disk <= n.Disk-n.DiskAllocated
}

func calculateCpuPercent(n *node.Node) (float64, error) {
	if n.Stats.CpuStats == nil {
		return 0, nil
	}
	previousIdle := n.Stats.CpuStats.Idle + n.Stats.CpuStats.IOWait
	idle := n.Stats.CpuStats.Idle + n.Stats.CpuStats.IOWait
	previousNonIdle := n.Stats.CpuStats.User + n.Stats.CpuStats.Nice +
		n.Stats.CpuStats.System + n.Stats.CpuStats.IRQ +
		n.Stats.CpuStats.SoftIRQ + n.Stats.CpuStats.Steal
	nonIdle := n.Stats.CpuStats.User + n.Stats.CpuStats.Nice +
		n.Stats.CpuStats.System + n.Stats.CpuStats.IRQ +
		n.Stats.CpuStats.SoftIRQ + n.Stats.CpuStats.Steal
	previousTotal := previousIdle + previousNonIdle
	total := idle + nonIdle
	totald := total - previousTotal
	idled := idle - previousIdle
	if totald == 0 {
		return 0.0, nil
	}
	return (float64(totald) - float64(idled)) / float64(totald), nil
}

func calculateLoad(usage float64, capacity float64) float64 {
	return -capacity * math.Log2(1-usage) * float64(time.Millisecond)
}
