package node

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Node represents a worker host in the cluster.
type Node struct {
	Name            string
	Ip              string
	Api             string
	Cores           int
	Memory          int64
	MemoryAllocated int64
	Disk            int64
	DiskAllocated   int64
	Stats           NodeStats
	Role            string
	TaskCount       int
	Healthy         bool
	LastSeen        time.Time
}

// NodeStats holds the latest resource metrics retrieved from the worker API.
type NodeStats struct {
	MemStats  *MemStats  `json:"memStats"`
	DiskStats *DiskStats `json:"diskStats"`
	CpuStats  *CpuStats  `json:"cpuStats"`
	LoadStats *LoadStats `json:"loadStats"`
	TaskCount int        `json:"taskCount"`
}

// MemStats is the JSON representation of memory metrics.
type MemStats struct {
	MemTotal     uint64 `json:"MemTotal"`
	MemAvailable uint64 `json:"MemAvailable"`
}

// DiskStats is the JSON representation of disk metrics.
type DiskStats struct {
	All  uint64 `json:"all"`
	Free uint64 `json:"free"`
	Used uint64 `json:"used"`
}

// CpuStats is the JSON representation of CPU metrics.
type CpuStats struct {
	User    uint64 `json:"user"`
	Nice    uint64 `json:"nice"`
	System  uint64 `json:"system"`
	Idle    uint64 `json:"idle"`
	IOWait  uint64 `json:"iowait"`
	IRQ     uint64 `json:"irq"`
	SoftIRQ uint64 `json:"softirq"`
	Steal   uint64 `json:"steal"`
}

// LoadStats is the JSON representation of load average metrics.
type LoadStats struct {
	Last1Min  float64 `json:"last1Min"`
	Last5Min  float64 `json:"last5Min"`
	Last15Min float64 `json:"last15Min"`
}

// New creates a Node and validates connectivity by fetching initial stats.
func New(name, api, role string) (*Node, error) {
	n := &Node{
		Name: name,
		Ip:   api,
		Api:  api,
		Role: role,
	}

	if err := n.GetStats(); err != nil {
		log.Printf("Error getting stats from node %s: %v\n", name, err)
		return nil, err
	}

	return n, nil
}

// GetStats fetches the latest stats from the node's /stats endpoint.
func (n *Node) GetStats() error {
	url := fmt.Sprintf("%s/stats", n.Api)
	resp, err := http.Get(url)
	if err != nil {
		n.Healthy = false
		return fmt.Errorf("unable to connect to %s: %w", n.Api, err)
	}

	if resp.StatusCode != 200 {
		n.Healthy = false
		return fmt.Errorf("error retrieving stats from %s: %d", n.Api, resp.StatusCode)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		n.Healthy = false
		return fmt.Errorf("error reading response body: %w", err)
	}

	var stats NodeStats
	if err = json.Unmarshal(body, &stats); err != nil {
		n.Healthy = false
		return fmt.Errorf("error unmarshalling stats: %w", err)
	}

	n.Stats = stats
	n.Memory = int64(stats.MemStats.MemTotal)
	n.Disk = int64(stats.DiskStats.All)
	n.TaskCount = stats.TaskCount
	n.Healthy = true
	n.LastSeen = time.Now().UTC()

	return nil
}
