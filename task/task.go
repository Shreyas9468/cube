package task

import (
	"context"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// State represents the lifecycle state of a Task.
type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

// Task is the smallest unit of work in the Cube orchestration system.
type Task struct {
	ID            uuid.UUID
	ContainerID   string
	Name          string
	State         State
	Image         string
	CPU           float64
	Memory        int64
	Disk          int64
	ExposedPorts  network.PortSet
	PortBindings  map[string]string
	RestartPolicy string
	StartTime     time.Time
	FinishTime    time.Time
}

// TaskEvent represents a request to transition a Task to a new State.
type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}

// Config holds the Docker-level configuration needed to run a task's container.
type Config struct {
	Name          string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	ExposedPorts  network.PortSet
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
}

// Docker wraps a Docker client and task Config.
type Docker struct {
	Client *client.Client
	Config Config
}

// DockerResult captures the outcome of a Docker operation.
type DockerResult struct {
	Error       error
	Action      string
	ContainerId string
	Result      string
}

// NewConfig creates a Config from a Task.
func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		ExposedPorts:  t.ExposedPorts,
		Image:         t.Image,
		Cpu:           t.CPU,
		Memory:        t.Memory,
		Disk:          t.Disk,
		RestartPolicy: t.RestartPolicy,
	}
}

// NewDocker creates a Docker wrapper using environment-based Docker config.
func NewDocker(c *Config) *Docker {
	dc, _ := client.NewClientWithOpts(client.FromEnv)
	return &Docker{
		Client: dc,
		Config: *c,
	}
}

// Run pulls the image, creates, and starts the container described in Config.
func (d *Docker) Run() DockerResult {
	ctx := context.Background()

	// Pull image.
	reader, err := d.Client.ImagePull(ctx, d.Config.Image, client.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	defer reader.Close()
	if _, err = io.Copy(os.Stdout, reader); err != nil {
		log.Printf("Error streaming image pull output: %v\n", err)
	}

	// Restart & resource policy.
	rp := container.RestartPolicy{
		Name: container.RestartPolicyMode(d.Config.RestartPolicy),
	}
	r := container.Resources{
		Memory:   d.Config.Memory,
		NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9)),
	}

	// Container config.
	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	// Host config.
	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	// Create container.
	resp, err := d.Client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &cc,
		HostConfig: &hc,
		Name:       d.Config.Name,
	})
	if err != nil {
		log.Printf("Error creating container using image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	// Start container.
	if _, err = d.Client.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{}); err != nil {
		log.Printf("Error starting container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	// Stream logs.
	out, err := d.Client.ContainerLogs(ctx, resp.ID,
		client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		log.Printf("Error getting logs for container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}
	defer out.Close()
	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return DockerResult{ContainerId: resp.ID, Action: "start", Result: "success"}
}

// Stop stops and removes the container with the given id.
func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v\n", id)
	ctx := context.Background()

	if _, err := d.Client.ContainerStop(ctx, id, client.ContainerStopOptions{}); err != nil {
		log.Printf("Error stopping container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}

	if _, err := d.Client.ContainerRemove(ctx, id, client.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         false,
	}); err != nil {
		log.Printf("Error removing container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}

	return DockerResult{Action: "stop", Result: "success", Error: nil}
}
