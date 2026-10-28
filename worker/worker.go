package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/store"
	"github.com/Shreyas9468/cube/task"
)

// Worker is responsible for executing and managing tasks on a single node.
type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	Stats     *Stats
	TaskCount int
	Store     store.Store
}

// CollectStats runs in a loop, gathering host metrics every 15 seconds.
func (w *Worker) CollectStats() {
	for {
		slog.Debug("Collecting host stats", "worker", w.Name)
		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

// RunTaskLoop dequeues and processes tasks in a continuous loop.
func (w *Worker) RunTaskLoop() {
	for {
		if w.Queue.Len() != 0 {
			result := w.RunTask()
			if result.Error != nil {
				slog.Error("Task run failed", "worker", w.Name, "error", result.Error)
			}
		} else {
			slog.Debug("No tasks in queue, sleeping", "worker", w.Name)
		}
		time.Sleep(10 * time.Second)
	}
}

// AddTask enqueues a task for processing.
func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

// RunTask dequeues the next task and starts or stops it based on its requested state.
func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()

	if t == nil {
		slog.Debug("Queue dequeue returned nil", "worker", w.Name)
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)

	taskPersisted := w.Db[taskQueued.ID]
	if taskPersisted == nil {
		taskPersisted = &taskQueued
		w.Db[taskQueued.ID] = taskPersisted
	}

	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("unexpected task state in queue")
		}
	} else {
		err := fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		result.Error = err
		slog.Warn("Invalid state transition", "task_id", taskQueued.ID,
			"from", taskPersisted.State, "to", taskQueued.State)
	}

	return result
}

// StartTask starts the Docker container for a task and updates its state to Running.
func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Run()
	if result.Error != nil {
		slog.Error("Failed to start container", "task_id", t.ID, "image", t.Image, "error", result.Error)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t
	w.TaskCount++
	w.persistTask(&t)
	slog.Info("Container started", "task_id", t.ID, "container_id", t.ContainerID, "image", t.Image)
	return result
}

// StopTask stops and removes the container for a task and updates its state to Completed.
func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		slog.Error("Failed to stop container", "task_id", t.ID, "container_id", t.ContainerID, "error", result.Error)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = &t
	w.TaskCount--
	w.persistTask(&t)
	slog.Info("Container stopped", "task_id", t.ID, "container_id", t.ContainerID)
	return result
}

func (w *Worker) persistTask(t *task.Task) {
	if w.Store == nil {
		return
	}
	data, err := json.Marshal(t)
	if err != nil {
		slog.Error("Failed to marshal task for persistence", "task_id", t.ID, "error", err)
		return
	}
	if err := w.Store.Put("tasks", t.ID.String(), data); err != nil {
		slog.Error("Failed to persist task", "task_id", t.ID, "error", err)
	}
}

// New creates a Worker instance, recovering any persisted tasks from store.
func New(name string, st store.Store) *Worker {
	db := make(map[uuid.UUID]*task.Task)
	w := &Worker{
		Name:  name,
		Queue: *queue.New(),
		Db:    db,
		Store: st,
	}

	if st != nil {
		items, err := st.List("tasks")
		if err == nil {
			for _, item := range items {
				var t task.Task
				if err := json.Unmarshal(item, &t); err == nil {
					w.Db[t.ID] = &t
				}
			}
			w.TaskCount = len(w.Db)
			slog.Info("Recovered tasks from store", "worker", name, "count", len(w.Db))
		}
	}

	return w
}

// GetTasks returns a slice of all tasks in the worker's in-memory DB.
func (w *Worker) GetTasks() []*task.Task {
	tasks := make([]*task.Task, 0, len(w.Db))
	for _, t := range w.Db {
		tasks = append(tasks, t)
	}
	return tasks
}

// GetLogs retrieves stdout/stderr logs for a given task's container.
func (w *Worker) GetLogs(taskID uuid.UUID) ([]byte, error) {
	t, ok := w.Db[taskID]
	if !ok {
		return nil, fmt.Errorf("task %s not found on worker", taskID)
	}
	if t.ContainerID == "" {
		return nil, fmt.Errorf("task %s has no active container", taskID)
	}
	cfg := task.NewConfig(t)
	d := task.NewDocker(cfg)
	return d.Logs(t.ContainerID)
}
