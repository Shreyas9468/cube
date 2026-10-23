package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/node"
	"github.com/Shreyas9468/cube/scheduler"
	"github.com/Shreyas9468/cube/store"
	"github.com/Shreyas9468/cube/task"
)

// Manager orchestrates tasks across a set of worker nodes using a pluggable Scheduler.
type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID][]*task.Task
	EventDb       map[uuid.UUID][]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	WorkerNodes   []*node.Node
	Scheduler     scheduler.Scheduler
	Store         store.Store
}

// SelectWorker picks the best healthy worker for the given task using the configured Scheduler.
func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	var healthyNodes []*node.Node
	for _, n := range m.WorkerNodes {
		if n.Healthy {
			healthyNodes = append(healthyNodes, n)
		}
	}
	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy worker nodes currently available in cluster")
	}

	candidates := m.Scheduler.SelectCandidateNodes(t, healthyNodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidate nodes match resource request for task %v", t.ID)
	}
	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)
	return selectedNode, nil
}

// UpdateTasks polls every worker every 15 s and syncs task state into TaskDb.
func (m *Manager) UpdateTasks() {
	for {
		slog.Debug("Polling workers for task updates")
		m.updateTasks()
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		slog.Debug("Polling worker", "worker", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			slog.Warn("Failed to connect to worker", "worker", worker, "error", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			slog.Warn("Unexpected status from worker", "worker", worker, "status", resp.StatusCode)
			continue
		}

		var tasks []*task.Task
		if err = json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
			slog.Error("Failed to decode tasks from worker", "worker", worker, "error", err)
			continue
		}

		for _, t := range tasks {
			if _, ok := m.TaskWorkerMap[t.ID]; !ok {
				slog.Debug("Unknown task reported by worker — skipping", "task_id", t.ID, "worker", worker)
				continue
			}
			taskList := m.TaskDb[t.ID]
			if len(taskList) == 0 {
				continue
			}
			if taskList[len(taskList)-1].State != t.State {
				slog.Info("Task state updated",
					"task_id", t.ID,
					"old_state", taskList[len(taskList)-1].State,
					"new_state", t.State,
				)
				taskList[len(taskList)-1].State = t.State
				m.persistTask(taskList[len(taskList)-1])
			}
		}
	}
}

// SendWork dequeues the next pending TaskEvent, selects a worker, and dispatches the task.
func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		e := m.Pending.Dequeue()
		te := e.(task.TaskEvent)
		t := te.Task
		slog.Info("Dispatching task from pending queue", "task_id", t.ID, "image", t.Image)

		selectedNode, err := m.SelectWorker(t)
		if err != nil {
			slog.Error("Worker selection failed", "task_id", t.ID, "error", err)
			return
		}
		w := selectedNode.Name
		slog.Info("Task assigned to worker", "task_id", t.ID, "worker", w)

		m.EventDb[te.ID] = append(m.EventDb[te.ID], &te)
		m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], t.ID)
		m.TaskWorkerMap[t.ID] = w

		t.State = task.Scheduled
		m.TaskDb[t.ID] = append(m.TaskDb[t.ID], &t)
		m.persistTask(&t)
		m.persistEvent(&te)

		data, err := json.Marshal(te)
		if err != nil {
			slog.Error("Failed to marshal TaskEvent", "task_id", t.ID, "error", err)
			return
		}

		url := fmt.Sprintf("http://%s/tasks", w)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			slog.Error("Failed to send task to worker", "task_id", t.ID, "worker", w, "error", err)
			m.Pending.Enqueue(te)
			return
		}

		var result task.DockerResult
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			slog.Error("Failed to decode worker response", "task_id", t.ID, "error", err)
		}
		slog.Info("Worker acknowledged task", "task_id", t.ID, "worker", w)
	} else {
		slog.Debug("Pending queue empty — nothing to dispatch")
	}
}

// AddTask enqueues a new TaskEvent for the manager to dispatch.
func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

// GetTasks returns the latest snapshot of every known task.
func (m *Manager) GetTasks() []*task.Task {
	var tasks []*task.Task
	for _, taskList := range m.TaskDb {
		for _, t := range taskList {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

// GetNodes returns all registered worker nodes in the cluster.
func (m *Manager) GetNodes() []*node.Node {
	return m.WorkerNodes
}

// SendWorkLoop processes pending work every 10 s.
func (m *Manager) SendWorkLoop() {
	for {
		m.SendWork()
		time.Sleep(10 * time.Second)
	}
}

// DoHealthChecks runs a background loop to verify connectivity and health of all worker nodes.
func (m *Manager) DoHealthChecks() {
	for {
		slog.Debug("Running worker health checks")
		for _, n := range m.WorkerNodes {
			if err := n.GetStats(); err != nil {
				slog.Warn("Health check failed", "node", n.Name, "error", err)
				n.Healthy = false
			} else {
				if !n.Healthy {
					slog.Info("Node recovered", "node", n.Name)
				}
				n.Healthy = true
			}
		}
		time.Sleep(15 * time.Second)
	}
}

// ReconcileTasks reconciles task state against node health and auto-recovers failed tasks.
func (m *Manager) ReconcileTasks() {
	for {
		time.Sleep(20 * time.Second)
		slog.Info("Running task reconciliation")
		m.reconcile()
	}
}

func (m *Manager) reconcile() {
	nodeHealth := make(map[string]bool)
	for _, n := range m.WorkerNodes {
		nodeHealth[n.Name] = n.Healthy
	}

	for taskID, workerName := range m.TaskWorkerMap {
		taskList, ok := m.TaskDb[taskID]
		if !ok || len(taskList) == 0 {
			continue
		}
		latestTask := taskList[len(taskList)-1]

		// Condition 1: Node assigned to the task is unhealthy
		isNodeHealthy := nodeHealth[workerName]
		if !isNodeHealthy && (latestTask.State == task.Running || latestTask.State == task.Scheduled) {
			slog.Warn("Task on unhealthy node — reconciling",
				"task_id", latestTask.ID, "worker", workerName)
			m.maybeRestartTask(latestTask, "node_failure")
			continue
		}

		// Condition 2: Task has failed unexpectedly
		if latestTask.State == task.Failed {
			m.maybeRestartTask(latestTask, "task_failed")
		}
	}
}

func (m *Manager) maybeRestartTask(t *task.Task, reason string) {
	const maxRetries = 3
	shouldRestart := false

	switch t.RestartPolicy {
	case task.RestartPolicyAlways:
		shouldRestart = true
	case task.RestartPolicyOnFailure:
		if reason == "task_failed" || reason == "node_failure" {
			shouldRestart = true
		}
	}

	if shouldRestart && t.RestartCount < maxRetries {
		t.RestartCount++
		slog.Info("Restarting task",
			"task_id", t.ID,
			"attempt", t.RestartCount,
			"max_retries", maxRetries,
			"reason", reason,
		)
		retryTask := *t
		retryTask.State = task.Scheduled
		retryEvent := task.TaskEvent{
			ID:        uuid.New(),
			State:     task.Scheduled,
			Timestamp: time.Now().UTC(),
			Task:      retryTask,
		}
		m.AddTask(retryEvent)
	} else if shouldRestart {
		slog.Warn("Task exceeded max retries — permanently failed",
			"task_id", t.ID, "max_retries", maxRetries, "reason", reason)
		t.State = task.Failed
	}
}

func (m *Manager) persistTask(t *task.Task) {
	if m.Store == nil {
		return
	}
	data, err := json.Marshal(t)
	if err != nil {
		slog.Error("Failed to marshal task", "task_id", t.ID, "error", err)
		return
	}
	if err := m.Store.Put("tasks", t.ID.String(), data); err != nil {
		slog.Error("Failed to persist task", "task_id", t.ID, "error", err)
	}
}

func (m *Manager) persistEvent(te *task.TaskEvent) {
	if m.Store == nil {
		return
	}
	data, err := json.Marshal(te)
	if err != nil {
		slog.Error("Failed to marshal event", "event_id", te.ID, "error", err)
		return
	}
	if err := m.Store.Put("events", te.ID.String(), data); err != nil {
		slog.Error("Failed to persist event", "event_id", te.ID, "error", err)
	}
}

// ProcessTasks starts send-work, update-tasks, health-checks, and task-reconciliation loops.
func (m *Manager) ProcessTasks() {
	go m.SendWorkLoop()
	go m.UpdateTasks()
	go m.DoHealthChecks()
	go m.ReconcileTasks()
}

// New creates a Manager wired with the given workers and scheduler without persistent store.
func New(workers []string, s scheduler.Scheduler) *Manager {
	return NewWithStore(workers, s, nil)
}

// NewWithStore creates a Manager with an optional persistent store, recovering historical state.
func NewWithStore(workers []string, s scheduler.Scheduler, st store.Store) *Manager {
	workerNodes := make([]*node.Node, 0, len(workers))
	for _, w := range workers {
		workerNodes = append(workerNodes, &node.Node{
			Name:    w,
			Ip:      w,
			Api:     w,
			Role:    "worker",
			Healthy: true,
		})
	}

	taskDb := make(map[uuid.UUID][]*task.Task)
	eventDb := make(map[uuid.UUID][]*task.TaskEvent)
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	if st != nil {
		taskItems, err := st.List("tasks")
		if err == nil {
			for _, item := range taskItems {
				var t task.Task
				if err := json.Unmarshal(item, &t); err == nil {
					taskDb[t.ID] = append(taskDb[t.ID], &t)
				}
			}
			slog.Info("Recovered tasks from store", "count", len(taskDb))
		}

		eventItems, err := st.List("events")
		if err == nil {
			for _, item := range eventItems {
				var te task.TaskEvent
				if err := json.Unmarshal(item, &te); err == nil {
					eventDb[te.ID] = append(eventDb[te.ID], &te)
				}
			}
			slog.Info("Recovered events from store", "count", len(eventDb))
		}
	}

	return &Manager{
		Pending:       *queue.New(),
		TaskDb:        taskDb,
		EventDb:       eventDb,
		Workers:       workers,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		WorkerNodes:   workerNodes,
		Scheduler:     s,
		Store:         st,
	}
}
