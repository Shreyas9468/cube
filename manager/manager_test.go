package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/node"
	"github.com/Shreyas9468/cube/scheduler"
	"github.com/Shreyas9468/cube/task"
)

func TestManagerSelectWorker(t *testing.T) {
	s := &scheduler.RoundRobin{Name: "round-robin"}
	m := New([]string{"node-1:5555", "node-2:5555"}, s)

	// Set nodes capacity
	for _, n := range m.WorkerNodes {
		n.Disk = 10 * 1024 * 1024 * 1024
		n.Memory = 4 * 1024 * 1024 * 1024
		n.Healthy = true
	}

	task1 := task.Task{
		ID:     uuid.New(),
		Name:   "test-task",
		Memory: 1024 * 1024 * 1024,
		Disk:   2 * 1024 * 1024 * 1024,
	}

	node1, err := m.SelectWorker(task1)
	if err != nil {
		t.Fatalf("unexpected error selecting worker: %v", err)
	}
	if node1.Name != "node-2:5555" {
		t.Errorf("expected node-2:5555, got %s", node1.Name)
	}

	node2, err := m.SelectWorker(task1)
	if err != nil {
		t.Fatalf("unexpected error selecting worker: %v", err)
	}
	if node2.Name != "node-1:5555" {
		t.Errorf("expected node-1:5555, got %s", node2.Name)
	}
}

func TestManagerReconcileTaskFailure(t *testing.T) {
	s := &scheduler.RoundRobin{Name: "round-robin"}
	m := New([]string{"worker-1:5555"}, s)

	taskID := uuid.New()
	failedTask := task.Task{
		ID:            taskID,
		Name:          "crash-task",
		State:         task.Failed,
		RestartPolicy: task.RestartPolicyOnFailure,
		RestartCount:  0,
	}

	m.TaskDb[taskID] = []*task.Task{&failedTask}
	m.TaskWorkerMap[taskID] = "worker-1:5555"

	// Trigger reconciliation
	m.reconcile()

	// Should have enqueued a retry task event
	if m.Pending.Len() != 1 {
		t.Fatalf("expected 1 task in pending queue after reconciliation, got %d", m.Pending.Len())
	}
	item := m.Pending.Dequeue()
	te := item.(task.TaskEvent)
	if te.Task.ID != taskID {
		t.Errorf("expected retried task ID %s, got %s", taskID, te.Task.ID)
	}
	if te.Task.RestartCount != 1 {
		t.Errorf("expected restart count 1, got %d", te.Task.RestartCount)
	}
}

func TestManagerApiEndpoints(t *testing.T) {
	s := &scheduler.RoundRobin{Name: "round-robin"}
	m := New([]string{"worker-test:5555"}, s)

	task1 := task.Task{
		ID:        uuid.New(),
		Name:      "api-task",
		State:     task.Running,
		StartTime: time.Now().UTC(),
	}
	m.TaskDb[task1.ID] = []*task.Task{&task1}
	m.TaskWorkerMap[task1.ID] = "worker-test:5555"

	api := &Api{Manager: m}
	handler := api.BuildRouter()

	// 1. GET /health
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on /health, got %d", rec.Code)
	}

	// 2. GET /tasks
	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on /tasks, got %d", rec.Code)
	}

	var tasks []*task.Task
	if err := json.NewDecoder(rec.Body).Decode(&tasks); err != nil {
		t.Fatalf("failed decoding /tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Name != "api-task" {
		t.Errorf("unexpected tasks response: %+v", tasks)
	}

	// 3. GET /tasks/{taskID}
	req = httptest.NewRequest(http.MethodGet, "/tasks/"+task1.ID.String(), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on /tasks/{id}, got %d", rec.Code)
	}

	// 4. GET /nodes
	req = httptest.NewRequest(http.MethodGet, "/nodes", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on /nodes, got %d", rec.Code)
	}
	var nodes []*node.Node
	if err := json.NewDecoder(rec.Body).Decode(&nodes); err != nil {
		t.Fatalf("failed decoding /nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Name != "worker-test:5555" {
		t.Errorf("unexpected nodes response: %+v", nodes)
	}
}
