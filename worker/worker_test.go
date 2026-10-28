package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/task"
)

func TestWorkerTaskManagement(t *testing.T) {
	w := New("worker-test", nil)

	task1 := task.Task{
		ID:    uuid.New(),
		Name:  "task-1",
		State: task.Scheduled,
		Image: "busybox",
	}

	w.AddTask(task1)
	if w.Queue.Len() != 1 {
		t.Fatalf("expected queue length 1, got %d", w.Queue.Len())
	}

	w.Db[task1.ID] = &task1
	tasks := w.GetTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task in DB, got %d", len(tasks))
	}
	if tasks[0].Name != "task-1" {
		t.Errorf("expected task name 'task-1', got '%s'", tasks[0].Name)
	}
}

func TestWorkerApiEndpoints(t *testing.T) {
	w := New("worker-api-test", nil)
	task1 := task.Task{
		ID:    uuid.New(),
		Name:  "task-alpha",
		State: task.Running,
		Image: "alpine",
	}
	w.Db[task1.ID] = &task1
	w.TaskCount = 1

	api := &Api{Worker: w}
	handler := api.BuildRouter()

	// 1. Test /health
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200 on /health, got %d", rec.Code)
	}

	// 2. Test /tasks
	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200 on /tasks, got %d", rec.Code)
	}

	var tasks []*task.Task
	if err := json.NewDecoder(rec.Body).Decode(&tasks); err != nil {
		t.Fatalf("failed decoding /tasks response: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Name != "task-alpha" {
		t.Errorf("unexpected tasks response: %+v", tasks)
	}

	// 3. Test /tasks/{taskID}
	req = httptest.NewRequest(http.MethodGet, "/tasks/"+task1.ID.String(), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200 on /tasks/{id}, got %d", rec.Code)
	}

	// 4. Test non-existent task
	req = httptest.NewRequest(http.MethodGet, "/tasks/"+uuid.New().String(), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404 for unknown task, got %d", rec.Code)
	}
}
