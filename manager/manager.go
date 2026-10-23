package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/node"
	"github.com/Shreyas9468/cube/scheduler"
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
}

// SelectWorker picks the best worker for the given task using the configured Scheduler.
func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		return nil, fmt.Errorf("no available candidates match resource request for task %v", t.ID)
	}
	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)
	return selectedNode, nil
}

// UpdateTasks polls every worker every 15 s and syncs task state into TaskDb.
func (m *Manager) UpdateTasks() {
	for {
		log.Println("Checking for task updates from workers")
		m.updateTasks()
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("Checking worker %v for task updates\n", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", worker, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("Unexpected status from %v: %d\n", worker, resp.StatusCode)
			continue
		}

		var tasks []*task.Task
		if err = json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
			log.Printf("Error unmarshalling tasks from %s: %v\n", worker, err)
			continue
		}

		for _, t := range tasks {
			if _, ok := m.TaskWorkerMap[t.ID]; !ok {
				log.Printf("Task %s not found in TaskWorkerMap\n", t.ID)
				continue
			}
			taskList := m.TaskDb[t.ID]
			if len(taskList) == 0 {
				continue
			}
			if taskList[len(taskList)-1].State != t.State {
				taskList[len(taskList)-1].State = t.State
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
		log.Printf("Pulled %v off pending queue\n", t)

		selectedNode, err := m.SelectWorker(t)
		if err != nil {
			log.Printf("Error selecting worker for task %s: %v\n", t.ID, err)
			return
		}
		w := selectedNode.Name

		m.EventDb[te.ID] = append(m.EventDb[te.ID], &te)
		m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], t.ID)
		m.TaskWorkerMap[t.ID] = w

		t.State = task.Scheduled
		m.TaskDb[t.ID] = append(m.TaskDb[t.ID], &t)

		data, err := json.Marshal(te)
		if err != nil {
			log.Printf("Unable to marshal TaskEvent: %v\n", err)
			return
		}

		url := fmt.Sprintf("http://%s/tasks", w)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(te)
			return
		}

		var result task.DockerResult
		if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
			log.Printf("Error decoding response: %v\n", err)
		}
		log.Printf("Response from worker: %v\n", result)
	} else {
		log.Println("No work in the queue")
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

// SendWorkLoop processes pending work every 10 s.
func (m *Manager) SendWorkLoop() {
	for {
		log.Println("Sending work to workers")
		m.SendWork()
		time.Sleep(10 * time.Second)
	}
}

// ProcessTasks starts both the send-work and update-tasks loops.
func (m *Manager) ProcessTasks() {
	go m.SendWorkLoop()
	go m.UpdateTasks()
}

// New creates a Manager wired with the given workers and scheduler.
func New(workers []string, s scheduler.Scheduler) *Manager {
	// Build node objects from worker address strings.
	workerNodes := make([]*node.Node, 0, len(workers))
	for _, w := range workers {
		workerNodes = append(workerNodes, &node.Node{
			Name: w,
			Ip:   w,
			Api:  w,
			Role: "worker",
		})
	}

	return &Manager{
		Pending:       *queue.New(),
		TaskDb:        make(map[uuid.UUID][]*task.Task),
		EventDb:       make(map[uuid.UUID][]*task.TaskEvent),
		Workers:       workers,
		WorkerTaskMap: make(map[string][]uuid.UUID),
		TaskWorkerMap: make(map[uuid.UUID]string),
		WorkerNodes:   workerNodes,
		Scheduler:     s,
	}
}
