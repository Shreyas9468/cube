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

	"github.com/Shreyas9468/cube/task"
	"github.com/Shreyas9468/cube/worker"
)

// Manager orchestrates tasks across a set of worker nodes.
type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID][]*task.Task
	EventDb       map[uuid.UUID][]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
}

// SelectWorker uses a round-robin strategy to pick the next worker.
func (m *Manager) SelectWorker() string {
	var newWorker int
	if m.LastWorker+1 < len(m.Workers) {
		newWorker = m.LastWorker + 1
		m.LastWorker++
	} else {
		newWorker = 0
		m.LastWorker = 0
	}
	return m.Workers[newWorker]
}

// UpdateTasks polls every worker and refreshes task state in the local DB.
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
			log.Printf("Error sending request: %v\n", err)
			continue
		}

		var tasks []*task.Task
		if err = json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
			log.Printf("Error unmarshalling tasks: %v\n", err)
			continue
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t.ID)
			_, ok := m.TaskWorkerMap[t.ID]
			if !ok {
				log.Printf("Task with ID %s not found\n", t.ID)
				continue
			}

			if m.TaskDb[t.ID] == nil || len(m.TaskDb[t.ID]) == 0 {
				log.Printf("Task %s not found in DB\n", t.ID)
				continue
			}

			taskList := m.TaskDb[t.ID]
			if taskList[len(taskList)-1].State != t.State {
				taskList[len(taskList)-1].State = t.State
			}
		}
	}
}

// SendWork picks a worker and sends the next pending TaskEvent to it.
func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		w := m.SelectWorker()
		e := m.Pending.Dequeue()
		te := e.(task.TaskEvent)
		t := te.Task
		log.Printf("Pulled %v off pending queue\n", t)

		m.EventDb[te.ID] = append(m.EventDb[te.ID], &te)
		m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], t.ID)
		m.TaskWorkerMap[t.ID] = w

		t.State = task.Scheduled
		m.TaskDb[t.ID] = append(m.TaskDb[t.ID], &t)

		data, err := json.Marshal(te)
		if err != nil {
			log.Printf("Unable to marshal task object: %v\n", te)
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
			return
		}

		log.Printf("Response from worker: %v\n", result)
	} else {
		log.Println("No work in the queue")
	}
}

// AddTask enqueues a new TaskEvent for the manager to send to workers.
func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

// GetTasks collects the latest version of every task across all workers.
func (m *Manager) GetTasks() []*task.Task {
	tasks := []*task.Task{}
	for _, workerTasks := range m.TaskDb {
		for _, t := range workerTasks {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

// SendWorkLoop continuously processes pending work every 10 seconds.
func (m *Manager) SendWorkLoop() {
	for {
		log.Println("Sending work to workers")
		m.SendWork()
		time.Sleep(10 * time.Second)
	}
}

// ProcessTasks runs both the send-work loop and the update-tasks loop.
func (m *Manager) ProcessTasks() {
	go m.SendWorkLoop()
	go m.UpdateTasks()
}

// New creates a Manager with the given workers.
func New(workers []string) *Manager {
	taskDb := make(map[uuid.UUID][]*task.Task)
	eventDb := make(map[uuid.UUID][]*task.TaskEvent)
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	return &Manager{
		Pending:       *queue.New(),
		TaskDb:        taskDb,
		EventDb:       eventDb,
		Workers:       workers,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
	}
}

// WorkerNode wraps a worker client to talk to a remote worker node.
type WorkerNode struct {
	Name    string
	Api     *worker.Api
	Client  *http.Client
	Address string
	Port    int
}
