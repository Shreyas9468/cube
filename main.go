package main

import (
	"fmt"
	"os"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/manager"
	"github.com/Shreyas9468/cube/task"
	"github.com/Shreyas9468/cube/worker"
)

func main() {
	whost := "0.0.0.0"
	wport := 5555

	mhost := "0.0.0.0"
	mport := 5556

	fmt.Println("Starting Cube worker")
	w := worker.Worker{
		Name:  "worker-1",
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wApi := worker.Api{Address: whost, Port: wport, Worker: &w}

	fmt.Println("Starting Cube manager")
	workers := []string{fmt.Sprintf("%s:%d", whost, wport)}
	m := manager.New(workers)
	mApi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go w.RunTaskLoop()
	go w.CollectStats()
	go m.ProcessTasks()
	go wApi.Start()

	// Give the worker API a moment to bind.
	time.Sleep(time.Millisecond * 500)

	// Submit a test task via the manager's in-memory queue.
	testTask := task.Task{
		ID:    uuid.New(),
		Name:  "test-chapter-5",
		State: task.Scheduled,
		Image: "strm/helloworld-http",
	}
	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      testTask,
	}
	m.AddTask(te)
	fmt.Printf("Submitted task %s to manager\n", testTask.ID)

	fmt.Printf("Manager API listening on %s:%d\n", mhost, mport)
	fmt.Printf("Worker API listening on %s:%d\n", whost, wport)
	fmt.Fprintf(os.Stdout, "Press Ctrl-C to stop.\n")

	mApi.Start()
}
