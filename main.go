package main

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/manager"
	"github.com/Shreyas9468/cube/scheduler"
	"github.com/Shreyas9468/cube/store"
	"github.com/Shreyas9468/cube/task"
	"github.com/Shreyas9468/cube/worker"
)

func main() {
	whost := "0.0.0.0"
	wport := 5555

	mhost := "0.0.0.0"
	mport := 5556

	// Initialize persistent storage for worker and manager
	wStore, err := store.NewBoltStore("data/worker.db", 0600, []string{"tasks"})
	if err != nil {
		fmt.Printf("Notice: starting worker in in-memory storage mode (%v)\n", err)
	} else {
		defer wStore.Close()
	}

	mStore, err := store.NewBoltStore("data/manager.db", 0600, []string{"tasks", "events"})
	if err != nil {
		fmt.Printf("Notice: starting manager in in-memory storage mode (%v)\n", err)
	} else {
		defer mStore.Close()
	}

	fmt.Println("Starting Cube worker daemon")
	w := worker.New("worker-1", wStore)
	wApi := worker.Api{Address: whost, Port: wport, Worker: w}

	fmt.Println("Starting Cube cluster manager with EPVM scheduler")
	workers := []string{fmt.Sprintf("%s:%d", whost, wport)}
	s := &scheduler.Epvm{Name: "epvm"}
	m := manager.NewWithStore(workers, s, mStore)
	mApi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go w.RunTaskLoop()
	go w.CollectStats()
	go m.ProcessTasks()
	go wApi.Start()

	time.Sleep(time.Millisecond * 500)

	// Submit an initial sample task via the manager control plane
	testTask := task.Task{
		ID:            uuid.New(),
		Name:          "cube-task-sample",
		State:         task.Scheduled,
		Image:         "strm/helloworld-http",
		RestartPolicy: task.RestartPolicyOnFailure,
	}
	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now().UTC(),
		Task:      testTask,
	}
	m.AddTask(te)
	fmt.Printf("Submitted task %s to manager queue\n", testTask.ID)

	fmt.Printf("Manager API listening on %s:%d\n", mhost, mport)
	fmt.Printf("Worker  API listening on %s:%d\n", whost, wport)
	fmt.Fprintln(os.Stdout, "Press Ctrl-C to stop.")

	mApi.Start()
}
