package main

import (
	"fmt"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/task"
	"github.com/Shreyas9468/cube/worker"
)

func main() {
	host := "0.0.0.0"
	port := 5555

	fmt.Println("Starting Cube worker")

	w := worker.Worker{
		Name:  "worker-1",
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	api := worker.Api{Address: host, Port: port, Worker: &w}

	go w.RunTaskLoop()
	go w.CollectStats()
	api.Start()

	_ = time.Now() // imported for future use
}
