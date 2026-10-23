package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/task"
)

// Worker is responsible for executing and managing tasks on a single node.
type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	Stats     *Stats
	TaskCount int
}

// CollectStats runs in a loop, gathering host metrics every 15 seconds.
func (w *Worker) CollectStats() {
	for {
		log.Println("Collecting stats")
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
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("No tasks to process currently.\n")
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
		log.Println("No tasks in the queue")
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
			result.Error = errors.New("we should not get here")
		}
	} else {
		err := fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		result.Error = err
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
		log.Printf("Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t
	w.TaskCount++
	log.Printf("Started container %s for task %s\n", t.ContainerID, t.ID)
	return result
}

// StopTask stops and removes the container for a task and updates its state to Completed.
func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping task %v: %v\n", t.ID, result.Error)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = &t
	w.TaskCount--
	log.Printf("Stopped container %s for task %s\n", t.ContainerID, t.ID)
	return result
}

// GetTasks returns a slice of all tasks in the worker's in-memory DB.
func (w *Worker) GetTasks() []*task.Task {
	tasks := make([]*task.Task, 0, len(w.Db))
	for _, t := range w.Db {
		tasks = append(tasks, t)
	}
	return tasks
}
