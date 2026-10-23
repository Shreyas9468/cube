package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/task"
)

// ErrResponse is the JSON body returned on error.
type ErrResponse struct {
	HttpStatusCode int    `json:"httpStatusCode"`
	Message        string `json:"message"`
}

// StartTaskHandler accepts a TaskEvent and enqueues the contained Task.
func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}
	if err := d.Decode(&te); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Print(msg)
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(ErrResponse{HttpStatusCode: 400, Message: msg})
		return
	}

	a.Worker.AddTask(te.Task)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(te.Task)
}

// GetTasksHandler returns all tasks in the worker's DB.
func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

// StopTaskHandler enqueues a Completed state change for the specified task.
func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("No taskID passed in request\n")
		w.WriteHeader(400)
		return
	}

	tID, err := uuid.Parse(taskID)
	if err != nil {
		log.Printf("Invalid taskID %s: %v\n", taskID, err)
		w.WriteHeader(400)
		return
	}

	taskToStop, ok := a.Worker.Db[tID]
	if !ok {
		log.Printf("Task %s not found\n", taskID)
		w.WriteHeader(404)
		return
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	a.Worker.AddTask(taskCopy)
	log.Printf("Added task %v to stop queue\n", taskToStop.ID)
	w.WriteHeader(204)
}

// GetStatsHandler returns current worker statistics.
func (a *Api) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Worker.Stats)
}
