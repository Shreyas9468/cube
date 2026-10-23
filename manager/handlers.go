package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/task"
)

// ErrResponse is the JSON body returned on error.
type ErrResponse struct {
	HttpStatusCode int    `json:"httpStatusCode"`
	Message        string `json:"message"`
}

// StartTaskHandler accepts a TaskEvent and enqueues it for dispatch to a worker.
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

	a.Manager.AddTask(te)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(te.Task)
}

// GetTasksHandler returns the latest state of all managed tasks.
func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

// StopTaskHandler sends a stop event for the specified task to the appropriate worker.
func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Println("No taskID in request")
		w.WriteHeader(400)
		return
	}

	tID, err := uuid.Parse(taskID)
	if err != nil {
		log.Printf("Invalid taskID %s: %v\n", taskID, err)
		w.WriteHeader(400)
		return
	}

	taskList, ok := a.Manager.TaskDb[tID]
	if !ok || len(taskList) == 0 {
		log.Printf("Task %s not found\n", taskID)
		w.WriteHeader(404)
		return
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
		Task:      *taskList[len(taskList)-1],
	}
	te.Task.State = task.Completed
	a.Manager.AddTask(te)
	log.Printf("Added task %v to stop queue\n", te.Task.ID)
	w.WriteHeader(204)
}

// GetNodesHandler returns all registered worker nodes and their status.
func (a *Api) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.GetNodes())
}
