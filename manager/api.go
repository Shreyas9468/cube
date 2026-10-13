package manager

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// Api exposes the Manager over HTTP.
type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux
}

// initRouter configures all manager routes.
func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Get("/tasks", a.GetTasksHandler)
	a.Router.Post("/tasks", a.StartTaskHandler)
	a.Router.Delete("/tasks/{taskID}", a.StopTaskHandler)
}

// Start initialises the router and serves HTTP.
func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}
