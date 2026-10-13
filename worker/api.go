package worker

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// Api exposes the Worker over HTTP using the chi router.
type Api struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
}

// initRouter sets up the HTTP routes.
func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Get("/tasks", a.GetTasksHandler)
	a.Router.Post("/tasks", a.StartTaskHandler)
	a.Router.Delete("/tasks/{taskID}", a.StopTaskHandler)
	a.Router.Get("/stats", a.GetStatsHandler)
}

// Start initialises the router and begins serving requests.
func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%v:%v", a.Address, a.Port), a.Router)
}
