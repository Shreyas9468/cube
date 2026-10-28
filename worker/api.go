package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Api exposes the Worker over HTTP using the chi router.
type Api struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
}

// BuildRouter configures and returns the HTTP router. It is also used by
// main.go so the caller can hand the handler to an http.Server directly.
func (a *Api) BuildRouter() http.Handler {
	r := chi.NewRouter()

	// ── Middleware stack ──────────────────────────────────────────────────────
	r.Use(requestLogger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(middleware.Timeout(30 * time.Second))

	// ── Routes ────────────────────────────────────────────────────────────────
	r.Get("/health", a.HealthHandler)
	r.Get("/tasks", a.GetTasksHandler)
	r.Get("/tasks/{taskID}", a.GetTaskHandler)
	r.Get("/tasks/{taskID}/logs", a.GetLogsHandler)
	r.Post("/tasks", a.StartTaskHandler)
	r.Delete("/tasks/{taskID}", a.StopTaskHandler)
	r.Get("/stats", a.GetStatsHandler)

	a.Router = r
	return r
}

// Start is kept for backwards-compatibility; prefer using BuildRouter with
// an http.Server so you get timeouts and graceful shutdown.
func (a *Api) Start() {
	h := a.BuildRouter()
	addr := fmt.Sprintf("%s:%d", a.Address, a.Port)
	slog.Info("Worker API listening (legacy Start)", "addr", addr)
	if err := http.ListenAndServe(addr, h); err != nil {
		slog.Error("Worker API exited", "error", err)
	}
}

// HealthHandler returns 200 OK with basic service metadata.
func (a *Api) HealthHandler(w http.ResponseWriter, r *http.Request) {
	payload := map[string]interface{}{
		"status":     "ok",
		"worker":     a.Worker.Name,
		"task_count": a.Worker.TaskCount,
		"timestamp":  time.Now().UTC(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payload)
}

// requestLogger is a minimal structured-log middleware for chi.
func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		slog.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"bytes", ww.BytesWritten(),
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", middleware.GetReqID(r.Context()),
		)
	})
}
