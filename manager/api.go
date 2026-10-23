package manager

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Api exposes the Manager over HTTP.
type Api struct {
	Address string
	Port    int
	Manager *Manager
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
	r.Post("/tasks", a.StartTaskHandler)
	r.Delete("/tasks/{taskID}", a.StopTaskHandler)
	r.Get("/nodes", a.GetNodesHandler)

	a.Router = r
	return r
}

// Start is kept for backwards-compatibility; prefer using BuildRouter with
// an http.Server so you get timeouts and graceful shutdown.
func (a *Api) Start() {
	h := a.BuildRouter()
	addr := fmt.Sprintf("%s:%d", a.Address, a.Port)
	slog.Info("Manager API listening (legacy Start)", "addr", addr)
	if err := http.ListenAndServe(addr, h); err != nil {
		slog.Error("Manager API exited", "error", err)
	}
}

// HealthHandler returns 200 OK with basic cluster-level metadata.
func (a *Api) HealthHandler(w http.ResponseWriter, r *http.Request) {
	nodes := a.Manager.GetNodes()
	healthy := 0
	for _, n := range nodes {
		if n.Healthy {
			healthy++
		}
	}
	payload := map[string]interface{}{
		"status":        "ok",
		"total_nodes":   len(nodes),
		"healthy_nodes": healthy,
		"pending_tasks": a.Manager.Pending.Len(),
		"timestamp":     time.Now().UTC(),
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
