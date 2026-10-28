package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/Shreyas9468/cube/manager"
	"github.com/Shreyas9468/cube/scheduler"
	"github.com/Shreyas9468/cube/store"
	"github.com/Shreyas9468/cube/task"
	"github.com/Shreyas9468/cube/version"
	"github.com/Shreyas9468/cube/worker"
)

func main() {
	// ─── Structured logger ────────────────────────────────────────────────────
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Cube orchestrator starting",
		"version", version.Version,
		"commit", version.Commit,
		"built", version.BuildTime,
	)

	// ─── Addresses ────────────────────────────────────────────────────────────
	whost := "0.0.0.0"
	wport := 5555
	mhost := "0.0.0.0"
	mport := 5556

	// ─── Persistent stores ────────────────────────────────────────────────────
	if err := os.MkdirAll("data", 0o755); err != nil {
		slog.Error("Cannot create data directory", "error", err)
		os.Exit(1)
	}

	wStore, err := store.NewBoltStore("data/worker.db", 0600, []string{"tasks"})
	if err != nil {
		slog.Warn("Worker store unavailable, using in-memory mode", "error", err)
	} else {
		defer wStore.Close()
	}

	mStore, err := store.NewBoltStore("data/manager.db", 0600, []string{"tasks", "events"})
	if err != nil {
		slog.Warn("Manager store unavailable, using in-memory mode", "error", err)
	} else {
		defer mStore.Close()
	}

	// ─── Worker daemon ────────────────────────────────────────────────────────
	slog.Info("Initialising worker daemon", "address", fmt.Sprintf("%s:%d", whost, wport))
	w := worker.New("worker-1", wStore)
	wApi := worker.Api{Address: whost, Port: wport, Worker: w}

	// ─── Manager control plane ────────────────────────────────────────────────
	slog.Info("Initialising manager with EPVM scheduler", "address", fmt.Sprintf("%s:%d", mhost, mport))
	// shortcut: connect via loopback for local cluster node, upgrade to flag/config for distributed nodes
	workers := []string{fmt.Sprintf("127.0.0.1:%d", wport)}
	s := &scheduler.Epvm{Name: "epvm"}
	m := manager.NewWithStore(workers, s, mStore)
	mApi := manager.Api{Address: mhost, Port: mport, Manager: m}

	// ─── Background loops ─────────────────────────────────────────────────────
	go w.RunTaskLoop()
	go w.CollectStats()
	go m.ProcessTasks()

	// Give workers a moment to start before the manager fires its first loop.
	time.Sleep(500 * time.Millisecond)

	// ─── HTTP servers (graceful shutdown-aware) ───────────────────────────────
	wSrv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", whost, wport),
		Handler:      wApi.BuildRouter(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	mSrv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", mhost, mport),
		Handler:      mApi.BuildRouter(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("Worker API listening", "addr", wSrv.Addr)
		if err := wSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Worker API server error", "error", err)
		}
	}()
	go func() {
		slog.Info("Manager API listening", "addr", mSrv.Addr)
		if err := mSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Manager API server error", "error", err)
		}
	}()

	// ─── Sample task ──────────────────────────────────────────────────────────
	time.Sleep(200 * time.Millisecond)
	sampleTask := task.Task{
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
		Task:      sampleTask,
	}
	m.AddTask(te)
	slog.Info("Sample task submitted", "task_id", sampleTask.ID, "image", sampleTask.Image)

	// ─── Graceful shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	slog.Info("Shutdown signal received", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := wSrv.Shutdown(ctx); err != nil {
		slog.Error("Worker server forced to shut down", "error", err)
	}
	if err := mSrv.Shutdown(ctx); err != nil {
		slog.Error("Manager server forced to shut down", "error", err)
	}

	slog.Info("Cube orchestrator stopped cleanly")
}
