# Cube: Distributed Container Orchestrator in Go

[![Go Version](https://img.shields.io/badge/Go-1.25%2B-blue.svg)](https://golang.org)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()
[![Tests](https://img.shields.io/badge/Tests-100%25%20Race--Clean-success.svg)]()
[![Architecture](https://img.shields.io/badge/Design-Borg%20%2F%20K8s%20Pattern-orange.svg)]()
[![License](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)

**Cube** is a production-grade distributed container orchestrator engineered from scratch in Go, modeled after the foundational designs of **Google Borg**, **Kubernetes**, and **HashiCorp Nomad**. 

Cube coordinates containerized workloads across a distributed cluster of compute nodes. It solves the core challenges of modern distributed infrastructure: **automated workload scheduling, resource capacity accounting, level-triggered health reconciliation, zero-downtime crash recovery, and runtime container lifecycle management** via the Docker Engine API.

---

## Table of Contents

- [Why Cube? Problem Statement & Mental Model](#why-cube-problem-statement--mental-model)
- [High-Level Architecture](#high-level-architecture)
- [Core Subsystems](#core-subsystems)
  - [1. Task Lifecycle & Finite-State Machine (FSM)](#1-task-lifecycle--finite-state-machine-fsm)
  - [2. Docker Runtime & Linux Kernel Cgroups](#2-docker-runtime--linux-kernel-cgroups)
  - [3. Pluggable Schedulers: Round-Robin & Cost-Aware EPVM](#3-pluggable-schedulers-round-robin--cost-aware-epvm)
  - [4. Worker Node Daemon & Telemetry Engine](#4-worker-node-daemon--telemetry-engine)
  - [5. Cluster Manager & Level-Triggered Reconciler](#5-cluster-manager--level-triggered-reconciler)
  - [6. Persistent Embedded Storage Engine (BoltDB)](#6-persistent-embedded-storage-engine-boltdb)
- [Failure Modes & Automated Self-Healing](#failure-modes--automated-self-healing)
- [REST API Reference](#rest-api-reference)
- [Getting Started & Local Development](#getting-started--local-development)
- [`cubectl` Operator CLI Guide](#cubectl-operator-cli-guide)
- [Distributed Multi-Node Deployment](#distributed-multi-node-deployment)
- [Resume & Technical Interview Highlights](#resume--technical-interview-highlights)

---

## Why Cube? Problem Statement & Mental Model

Running containers across multiple compute instances introduces hard distributed systems problems:
1. **Workload Placement:** How do you select the best host for a container without over-subscribing CPU or memory?
2. **Host & Container Failures:** What happens when a compute host crashes, a network cable is pulled, or a process is OOM-killed?
3. **State Consistency:** How does the control plane remember what was running after its own process crashes?
4. **Decoupled Architecture:** How do you isolate operational client commands (`cubectl`) from long-running host container lifecycles?

Cube implements the battle-tested **Control Plane vs. Data Plane** separation:
- **Control Plane (`Manager`):** The brain. Evaluates cluster capacity, scores nodes, dispatches work, and continuously reconciles desired vs. actual cluster state.
- **Data Plane (`Worker Daemons`):** The muscle. Accepts task assignments, isolates processes using Docker and Linux kernel cgroups, streams I/O logs, and reports telemetry.

---

## High-Level Architecture

```
                             +-----------------------------------+
                             |           cubectl (CLI)           |
                             |   run | inspect | logs | stop     |
                             +-----------------+-----------------+
                                               | HTTP REST / JSON
                                               v
+---------------------------------------------------------------------------------------------------+
|                                  CUBE MANAGER (Control Plane)                                     |
|                                                                                                   |
|  +--------------------+      +--------------------+      +--------------------+                   |
|  |   HTTP API / Chi   | <--> |   Task Dispatch    | <--> | Pluggable Scheduler|                   |
|  | (Port 5556, slog)  |      |   (SendWorkLoop)   |      |  (RoundRobin/EPVM) |                   |
|  +--------------------+      +--------------------+      +--------------------+                   |
|            |                           |                           |                              |
|            v                           v                           v                              |
|  +--------------------+      +--------------------+      +--------------------+                   |
|  | TaskDb & EventDb   |      | Reconciler Engine  |      |  Node Health Probe |                   |
|  | (In-Memory Lookup) |      | (Orphan Detection) |      |  (15s Heartbeat)   |                   |
|  +--------------------+      +--------------------+      +--------------------+                   |
|            |                                                                                      |
|            v                                                                                      |
|  +-------------------------------------------------------------+                                  |
|  |          Transactional BoltDB Storage (manager.db)          |                                  |
|  |            Buckets: "tasks" (WAL) | "events"                |                                  |
|  +-------------------------------------------------------------+                                  |
+---------------------------------------------------------------------------------------------------+
                   |                                             |
                   | HTTP POST /tasks (Dispatch)                 | HTTP POST /tasks
                   | HTTP DELETE /tasks/{id} (Stop)              | HTTP DELETE /tasks/{id}
                   | HTTP GET /stats (Telemetry)                 | HTTP GET /stats
                   v                                             v
+-------------------------------------+       +-------------------------------------+
|      WORKER NODE 1 (Daemon)         |       |      WORKER NODE 2 (Daemon)         |
|                                     |       |                                     |
|  +-------------------------------+  |       |  +-------------------------------+  |
|  |  Worker HTTP API (Port 5555)  |  |       |  |  Worker HTTP API (Port 5557)  |  |
|  +---------------+---------------+  |       |  +---------------+---------------+  |
|                  |                  |       |                  |                  |
|                  v                  |       |                  v                  |
|  +-------------------------------+  |       |  +-------------------------------+  |
|  |       FIFO Task Queue         |  |       |  |       FIFO Task Queue         |  |
|  +---------------+---------------+  |       |  +---------------+---------------+  |
|                  |                  |       |                  |                  |
|                  v                  |       |                  v                  |
|  +-------------------------------+  |       |  +-------------------------------+  |
|  |  Docker SDK Container Runtime |  |       |  |  Docker SDK Container Runtime |  |
|  |  (ImagePull, Create, Start)   |  |       |  |  (ImagePull, Create, Start)   |  |
|  +---------------+---------------+  |       |  +---------------+---------------+  |
|                  |                  |       |                  |                  |
|                  v                  |       |                  v                  |
|  +-------------------------------+  |       |  +-------------------------------+  |
|  | Local BoltDB Store (worker.db)|  |       |  | Local BoltDB Store (worker.db)|  |
|  | Telemetry Collector (/proc)   |  |       |  | Telemetry Collector (/proc)   |  |
|  +-------------------------------+  |       |  +-------------------------------+  |
+-------------------------------------+       +-------------------------------------+
```

---

## Core Subsystems

### 1. Task Lifecycle & Finite-State Machine (FSM)

Every containerized workload in Cube is modeled as an immutable `Task` struct governed by a strict, validated Finite-State Machine (`task.State`). Non-deterministic state transitions are rejected at runtime.

```
        +---------------+
        |    Pending    | (Task submitted to Manager queue)
        +-------+-------+
                |
                v
        +---------------+
        |   Scheduled   | (Node assigned by Scheduler, dispatched to Worker)
        +-------+-------+
                |
                v
        +---------------+
        |    Running    | (Container started by Docker Engine, health verified)
        +---+-------+---+
            |       |
            |       +------------> +---------+
            |                      |  Failed | (Non-zero exit / OOM / crash)
            v                      +----+----+
    +---------------+                   |
    |   Completed   |                   +-----> Rescheduled if Restart Policy applies
    +---------------+ (Clean exit 0)
```

#### Valid Transition Matrix

| Current State | Allowed Next States | Illegal Transitions | Rationale |
|---|---|---|---|
| `Pending` | `Scheduled` | `Running`, `Completed`, `Failed` | A task cannot execute without an assigned compute node. |
| `Scheduled` | `Running`, `Failed` | `Pending`, `Completed` | Container creation begins immediately upon worker acknowledgment. |
| `Running` | `Completed`, `Failed` | `Pending`, `Scheduled` | Running containers exit cleanly (`0`) or terminate with failure (`>0`). |
| `Completed` | *(Terminal)* | All | Clean completion is an immutable terminal state. |
| `Failed` | `Scheduled` (via Retry) | `Pending`, `Running`, `Completed` | Failed tasks can only be rescheduled as fresh events under restart policies. |

---

### 2. Docker Runtime & Linux Kernel Cgroups

Cube interacts programmatically with the local Docker daemon via the Docker Go SDK (`moby/moby/client`):
- **CPU Quota Enforcement:** Converts task CPU requests into Linux Completely Fair Scheduler (CFS) bandwidth quotas using `container.Resources.NanoCPUs` ($NanoCPUs = CPU \times 10^9$).
- **Memory Hard Limits:** Configures hard kernel memory limits via `container.Resources.Memory`. When a container exceeds this boundary, the Linux kernel Memory Controller invokes the OOM killer (`SIGKILL`, exit code `137`), which Cube detects and handles.
- **Log Demultiplexing:** Implements standard stream demultiplexing via `stdcopy.StdCopy` to ingest raw Docker header frames and expose clean stdout/stderr logs over the HTTP API.

---

### 3. Pluggable Schedulers: Round-Robin & Cost-Aware EPVM

Cube decouples task scheduling behind a clean Go interface:

```go
type Scheduler interface {
    SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
    Score(t task.Task, nodes []*node.Node) map[string]float64
    Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}
```

#### Schedulers Included:
1. **Round-Robin (`RoundRobin`):** Sequentially rotates task placement across available healthy worker nodes. Best for homogenous clusters with uniform task requirements.
2. **EPVM (Enhanced Parallel Virtual Machine):** Advanced multi-dimensional cost-based scheduler modeled after Google Borg:
   - **Predicate Filter:** Eliminates nodes lacking sufficient free disk space ($\text{FreeDisk} < \text{Task.Disk}$).
   - **Cost Function Scoring:** Evaluates surviving candidate nodes using a weighted composite cost formula:
     $$\text{Cost}(n) = w_1 \cdot U_{mem}(n) + w_2 \cdot U_{cpu}(n) + w_3 \cdot U_{density}(n)$$
     - $U_{mem}(n)$: Projected memory utilization inclusive of the incoming task.
     - $U_{cpu}(n)$: Current node 1-minute load average.
     - $U_{density}(n)$: Total active task count on the node.
   - **Optimal Selection:** The node with the minimum projected cost ($\arg\min \text{Cost}(n)$) is picked, preventing cluster hotspotting and resource starvation.

---

### 4. Worker Node Daemon & Telemetry Engine

Each compute host runs the lightweight `worker` daemon:
- **Asynchronous Ingestion:** HTTP API endpoints enqueue tasks into an internal FIFO queue (`queue.Queue`), returning immediate acknowledgments to prevent network timeouts.
- **Host Metrics Telemetry:** Samples host hardware statistics every 15 seconds from `/proc/meminfo`, `/proc/stat`, `/proc/loadavg`, and filesystem syscalls (`syscall.Statfs`), exposing telemetry over `GET /stats`.
- **Local State Resilience:** Maintains an embedded BoltDB instance (`worker.db`) to record container assignments and ensure recovery after daemon restarts.

---

### 5. Cluster Manager & Level-Triggered Reconciler

The central manager coordinates cluster state:
- **Task Dispatch Loop (`SendWorkLoop`):** Periodically dequeues pending workloads, evaluates node candidates using the configured Scheduler, updates node resource accounting, and dispatches container tasks.
- **Task Synchronization Loop (`UpdateTasks`):** Polls all active workers every 15 seconds to sync running container states into the centralized `TaskDb`.
- **Continuous Health Probing (`DoHealthChecks`):** Queries `/stats` across registered workers. Consecutive connection failures automatically mark the node `Healthy = false`.
- **Autonomous Task Reconciliation (`ReconcileTasks`):** Level-triggered control loop that runs every 20 seconds:
  1. Identifies running tasks stranded on unhealthy or partitioned nodes (`node_failure`).
  2. Identifies tasks that have transitioned to `Failed` (`task_failed`).
  3. Evaluates task restart policies (`always`, `on-failure`, `never`) and retry bounds (up to 3 retries).
  4. Automatically reschedules orphaned workloads onto surviving healthy nodes.

---

### 6. Persistent Embedded Storage Engine (BoltDB)

Rather than introducing heavy external dependencies like etcd or PostgreSQL, Cube integrates embedded, ACID-compliant **BoltDB (`bbolt`)**:
- **B+ Tree Architecture:** Uses Copy-on-Write (CoW) B+ trees with memory-mapped I/O (`mmap`), providing zero-allocation, ultra-fast point reads.
- **Crash Consistency:** Atomically flushes transactions to disk. Process crashes leave the database in an untainted, transactionally consistent state.
- **State Hydration:** Upon manager or worker startup, historical task logs and scheduled events are automatically read from disk into in-memory lookup caches.

---

## Failure Modes & Automated Self-Healing

| Failure Scenario | How Cube Detects It | Automated Mitigation |
|---|---|---|
| **Worker Host Crash / Network Split** | Manager `DoHealthChecks()` loop catches TCP connection refused or timeouts. | Marks node `Unhealthy`. Reconciler detects stranded running tasks and reschedules them to healthy nodes. |
| **Container OOM / Process Crash** | Docker runtime captures non-zero exit code or death signal. | Worker marks task `Failed`. Manager sync loop detects state change. Reconciler triggers restart policy. |
| **Manager Process Crash** | Host restart or process termination. | On reboot, `manager.NewWithStore()` hydrates `TaskDb` and `EventDb` directly from `data/manager.db`. |
| **Worker Process Crash** | Daemon death. | On restart, `worker.New()` recovers active task IDs from `data/worker.db` and resumes monitoring. |
| **Duplicate Stop Signals** | CLI user stops already completed task. | Manager stop handler validates state; worker deletes container idempotently. |

---

## REST API Reference

### Manager API (Default Port: `5556`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Control plane status, registered worker count, and active task count |
| `GET` | `/tasks` | List all tasks across the cluster with status and container IDs |
| `GET` | `/tasks/{taskID}` | Inspect detailed metadata, resource requests, and timestamps for a task |
| `GET` | `/tasks/{taskID}/logs` | Stream live stdout/stderr container logs from the assigned worker node |
| `POST` | `/tasks` | Submit a new task event for scheduling and dispatch |
| `DELETE` | `/tasks/{taskID}` | Stop a running task and reclaim allocated node memory/disk resources |
| `GET` | `/nodes` | List registered worker nodes, health status, and live hardware capacity |

### Worker API (Default Port: `5555`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Worker daemon health, worker name, and active container count |
| `GET` | `/tasks` | List all container tasks assigned to this local worker |
| `GET` | `/tasks/{taskID}` | Inspect local task details and container ID |
| `GET` | `/tasks/{taskID}/logs` | Fetch container stdout/stderr log output |
| `POST` | `/tasks` | Accept and enqueue a task event into the local execution queue |
| `DELETE` | `/tasks/{taskID}` | Stop and remove the local Docker container |
| `GET` | `/stats` | Live hardware metrics: Memory (Total/Free), Disk (Total/Free), Load Average |

---

## Getting Started & Local Development

### Prerequisites
- **Go 1.25+**
- **Docker Engine** running locally (or remote Docker socket via `DOCKER_HOST`)
- **Make** (optional, for build shortcuts)

### Build & Run

```bash
# 1. Clone repository
git clone https://github.com/Shreyas9468/cube.git
cd cube

# 2. Verify dependencies
go mod tidy

# 3. Run all unit & integration tests with race detector
go test -race -v ./...

# 4. Build both orchestrator and CLI binaries
make build
make build-ctl
```

### Launch the Orchestrator

```bash
# Start cluster orchestrator (Manager + Worker-1)
go run main.go
```

The orchestrator will:
1. Initialize BoltDB stores in `data/manager.db` and `data/worker.db`.
2. Start the Worker daemon on `0.0.0.0:5555`.
3. Start the Manager control plane on `0.0.0.0:5556` with the EPVM cost-based scheduler.
4. Launch background health-check probes, task dispatch loops, and state reconcilers.

---

## `cubectl` Operator CLI Guide

Build the CLI binary:
```bash
go build -o bin/cubectl ./cmd/cubectl
```

### 1. Inspect Cluster Worker Nodes
```bash
./bin/cubectl nodes
```
```
NODE NAME       ROLE    STATUS   TASKS  MEMORY (TOTAL)  DISK (TOTAL)
127.0.0.1:5555  worker  Healthy  1      16384 MB        256 GB
```

### 2. Submit a Task from JSON Specification
Submit a declarative specification (`examples/hello.json`):
```json
{
  "name": "hello-service",
  "image": "strm/helloworld-http",
  "cpu": 0.25,
  "memory": 268435456,
  "disk": 1073741824,
  "restartPolicy": "on-failure"
}
```
```bash
./bin/cubectl run -f examples/hello.json
```
```
Task 4a2b9f10-1823-4c91-b3f2-c90a12b489a1 (hello-service) successfully submitted to cluster.
```

Or submit directly via command-line flags:
```bash
./bin/cubectl run --name web --image nginx:alpine --cpu 0.5 --memory 536870912 --restart always
```

### 3. List Cluster Tasks & Status
```bash
./bin/cubectl status
```
```
TASK ID   NAME           IMAGE                 STATE    RESTARTS  CONTAINER     STARTED
4a2b9f10  hello-service  strm/helloworld-http  Running  0         a1b2c3d4e5f6  14:02:18
```

### 4. Inspect Detailed Task Metadata
```bash
./bin/cubectl inspect 4a2b9f10-1823-4c91-b3f2-c90a12b489a1
```
```json
{
  "ID": "4a2b9f10-1823-4c91-b3f2-c90a12b489a1",
  "ContainerID": "a1b2c3d4e5f67890abcdef1234567890",
  "Name": "hello-service",
  "State": 2,
  "Image": "strm/helloworld-http",
  "CPU": 0.25,
  "Memory": 268435456,
  "Disk": 1073741824,
  "RestartPolicy": "on-failure",
  "RestartCount": 0,
  "StartTime": "2026-10-28T14:02:18Z"
}
```

### 5. Stream Container Logs
```bash
./bin/cubectl logs 4a2b9f10-1823-4c91-b3f2-c90a12b489a1
```
```
Listening on port 80...
Ready for incoming HTTP requests.
```

### 6. Stop and Remove a Task
```bash
./bin/cubectl stop 4a2b9f10-1823-4c91-b3f2-c90a12b489a1
```
```
Stop signal successfully sent for task 4a2b9f10-1823-4c91-b3f2-c90a12b489a1.
```

---

## Distributed Multi-Node Deployment

Cube can be deployed across separate physical or virtual machines:

```
[Machine A - 192.168.1.10]                     [Machine B - 192.168.1.11]
Cube Manager (Port 5556)   <--- HTTP REST ---> Cube Worker 2 (Port 5555)
Cube Worker 1 (Port 5555)
```

1. **Start Worker on Machine B (`192.168.1.11`):**
   ```go
   w := worker.New("worker-2", wStore)
   api := worker.Api{Address: "0.0.0.0", Port: 5555, Worker: w}
   api.BuildRouter()
   ```
2. **Start Manager on Machine A (`192.168.1.10`):**
   ```go
   workers := []string{"127.0.0.1:5555", "192.168.1.11:5555"}
   m := manager.NewWithStore(workers, &scheduler.Epvm{Name: "epvm"}, mStore)
   ```
3. **Point `cubectl` from any workstation:**
   ```bash
   cubectl --manager 192.168.1.10:5556 status
   ```

---

## Live Demo Walkthrough

End-to-end demo with real output. Requires Docker Desktop running.

### Step 1 — Start the Cluster

```bash
go run main.go
```

**Output:**
```json
{"time":"2026-10-31T00:52:17Z","level":"INFO","msg":"Cube orchestrator starting","version":"dev","commit":"unknown","built":"unknown"}
{"time":"2026-10-31T00:52:17Z","level":"INFO","msg":"Initialising worker daemon","address":"0.0.0.0:5555"}
{"time":"2026-10-31T00:52:17Z","level":"INFO","msg":"Recovered tasks from store","worker":"worker-1","count":0}
{"time":"2026-10-31T00:52:17Z","level":"INFO","msg":"Initialising manager with EPVM scheduler","address":"0.0.0.0:5556"}
{"time":"2026-10-31T00:52:18Z","level":"INFO","msg":"Worker API listening","addr":"0.0.0.0:5555"}
{"time":"2026-10-31T00:52:18Z","level":"INFO","msg":"Manager API listening","addr":"0.0.0.0:5556"}
{"time":"2026-10-31T00:52:18Z","level":"INFO","msg":"Sample task submitted","task_id":"38d4fae4-a1e3-45f7-9eee-e57abdf355bb","image":"strm/helloworld-http"}
```

### Step 2 — Inspect Cluster Nodes

```bash
curl -s http://localhost:5556/nodes | jq
```

**Output:**
```json
[
  {
    "Name": "127.0.0.1:5555",
    "Ip": "127.0.0.1:5555",
    "Memory": 1073741824,
    "Disk": 10737418240,
    "Role": "worker",
    "TaskCount": 0,
    "Healthy": true,
    "LastSeen": "2026-10-30T19:22:32Z"
  }
]
```

### Step 3 — Submit a Task (nginx:alpine)

> Memory must be ≥ 6 MB (Docker enforced minimum). Use `67108864` = 64 MB.

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"Task":{"Name":"demo-nginx","Image":"nginx:alpine","Disk":1000,"Memory":67108864}}' \
  http://localhost:5556/tasks
```

**Output:**
```json
{
  "ID": "8ab209cd-647d-4e01-a108-0054e9a0e24b",
  "Name": "demo-nginx",
  "State": 1,
  "Image": "nginx:alpine",
  "Memory": 67108864,
  "Disk": 1000
}
```

Manager log (background):
```json
{"level":"INFO","msg":"Dispatching task from pending queue","task_id":"8ab209cd-...","image":"nginx:alpine"}
{"level":"INFO","msg":"Task assigned to worker","task_id":"8ab209cd-...","worker":"127.0.0.1:5555"}
{"level":"INFO","msg":"Worker acknowledged task","task_id":"8ab209cd-...","worker":"127.0.0.1:5555"}
{"level":"INFO","msg":"Container started","container_id":"ddbeccf5c1a6...","image":"nginx:alpine"}
{"level":"INFO","msg":"Container started","task_id":"8ab209cd-...","container_id":"ddbeccf5c1a6..."}
```

### Step 4 — Verify Container is Running

```bash
docker ps
```

**Output:**
```
CONTAINER ID   IMAGE          COMMAND                  CREATED        STATUS        PORTS                   NAMES
ddbeccf5c1a6   nginx:alpine   "/docker-entrypoint.…"   5 seconds ago  Up 5 seconds  0.0.0.0:32768->80/tcp   demo-nginx
```

### Step 5 — Check Task State via Worker & Manager APIs

```bash
# Worker view (runtime truth)
curl -s http://localhost:5555/tasks | jq

# Manager view (control plane)
curl -s http://localhost:5556/tasks | jq
```

**Worker output (State 2 = Running):**
```json
[{
  "ID": "8ab209cd-647d-4e01-a108-0054e9a0e24b",
  "ContainerID": "ddbeccf5c1a6012e54aad2a59229218da7fdfe574cbed66a04c3a944087693e5",
  "Name": "demo-nginx",
  "State": 2,
  "Image": "nginx:alpine",
  "StartTime": "2026-10-30T19:27:14Z"
}]
```

### Step 6 — Stop the Task

```bash
curl -s -X DELETE http://localhost:5556/tasks/8ab209cd-647d-4e01-a108-0054e9a0e24b
```

Manager log:
```json
{"level":"INFO","msg":"Task stop completed","task_id":"8ab209cd-...","worker":"127.0.0.1:5555"}
{"level":"INFO","msg":"Stopping container","container_id":"ddbeccf5c1a6..."}
{"level":"INFO","msg":"Container removed","container_id":"ddbeccf5c1a6..."}
{"level":"INFO","msg":"Container stopped","task_id":"8ab209cd-...","container_id":"ddbeccf5c1a6..."}
```

**Verify gone:**
```bash
docker ps
```
```
CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES
```

Container destroyed. Task lifecycle complete.

---

### State Reference

| State Value | Name      | Meaning                              |
|-------------|-----------|--------------------------------------|
| `0`         | Pending   | Submitted, awaiting dispatch         |
| `1`         | Scheduled | Assigned to worker, not yet running  |
| `2`         | Running   | Container actively running           |
| `3`         | Completed | Gracefully stopped                   |
| `4`         | Failed    | Container exited with error          |

---

## Resume & Technical Interview Highlights

Key architectural achievements to highlight on your resume and in senior engineering interviews:

- **Distributed Control Plane & Data Plane Separation:** Engineered a decentralized container orchestration architecture modeled after Google Borg and Kubernetes, decoupling centralized scheduling from autonomous worker daemons.
- **Resource-Aware Cost Scheduling (EPVM):** Formulated and implemented a multi-dimensional cost scoring algorithm balancing CPU load average, projected memory utilization, and container density to prevent cluster hotspotting.
- **Finite-State Machine & Level-Triggered Reconciliation:** Designed an explicit task state machine (`Pending` → `Scheduled` → `Running` → `Completed`/`Failed`) backed by an automated 20-second reconciliation loop that detects partitioned or crashed hosts and reschedules workloads under strict restart policies.
- **Transactional Embedded Persistence:** Integrated zero-dependency embedded BoltDB (`bbolt`) using Copy-on-Write B+ trees with memory-mapped I/O (`mmap`) for single-writer multi-reader ACID transactions and instant crash recovery.
- **Linux Kernel & Container Runtime Primitives:** Interfaced directly with the Docker Go SDK (`moby/moby/client`) to enforce CPU bandwidth limits via Linux kernel CFS quotas (`cpu.cfs_quota_us`) and memory boundaries with OOM killer handling.
- **Production Observability & Resilience:** Standardized zero-allocation structured JSON logging using Go `log/slog`, per-request tracing middleware, graceful shutdown via POSIX signal trapping, and 100% race-condition free unit and integration tests.

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.