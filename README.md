# Cube: Distributed Container Orchestrator in Go

Cube is a production-grade distributed container orchestrator built from scratch in Go, inspired by systems like Google Borg, Kubernetes, and HashiCorp Nomad. It demystifies distributed systems, container scheduling, fault tolerance, and lifecycle management using Go and the Docker Engine API.

---

## Architecture Overview

Cube coordinates containerized workloads across a cluster of compute nodes via a centralized manager control plane and distributed worker daemons.

```
                      +-----------------------------+
                      |       cubectl (CLI)         |
                      |  (run, stop, status, nodes) |
                      +--------------+--------------+
                                     | HTTP REST
                      +--------------v--------------+
                      |      Cube Manager API       |
                      |     (Port 5556 / HTTP)      |
                      +--------------+--------------+
                                     |
               +---------------------+---------------------+
               |                     |                     |
      [In-Memory Queue]      [BoltDB Store]       [Reconciler & Probes]
               |              (manager.db)                 |
               +---------------------+---------------------+
                                     |
                         +-----------v-----------+
                         |  Scheduler (Pluggable)|
                         |  - Round-Robin        |
                         |  - EPVM (Cost-Based)  |
                         +-----------+-----------+
                                     |
                     +---------------+---------------+
                     |                               |
             +-------v-------+               +-------v-------+
             |   Worker 1    |               |   Worker 2    |
             |  (Port 5555)  |               |  (Port 5557)  |
             +-------+-------+               +-------+-------+
                     |                               |
            [BoltDB / Docker]               [BoltDB / Docker]
```

### Core Subsystems

1. **Task & State Machine (`task/`)**
   - **Task Lifecycle:** `Pending` → `Scheduled` → `Running` → `Completed` / `Failed`.
   - **Docker Engine Runtime:** Automated container creation, image pulling, execution, log streaming, and teardown via the Docker Go SDK (`moby/moby/client`).
   - **Restart Policies:** Supports `always`, `on-failure`, and `never` with exponential retry bounds.

2. **Worker Node Daemon (`worker/`)**
   - Receives container task events via HTTP REST API.
   - Executes task lifecycles through an internal asynchronous queue.
   - Collects live host metrics (CPU utilization, memory usage, disk space, load average) from `/proc` with cross-platform fallback.
   - Persists local container assignments in an embedded BoltDB instance (`worker.db`).

3. **Cluster Manager & Control Plane (`manager/`)**
   - Orchestrates task placement across registered worker hosts.
   - Syncs container states from workers every 15 seconds.
   - Periodically probes node health and automatically reconciles orphaned tasks when nodes fail.
   - Persists all cluster tasks and events in an embedded BoltDB database (`manager.db`).

4. **Pluggable Schedulers (`scheduler/`)**
   - **RoundRobin:** Distributes tasks sequentially across candidate nodes.
   - **EPVM (Enhanced Parallel Virtual Machine):** Advanced cost-based scheduler calculating load scores from projected memory usage, CPU load, and task density to place workloads with minimal contention.

5. **Fault Tolerance & State Reconciliation (`manager/`)**
   - Automated health monitoring detects unreachable or partitioned worker nodes.
   - Automatic rescheduling of failed or interrupted workloads onto surviving healthy nodes according to task restart policies.

6. **Embedded Persistent Storage (`store/`)**
   - Pluggable storage abstraction backed by transactional BoltDB (`bbolt`).
   - Guarantees that task history, scheduled events, and cluster topology survive process restarts.

7. **Operator CLI — `cubectl` (`cmd/cubectl/`)**
   - Dedicated command-line tool with subcommands for running, inspecting, stopping, and monitoring tasks and worker nodes.

---

## Phased Build Progression

The repository follows a clean, streamlined phased build structure:

| Phase | Milestone | Key Features |
|---|---|---|
| **Phase 1** | Project Scaffolding | Module configuration, dependencies, and base layout |
| **Phase 2** | Task Model | Task definitions, state machine validation, and Docker container engine |
| **Phase 3** | Worker Daemon | Task queue, state machine execution, telemetry metrics, and Worker API |
| **Phase 4** | Manager Control Plane | Centralized state store, task dispatch loop, and Manager HTTP API |
| **Phase 5** | Node & Schedulers | Node registry, capacity accounting, Round-Robin and EPVM scheduling |
| **Phase 6** | Core Orchestrator | End-to-end integration wiring manager, worker daemon, and automated task execution |
| **Phase 7** | Fault Tolerance | Node health probing, task failure detection, and automatic state reconciliation |
| **Phase 8** | Persistent Storage | Embedded BoltDB storage engine for cluster state persistence |
| **Phase 9** | Operator CLI | `cubectl` tool with `run`, `status`, `stop`, and `nodes` subcommands |
| **Phase 10** | Production Readiness | Unified binary, sample specs, persistent storage integration, and docs |

---

## API Endpoints

### Manager API (Default Port: `5556`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Cluster control plane health and registered node counts |
| `GET` | `/tasks` | List all tasks across worker nodes |
| `GET` | `/tasks/{taskID}` | Inspect detailed metadata and status for a specific task |
| `GET` | `/tasks/{taskID}/logs` | Stream container stdout/stderr logs from worker node |
| `POST` | `/tasks` | Submit a new task event |
| `DELETE` | `/tasks/{taskID}` | Stop a running task and reclaim allocated node resources |
| `GET` | `/nodes` | List registered worker nodes and live health status |

### Worker API (Default Port: `5555`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Worker health status, name, and active container count |
| `GET` | `/tasks` | List tasks assigned to this worker |
| `GET` | `/tasks/{taskID}` | Inspect local task container details |
| `GET` | `/tasks/{taskID}/logs` | Fetch container stdout/stderr output |
| `POST` | `/tasks` | Accept and enqueue a task event |
| `DELETE` | `/tasks/{taskID}` | Stop and remove task container |
| `GET` | `/stats` | Retrieve node resource metrics (CPU, RAM, Disk, Load) |

---

## Getting Started

### Prerequisites
- **Go 1.25+**
- **Docker Engine** running locally (or remote Docker daemon via `DOCKER_HOST`)

### Running the Orchestrator

```bash
# Verify dependencies
go mod tidy

# Run test suite with race detector
go test -race ./...

# Build all binaries
make build
make build-ctl

# Start the cluster orchestrator
go run main.go
```

When started, `main.go`:
1. Opens persistent storage in `data/worker.db` and `data/manager.db`.
2. Starts the worker daemon on `0.0.0.0:5555`.
3. Starts the manager control plane on `0.0.0.0:5556` with the EPVM cost-based scheduler.
4. Starts background health-check probes, task dispatch loops, and state reconcilers.

---

## Using `cubectl` CLI

Build the CLI:

```bash
go build -o bin/cubectl ./cmd/cubectl
```

### Inspect Cluster Nodes
```bash
./bin/cubectl nodes
```
```
NODE NAME     ROLE    STATUS   TASKS  MEMORY (TOTAL)  DISK (TOTAL)
0.0.0.0:5555  worker  Healthy  1      16384 MB        256 GB
```

### Run a Task from JSON Specification
```bash
./bin/cubectl run -f examples/hello.json
```
```
Task 4a2b9f10-1823-4c91-b3f2-c90a12b489a1 (hello-service) successfully submitted to cluster.
```

Or run directly via flags:
```bash
./bin/cubectl run --name web --image nginx:alpine --cpu 0.5 --memory 536870912 --restart always
```

### List Cluster Tasks
```bash
./bin/cubectl status
```
```
TASK ID   NAME           IMAGE                 STATE    RESTARTS  CONTAINER     STARTED
4a2b9f10  hello-service  strm/helloworld-http  Running  0         a1b2c3d4e5f6  14:02:18
```

### Inspect Task Details
```bash
./bin/cubectl inspect 4a2b9f10-1823-4c91-b3f2-c90a12b489a1
```

### View Task Container Logs
```bash
./bin/cubectl logs 4a2b9f10-1823-4c91-b3f2-c90a12b489a1
```

### Stop a Running Task
```bash
./bin/cubectl stop 4a2b9f10-1823-4c91-b3f2-c90a12b489a1
```
```
Stop signal successfully sent for task 4a2b9f10-1823-4c91-b3f2-c90a12b489a1.
```

---

## Resume & Engineering Highlights

- **Distributed Control Plane & Data Plane Separation:** Engineered a decentralized container orchestration architecture inspired by Google Borg and Kubernetes, decoupling centralized cluster state from autonomous node worker daemons.
- **Resource-Aware Cost Scheduling (EPVM):** Implemented an Enhanced Parallel Virtual Machine cost-function scheduler that balances node CPU, memory load, and task density to prevent hotspotting and over-subscription.
- **State Machine & Lifecycle Reconciliation:** Designed an explicit state machine (`Pending` → `Scheduled` → `Running` → `Completed`/`Failed`) backed by an automated reconciliation loop that detects partitioned or crashed nodes and reschedules orphaned workloads under strict restart policies (`always`, `on-failure`, `never`).
- **Crash Consistency & Persistence:** Integrated transactional, zero-dependency embedded BoltDB (`bbolt`) storage engines for both control plane and workers to guarantee immediate state recovery across daemon restarts.
- **Production Observability & Resilience:** Standardized structured JSON logging using Go standard library `log/slog`, request tracing middleware, health endpoints, graceful shutdown via OS signal trapping, and comprehensive unit tests with Go race detection.

---

## License

MIT License.