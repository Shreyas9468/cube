# Cube: Distributed Container Orchestrator in Go

Cube is a lightweight container orchestrator built from scratch in Go, inspired by systems like Google Borg, Kubernetes, and HashiCorp Nomad. It demystifies distributed container scheduling and lifecycle management using Go and the Docker Engine API.

---

## Architecture Overview

Cube coordinates containerized tasks across worker nodes via a centralized manager and distributed worker daemons.

```
                  +-------------------------+
                  |       Manager API       |
                  |     (Port 5556 / HTTP)  |
                  +------------+------------+
                               |
                +--------------+---------------+
                |                              |
         [Pending Queue]               [Task / Event DB]
                |                              |
                +--------------+---------------+
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
        [Docker Engine]                 [Docker Engine]
```

### Core Components

1. **Task & State Machine (`task/`)**
   - **Task Lifecycle:** `Pending` → `Scheduled` → `Running` → `Completed` / `Failed`.
   - **Docker Engine Integration:** Programmatic container creation, image pulling, execution, log streaming, and teardown using the Docker Go SDK (`moby/moby/client`).

2. **Worker Daemon (`worker/`)**
   - Receives tasks from the manager via HTTP REST API.
   - Executes container run/stop lifecycles through an internal task queue.
   - Collects real-time host metrics (CPU utilization, memory usage, disk space, load average) from `/proc` with cross-platform fallback.

3. **Cluster Manager (`manager/`)**
   - Control plane managing cluster state and task routing.
   - Polls worker nodes every 15 seconds to synchronize runtime container states.
   - Dispatches pending task events to eligible nodes selected by the scheduler.

4. **Schedulers (`scheduler/`)**
   - **RoundRobin:** Distributes tasks evenly across registered nodes.
   - **EPVM (Enhanced Parallel Virtual Machine):** Scores candidate nodes based on available CPU, memory headroom, and current task density to minimize cluster resource contention.

5. **Node Registry (`node/`)**
   - Represents compute hosts in the cluster.
   - Connects to worker metrics endpoints to assess capacity and health before task placement.

---

## Phased Build Progression

The repository is structured into distinct, logical phases:

| Phase | Component | Description |
|---|---|---|
| **Phase 1** | Scaffolding | Module configuration, dependencies, and base layout |
| **Phase 2** | Task Model | Task definitions, state transitions, and Docker container engine |
| **Phase 3** | Worker Daemon | Task queue, state machine execution, telemetry metrics, and Worker API |
| **Phase 4** | Manager Control Plane | Centralized state store, task dispatch loop, and Manager HTTP API |
| **Phase 5** | Node & Schedulers | Node registry, capacity accounting, Round-Robin and EPVM scheduling |
| **Phase 6** | Full Orchestrator | End-to-end integration wiring manager, worker daemon, and automated task execution |

---

## API Endpoints

### Manager API (Default Port: `5556`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/tasks` | List all tasks across worker nodes |
| `POST` | `/tasks` | Submit a new task event |
| `DELETE` | `/tasks/{taskID}` | Stop a running task |

### Worker API (Default Port: `5555`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/tasks` | List tasks assigned to this worker |
| `POST` | `/tasks` | Accept and enqueue a task |
| `DELETE` | `/tasks/{taskID}` | Stop and remove a task container |
| `GET` | `/stats` | Retrieve node resource metrics (CPU, RAM, Disk, Load) |

---

## Getting Started

### Prerequisites
- **Go 1.20+**
- **Docker Engine** running locally (or remote Docker daemon via `DOCKER_HOST`)

### Running Locally

```bash
# Verify dependencies
go mod tidy

# Build all packages
go build ./...

# Run the cluster orchestrator
go run main.go
```

When started, `main.go` initializes:
1. Worker daemon on `0.0.0.0:5555`
2. Manager control plane on `0.0.0.0:5556` with pluggable scheduler
3. Background state reconciliation and task dispatch loops
4. Submits a sample task (`strm/helloworld-http`) and verifies container startup

---

## License

MIT License.