# Cube: System Architecture & Design Specification

## 1. Executive Summary & Design Philosophy

Cube is a distributed, containerized workload orchestrator engineered in Go, designed around the architectural principles of Google Borg and Kubernetes. It provides an autonomous control plane that decouples declarative task scheduling from host execution, ensuring high availability, continuous health reconciliation, and persistent state recovery across worker nodes.

The core design philosophy is rooted in **minimalism, zero hidden abstractions, and mechanical sympathy**:
- **Explicit State Transitions:** Workloads move through a strictly validated finite-state machine (FSM).
- **Asynchronous Decoupled Execution:** Control plane decisions (scheduling, dispatching, probing) run asynchronously from worker node container lifecycles.
- **Embedded Persistence:** State is transactionally persisted into an ACID-compliant embedded B+ tree engine (BoltDB), enabling deterministic post-crash recovery without external database dependencies.
- **Pluggable Scheduling Heuristics:** Workload placement is decoupled behind a unified interface supporting sequential rotation (Round-Robin) and multi-dimensional cost optimization (EPVM).

---

## 2. End-to-End System Architecture

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

## 3. Layer-by-Layer Architectural Breakdown

### Layer 1: Workload & State Specification (`task/`)
* **Problem Addressed:** Workloads need a formal data contract specifying resource requests (CPU, memory, disk), container images, network port maps, restart semantics, and lifecycle progression.
* **Mechanism:**
  - `Task`: Immutable descriptor capturing unique UUID, container ID, image, resource allocations, restart policies (`always`, `on-failure`, `never`), and execution timestamps.
  - `TaskEvent`: Event-driven state mutation envelope recording state transition requests with monotonic UTC timestamps.
  - Finite State Machine (`ValidStateTransition`): Strict state transition matrix preventing invalid mutations (e.g., direct jump from `Pending` to `Completed` or resurrection of a `Completed` task).

### Layer 2: Container Runtime Integration (`task/docker.go`)
* **Problem Addressed:** Programmatic interaction with the local or remote Docker daemon without relying on external shell commands or fragile CLI sub-processes.
* **Mechanism:**
  - Leverages the official Docker Go SDK (`github.com/moby/moby/client`).
  - Manages image pulling with output stream drains to avoid client deadlocks.
  - Enforces kernel-level resource constraints using Linux CFS (Completely Fair Scheduler) quotas via `container.Resources.NanoCPUs` and hard memory limits (`container.Resources.Memory`).
  - Supports non-multiplexed and multiplexed standard I/O streaming via `stdcopy.StdCopy` for real-time log ingestion and terminal attachment.

### Layer 3: Worker Node Daemon (`worker/`)
* **Problem Addressed:** Compute hosts must execute assigned workloads, isolate host failures, buffer incoming tasks, collect node telemetry, and survive process crashes.
* **Mechanism:**
  - **Asynchronous Task Queue:** Tasks received over REST API are buffered in an in-memory FIFO queue (`queue.Queue`) and executed by a decoupled worker goroutine (`RunTaskLoop`).
  - **Local Persistence:** Container states are written to an embedded BoltDB instance (`worker.db`) before and after container creation, ensuring state reconstruction after host restarts.
  - **Telemetry Engine (`metrics.go`):** Parses raw `/proc/meminfo`, `/proc/stat`, `/proc/loadavg`, and filesystem syscalls (`syscall.Statfs`) with cross-platform fallbacks for non-Linux kernels.

### Layer 4: Centralized Cluster Manager (`manager/`)
* **Problem Addressed:** Multi-node orchestration requires centralized placement decisions, cluster topology tracking, worker heartbeat aggregation, and orphaned task recovery.
* **Mechanism:**
  - **Dispatch Loop (`SendWorkLoop`):** Dequeues pending task requests, consults the scheduler for optimal worker node selection, marks the task `Scheduled`, registers node resource allocations, and dispatches an HTTP payload to the assigned worker.
  - **Polling Syncer (`UpdateTasks`):** Periodically polls all registered workers every 15 seconds to sync task lifecycle states back into the control plane DB.
  - **Heartbeat Prober (`DoHealthChecks`):** Queries `/stats` on every registered node. Upon consecutive connectivity timeouts or HTTP errors, marks the node `Unhealthy`.

### Layer 5: Pluggable Scheduling Engine (`scheduler/`)
* **Problem Addressed:** Different cluster environments require distinct placement strategies—from lightweight round-robin to cost-aware multi-variable optimization.
* **Mechanism:**
  - Standardized Go interface:
    ```go
    type Scheduler interface {
        SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
        Score(t task.Task, nodes []*node.Node) map[string]float64
        Pick(scores map[string]float64, candidates []*node.Node) *node.Node
    }
    ```
  - **RoundRobin:** Sequentially rotates task distribution across candidate nodes.
  - **EPVM (Enhanced Parallel Virtual Machine):** Filters nodes lacking sufficient disk capacity, calculates projected memory and CPU utilization, and scores candidate nodes using a cost function to minimize cluster resource fragmentation.

### Layer 6: Self-Healing & Reconciliation Engine (`manager/manager.go`)
* **Problem Addressed:** Hardware crashes, network partitions, and container crashes must be detected and resolved autonomously without operator intervention.
* **Mechanism:**
  - Runs a 20-second continuous reconciliation loop (`ReconcileTasks`).
  - Cross-references known running tasks with live worker node health.
  - Detects tasks stranded on partitions or crashed nodes (`node_failure`) or tasks in `Failed` state.
  - Evaluates the task's `RestartPolicy` (`always`, `on-failure`, `never`) and retry counter (up to 3 attempts), automatically generating a new `TaskEvent` to reschedule the workload onto an alternate healthy node.

### Layer 7: Embedded Storage Engine (`store/`)
* **Problem Addressed:** Memory-only orchestrators lose all cluster history and state upon process restart.
* **Mechanism:**
  - Generic transactional key-value interface (`Put`, `Get`, `List`, `Delete`).
  - Implemented using BoltDB (`go.etcd.io/bbolt`), providing full ACID transactions, single-writer multi-reader concurrency, and page-level B+ tree disk serialization.
  - Buckets partitioned into `tasks` and `events`.

### Layer 8: Operator Command-Line Interface (`cmd/cubectl/`)
* **Problem Addressed:** Operators need an intuitive, POSIX-compliant CLI tool to manage cluster workloads and inspect telemetry.
* **Mechanism:**
  - Built with `spf13/cobra` and formatted using `text/tabwriter`.
  - Subcommands:
    - `cubectl run`: Submits workload via JSON manifest or inline CLI flags.
    - `cubectl status`: Tabular cluster-wide task view with state, restarts, container ID, and runtime.
    - `cubectl inspect`: Returns complete JSON state envelope for a specific task.
    - `cubectl logs`: Fetches container stdout/stderr output from the target worker node.
    - `cubectl nodes`: Real-time node capacity, status (Healthy/Unhealthy), and task density.
    - `cubectl stop`: Gracefully terminates a task and reclaims node resources.

---

## 4. Workload Lifecycle Finite-State Machine (FSM)

```
        +---------------+
        |    Pending    |
        +-------+-------+
                |
                v
        +---------------+
        |   Scheduled   | <-------------+
        +-------+-------+               |
                |                       | Restart Policy Trigger
                v                       | (Reconciliation Loop)
        +---------------+               |
        |    Running    |               |
        +---+-------+---+               |
            |       |                   |
            |       +------------> +----+----+
            |                      |  Failed |
            v                      +---------+
    +---------------+
    |   Completed   |
    +---------------+
```

### Valid Transition Matrix

| From State | Allowed Target States | Disallowed Transitions | Rationale |
|---|---|---|---|
| `Pending` | `Scheduled` | `Running`, `Completed`, `Failed` | Task must be assigned to a worker node before execution. |
| `Scheduled` | `Running`, `Failed` | `Pending`, `Completed` | Container creation begins immediately upon scheduling. |
| `Running` | `Completed`, `Failed` | `Pending`, `Scheduled` | Running containers terminate either cleanly (`0`) or with error (`>0`). |
| `Completed` | *(Terminal)* | `Pending`, `Scheduled`, `Running`, `Failed` | Clean termination is immutable. |
| `Failed` | `Scheduled` (via Retry) | `Pending`, `Running`, `Completed` | Failed tasks can only be rescheduled as fresh events under restart policies. |

---

## 5. Failure Scenarios & Mitigations

| Failure Scenario | Detection Mechanism | Automated Mitigation |
|---|---|---|
| **Worker Host Crash / Hard Network Partition** | Manager `DoHealthChecks()` loop detects consecutive TCP connection refused or timeout errors. | Node marked `Healthy = false`. `ReconcileTasks()` detects running tasks assigned to unhealthy node, triggers `maybeRestartTask("node_failure")`, and reschedules tasks onto healthy surviving nodes. |
| **Container OOM / Process Crash** | Worker container monitor captures non-zero container exit code or daemon death. | Worker marks task `Failed`. Manager sync loop updates `TaskDb`. Reconciler checks `RestartPolicy`; if `on-failure` or `always`, bumps `RestartCount` and dispatches new execution event. |
| **Manager Process Crash** | OS process termination / machine restart. | On startup, `manager.NewWithStore()` reads all keys from BoltDB `tasks` and `events` buckets, reconstructing cluster state, task history, and worker mappings. |
| **Worker Process Crash** | Local daemon crash. | On startup, `worker.New()` reads local BoltDB `tasks` bucket, re-populates task database, and resumes background telemetry loops. |
| **Resource Hotspotting** | EPVM scheduler load score calculation. | Nodes with high memory utilization, low free disk, or high load averages receive penalizing cost scores, steering new workloads toward underutilized nodes. |

---

## 6. Kubernetes vs. Borg vs. Cube Comparison

| Architectural Dimension | Google Borg | Kubernetes | Cube (Our System) |
|---|---|---|---|
| **Coordination Model** | Centralized BorgMaster with Paxos | Centralized Control Plane with etcd (Raft) | Centralized Manager with BoltDB (Single-node ACID) |
| **Node Daemon** | Borglet | Kubelet | Cube Worker Daemon |
| **Workload Abstraction** | Alloc / Package | Pod (Multi-container unit) | Task (Single-container unit) |
| **State Storage** | Paxos-replicated checkpoint store | Distributed etcd key-value store | Embedded transactional BoltDB (`bbolt`) |
| **Scheduler Algorithm** | Worst-Fit & Best-Fit (Scoring) | Predicates & Priorities (Filter/Score) | EPVM (Resource Cost Scoring) & Round-Robin |
| **CLI Management** | `borgcfg` / internal tools | `kubectl` | `cubectl` |
| **Container Engine** | Internal Linux cgroups/chroot | Container Runtime Interface (CRI / containerd) | Docker Engine API (`moby/moby/client`) |
