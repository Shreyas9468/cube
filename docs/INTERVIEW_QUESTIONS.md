# Cube Orchestrator: Comprehensive FAANG/Tier-1 Technical Interview Question Bank

This document prepares you for technical interviews at Google, Meta, Amazon, Apple, Netflix, Uber, and Stripe. Each question is analyzed through the lens of an L5/L6/L7 systems interviewer, contrasting mediocre answers with Staff-level responses that use exact distributed systems terminology.

---

## Tier 1: Core Fundamentals & System Architecture (L4/L5)

### Q1: "Why did you implement an explicit Finite-State Machine (FSM) for tasks instead of simply updating task status strings dynamically?"

* **What the Interviewer is Evaluating:** Mechanical sympathy, distributed state consistency, defensive programming against race conditions and invalid transitions.
* **Flawed Answer:** *"Because an enum is cleaner than strings and it helps prevent typos when setting task state."*
* **Staff-Level Model Answer:**
  > "In an asynchronous distributed orchestrator, state transitions must be mathematically deterministic and invariant-preserving. An arbitrary string update allows illegal non-deterministic transitions—for example, transitioning a task directly from `Pending` to `Completed` without passing through `Scheduled` and `Running`, or resurrecting a `Completed` container into `Running`.
  >
  > I implemented an explicit state machine with a transition validator (`task.ValidStateTransition(from, to)`). This guarantees that terminal states (`Completed`, `Failed`) are immutable, and intermediate states enforce prerequisite invariants—such as allocating node resources and capturing container IDs before entering `Running`. Furthermore, this FSM is fundamental to our reconciliation engine: when the reconciler observes a state mismatch between the control plane and the node daemon, the state transition matrix dictates whether a transition is a valid forward progression, a duplicate out-of-order event, or an illegal state mutation requiring eviction."

---

### Q2: "How does your worker daemon isolate container resources, and what occurs at the OS kernel level when a container exceeds its memory allocation?"

* **What the Interviewer is Evaluating:** Understanding of Linux kernel primitives, cgroups v1/v2, memory management, and process lifecycle handling.
* **Flawed Answer:** *"Docker handles it automatically. If it uses too much memory, it crashes."*
* **Staff-Level Model Answer:**
  > "Cube interacts programmatically with the Docker Go SDK (`moby/moby/client`), where we translate task resource definitions into Linux kernel cgroup constraints via `container.HostConfig`.
  >
  > For CPU allocation, we set `NanoCPUs`, which the kernel enforces using the Completely Fair Scheduler (CFS) bandwidth controller knobs (`cpu.cfs_quota_us` and `cpu.cfs_period_us`). For memory, we configure hard limits in `container.Resources.Memory`. 
  >
  > When a container process exceeds its memory quota, the Linux kernel Memory Controller refuses additional memory page allocation. If anonymous memory cannot be reclaimed via page swapping, the kernel invokes the Out-Of-Memory (OOM) killer. The kernel selects the process with the highest `oom_score` within that memory cgroup and terminates it with a `SIGKILL` (exit code `137`). Our worker runtime intercepts this container death via the Docker engine event stream, updates the local task state to `Failed`, and persists it to local BoltDB, triggering the manager's reconciliation loop."

---

## Tier 2: Distributed Scheduling & Persistence (L5/L6)

### Q3: "Walk me through your EPVM scheduler. How does it balance workloads, and what are its trade-offs compared to Kubernetes' scheduler?"

* **What the Interviewer is Evaluating:** Algorithmic complexity, multi-dimensional optimization, resource fragmentation, and scheduling heuristics.
* **Flawed Answer:** *"It checks which node has the most free disk and memory and picks the one with the lowest score."*
* **Staff-Level Model Answer:**
  > "Cube’s EPVM (Enhanced Parallel Virtual Machine) scheduler implements a two-stage scheduling pipeline: **Predicates (Filtering)** and **Priorities (Scoring)**, directly mirroring the architecture of Google Borg and Kubernetes `kube-scheduler`.
  >
  > In the **Predicate phase**, we filter candidate nodes using hard capacity constraints: candidate nodes must satisfy $\text{DiskFree} \ge \text{Task.Disk}$. Nodes failing hard constraints are dropped immediately.
  >
  > In the **Scoring phase**, we apply a multi-variable cost function across surviving candidates:
  > $$C(n) = w_1 \cdot U_{mem}(n) + w_2 \cdot U_{cpu}(n) + w_3 \cdot U_{density}(n)$$
  > Where $U_{mem}$ represents projected memory saturation inclusive of the new task, $U_{cpu}$ represents the node's normalized load average, and $U_{density}$ accounts for container density. The candidate with the minimum cost score ($\arg\min C(n)$) is selected.
  >
  > **Trade-offs compared to Kubernetes:**
  > 1. *Throughput vs. Optimality:* Cube evaluates nodes sequentially in $O(N)$ time, which is optimal for small-to-medium clusters. Kubernetes utilizes parallelized score plugins with preemption cycles and node sampling percentage flags (`percentageOfNodesToScore`) to prevent scheduler Head-of-Line (HoL) blocking on 5,000+ node clusters.
  > 2. *Resource Fragmentation:* Our cost function balances load (load-leveling), which can cause resource fragmentation (leaving small slivers of RAM/CPU across all nodes). A pure bin-packing scheduler packs nodes densely to leave entire nodes idle for scale-down, but increases thermal hotspotting."

---

### Q4: "Why did you choose an embedded B+ Tree (BoltDB) over an external store like PostgreSQL or Redis, and how does crash recovery work?"

* **What the Interviewer is Evaluating:** Storage engine trade-offs, ACID semantics, crash consistency, zero-dependency engineering.
* **Flawed Answer:** *"BoltDB is written in Go, easy to import, and doesn't require setting up a separate database server."*
* **Staff-Level Model Answer:**
  > "We selected an embedded, transactional key-value store (`bbolt`) to achieve zero-dependency operational simplicity while maintaining strict ACID guarantees. 
  >
  > Under the hood, BoltDB utilizes a **Copy-on-Write (CoW) B+ Tree** backed by memory-mapped files (`mmap`). This provides three key architectural advantages:
  > 1. *Single-Writer Multi-Reader (SWMR) Concurrency:* Read transactions access OS page cache pages directly without acquiring user-space locks or allocating heap memory. This gives our HTTP inspection and health probes near-instantaneous throughput.
  > 2. *Crash Consistency without Write-Ahead Log (WAL) Replay Overhead:* Because writes modify newly allocated pages and atomically swing the root page pointer on commit via a single `fsync`, the on-disk file is never in an inconsistent state. A crash during write leaves the previous atomic version intact; zero log replay is required on restart.
  > 3. *Deterministic State Hydration:* On startup, `manager.NewWithStore()` executes a linear bucket scan over `tasks` and `events`, immediately reconstructing the in-memory lookup indices (`TaskDb`, `WorkerTaskMap`, and `EventDb`). This guarantees seamless recovery across process restarts."

---

## Tier 3: Fault Tolerance, Concurrency & Edge Cases (L6 Staff)

### Q5: "Suppose a network partition occurs: Worker 1 can no longer communicate with the Manager, but it can still communicate with external users. How does Cube handle this, and what is the risk of Split-Brain?"

* **What the Interviewer is Evaluating:** Deep understanding of distributed failure modes, CAP boundaries, split-brain phenomenon, fencing tokens, and distributed coordination limits.
* **Flawed Answer:** *"The manager detects the node is down and restarts the task on another worker. Everything keeps working."*
* **Staff-Level Model Answer:**
  > "This is a classic **Asymmetric Network Partition** leading directly to the **Split-Brain Workload Execution** problem:
  >
  > 1. *Detection:* The Manager’s background probe loop (`DoHealthChecks`) attempts to fetch `/stats` from Worker 1. Due to the partition, the HTTP connection times out. The Manager sets Worker 1's status to `Healthy = false`.
  > 2. *Reconciliation & Rescheduling:* The `ReconcileTasks` control loop inspects running tasks, identifies that Worker 1 is unhealthy, and triggers `maybeRestartTask("node_failure")`. It schedules an identical task onto Worker 2.
  > 3. *The Split-Brain Risk:* Worker 1 is still physically running the original container! If the workload is a stateless HTTP service behind a load balancer, traffic splits between two instances. However, if the workload is stateful—such as a database writing to an external persistent volume or an exclusive message queue consumer—both instances will write simultaneously, causing **unrecoverable data corruption**.
  >
  > **Staff-Level Mitigation Strategy for Production:**
  > - *Leases & Fencing Tokens:* Implement a distributed lease (e.g., via Raft/etcd). A worker is only authorized to run containers if it holds an active lease. If it cannot heartbeat the manager within lease duration $T_{lease}$, the worker self-terminates its local containers.
  > - *Generation / Fencing IDs:* Any external I/O operation must carry a monotonically increasing fencing counter issued by the control plane. External storage rejects write requests with stale generation numbers.
  > - *Node Eviction / STONITH:* Issue hardware or cloud provider fence calls (e.g., AWS EC2 StopInstances) before rescheduling stateful tasks onto surviving nodes."

---

### Q6: "In `manager.SendWork()`, you dequeue a task event, persist it to BoltDB, update in-memory maps, and dispatch it via HTTP POST. What happens if the manager process crashes right after persisting to BoltDB but BEFORE the HTTP POST succeeds?"

* **What the Interviewer is Evaluating:** Distributed dual-write problem, at-least-once vs. at-most-once delivery, idempotency, and transactional consistency.
* **Flawed Answer:** *"The task is lost in memory or we just re-run main.go and it starts over."*
* **Staff-Level Model Answer:**
  > "This highlights the fundamental **Dual-Write Problem** in distributed systems without two-phase commit (2PC).
  >
  > If the manager terminates after writing to BoltDB but prior to the worker acknowledging the HTTP POST:
  > 1. *On-Disk State:* BoltDB records the task in `Scheduled` state.
  > 2. *Worker State:* The worker has zero knowledge of the task; no container was created.
  >
  > **How our architecture resolves this:**
  > On restart, the manager recovers all tasks into `TaskDb`. When the background `UpdateTasks` polling loop queries the worker for active tasks, the worker returns an empty task list for that UUID. 
  >
  > Our reconciliation engine (`ReconcileTasks`) detects an invariant violation: the control plane records the task as `Scheduled`, but the assigned worker does not report it as `Running` after the grace timeout. The reconciler re-enqueues the task for dispatch.
  >
  > **Idempotency Guarantee on the Worker:**
  > To make this resilient against duplicate deliveries (At-Least-Once delivery semantics), the worker daemon’s `StartTaskHandler` is idempotent: it checks if a container for that UUID already exists. If present, it returns HTTP 200 without recreating the container."

---

## Tier 4: Out-of-the-Box Architectural Evolution (Principal / Staff+)

### Q7: "How would you evolve Cube from a single-manager control plane into a high-availability, 10,000-node cluster capable of handling 50,000 containers?"

* **What the Interviewer is Evaluating:** Systems design scalability, consensus architecture, RPC protocols, network overhead, and decoupling control/data planes.
* **Staff-Level Model Answer:**
  > "To scale Cube to 10,000 nodes and 50,000 containers, we must eliminate four structural bottlenecks:
  >
  > #### 1. Consensus & Multi-Manager HA: From Embedded BoltDB to Raft
  > - Replace single-node BoltDB with an active-passive leader election model using **Raft consensus** (e.g., HashiCorp Raft or etcd).
  > - Only the elected leader manager handles mutations and runs the scheduler; follower managers maintain read-only replicas to serve `cubectl status` queries.
  > - Use distributed leases: if the leader fails to renew its lease, standby managers initiate a new Raft term and elect a new leader in $< 3$ seconds.
  >
  > #### 2. Communication Protocol: From HTTP/1.1 Polling to gRPC Streaming
  > - *Current Bottleneck:* The manager polling 10,000 nodes every 15 seconds generates:
  >   $$\frac{10,000 \text{ requests}}{15 \text{ seconds}} \approx 667 \text{ req/sec}$$
  >   Incurring severe TCP handshake, header parsing, and serialization overhead.
  > - *Evolution:* Transition to persistent **bidirectional HTTP/2 gRPC streaming**. Workers establish a single multiplexed TCP connection to the control plane, streaming telemetry and task state updates *reactively on change* (Event-Driven Edge-Triggered) with a lightweight 30-second keep-alive ping.
  >
  > #### 3. Scheduling Scalability: Two-Level & Shared-State Scheduling
  > - A centralized sequential scheduler becomes a CPU bottleneck under high churn (e.g., 500 task starts/sec).
  > - Implement a **Two-Level Scheduler (Mesos)** or **Optimistic Shared-State Scheduler (Google Omega)**:
  >   - Partition candidate evaluation across multiple scheduler workers using parallel worker threads.
  >   - Use optimistic concurrency control (OCC) with version tags on node objects: schedulers place tasks concurrently; if two schedulers collide on the same node capacity, the transaction aborts and retries.
  >
  > #### 4. Telemetry Offloading: eBPF Kernel Ingestion
  > - Instead of worker daemons reading and parsing `/proc` text files in user space (causing high context switching and memory allocation), attach **eBPF (Extended Berkeley Packet Filter)** probes directly to kernel tracepoints (`cgroup/cgroup_skb`, `sched/sched_switch`).
  > - Telemetry is aggregated directly inside kernel ring buffers and shipped out-of-band to an observability pipeline (Prometheus/Vector), eliminating user-space polling overhead."
