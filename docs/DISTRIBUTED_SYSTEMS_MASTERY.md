# Distributed Systems Mastery Curriculum: Advanced Engineering Guide

This curriculum is designed for software engineers preparing for Senior, Staff, and Principal Distributed Systems interviews at tier-1 product companies (Google, Meta, AWS, Uber, Stripe). It uses the technical vernacular, mathematical models, and architectural patterns of modern infrastructure engineering.

---

## 1. Technical Terminology Matrix: Colloquial vs. Industry-Standard

When interviewing with Principal Engineers and Systems Architects, eliminate colloquial terminology. Use exact industry-standard vocabulary:

| Colloquial / Basic Phrasing | Professional Technical Term | Technical Definition & Nuance |
|---|---|---|
| "The server sends pings" | **Liveness Probe / Heartbeat Telemetry** | Periodic active sampling of remote node availability over an out-of-band communication channel. |
| "Checking if something broke" | **State Reconciliation Loop / Control Loop** | An edge-triggered or level-triggered feedback loop driving observed system state toward desired declared state. |
| "Saving data to disk" | **Write-Ahead Log (WAL) & B+ Tree Serialization** | Transactional append-only journal guaranteeing crash consistency followed by indexed B+ tree disk page flush. |
| "Picking the best node" | **Multi-Attribute Utility Function / Cost-Based Placement** | Multi-dimensional constraint filtering (predicates) followed by heuristic cost minimization (priorities). |
| "Splitting CPU and RAM" | **Kernel CFS Quotas & Cgroup Memory Hard Limits** | Completely Fair Scheduler (CFS) bandwidth allocation via `cpu.cfs_quota_us` and kernel OOM invocation on memory exhaustion. |
| "The node went down" | **Fail-Stop Model / Asymmetric Network Partition** | A node crash where execution halts completely without Byzantine faults, or partial connectivity loss where inbound traffic fails while outbound succeeds. |
| "Stopping the container" | **POSIX Signal Escalation (`SIGTERM` → `SIGKILL`)** | Graceful cooperative termination signaling with a deterministic deadline before kernel-enforced immediate process termination. |
| "Locking the database" | **Single-Writer Multi-Reader (SWMR) MVCC Concurrency** | Concurrency control where multiple read transactions run concurrently with zero locks using memory-mapped pages, while writes serialize via an exclusive lock. |

---

## 2. Distributed Systems Foundations

### 2.1 CAP & PACELC Theorem Positioning

In Eric Brewer’s **CAP Theorem**, Cube operates as a **CP-biased system** (Consistency & Partition Tolerance) on a per-node persistence level, transitioning to **AP-like behavior** (Availability & Partition Tolerance) in its current single-manager multi-worker topology:
- **Consistency ($C$):** Local storage transactions in BoltDB provide strict Serializability via ACID transactions. However, cross-node task state is *eventually consistent* due to the 15-second polling interval (`UpdateTasks`).
- **Partition Tolerance ($P$):** The manager reconciler detects partitioned nodes via heartbeat timeouts and migrates workloads.

Under Daniel Abadi’s **PACELC Theorem** (If Partition: Availability or Consistency; Else: Latency or Consistency):
- **Partition State ($PA/EL$):** During a partition, Cube trades immediate cluster consistency for availability—it assumes an unreachable node has failed and spawns replacement tasks on healthy nodes.
- **Normal State ($EL$):** In the absence of partitions, Cube prioritizes low latency ($L$) over distributed consensus ($C$) by avoiding cross-node Paxos/Raft overhead for individual container status updates.

### 2.2 Consensus vs. Autonomous Control Planes
- **Consensus-Driven Systems (Kubernetes + etcd):** State is governed by the Raft consensus algorithm. Every state mutation (e.g., Pod scheduled) requires a quorum of nodes ($N/2 + 1$) to acknowledge log replication. This guarantees linearizable reads and writes but incurs network round-trip latency and introduces quorum-loss risks during network splits.
- **Autonomous Feedback Loops (Cube Manager Reconciler):** Cube adopts a reconciliation pattern inspired by Google Borg and Kubernetes Controllers. Instead of synchronous distributed locks, the system continuously reads **observed state** ($O$) from workers, compares it to **desired state** ($D$) in `TaskDb`, and computes a correction delta ($\Delta = D - O$). This is **level-triggered reconciliation**, which is fundamentally more resilient than edge-triggered systems because lost events are naturally corrected on the next iteration.

### 2.3 Failure Detectors & The Split-Brain Problem
- **Failure Detector Classification:** Cube implements a synchronous polling heartbeat detector with a timeout threshold ($T = 15s$). In formal distributed systems literature (Chandra and Toueg), this is a partially synchronous failure detector ($\diamondsuit P$ - Eventually Perfect).
- **Split-Brain Risk in Single-Leader Architecture:**
  - *The Danger:* If a network partition isolates Worker 1 from Manager, Manager assumes Worker 1 has crashed and spins up a duplicate container on Worker 2. However, Worker 1 may still be executing the container! If the workload writes to shared disk or binds to external network endpoints, data corruption occurs.
  - *Mitigation (Fencing Tokens):* In production enterprise systems, split-brain is mitigated using **Fencing Tokens** (monotonically increasing epoch counters passed to external resources) and **STONITH** (*Shoot The Other Node In The Head* / node-level power cycling via IPMI).

---

## 3. Container Runtime Internals & Linux Kernel Primitives

### 3.1 Linux cgroups (Control Groups) Deep Dive
Container isolation is not hardware virtualization; it is kernel-enforced resource metering:
1. **CPU CFS Bandwidth Control:**
   - In Cube’s Docker runtime configuration:
     $$\text{NanoCPUs} = \text{CPU} \times 10^9$$
   - The Linux kernel Completely Fair Scheduler enforces CPU limits using two kernel knobs:
     - `cpu.cfs_period_us`: The duration of a scheduling period (typically $100,000 \mu s$ or $100ms$).
     - `cpu.cfs_quota_us`: The total CPU time processes in the cgroup may run within that period.
     - For a task requesting $0.5$ CPU: $\text{quota} = 50,000 \mu s$ per $100,000 \mu s$ period. Once exhausted, the kernel throttles the thread until the next period.
2. **Memory Hard Limits & OOM Invocation:**
   - When a task exceeds `Config.Memory`, the Linux kernel Memory Controller invokes the out-of-memory killer (`oom-killer`).
   - The kernel inspects `oom_score` and terminates the highest memory-consuming process in that container namespace with exit code `137` (`SIGKILL` = $128 + 9$). Cube captures this non-zero exit code to mark the task `Failed`.

### 3.2 Linux Namespaces
A container is a regular Linux host process constrained within 7 distinct kernel namespaces:
- **PID Namespace:** Virtualizes process IDs. PID 1 inside the container is a standard high-numbered PID on the host.
- **Mount Namespace (`mnt`):** Isolates filesystem mount points. The container sees only its rootfs layer.
- **Network Namespace (`net`):** Virtualizes network interfaces, routing tables, and firewall rules (`veth` pairs).
- **IPC Namespace:** Isolates Inter-Process Communication (POSIX message queues, SysV shared memory).
- **UTS Namespace:** Virtualizes hostname and domain names.
- **User Namespace:** Maps container root (`UID 0`) to an unprivileged UID on the host for defense-in-depth.
- **cgroup Namespace:** Masks full cgroup path visibility from within the container.

---

## 4. Advanced Scheduling Algorithms: Theory & Mathematical Formulation

### 4.1 Scheduling Paradigms
1. **Sequential Allocation (Round-Robin):**
   - $O(1)$ time complexity. Distributes tasks without regard to resource availability. Prone to severe resource fragmentation and catastrophic node over-subscription.
2. **Multi-Dimensional Bin-Packing (NP-Hard):**
   - Workload placement maps to the multi-dimensional cutting-stock problem. Solving for optimal packing is NP-hard, requiring heuristic approximations.

### 4.2 EPVM (Enhanced Parallel Virtual Machine) Mathematical Model
Cube implements an EPVM heuristic algorithm designed to minimize resource variance and hotspotting:

1. **Filtering Phase (Hard Constraints / Predicates):**
   For each node $n \in Nodes$, node is a candidate if:
   $$\text{DiskAvailable}(n) \ge \text{Task.Disk}$$
   Nodes failing hard constraints are eliminated immediately.

2. **Scoring Phase (Soft Constraints / Priorities):**
   For every candidate node $n$, compute:
   - **Projected Memory Utilization:**
     $$U_{mem}(n) = \frac{\text{MemoryAllocated}(n) + \text{Task.Memory}}{\text{TotalMemory}(n)}$$
   - **Load Average / CPU Factor:**
     $$U_{cpu}(n) = \text{LoadStats.Last1Min}(n)$$
   - **Task Density:**
     $$U_{density}(n) = \text{TaskCount}(n)$$

   The composite **Cost Function** $C(n)$ is formulated as:
   $$C(n) = w_1 \cdot U_{mem}(n) + w_2 \cdot U_{cpu}(n) + w_3 \cdot U_{density}(n)$$
   Where $w_1, w_2, w_3$ are balancing weights.

3. **Selection Phase (Pick):**
   $$\text{SelectedNode} = \arg\min_{n \in Candidates} C(n)$$
   The node with the minimum projected cost receives the workload, guaranteeing balanced cluster load and preventing resource exhaustion.

---

## 5. Storage Engine Internals: B+ Tree vs. LSM-Tree

Cube uses **BoltDB (`bbolt`)**, a transactional key-value store based on an on-disk **B+ Tree**, contrasted with **LSM-Trees (Log-Structured Merge-Trees)** used in systems like RocksDB and Cassandra:

| Dimension | BoltDB (B+ Tree) | RocksDB / Cassandra (LSM-Tree) |
|---|---|---|
| **Write Path** | In-place page updates with Copy-on-Write (CoW). Slower random writes due to page allocation. | Fast sequential writes to memory memtable + append-only disk commit log. Background compaction. |
| **Read Path** | $O(\log N)$ predictable page traversal. Directly reads OS page cache via `mmap`. Ultra-fast point reads. | Multi-level reads: Memtable → Bloom Filter → Multiple SSTables on disk. High read amplification without cache. |
| **Concurrency Model** | Single-Writer Multi-Reader (SWMR). Unlimited concurrent readers with zero locking. | Multi-threaded writes and reads; requires compactor locks and complex MVCC tombstone handling. |
| **Crash Recovery** | Zero-recovery time. Read root page from memory-mapped file; transaction commit is atomic via single pointer update. | Must replay WAL from disk and rebuild uncommitted memtables on restart. |
| **Suitability for Cube** | **Ideal:** Cluster orchestrator metadata has moderate write frequency, high read frequency, and demands absolute crash consistency. | Sub-optimal for metadata: excessive write compaction overhead and memory footprint. |

---

## 6. High-Performance Go Systems Engineering

### 6.1 Goroutine Lifecycle & CSP Concurrency
Cube adheres to Tony Hoare's **Communicating Sequential Processes (CSP)**:
- Background loops (`SendWorkLoop`, `UpdateTasks`, `DoHealthChecks`, `ReconcileTasks`) run as isolated goroutines.
- Decoupling producer (HTTP API) and consumer (execution loop) via thread-safe queues avoids mutex lock contention.
- Zero shared mutable state across network boundaries: nodes communicate via typed JSON payloads.

### 6.2 Zero-Allocation Structured Logging (`log/slog`)
Legacy logging (`log.Printf`) incurs dynamic string formatting allocations, interface boxing overhead, and lacks machine-parseable structure.
Cube standardizes on Go’s standard library `log/slog` with a JSON handler:
```go
slog.Info("Task state updated",
    "task_id", t.ID,
    "old_state", oldState,
    "new_state", newState,
)
```
- **Performance:** Pre-allocates structured key-value pairs without runtime heap reflection.
- **Observability:** Compatible with log aggregation engines (Datadog, Elastic, CloudWatch) for distributed tracing and anomaly alerting.
