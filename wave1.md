# inferd — Wave 1: Building the Concurrent Inference Server

## Architecture Overview

`inferd` is an HTTP inference serving engine designed to batch, prioritize, and dispatch inference requests across a bounded worker pool under backpressure.

```
Client Requests (HTTP)
       │ [X-Priority: N]
       ▼
┌─────────────────────────────────────────────────────────────┐
│ HTTP Handlers (server.go)                                   │
│ - Allocates Job instance & result channel                   │
│ - Enforces request lifecycle & client timeout               │
└──────────────────────────────┬──────────────────────────────┘
                               │ Submit(job)
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Scheduler (scheduler.go)                                    │
│ - Mutex-protected PriorityQueue (Max Heap, capacity: 50)    │
│ - Dynamic Batch Formation Window (10ms batching delay)      │
│ - Bounded dispatch to persistent Worker channels            │
└──────────────────────────────┬──────────────────────────────┘
                               │ jobs chan Batch (size: 1..3)
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Worker Pool (main.go, 5 Persistent Workers)                 │
│ - Persistent goroutines reading from dedicated job channels │
│ - Simulated Compute: 500ms base + 100ms per additional job  │
│ - Delivers result to job.result channel                     │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Metrics Engine (metrics.go)                                 │
│ - Lock-free atomic counters for states & queue depth        │
│ - Percentile latency tracking (p50, p95, p99)               │
│ - Histogram counters for batch sizes (1, 2, 3)              │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Systems Lessons

### 1. Concurrency Demands Explicit Ownership

In a multi-goroutine server with request handlers, queues, dispatchers, and workers, concurrency bugs are fundamentally **ownership bugs**. Shared state without clear lifecycle boundaries leads to data races, resource leaks, or lost responses.

Explicit boundaries established in `inferd`:

| Component | Owned Resource | Lifecycle Boundary |
|---|---|---|
| **HTTP Handler** (`infer`) | Request context, client socket, `Job.result` channel allocation | Spans from HTTP request arrival to response write or client disconnect |
| **Scheduler** (`Scheduler`) | `PriorityQueue` heap, queue capacity counter | Spans from `Submit()` admission to batch extraction in `dispatch()` |
| **Dispatcher Goroutine** | Batch formation, worker assignment | From heap `Pop()` to successful channel push `w.jobs <- batch` |
| **Worker Goroutine** (`Worker`) | Assigned `Batch`, execution timing, result generation | From channel receive `<-w.jobs` to `j.result <- output` |
| **Metrics Engine** (`Metrics`) | Atomic counters, latency slice | Global observability; write-only event updates from components |

**Failure Mode Avoided:**
Early prototypes spawned ephemeral worker goroutines inside loops:
```go
// ANTI-PATTERN: Spawns unbounded goroutines; destroys persistent worker isolation
for w := 1; w <= len(batch); w++ {
    go worker(w, &wg, batch[w-1:w])
}
```
Replacing this with fixed persistent workers (`make([]*Worker, 5)`) reading from dedicated channels (`jobs chan Batch`) prevented unbounded thread explosion and localized execution state.

---

### 2. Backpressure is Architectural Control, Not an Error Condition

An inference queue cannot be infinite. Memory is bounded, and unbounded queueing causes queuing latency to inflate monotonically until clients time out before their requests are ever scheduled.

Once a queue limit is set (`maxQueueCapacity = 50`), demand exceeding capacity must be rejected immediately at admission.

```
Incoming Request
       │
       ▼
Queue Length < 50 ──(Yes)──► Accept (HTTP 200 path, Heap Push)
       │
      (No)
       ▼
Reject immediately (HTTP 503 Service Unavailable, Drop)
```

#### The Semantic Distinction
```text
rejected ≠ failed
```
* **`rejected`**: The server shed load at the door. Zero compute was allocated. No scheduler or worker resources were consumed.
* **`failed`**: The request was admitted, but an execution or internal error occurred while processing it.

#### Empirical Load-Shedding Proof
Under a concurrency burst saturating 5 workers with `maxQueueCapacity = 50`, scraping `/metrics` produced:

```text
inferd_completed_requests 91
inferd_rejected_requests  934
inferd_queue_depth        0
inferd_p50                1.405476114s
inferd_p95                2.821459448s
inferd_p99                3.51851318s
inferd_batch_size_1       26
inferd_batch_size_2       1
inferd_batch_size_3       21
```

**Analysis:**
* **934 requests were cleanly rejected** with HTTP 503 before entering the system.
* **91 requests completed successfully** within server capacity.
* The system maintained bounded queue depth (`0` remaining at conclusion) and avoided memory starvation or process crashes.

---

### 3. Batching Decouples the Scheduling Unit from the Lifecycle Unit

In scalar request servers, scheduling unit = request lifecycle unit. In batched inference, the scheduler groups multiple independent requests into a single compute payload:

```text
Requests:  [J1]  [J2]  [J3]
             └────┬────┘
                  ▼
Scheduling:     Batch (len=3)
                  │
                  ▼
Execution:   Worker Compute (700ms total)
                  │
                  ├─► J1 complete ─► Client 1 HTTP 200
                  ├─► J2 complete ─► Client 2 HTTP 200
                  └─► J3 complete ─► Client 3 HTTP 200
```

#### Batch Cost Model
Simulated inference cost follows a non-linear amortized profile:
$$\text{Duration} = \text{baseCost} + \text{perJobCost} \times (\text{len}(\text{batch}) - 1)$$
* Base cost: $500\text{ms}$
* Incremental cost: $100\text{ms}$
* Batch of 1: $500\text{ms}$ ($500\text{ms}$ / job)
* Batch of 2: $600\text{ms}$ ($300\text{ms}$ / job)
* Batch of 3: $700\text{ms}$ ($233\text{ms}$ / job)

#### The Batch Formation Window
Eager dispatching drains individual jobs as soon as they arrive, degrading the system into single-item batches. To achieve higher-density batches without indefinite stalling:

```go
// scheduler.go
maxBatchSize := 3
if s.queue.Len() < maxBatchSize {
    s.mu.Unlock()
    time.Sleep(10 * time.Millisecond) // Batch formation window
    s.mu.Lock()
}
```

#### Empirical Proof of Dynamic Batch Formation
Testing 20 concurrent requests under the 10ms formation window:

```text
inferd_completed_requests 20
inferd_rejected_requests  0
inferd_failed_requests    0
inferd_queue_depth        0
inferd_p50                703.9497ms
inferd_p95                1.3172651s
inferd_p99                1.3172651s
inferd_batch_size_1       8
inferd_batch_size_2       3
inferd_batch_size_3       3
```

**Analysis:**
Instead of 20 single-item dispatches, the 10ms window allowed requests arriving concurrently to form batches of 2 and 3, reducing aggregate worker time while keeping p50 at ~704ms.

---

### 4. Priority Queues Require Operational Definitions

A priority queue order (`container/heap`) defines which job is popped first:

```go
type PriorityQueue []*Job

func (pq PriorityQueue) Less(i, j int) bool {
    return pq[i].priority > pq[j].priority // Max-heap: higher priority popped first
}
```

However, priority in batched systems is bounded by three operational constraints:
1. **Queue-time only**: Once a job is placed in a dispatched `Batch`, priority inversion can occur relative to newly arriving higher-priority jobs.
2. **Batch packing**: A priority 10 job may be co-batched with priority 2 and priority 1 jobs if they are in the queue during batch formation.
3. **Capacity limits**: A priority 10 job arriving when `queue.Len() >= 50` is rejected identically to a priority 1 job.

**Validation in HTTP Headers:**
Priority was parsed per request via `X-Priority` (default: 5):
```go
if p := r.Header.Get("X-Priority"); p != "" {
    if parsed, err := strconv.Atoi(p); err == nil {
        priority = parsed
    }
}
```
Under concurrent dispatch `[10, 8, 6, 5, 1]`, server logs confirmed strict heap ordering upon extraction:
```text
dispatching batch: 10 8 6
dispatching batch: 5 1
```

---

### 5. Metric Semantics Must Precede Implementation

A counter without an exact event boundary produces ambiguous or misleading telemetry.

#### Case Study: `inferd_batch_size_N`
Initially, batch sizing was recorded inside `process()`:
```go
// AMBIGUOUS: When is the metric counted?
func process(w *Worker, batch Batch, m *Metrics) {
    // ...
    m.AddBatchSize(int64(len(batch))) // Placed at end of worker function
}
```

**Flaw Exposed:**
If a worker crashes during job execution or drops work, does `inferd_batch_size_3` measure:
1. Batches dispatched by the scheduler?
2. Batches completed by the worker?
3. Completed requests divided by 3?

**The Semantic Fix:**
Separate scheduling decisions from execution outcomes.
* `inferd_batch_size_N` increments at the **dispatch boundary** inside `dispatch()` when the batch is successfully transferred to a worker channel.
* Request completions increment strictly inside `process()` as individual results are sent.

```go
// scheduler.go - Explicit dispatch event boundary
if assigned {
    s.metrics.AddBatchSize(int64(len(batch)))
}
```

> **Invariant**: A metric without a defined event boundary is ambiguous. Retries, crashes, and drops cause attempt counters to diverge from completion counters.

---

### 6. Observability as an Invariant Verification Tool

Metrics are not merely dashboards; they are mathematical assertions about system correctness.

For Wave 1 (prior to node failures or cancellations), the system obeyed the **External Admission Invariant**:

$$\text{Incoming Requests} = \text{Rejected Requests} + \text{Completed Requests}$$

```
                ┌────────────────────────┐
                │ Total Incoming Traffic │
                └───────────┬────────────┘
                            │
            ┌───────────────┴───────────────┐
            ▼                               ▼
┌───────────────────────┐       ┌───────────────────────┐
│ Rejected Requests     │       │ Completed Requests    │
│ (Queue full at entry) │       │ (Successfully served) │
└───────────────────────┘       └───────────────────────┘
```

#### Measured Audit Proof
From the saturation test:
* Total requests submitted: $1025$
* `inferd_rejected_requests`: $934$
* `inferd_completed_requests`: $91$
* Mathematical balance:
  $$934 + 91 = 1025$$
  $$\text{Divergence} = 0$$

When this equation failed to balance in early iterations, it immediately exposed lifecycle bugs (e.g. dropped jobs, unbuffered channel deadlocks, unrecorded rejections).
