# inferd — Wave 2: Failure, Cancellation, and Lifecycle Correctness

## Architecture Overview

Wave 2 introduces failure resilience, request context cancellation, and strict terminal accounting to `inferd`.

```
                    ┌──────────────┐
                    │ HTTP Handler │
                    └──────┬───────┘
                           │ Submit(job)  [External Admission]
                           ▼
                    ┌──────────────┐
                    │    Queue     │◄─────────┐
                    └──────┬───────┘          │ Requeue(job)
                           │                  │ [Internal Recovery]
                       scheduler              │
                           │                  │
                           ▼                  │
                    ┌──────────────┐          │
                    │    Worker    │──────────┘
                    └──────┬───────┘
                           │
       ┌───────────────────┼───────────────────┐
       ▼                   ▼                   ▼
    COMPLETE             CANCEL              FAILED
 (Result sent)      (Context Done)     (Retries exhausted /
                                        Requeue dropped)
```

---

## Core Systems Lessons

### 7. Process Crashing vs. Worker Crashing

In Go, an unhandled panic in any goroutine terminates the entire OS process, regardless of how many other goroutines are running healthy workloads.

```text
Unhandled Worker Panic ──► Process Exit Code 2 ──► Entire Server Dies
```

Goroutines do not provide automatic fault isolation. To turn a worker crash into an isolated failure:
* Each batch execution must be wrapped in a deferred `recover()` block.
* The worker channel must remain open or be re-initialized.
* Panics must trigger a structured recovery sequence rather than tearing down the host process.

```go
// main.go: Isolated worker recovery boundary
for batch := range worker.jobs {
    func() {
        defer func() {
            if r := recover(); r != nil {
                fmt.Printf("Worker %d panicked: %v\n", worker.id, r)
                // Structured recovery on remaining work
            }
        }()
        process(worker, batch, m)
    }()
}
```

---

### 8. Recovery Requires Explicit Progress State (`w.remaining`)

Tracking only a single active job pointer (`worker.currJob`) is fundamentally broken for batched operations.

#### The `currJob` Bug
Consider a batch `[A, B, C]`:
1. `A` completes and writes result to `j.result`.
2. `B` begins execution.
3. Worker encounters a hardware fault or unhandled panic.

If recovery inspects `w.currJob` and finds it cleared or ambiguous, it either:
* Requeues `[A, B, C]` (causing `A` to execute twice and deliver duplicate results to client A).
* Drops `C` entirely because it only recovers `w.currJob` (`B`).

#### The Sliced Progress Invariant
Progress state must be maintained explicitly as a remaining sub-slice:

```go
// Worker tracks precisely what has NOT yet finished
type Worker struct {
    id        int
    jobs      chan Batch
    remaining Batch // Invariant: unfinished, non-cancelled work
}
```

Inside `process()`, `w.remaining` is narrowed immediately as each job completes:
```go
w.remaining = batch // Initial: [A, B, C]
time.Sleep(computeDuration)

for i, j := range batch {
    // Deliver result
    j.result <- output
    m.IncCompleted(1)

    // Progress update: atomically remove completed job from recovery scope
    w.remaining = batch[i+1:] 
}
w.remaining = nil
```

> **Invariant**: `w.remaining` strictly contains jobs that have **neither delivered results nor been terminally cancelled**. If a panic occurs at step $k$, recovery inspects only `batch[k:]`.

---

### 9. Batch Membership Does Not Determine Job State

A `Batch` is an ephemeral scheduling vehicle; it is **not** an atomic lifecycle unit.

Within a single batch `[A, B, C]`:
```text
Batch Dispatched:  [   A   ,   B   ,   C   ]
Execution State:   COMPLETE  CANCEL  RUNNING
```
* Job `A` can complete successfully.
* Job `B` can have its HTTP client disconnect (`ctx.Done()`).
* Job `C` can trigger a panic.

Attempting to apply lifecycle actions to the batch as a whole produces severe correctness violations. Every state transition (complete, cancel, retry, fail) must evaluate the **individual job**.

---

### 10. Retries are State Transitions, Not Terminal Outcomes

A retry is an intermediate state machine transition. It is not an end state.

```text
RUNNING ──► Worker Panic ──► RETRY ──► QUEUED ──► RUNNING ──► COMPLETE
```

If metrics classify "retry" as an outcome on the same tier as "completed" or "failed", the accounting model collapses:
$$\text{Accepted} \neq \text{Completed} + \text{Failed} + \text{Retried}$$

A retried job will later increment `completed` or `failed`. Counting retries as terminal outcomes results in double-counting requests.

#### The Terminal State Rule
Every request admitted to the server must terminate in exactly **one** of three terminal states:
$$\text{Accepted} = \text{Completed} + \text{Failed} + \text{Cancelled}$$

Retries occur exclusively inside `Accepted` and do not alter terminal metrics until retries are exhausted ($> 3$).

---

### 11. Admission Rejection and Retry Failure are Different Events

In early implementations, the recovery loop called the external admission function `scheduler.Submit(j)` to re-add crashed jobs to the queue.

#### The Double-Counting Defect
```go
// scheduler.go
func (s *Scheduler) Submit(j *Job) bool {
    if s.queue.Len() >= maxQueueCapacity {
        s.metrics.IncRejected() // <--- PROBLEM: External admission metric
        return false
    }
    // ...
}

// main.go (panic handler)
if !scheduler.Submit(j) {
    m.IncFailed() // <--- Caller increments failed
}
```

When the queue was at capacity during a worker crash:
1. `Submit()` saw `queue.Len() >= 50` and incremented `inferd_rejected_requests` (+1).
2. `Submit()` returned `false`.
3. Recovery saw `false` and incremented `inferd_failed_requests` (+1).

**Result**: A single job was counted as **both rejected and failed**, breaking the top-level accounting invariant:
$$\text{Incoming} \neq \text{Rejected} + \text{Completed} + \text{Failed}$$

#### The Architectural Fix: Path Separation
Separate external admission from internal requeuing:

```go
// scheduler.go

// Submit: External admission path (affects rejected metrics)
func (s *Scheduler) Submit(j *Job) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.queue.Len() >= maxQueueCapacity {
        s.metrics.IncRejected()
        return false
    }
    heap.Push(&s.queue, j)
    s.metrics.IncQueueDepth()
    return true
}

// Requeue: Internal recovery path (does NOT increment rejected metrics)
func (s *Scheduler) Requeue(j *Job) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.queue.Len() >= maxQueueCapacity {
        return false // Drops internally, caller handles failure accounting
    }
    heap.Push(&s.queue, j)
    s.metrics.IncQueueDepth()
    return true
}
```

---

### 12. Race-Free Does Not Mean Correct

Running the Go race detector:
```bash
go run -race .
```
verifies only that no two goroutines access the same memory location concurrently with at least one write without synchronization.

It does **not** verify:
* Whether a recovered job was already completed (`currJob` stale pointer bug).
* Whether retry attempts exceed limits.
* Whether metrics balance against incoming requests.
* Whether channel sends block indefinitely.

> **Rule**: Tooling detects race conditions; lifecycle analysis detects logical concurrency and recovery flaws.

---

### 13. Cancellation is a Lifecycle Boundary Problem

Cancellation cannot simply be an HTTP-level timeout check. It must be checked at every boundary crossing where work changes state or ownership.

```text
┌──────────────┐
│ PriorityQueue│ ── Check 1: In dispatch(), drop before packing batch
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Worker Batch │ ── Check 2: In process(), skip before compute delivery
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Panic / Crash│ ── Check 3: In recovery, drop before Requeue()
└──────────────┘
```

#### The Three Operational Checkpoints
1. **Checkpoint 1: Scheduler Extraction (`dispatch`)**
   ```go
   k := heap.Pop(&s.queue).(*Job)
   s.metrics.DecQueueDepth()
   if k.cancelled() {
       s.metrics.IncCancelled()
       continue // Discard without assigning to worker
   }
   ```
2. **Checkpoint 2: Worker Delivery (`process`)**
   ```go
   for i, j := range batch {
       if j.cancelled() {
           s.metrics.IncCancelled()
           w.remaining = batch[i+1:]
           continue // Skip compute result
       }
       j.result <- output
       m.IncCompleted(1)
       w.remaining = batch[i+1:]
   }
   ```
3. **Checkpoint 3: Crash Recovery (`main.go`)**
   ```go
   for _, j := range worker.remaining {
       if j.cancelled() {
           m.IncCancelled()
           continue // Do not requeue dead client
       }
       // proceed to retry...
   }
   ```

---

### 14. Cancellation and Failure are Mutually Exclusive Outcomes

A cancelled request is not a server failure. The inference engine operated correctly; the client terminated interest prematurely.

```text
FAILED:    The server could not satisfy an admitted request (crash, retry exhausted, queue full on retry).
CANCELLED: The client revoked interest before delivery (HTTP timeout, client close).
```

Combining these outcomes masks infrastructure failures under client churn or vice versa. They require separate terminal states and metrics counters (`inferd_failed_requests` vs `inferd_cancelled_requests`).

---

### 15. The Completion Boundary Must Be Explicit

In distributed systems, you cannot guarantee the remote client successfully read the HTTP response bytes over TCP. If "completion" requires client ACK:
* Network disconnects during TCP write become server-side inference failures.
* Metrics become coupled to downstream client network conditions.

In `inferd`, the completion boundary is strictly defined:
```text
Worker delivers output to Job.result channel ──► State = COMPLETE
```
Once `COMPLETE` is reached, the inference workload is finished. Downstream network disconnects do not roll back execution state.

---

### 16. The Cancellation Cost Tradeoff: Toy vs. Production

In `inferd`'s toy implementation, simulated compute is simulated with `time.Sleep`:
```text
Client Cancels ──► Worker finishes Sleep ──► Drops result at delivery boundary
```
This is acceptable for inexpensive CPU sleep tests.

In **production LLM inference**, running cancelled requests to completion is catastrophic:
* **GPU Compute**: Sustained execution wastes matrix multiplier FLOPS.
* **KV Cache Memory**: Retains allocated page blocks in VRAM, blocking new prompts.
* **Batch Slotting**: Holds a batch slot across auto-regressive decode iterations.

> **Production Rule**: When the resource cost of continuing work exceeds the engineering complexity of preempting it, cancellation must interrupt compute at the iteration boundary (e.g. token-level decode checkpoint).

---

### 17. The Systems Invariant Test Proofs

The combined architecture was tested across three rigorous end-to-end failure scenarios.

#### Initial Clean State
```text
inferd_completed_requests 0
inferd_rejected_requests  0
inferd_failed_requests    0
inferd_cancelled_requests 0
inferd_queue_depth        0
```

---

#### Test Scenario A: Cancelled While Queued
* **Workload**: 15 requests submitted simultaneously, saturating all 5 workers (each taking a 3-job batch requiring ~700ms).
* **Target Request**: 16th request submitted with `X-Priority: 1` and a 50ms client deadline (`context.WithTimeout`).
* **Observation**: Client timed out while request was waiting in `PriorityQueue`.
* **Server Log Output**:
  ```text
  job (priority 1) cancelled while queued
  ```
* **Metrics Snapshot**:
  ```text
  inferd_completed_requests 15
  inferd_rejected_requests  0
  inferd_failed_requests    0
  inferd_cancelled_requests 1
  inferd_queue_depth        0
  inferd_batch_size_3       5
  ```
* **Audit**: 16 incoming = 0 rejected + 16 accepted (15 completed + 1 cancelled + 0 failed).

---

#### Test Scenario B: Cancelled Inside Batch Before Delivery
* **Workload**: Concurrent requests `[A (priority 10), B (priority 8), C (priority 6)]` formed a 3-job batch on Worker 1.
* **Target Request**: Client A timed out after 200ms (during the 700ms batch computation). Clients B and C retained connections.
* **Server Log Output**:
  ```text
  dispatching batch: 10 8 6
  worker 1 picked batch of 3 jobs
  job (priority 10) cancelled, skipping w1
  priority 8 processed by w1
  priority 6 processed by w1
  ```
* **Metrics Snapshot**:
  ```text
  inferd_completed_requests 17 (+2)
  inferd_rejected_requests  0
  inferd_failed_requests    0
  inferd_cancelled_requests 2 (+1)
  inferd_queue_depth        0
  inferd_batch_size_3       6 (+1)
  ```
* **Audit**: Client A was cleanly dropped; clients B and C completed without batch degradation.

---

#### Test Scenario C: Cancellation During Running Computation
* **Workload**: Single request with a 150ms client deadline dispatched as a batch of 1 (500ms compute cost).
* **Observation**: Client aborted at 150.4ms. Worker completed compute, evaluated Checkpoint 2, and skipped result write.
* **Server Log Output**:
  ```text
  dispatching batch: 5
  worker 1 picked batch of 1 jobs
  job (priority 5) cancelled, skipping w1
  ```
* **Metrics Snapshot**:
  ```text
  inferd_completed_requests 17 (+0)
  inferd_rejected_requests  0
  inferd_failed_requests    0
  inferd_cancelled_requests 3 (+1)
  inferd_queue_depth        0
  inferd_p50                709.0895ms
  inferd_p95                710.6169ms
  inferd_p99                710.6169ms
  inferd_batch_size_1       1 (+1)
  inferd_batch_size_3       6
  ```

---

### Terminal Accounting Invariant Proof

Across all 20 executed requests across Tests A, B, and C:

$$\text{Incoming Requests} = \text{Rejected} + \text{Accepted}$$
$$20 = 0 + 20$$

$$\text{Accepted Requests} = \text{Completed} + \text{Failed} + \text{Cancelled}$$
$$20 = 17 + 0 + 3$$

$$\text{Total Divergence} = 0$$

---

## Summary of Invariants Across Wave 1 & Wave 2

| Concept | Architectural Invariant |
|---|---|
| **Admission** | $\text{incoming} = \text{rejected} + \text{accepted}$ |
| **Outcomes** | $\text{accepted} = \text{completed} + \text{failed} + \text{cancelled}$ |
| **Progress State** | `w.remaining` contains only unfinished, uncancelled jobs |
| **Recovery** | $\text{retried} \subseteq \text{accepted}$ (internal transition; not a terminal outcome) |
| **Path Isolation** | `Submit()` handles external admission; `Requeue()` handles internal recovery |
| **Queue Bound** | $\text{queue\_depth} \le 50$ at all times |
