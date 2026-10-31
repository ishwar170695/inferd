# inferd

A distributed LLM inference-serving proxy in Go to study request queuing, concurrency boundaries, health-checking, cancellation pruning, and load-aware dispatching across backend model workers.

## Architecture

- **Load-Aware Scheduler**: Priority-queue dispatch balancing incoming token load and concurrency across healthy model workers.
- **Cancellation Pruning**: Proactively prunes cancelled/abandoned client HTTP request contexts at dispatch and mid-batch to prevent wasted worker execution.
- **Crash Detection & Requeueing**: Detects worker panics/failures and automatically requeues healthy in-flight batch items back to the priority pool with zero dropped requests.

## Benchmarks: Cancellation Pruning & Failure Recovery

Measured under **100 concurrent HTTP requests** across 5 worker instances (batch size 3):

| Scenario | System | Completed (200) | p50 | p95 | p99 | Wasted Worker Time | Recovery Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Normal Workload** | Baseline | 100 | 393ms | 676ms | 676ms | 0s | — |
| | **inferd** | 100 | **390ms** | **673ms** | **674ms** | **0s** | — |
| **25% Client Cancellations** | Baseline | 75 | 287ms | 656ms | 662ms | 915ms (wasted) | — |
| | **inferd** | 75 | **196ms** | **476ms** | **564ms** | **0s (pruned)** | — |
| **Worker Crash & Requeue** | Baseline | 100 | 384ms | 665ms | 668ms | Batch dropped | — |
| | **inferd** | 100 | **387ms** | **665ms** | **740ms** | **0s** | **95.2ms** |

### Key Results
- **Cancellation Pruning**: Pruning abandoned request contexts at dispatch and mid-batch avoided 915ms of wasted compute, dropping **p95 latency by 27.4%** (656ms → 476ms).
- **Crash Recovery**: When simulated worker panics occurred, in-flight batch items were automatically requeued to the priority pool with the first recovered job completing in **95.2ms**.
- **Scheduler Overhead**: Priority queuing and atomic metric updates added **<3ms** overhead at p50 under full load.

### Reproduce
```bash
go test -v -run TestBenchmarkScenarios .
```
