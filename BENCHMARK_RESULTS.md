# Benchmark Results: Baseline vs inferd

Comparative benchmark measuring cancellation pruning, worker-time savings, and worker failure recovery under **100 concurrent HTTP requests** across 5 workers (batch size 3).

### Reproduce
```bash
go test -v -run TestBenchmarkScenarios .
```

---

### Results (100 Concurrent Requests)

| Scenario | System | Completed (200) | Cancelled | Requeued Items | Failed | $p50$ | $p95$ | $p99$ | First Requeued Completion | Modeled Worker-Time on Dead Reqs |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Normal Workload** | Baseline | 100 | 0 | 0 | 0 | 393ms | 676ms | 676ms | N/A | 0s |
| | **inferd** | 100 | 0 | 0 | 0 | 390ms | 673ms | 674ms | N/A | 0s |
| **2. 25% Cancellations** | Baseline | 75 | 25 | 0 | 0 | 287ms | 656ms | 662ms | N/A | 915ms wasted proxy |
| | **inferd** | 75 | 25 | 0 | 0 | **196ms** | **476ms** | **564ms** | N/A | **0s wasted** |
| **3. Worker Crash** | Baseline | 100 | 0 | 0 | 0 | 384ms | 665ms | 668ms | N/A (Batch dropped) | 0s |
| | **inferd** | 100 | 0 | **9** | 0 | 387ms | 665ms | 740ms | **95.2ms** | 0s |
| **4. Cancel + Crash** | Baseline | 75 | 25 | 0 | 0 | 295ms | 670ms | 674ms | N/A (Batch dropped) | 875ms wasted proxy |
| | **inferd** | 75 | 25 | **9** | 0 | **283ms** | **565ms** | **567ms** | **95.2ms** | **0s wasted** |

---

### Key Findings

1. **Cancellation Pruning:** Pruning dead contexts at dispatch and mid-batch avoided 915ms of modeled worker execution, dropping $p95$ latency from 656ms to 476ms (-27.4%).
2. **Failure Recovery:** On simulated worker panics, healthy in-flight batch items were requeued back into the priority queue, with the first recovered job completing within 95.2ms of the crash event.
3. **Scheduler Overhead:** Under standard load without failures, priority-queue dispatch and atomic metric updates showed negligible overhead (<3ms delta at $p50$).
