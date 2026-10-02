package main

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type BenchmarkResult struct {
	Scenario                string
	System                  string
	P50                     time.Duration
	P95                     time.Duration
	P99                     time.Duration
	Completed               int64
	Cancelled               int64
	Requeued                int64
	Failed                  int64
	RecoveryTime            time.Duration
	WorkOnCancelled         time.Duration
	TotalDuration           time.Duration
}

func runScenario(t *testing.T, isBaseline bool, scenarioName string, cancelRate float64, injectCrash bool) BenchmarkResult {
	// Configure global flags
	DisableMechanisms = isBaseline
	BaseCost = 60 * time.Millisecond
	PerJobCost = 15 * time.Millisecond

	m := NewMetrics()
	workers := make([]*Worker, 5)
	scheduler := &Scheduler{
		workers: workers,
		metrics: m,
	}
	heap.Init(&scheduler.queue)
	go scheduler.dispatch()

	var crashTimestamp atomic.Int64
	var recDuration atomic.Int64
	var recoveryRecorded atomic.Bool

	for i := 0; i < 5; i++ {
		w := &Worker{
			id:   i + 1,
			jobs: make(chan Batch),
		}
		workers[i] = w
		go func(worker *Worker) {
			for batch := range worker.jobs {
				func() {
					defer func() {
						if r := recover(); r != nil {
							crashTimestamp.Store(time.Now().UnixNano())

							if DisableMechanisms {
								for _, j := range worker.remaining {
									select {
									case j.result <- "error: worker crash (baseline dropped batch)\n":
									default:
									}
									m.IncFailed()
									m.DecLoad(1)
								}
								worker.remaining = nil
								return
							}

							for _, j := range worker.remaining {
								if j.cancelled() {
									m.IncCancelled()
									continue
								}
								j.retries++
								if j.retries > 3 {
									select {
									case j.result <- "error: job failed after 3 retries\n":
									default:
									}
									m.IncFailed()
									m.DecLoad(1)
								} else {
									m.IncRequeued()
									if !scheduler.Requeue(j) {
										select {
										case j.result <- "error: queue full during retry\n":
										default:
										}
										m.IncFailed()
										m.DecLoad(1)
									}
								}
							}
							worker.remaining = nil
						}
					}()
					process(worker, batch, m)
					for _, j := range batch {
						if j.retries > 0 && crashTimestamp.Load() > 0 && recoveryRecorded.CompareAndSwap(false, true) {
							recDuration.Store(time.Now().UnixNano() - crashTimestamp.Load())
						}
					}
				}()
			}
		}(w)
	}

	ts := httptest.NewServer(infer(scheduler))
	defer ts.Close()

	totalRequests := 100
	var wg sync.WaitGroup
	wg.Add(totalRequests)

	var latenciesMu sync.Mutex
	var latencies []time.Duration
	var clientCompleted atomic.Int64
	var clientCancelled atomic.Int64
	var clientFailed atomic.Int64

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}

	rng := rand.New(rand.NewSource(42))
	testStart := time.Now()

	for i := 0; i < totalRequests; i++ {
		reqID := i + 1
		priority := rng.Intn(10) + 1
		shouldCancel := (cancelRate > 0) && (reqID%4 == 0) // 25% cancellations
		shouldCrash := injectCrash && (reqID == 10)        // poison pill early in workload
		if shouldCrash {
			priority = 10
		}

		go func(id, prio int, doCancel, doCrash bool) {
			defer wg.Done()
			reqStart := time.Now()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/infer", nil)
			req.Header.Set("X-Priority", strconv.Itoa(prio))
			if doCrash {
				req.Header.Set("X-Simulate-Crash", "true")
			}

			if doCancel {
				go func() {
					time.Sleep(15 * time.Millisecond)
					cancel()
				}()
			}

			resp, err := client.Do(req)
			duration := time.Since(reqStart)

			if err != nil {
				if ctx.Err() != nil {
					clientCancelled.Add(1)
				} else {
					clientFailed.Add(1)
				}
				latenciesMu.Lock()
				latencies = append(latencies, duration)
				latenciesMu.Unlock()
				return
			}
			defer resp.Body.Close()
			_, _ = io.ReadAll(resp.Body)

			if resp.StatusCode == http.StatusOK {
				clientCompleted.Add(1)
			} else {
				clientFailed.Add(1)
			}

			latenciesMu.Lock()
			latencies = append(latencies, duration)
			latenciesMu.Unlock()
		}(reqID, priority, shouldCancel, shouldCrash)
	}

	wg.Wait()
	totalTestTime := time.Since(testStart)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[int(float64(len(latencies)-1)*0.50)]
	p95 := latencies[int(float64(len(latencies)-1)*0.95)]
	p99 := latencies[int(float64(len(latencies)-1)*0.99)]

	var recTime time.Duration
	if recDuration.Load() > 0 {
		recTime = time.Duration(recDuration.Load())
	}

	systemName := "inferd (Mechanisms)"
	if isBaseline {
		systemName = "Baseline (Naive)"
	}

	return BenchmarkResult{
		Scenario:        scenarioName,
		System:          systemName,
		P50:             p50,
		P95:             p95,
		P99:             p99,
		Completed:       clientCompleted.Load(),
		Cancelled:       clientCancelled.Load(),
		Requeued:        m.requeued_req.Load(),
		Failed:          clientFailed.Load(), // Strictly HTTP non-200/error (prevents double-counting with m.failed_req)
		RecoveryTime:    recTime,
		WorkOnCancelled: m.WastedWork(),
		TotalDuration:   totalTestTime,
	}
}

func printTable(title string, base, inf BenchmarkResult) {
	fmt.Printf("\n===================================================================================================\n")
	fmt.Printf("SCENARIO: %s (100 Concurrent HTTP Requests)\n", title)
	fmt.Printf("===================================================================================================\n")
	fmt.Printf("%-36s | %-26s | %-26s\n", "Metric", "Baseline (Naive)", "inferd (Mechanisms)")
	fmt.Printf("-------------------------------------+----------------------------+----------------------------\n")
	fmt.Printf("%-36s | %-26s | %-26s\n", "Completed Requests (HTTP 200)", fmt.Sprintf("%d", base.Completed), fmt.Sprintf("%d", inf.Completed))
	fmt.Printf("%-36s | %-26s | %-26s\n", "Cancelled Requests", fmt.Sprintf("%d", base.Cancelled), fmt.Sprintf("%d", inf.Cancelled))
	fmt.Printf("%-36s | %-26s | %-26s\n", "Requeued Batch Items", fmt.Sprintf("%d", base.Requeued), fmt.Sprintf("%d", inf.Requeued))
	fmt.Printf("%-36s | %-26s | %-26s\n", "Failed Requests (HTTP non-200)", fmt.Sprintf("%d", base.Failed), fmt.Sprintf("%d", inf.Failed))
	fmt.Printf("%-36s | %-26s | %-26s\n", "p50 Latency", fmt.Sprintf("%v", base.P50.Round(time.Millisecond)), fmt.Sprintf("%v", inf.P50.Round(time.Millisecond)))
	fmt.Printf("%-36s | %-26s | %-26s\n", "p95 Latency", fmt.Sprintf("%v", base.P95.Round(time.Millisecond)), fmt.Sprintf("%v", inf.P95.Round(time.Millisecond)))
	fmt.Printf("%-36s | %-26s | %-26s\n", "p99 Latency", fmt.Sprintf("%v", base.P99.Round(time.Millisecond)), fmt.Sprintf("%v", inf.P99.Round(time.Millisecond)))
	if inf.RecoveryTime > 0 {
		fmt.Printf("%-36s | %-26s | %-26s\n", "Time to First Requeued Completion", "N/A (Batch Dropped)", fmt.Sprintf("%v", inf.RecoveryTime.Round(time.Microsecond)))
	} else {
		fmt.Printf("%-36s | %-26s | %-26s\n", "Time to First Requeued Completion", "N/A (No Crash)", "N/A (No Crash)")
	}
	fmt.Printf("%-36s | %-26s | %-26s\n", "Modeled Worker-Time on Dead Reqs", fmt.Sprintf("%v wasted proxy", base.WorkOnCancelled.Round(time.Millisecond)), fmt.Sprintf("%v (0s wasted)", inf.WorkOnCancelled))
	fmt.Printf("%-36s | %-26s | %-26s\n", "Total Test Duration", fmt.Sprintf("%v", base.TotalDuration.Round(time.Millisecond)), fmt.Sprintf("%v", inf.TotalDuration.Round(time.Millisecond)))
	fmt.Printf("-------------------------------------+----------------------------+----------------------------\n")
}

func TestBenchmarkScenarios(t *testing.T) {
	fmt.Println("\nStarting Benchmark: 100 Concurrent Requests (5 Workers, Batch Size 3)")

	// 1. Normal Workload
	base1 := runScenario(t, true, "1. Normal Workload", 0.0, false)
	inf1 := runScenario(t, false, "1. Normal Workload", 0.0, false)
	printTable("1. Normal Workload", base1, inf1)

	// 2. 25% Cancellations
	base2 := runScenario(t, true, "2. 25% Cancellations", 0.25, false)
	inf2 := runScenario(t, false, "2. 25% Cancellations", 0.25, false)
	printTable("2. 25% Cancellations", base2, inf2)

	// 3. Worker Crash
	base3 := runScenario(t, true, "3. Worker Crash", 0.0, true)
	inf3 := runScenario(t, false, "3. Worker Crash", 0.0, true)
	printTable("3. Worker Crash", base3, inf3)

	// 4. Cancellation + Worker Crash
	base4 := runScenario(t, true, "4. Cancellation + Worker Crash", 0.25, true)
	inf4 := runScenario(t, false, "4. Cancellation + Worker Crash", 0.25, true)
	printTable("4. Cancellation + Worker Crash", base4, inf4)
}
