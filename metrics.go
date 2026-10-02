package main

import (
	"sync"
	"sync/atomic"
	"time"
	"sort"
	"fmt"
	"net/http"
)

type Metrics struct {
	// need to add padding so that when any of these lock the cache line, the other won't have to validate/wait for the cache line to unlockeed as in newer systems..memory is not read byte by byte, it is read in chunks usually 64bits
	// _ [56]byte
	completed_req atomic.Int64
	// _ [56]byte
	rejected_req atomic.Int64
	// _ [56]byte
	failed_req atomic.Int64
	// _ [56]byte
	cancelled_req atomic.Int64
	requeued_req  atomic.Int64
	wasted_work   atomic.Int64 // nanoseconds spent computing dead/cancelled jobs
	// _ [56]byte
	queue_depth    atomic.Int64
	// _ [56]byte
	total_load atomic.Int64

	latency sync.Mutex
	latencies []time.Duration

	batchSizeMu sync.Mutex
	batchSizes []int64
	
}

func (m *Metrics) IncRejected() {
	m.rejected_req.Add(1)
}

func (m *Metrics) IncFailed() {
	m.failed_req.Add(1)
}

func (m *Metrics) IncCancelled() {
	m.cancelled_req.Add(1)
}

func (m *Metrics) IncRequeued() {
	m.requeued_req.Add(1)
}

func (m *Metrics) AddWastedWork(d time.Duration) {
	m.wasted_work.Add(int64(d))
}

func (m *Metrics) WastedWork() time.Duration {
	return time.Duration(m.wasted_work.Load())
}

func (m *Metrics) IncCompleted(i int64) {
	m.completed_req.Add(i)
}

func (m *Metrics) CurrentDepth() int {
	return int(m.queue_depth.Load())
}

func (m *Metrics) IncQueueDepth() {
	m.queue_depth.Add(1)
}

func (m *Metrics) DecQueueDepth() {
	m.queue_depth.Add(-1)
}

func (m *Metrics) AddLoad(){
	m.total_load.Add(1)
}

func (m *Metrics) DecLoad(i int64){
	m.total_load.Add(-i)
}

func (m *Metrics) AddLatency(d time.Duration){
	m.latency.Lock()
	m.latencies = append(m.latencies,d)
	m.latency.Unlock()
}

func (m *Metrics) percentile(p int) time.Duration{
	m.latency.Lock()
	sort.Slice(m.latencies, func(i, j int) bool {
		return m.latencies[i] < m.latencies[j]
	})
	defer m.latency.Unlock()
	if len(m.latencies) == 0 {
		return 0
	}
	idx:= int(float64(len(m.latencies)-1)*float64(p)/100.0)
	return m.latencies[idx]	
}

func (m *Metrics) P99() time.Duration{
	return m.percentile(99)
}

func (m *Metrics) P95() time.Duration{
	return m.percentile(95)
}

func (m *Metrics) P50() time.Duration{
	return m.percentile(50)
}

func (m *Metrics) AddBatchSize(i int64){
	m.batchSizeMu.Lock()
	defer m.batchSizeMu.Unlock()
	m.batchSizes[i-1]++
}

func (m *Metrics) BatchSizes() []int64 {
	m.batchSizeMu.Lock()
	defer m.batchSizeMu.Unlock()
	b := make([]int64, len(m.batchSizes))
    copy(b, m.batchSizes)
    return b
}

func NewMetrics() *Metrics {
	return &Metrics{
		batchSizes:make([]int64,4),
	}
}

func metricsHandler(m *Metrics) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, "inferd_completed_requests %d\n", m.completed_req.Load())
        fmt.Fprintf(w, "inferd_rejected_requests %d\n", m.rejected_req.Load())
        fmt.Fprintf(w, "inferd_failed_requests %d\n", m.failed_req.Load())
        fmt.Fprintf(w, "inferd_cancelled_requests %d\n", m.cancelled_req.Load())
        fmt.Fprintf(w, "inferd_queue_depth %d\n", m.queue_depth.Load())
		fmt.Fprintf(w, "inferd_p99 %s\n", m.P99())
		fmt.Fprintf(w, "inferd_p95 %s\n", m.P95())
		fmt.Fprintf(w, "inferd_p50 %s\n", m.P50())
		fmt.Fprintf(w, "inferd_load %d\n",m.total_load.Load())
		
		b := m.BatchSizes()
		for i,b := range b{
			fmt.Fprintf(w, "inferd_batch_size_%d %d\n",i+1,b)
		}
    }
}
