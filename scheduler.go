package main

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

type PriorityQueue []*Job

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(*Job)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}

type Job struct {
	name     string
	priority int
	result   chan string
	retries  int
	ctx      context.Context
}

func (j *Job) cancelled() bool {
	if j.ctx == nil {
		return false
	}
	select {
	case <-j.ctx.Done():
		return true
	default:
		return false
	}
}

type Batch []*Job

type Worker struct {
	id        int
	jobs      chan Batch
	remaining Batch
}

type Scheduler struct {
	mu      sync.Mutex
	queue   PriorityQueue
	workers []*Worker
	metrics *Metrics
}

const maxQueueCapacity = 150

var (
	BaseCost          = 500 * time.Millisecond
	PerJobCost        = 100 * time.Millisecond
	DisableMechanisms = false
)

func (s *Scheduler) Submit(j *Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() >= maxQueueCapacity {
		s.metrics.IncRejected()
		return false
	}
	heap.Push(&s.queue, j)
	s.metrics.IncQueueDepth()
	s.metrics.AddLoad()
	return true
}

func (s *Scheduler) Requeue(j *Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() >= maxQueueCapacity {
		return false
	}

	heap.Push(&s.queue, j)
	s.metrics.IncQueueDepth()
	return true
}

func (s *Scheduler) dispatch() {
	
	for {
		s.mu.Lock()
		if s.queue.Len() == 0 {
			s.mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			continue
		}

		maxBatchSize := 3
		if s.queue.Len() < maxBatchSize {
			s.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			s.mu.Lock()
		}

		var batch Batch
		for s.queue.Len() > 0 && len(batch) < maxBatchSize {
			k := heap.Pop(&s.queue).(*Job)
			s.metrics.DecQueueDepth()

			if !DisableMechanisms && k.cancelled() {
				fmt.Printf("job (priority %d) cancelled while queued\n", k.priority)
				s.metrics.IncCancelled()
				s.metrics.DecLoad(1)
				continue
			}

			batch = append(batch, k)
		}

		s.mu.Unlock()

		if len(batch) == 0 {
			continue
		}

		assigned := false
		for _, w := range s.workers {
			select {
			case w.jobs <- batch:
				assigned = true
				break
			default:
			}
			if assigned {
				break
			}
		}

		if !assigned {
			s.mu.Lock()
			for _, j := range batch {
				heap.Push(&s.queue, j)
				s.metrics.IncQueueDepth()
			}
			s.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		} else {
			s.metrics.AddBatchSize(int64(len(batch)))
			fmt.Printf("dispatching batch:")
			for _, j := range batch {
				fmt.Printf(" %d", j.priority)
			}
			fmt.Println()
		}
	}
}

func process(w *Worker, batch Batch, m *Metrics) {

	fmt.Printf("worker %d picked batch of %d jobs\n", w.id, len(batch))

	w.remaining = batch
	time.Sleep(BaseCost + PerJobCost*time.Duration(len(batch)-1))

	for i, j := range batch {
		if j.name == "crash" {
			panic(fmt.Sprintf("worker %d crashed on poison pill job %d", w.id, j.priority))
		}

		if j.cancelled() {
			if !DisableMechanisms {
				fmt.Printf("job (priority %d) cancelled, skipping w%d\n", j.priority, w.id)
				m.IncCancelled()
				m.DecLoad(1)
				w.remaining = batch[i+1:]
				continue
			} else {
				// Baseline does not skip cancelled jobs: tracks wasted work
				m.IncCancelled()
				m.AddWastedWork(PerJobCost + BaseCost/time.Duration(len(batch)))
			}
		}

		fmt.Printf("priority %d processed by w%d\n", j.priority, w.id)
		select {
		case j.result <- fmt.Sprintf("priority %d processed by w%d\n", j.priority, w.id):
		default:
		}
		m.IncCompleted(1)
		m.DecLoad(1)
		w.remaining = batch[i+1:]
	}
	w.remaining = nil
}
