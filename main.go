package main

import (
	"container/heap"
	"flag"
	"fmt"
	"net/http"
)

func main() {
	port := flag.String("port", "8081", "port to listen on")
	flag.Parse()

	m := NewMetrics()
	workers := make([]*Worker, 5)

	scheduler := &Scheduler{
		workers: workers,
		metrics: m,
	}
	heap.Init(&scheduler.queue)
	go scheduler.dispatch()

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
							fmt.Printf("Worker %d panicked: %v\n", worker.id, r)

							for _, j := range worker.remaining {
								if j.cancelled() {
									fmt.Printf("Job (priority %d) cancelled, dropping during recovery\n", j.priority)
									m.IncCancelled()
									continue
								}
								j.retries++
								if j.retries > 3 {
									fmt.Printf("Job (priority %d) failed after %d retries\n", j.priority, j.retries)
									j.result <- "error: job failed after 3 retries\n"
									m.IncFailed()
									m.DecLoad(1)
								} else {
									fmt.Printf("Requeuing job (priority %d, retry %d)\n", j.priority, j.retries)
									if !scheduler.Requeue(j) {
										fmt.Printf("Job (priority %d) retry dropped: queue full\n", j.priority)
										j.result <- "error: queue full during retry\n"
										m.IncFailed()
										m.DecLoad(1)
									}
								}
							}
							worker.remaining = nil
						}
					}()
					process(worker, batch, m)
				}()
			}
		}(w)
	}

	http.HandleFunc("/infer", infer(scheduler))
	http.HandleFunc("/metrics", metricsHandler(m))
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})
	fmt.Printf("inferd listening on :%s\n", *port)
	http.ListenAndServe(":"+*port, nil)
}
