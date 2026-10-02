package main

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func infer(scheduler *Scheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// x:=rand.IntN(10)
		priority := 5
		if p := r.Header.Get("X-Priority"); p != "" {
            parsed, err := strconv.Atoi(p)
            if err != nil {
                http.Error(w, "Invalid priority", http.StatusBadRequest)
                return
            }
            priority = parsed
        }
		name := ""
		if r.Header.Get("X-Simulate-Crash") == "true" {
			name = "crash"
		}
		start := time.Now()
		j := &Job{
			name:     name,
			result:   make(chan string, 1),
			priority: priority,
			ctx:      r.Context(),
		}

		if !scheduler.Submit(j) {
			http.Error(w, "Queue Full", http.StatusServiceUnavailable)
			return
		}

		select {
		case result := <-j.result:
			w.Header().Set("X-Server-Load",strconv.FormatInt(scheduler.metrics.total_load.Load(),10))
			fmt.Fprint(w, result)
			latency := time.Since(start)
			scheduler.metrics.AddLatency(latency)
		case <-r.Context().Done():
			return
		}
	}
}