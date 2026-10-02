package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct {
	URL          *url.URL
	Proxy        *httputil.ReverseProxy
	ReportedLoad atomic.Int64
	InFlight     atomic.Int64
	Alive        atomic.Bool
}

type responseTracker struct {
	http.ResponseWriter
	written bool
	err     error
}

func (w *responseTracker) WriteHeader(status int) {
	w.written = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseTracker) Write(b []byte) (int, error) {
	w.written = true
	return w.ResponseWriter.Write(b)
}

func (w *responseTracker) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func NewBackend(raw string) *Backend {
	u := mustParse(raw)
	b := &Backend{
		URL: u,
	}
	b.Alive.Store(true)

	proxy := httputil.NewSingleHostReverseProxy(u)

	proxy.ModifyResponse = func(res *http.Response) error {
		if val := res.Header.Get("X-Server-Load"); val != "" {
			if load, err := strconv.ParseInt(val, 10, 64); err == nil {
				b.ReportedLoad.Store(load)
			}
		}
		return nil
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		b.Alive.Store(false)
		if tr, ok := w.(*responseTracker); ok {
			tr.err = err
		}
	}

	b.Proxy = proxy
	return b
}

var backends []*Backend = []*Backend{
	NewBackend("http://localhost:8081"),
	NewBackend("http://localhost:8082"),
	NewBackend("http://localhost:8083"),
}

var routerMU sync.Mutex

func (b *Backend) load() int64 {
	return b.ReportedLoad.Load() + b.InFlight.Load()
}

func pickBackend() *Backend {
	routerMU.Lock()
	defer routerMU.Unlock()
	var best *Backend
	for _, b := range backends {
		if b.Alive.Load() && (best == nil || b.load() < best.load()) {
			best = b
		}
	}
	if best != nil {
		best.InFlight.Add(1)
	}
	return best
}

func main() {
	http.HandleFunc("/infer", func(w http.ResponseWriter, r *http.Request) {
		var bodyBytes []byte
		if r.Body != nil {
			bodyBytes, _ = io.ReadAll(r.Body)
			r.Body.Close()
		}

		tracker := &responseTracker{ResponseWriter: w}

		for attempt := 0; attempt < len(backends); attempt++ {
			target := pickBackend()
			if target == nil {
				http.Error(w, "No healthy backends available", http.StatusServiceUnavailable)
				return
			}

			reqCopy := r.Clone(r.Context())
			if bodyBytes != nil {
				reqCopy.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			}

			tracker.err = nil
			target.Proxy.ServeHTTP(tracker, reqCopy)
			target.InFlight.Add(-1)

			if tracker.err == nil || tracker.written {
				return
			}

			fmt.Printf("[router] backend %s failed: %v, retrying...\n", target.URL.Host, tracker.err)
		}

		http.Error(w, "All backend attempts failed", http.StatusBadGateway)
	})

	startHealthChecker(1 * time.Second)

	fmt.Println("Router listening on :8080 (routing to :8081, :8082, :8083)...")
	http.ListenAndServe(":8080", nil)
}

func startHealthChecker(interval time.Duration) {
	client := &http.Client{Timeout: 1 * time.Second}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			for _, b := range backends {
				if !b.Alive.Load() {
					resp, err := client.Get(b.URL.String() + "/health")
					if err == nil && resp.StatusCode == http.StatusOK {
						resp.Body.Close()
						if b.Alive.CompareAndSwap(false, true) {
							fmt.Printf("[router] backend %s recovered (Alive=true)\n", b.URL.Host)
						}
					} else if resp != nil {
						resp.Body.Close()
					}
				}
			}
		}
	}()
}

func mustParse(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}
