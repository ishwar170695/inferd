package main

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"strconv"
	"sync"
)

type Backend struct {
	URL *url.URL
	Proxy *httputil.ReverseProxy
	ReportedLoad atomic.Int64
	InFlight atomic.Int64
}

func NewBackend(raw string) *Backend{
	u:=mustParse(raw)
	b:=&Backend{
		URL: u,
	}
	proxy := httputil.NewSingleHostReverseProxy(u)

	proxy.ModifyResponse = func (res *http.Response) error {
		if val:=res.Header.Get("X-Server-Load");val!="" {
			if load,err:=strconv.ParseInt(val,10,64);err==nil{
				b.ReportedLoad.Store(load)
			}
		}
		return nil
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
func pickBackend() *Backend {
	routerMU.Lock()
	defer routerMU.Unlock()
	best := backends[0]
	for _,b := range backends[1:]{
		if (b.ReportedLoad.Load()+b.InFlight.Load())<(best.ReportedLoad.Load()+best.InFlight.Load()) {
			best = b
		}
	}
	best.InFlight.Add(1)
	return best
}

func main() {
	http.HandleFunc("/infer", func(w http.ResponseWriter, r *http.Request) {
		target := pickBackend()

		defer target.InFlight.Add(-1)
		target.Proxy.ServeHTTP(w, r)


	})

	fmt.Println("Router listening on :8080 (routing to :8081, :8082, :8083)...")
	http.ListenAndServe(":8080", nil)
}

func mustParse(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}
