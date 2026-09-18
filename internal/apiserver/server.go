// Package apiserver exposes the fake cluster over a Kubernetes-compatible
// HTTP API: discovery, LIST with pagination and selectors, WATCH with replay,
// and the handful of write paths k9s actually uses.
package apiserver

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/config"
	"github.com/danske-spil/k8s-cluster-emulator/internal/generate"
	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
	"github.com/danske-spil/k8s-cluster-emulator/internal/metrics"
	"github.com/danske-spil/k8s-cluster-emulator/internal/simulate"
)

// Server routes Kubernetes API requests to the in-memory cluster.
type Server struct {
	cfg     config.Scenario
	c       *cluster.Cluster
	eng     *simulate.Engine
	m       *metrics.Collector
	factory *generate.Factory
}

func New(cfg config.Scenario, c *cluster.Cluster, eng *simulate.Engine, f *generate.Factory, m *metrics.Collector) *Server {
	return &Server{cfg: cfg, c: c, eng: eng, factory: f, m: m}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serve)
}

// ---------------------------------------------------------------------------
// Request info
// ---------------------------------------------------------------------------

type reqInfo struct {
	group       string
	version     string
	namespace   string
	resource    string
	name        string
	subresource string
	ok          bool
}

func parsePath(p string) reqInfo {
	var ri reqInfo
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) == 0 {
		return ri
	}
	i := 0
	switch parts[0] {
	case "api":
		if len(parts) < 2 {
			return ri
		}
		ri.version = parts[1]
		i = 2
	case "apis":
		if len(parts) < 3 {
			return ri
		}
		ri.group, ri.version = parts[1], parts[2]
		i = 3
	default:
		return ri
	}
	if i >= len(parts) {
		return ri
	}
	if parts[i] == "namespaces" && len(parts) > i+2 {
		ri.namespace = parts[i+1]
		i += 2
	}
	ri.resource = parts[i]
	i++
	if i < len(parts) {
		ri.name = parts[i]
		i++
	}
	if i < len(parts) {
		ri.subresource = parts[i]
	}
	ri.ok = ri.resource != ""
	return ri
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rc := &capture{ResponseWriter: w, status: http.StatusOK}
	rec := metrics.Request{Time: start, Method: r.Method, Path: r.URL.Path}

	defer func() {
		rec.Duration = time.Since(start)
		rec.Status = rc.status
		rec.Bytes = rc.bytes
		rec.Items = rc.items
		s.m.Record(rec)
	}()

	path := r.URL.Path
	watch := r.URL.Query().Get("watch") == "true" || r.URL.Query().Get("watch") == "1"

	switch path {
	case "", "/":
		rec.Kind = metrics.KindDiscovery
		s.delay(rec.Kind)
		writeJSON(rc, map[string]any{"paths": []string{"/api", "/api/v1", "/apis", "/healthz", "/version"}})
		return
	case "/healthz", "/readyz", "/livez":
		rc.Header().Set("Content-Type", "text/plain")
		rc.Write([]byte("ok"))
		return
	case "/version":
		rec.Kind = metrics.KindDiscovery
		s.delay(rec.Kind)
		writeJSON(rc, versionInfo())
		return
	case "/api":
		rec.Kind = metrics.KindDiscovery
		s.delay(rec.Kind)
		writeJSON(rc, s.apiVersions())
		return
	case "/apis":
		rec.Kind = metrics.KindDiscovery
		s.delay(rec.Kind)
		writeJSON(rc, s.apiGroups())
		return
	}

	if strings.HasPrefix(path, "/openapi/") {
		writeJSON(rc, map[string]any{"swagger": "2.0", "info": map[string]string{"title": "k8s-cluster-emulator", "version": "v1.30.2"}})
		return
	}

	ri := parsePath(path)
	if !ri.ok {
		// Group/version discovery, e.g. /api/v1 or /apis/apps/v1.
		if list, ok := s.resourceList(path); ok {
			rec.Kind = metrics.KindDiscovery
			s.delay(rec.Kind)
			writeJSON(rc, list)
			return
		}
		writeStatus(rc, http.StatusNotFound, "NotFound", "the server could not find the requested resource")
		return
	}
	rec.Resource = ri.resource

	// Authorization shims: k9s asks what it is allowed to do at startup.
	if ri.group == "authorization.k8s.io" {
		rec.Kind = metrics.KindDiscovery
		s.delay(rec.Kind)
		s.handleAuthz(rc, r, ri)
		return
	}
	if ri.group == "apiextensions.k8s.io" && ri.resource == "customresourcedefinitions" {
		s.emptyList(rc, "CustomResourceDefinitionList", "apiextensions.k8s.io/v1", watch, r)
		return
	}
	if ri.group == "metrics.k8s.io" {
		rec.Kind = metrics.KindList
		s.handleMetricsAPI(rc, r, ri)
		return
	}

	v, ok := s.c.LookupView(ri.group, ri.version, ri.resource)
	if !ok {
		// Known-but-unpopulated kinds still need a valid empty response so
		// k9s renders an empty view instead of an error.
		if kind, gv, known := emptyKind(ri.group, ri.version, ri.resource); known {
			s.emptyList(rc, kind, gv, watch, r)
			return
		}
		writeStatus(rc, http.StatusNotFound, "NotFound",
			fmt.Sprintf("the server could not find the requested resource (%s)", ri.resource))
		return
	}

	if s.chaos(rc, &rec) {
		return
	}

	switch {
	case ri.subresource == "log" && ri.name != "":
		rec.Kind = metrics.KindGet
		s.handlePodLog(rc, r, ri)
	case ri.subresource == "scale" && ri.resource == "deployments":
		rec.Kind = metrics.KindWrite
		s.handleScale(rc, r, ri)
	case watch:
		rec.Kind = metrics.KindWatch
		s.handleWatch(rc, r, ri, v, &rec)
	case r.Method == http.MethodGet && ri.name != "":
		rec.Kind = metrics.KindGet
		s.delay(rec.Kind)
		s.handleGet(rc, ri, v)
	case r.Method == http.MethodGet:
		rec.Kind = metrics.KindList
		s.handleList(rc, r, ri, v, &rec)
	case r.Method == http.MethodDelete && ri.name != "":
		rec.Kind = metrics.KindWrite
		s.delay(rec.Kind)
		s.handleDelete(rc, ri, v)
	case (r.Method == http.MethodPatch || r.Method == http.MethodPut) && ri.name != "":
		rec.Kind = metrics.KindWrite
		s.delay(rec.Kind)
		s.handleUpdate(rc, r, ri, v)
	default:
		writeStatus(rc, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

// ---------------------------------------------------------------------------
// Latency & chaos
// ---------------------------------------------------------------------------

// delay models the fact that a real API server is not instantaneous, and that
// the cost of a request depends on what it touches.
func (s *Server) delay(kind metrics.Kind) {
	base := s.cfg.API.Latency.D()
	if base <= 0 {
		return
	}
	var factor float64
	switch kind {
	case metrics.KindDiscovery:
		factor = 0.15
	case metrics.KindGet:
		factor = 0.4
	case metrics.KindWrite:
		factor = 0.8
	case metrics.KindList:
		factor = 1.5
	case metrics.KindWatch:
		factor = 0.3
	default:
		factor = 0.5
	}
	d := time.Duration(float64(base) * factor)
	if j := s.cfg.API.Jitter; j > 0 {
		d = time.Duration(float64(d) * (1 - j + 2*j*rand.Float64()))
	}
	if d > 0 {
		time.Sleep(d)
	}
}

// serializeCost is the per-item price of writing a list page, which is what
// makes a 100,000-pod LIST expensive rather than a fixed delay.
func (s *Server) serializeCost(items int) {
	if d := time.Duration(items) * s.cfg.API.PerObjectLatency.D(); d > 0 {
		time.Sleep(d)
	}
}

func (s *Server) chaos(w *capture, rec *metrics.Request) bool {
	if !s.cfg.API.Chaos {
		return false
	}
	roll := rand.Float64() * 100
	switch {
	case roll < s.cfg.API.ChaosPercent*0.4:
		s.m.Chaos500.Add(1)
		writeStatus(w, http.StatusInternalServerError, "InternalError", "the server had an internal error")
		return true
	case roll < s.cfg.API.ChaosPercent*0.8:
		s.m.Chaos429.Add(1)
		w.Header().Set("Retry-After", "1")
		writeStatus(w, http.StatusTooManyRequests, "TooManyRequests", "the server has received too many requests")
		return true
	case roll < s.cfg.API.ChaosPercent:
		time.Sleep(time.Duration(rand.Intn(750)) * time.Millisecond)
	}
	return false
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

type capture struct {
	http.ResponseWriter
	status int
	bytes  int64
	items  int
	wrote  bool
}

func (c *capture) WriteHeader(code int) {
	if c.wrote {
		return
	}
	c.wrote = true
	c.status = code
	c.ResponseWriter.WriteHeader(code)
}

func (c *capture) Write(b []byte) (int, error) {
	c.wrote = true
	n, err := c.ResponseWriter.Write(b)
	c.bytes += int64(n)
	return n, err
}

func (c *capture) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func writeJSON(w http.ResponseWriter, obj any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(obj)
}

func writeStatus(w http.ResponseWriter, code int, reason, msg string) {
	st := kapi.NewStatus(code, reason, msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(st)
}

func versionInfo() kapi.Info {
	return kapi.Info{
		Major: "1", Minor: "30",
		GitVersion: "v1.30.2-emulator", GitCommit: "0000000000000000000000000000000000000000",
		GitTreeState: "clean", BuildDate: "2024-06-11T00:00:00Z",
		GoVersion: runtime.Version(), Compiler: "gc",
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
	}
}
