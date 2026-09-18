// Package metrics collects API server telemetry for the dashboard. It is
// written for the hot path: recording a request is a handful of atomic
// operations plus one bounded-slice append under a short mutex.
package metrics

import (
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Request is one completed (or newly opened) API call.
type Request struct {
	Time     time.Time
	Method   string
	Path     string
	Resource string
	Duration time.Duration
	Status   int
	Bytes    int64
	Items    int
	Kind     Kind
}

type Kind uint8

const (
	KindOther Kind = iota
	KindList
	KindGet
	KindWatch
	KindWrite
	KindDiscovery
)

func (k Kind) String() string {
	switch k {
	case KindList:
		return "LIST"
	case KindGet:
		return "GET"
	case KindWatch:
		return "WATCH"
	case KindWrite:
		return "WRITE"
	case KindDiscovery:
		return "DISC"
	}
	return "-"
}

// PathStat is the per-endpoint aggregate shown in the hot-paths table.
type PathStat struct {
	Path    string
	Count   int64
	Errors  int64
	TotalMs float64
	MaxMs   float64
	Bytes   int64
	Items   int64
	Last    time.Time
}

const (
	latencySamples = 4096
	recentRequests = 256
	rateWindow     = 10 * time.Second
)

// Collector is the metrics sink shared by the API server and the dashboard.
type Collector struct {
	Start time.Time

	Total    atomic.Int64
	Errors   atomic.Int64
	Bytes    atomic.Int64
	Lists    atomic.Int64
	Gets     atomic.Int64
	Writes   atomic.Int64
	Watches  atomic.Int64 // cumulative watch connections opened
	ActiveWA atomic.Int64 // currently open watch connections
	Items    atomic.Int64
	Chaos500 atomic.Int64
	Chaos429 atomic.Int64
	Gone410  atomic.Int64

	mu       sync.Mutex
	latency  []float64 // ring of recent latencies in ms
	latNext  int
	latFull  bool
	recent   []Request
	recNext  int
	recFull  bool
	paths    map[string]*PathStat
	window   []time.Time
	winStart int

	cpu cpuMeter
}

func New() *Collector {
	return &Collector{
		Start:   time.Now(),
		latency: make([]float64, latencySamples),
		recent:  make([]Request, recentRequests),
		paths:   make(map[string]*PathStat, 64),
		window:  make([]time.Time, 0, 4096),
	}
}

// Record ingests a finished request.
func (c *Collector) Record(r Request) {
	c.Total.Add(1)
	c.Bytes.Add(r.Bytes)
	c.Items.Add(int64(r.Items))
	if r.Status >= 400 {
		c.Errors.Add(1)
	}
	switch r.Kind {
	case KindList:
		c.Lists.Add(1)
	case KindGet:
		c.Gets.Add(1)
	case KindWrite:
		c.Writes.Add(1)
	case KindWatch:
		c.Watches.Add(1)
	}

	// A watch is a connection, not a request: its duration is how long the
	// client stayed subscribed, so counting it would swamp the percentiles.
	ms := float64(r.Duration.Microseconds()) / 1000
	if r.Kind == KindWatch {
		ms = 0
	}

	c.mu.Lock()
	if r.Kind != KindWatch {
		c.latency[c.latNext] = ms
		c.latNext++
		if c.latNext == len(c.latency) {
			c.latNext, c.latFull = 0, true
		}
	}

	c.recent[c.recNext] = r
	c.recNext++
	if c.recNext == len(c.recent) {
		c.recNext, c.recFull = 0, true
	}

	key := r.Method + " " + normalize(r.Path)
	ps := c.paths[key]
	if ps == nil {
		ps = &PathStat{Path: key}
		c.paths[key] = ps
	}
	ps.Count++
	ps.TotalMs += ms
	if ms > ps.MaxMs {
		ps.MaxMs = ms
	}
	ps.Bytes += r.Bytes
	ps.Items += int64(r.Items)
	ps.Last = r.Time
	if r.Status >= 400 {
		ps.Errors++
	}

	c.window = append(c.window, r.Time)
	c.mu.Unlock()
}

// normalize collapses object names and namespaces so the hot-path table
// aggregates per endpoint rather than per object.
func normalize(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "namespaces":
			// /api/v1/namespaces/<ns> — keep the literal when it is the
			// terminal segment (a namespace GET), else collapse it.
			if i+2 < len(parts) {
				parts[i+1] = "{ns}"
			}
		case "pods", "deployments", "replicasets", "services", "nodes",
			"configmaps", "secrets", "events", "jobs", "cronjobs":
			parts[i+1] = "{name}"
		}
	}
	return "/" + strings.Join(parts, "/")
}

// Snapshot is a consistent view of the collector for one UI frame.
type Snapshot struct {
	Uptime       time.Duration
	Total        int64
	Errors       int64
	Bytes        int64
	Items        int64
	Lists        int64
	Gets         int64
	Writes       int64
	WatchesTotal int64
	WatchesLive  int64
	Gone410      int64
	Chaos500     int64
	Chaos429     int64

	RPS      float64
	P50      float64
	P95      float64
	P99      float64
	Max      float64
	Mean     float64
	HotPaths []PathStat
	Recent   []Request

	HeapMB     float64
	SysMB      float64
	Goroutines int
	GCPause    time.Duration
	CPUCores   float64
}

func (c *Collector) Snapshot(maxPaths, maxRecent int) Snapshot {
	now := time.Now()
	s := Snapshot{
		Uptime:       now.Sub(c.Start),
		Total:        c.Total.Load(),
		Errors:       c.Errors.Load(),
		Bytes:        c.Bytes.Load(),
		Items:        c.Items.Load(),
		Lists:        c.Lists.Load(),
		Gets:         c.Gets.Load(),
		Writes:       c.Writes.Load(),
		WatchesTotal: c.Watches.Load(),
		WatchesLive:  c.ActiveWA.Load(),
		Gone410:      c.Gone410.Load(),
		Chaos500:     c.Chaos500.Load(),
		Chaos429:     c.Chaos429.Load(),
	}

	c.mu.Lock()
	cutoff := now.Add(-rateWindow)
	i := 0
	for i < len(c.window) && c.window[i].Before(cutoff) {
		i++
	}
	c.window = append(c.window[:0], c.window[i:]...)
	s.RPS = float64(len(c.window)) / rateWindow.Seconds()

	n := len(c.latency)
	if !c.latFull {
		n = c.latNext
	}
	if n > 0 {
		lat := make([]float64, n)
		copy(lat, c.latency[:n])
		sort.Float64s(lat)
		s.P50 = lat[n*50/100]
		s.P95 = lat[min(n*95/100, n-1)]
		s.P99 = lat[min(n*99/100, n-1)]
		s.Max = lat[n-1]
		sum := 0.0
		for _, v := range lat {
			sum += v
		}
		s.Mean = sum / float64(n)
	}

	paths := make([]PathStat, 0, len(c.paths))
	for _, p := range c.paths {
		paths = append(paths, *p)
	}
	sort.Slice(paths, func(i, j int) bool { return paths[i].TotalMs > paths[j].TotalMs })
	if len(paths) > maxPaths {
		paths = paths[:maxPaths]
	}
	s.HotPaths = paths

	total := len(c.recent)
	if !c.recFull {
		total = c.recNext
	}
	if maxRecent > total {
		maxRecent = total
	}
	s.Recent = make([]Request, 0, maxRecent)
	for k := 0; k < maxRecent; k++ {
		idx := (c.recNext - 1 - k + len(c.recent)*2) % len(c.recent)
		s.Recent = append(s.Recent, c.recent[idx])
	}
	c.mu.Unlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s.HeapMB = float64(m.HeapAlloc) / (1 << 20)
	s.SysMB = float64(m.Sys) / (1 << 20)
	s.Goroutines = runtime.NumGoroutine()
	if m.NumGC > 0 {
		s.GCPause = time.Duration(m.PauseNs[(m.NumGC+255)%256])
	}
	s.CPUCores = c.cpu.sample(now)
	return s
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// CPU
// ---------------------------------------------------------------------------

// cpuMeter converts cumulative process CPU time into a cores figure.
type cpuMeter struct {
	mu       sync.Mutex
	lastSecs float64
	lastAt   time.Time
	cores    float64
}

func (m *cpuMeter) sample(now time.Time) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	used, ok := processCPUSeconds()
	if !ok {
		return 0
	}
	if m.lastAt.IsZero() {
		m.lastSecs, m.lastAt = used, now
		return 0
	}
	if dt := now.Sub(m.lastAt).Seconds(); dt > 0.05 {
		m.cores = (used - m.lastSecs) / dt
		m.lastSecs, m.lastAt = used, now
	}
	if m.cores < 0 {
		m.cores = 0
	}
	return m.cores
}
