package apiserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
)

// handleMetricsAPI serves metrics.k8s.io. k9s polls it constantly for the
// CPU/MEM columns, which makes it a first-class stress surface rather than a
// nice-to-have.
func (s *Server) handleMetricsAPI(w *capture, r *http.Request, ri reqInfo) {
	if !s.cfg.API.Metrics {
		writeStatus(w, http.StatusNotFound, "NotFound", "metrics API disabled")
		return
	}
	switch ri.resource {
	case "pods":
		s.podMetrics(w, r, ri)
	case "nodes":
		s.nodeMetrics(w, r, ri)
	default:
		writeStatus(w, http.StatusNotFound, "NotFound", "unknown metrics resource")
	}
}

func (s *Server) podMetrics(w *capture, r *http.Request, ri reqInfo) {
	if ri.name != "" {
		s.c.RLock()
		p, ok := s.c.Pods.Get(ri.namespace, ri.name)
		var out any
		if ok {
			out = renderPodMetrics(p)
		}
		s.c.RUnlock()
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", "pod metrics not found")
			return
		}
		writeJSON(w, out)
		return
	}
	s.streamMetricsList(w, r, "PodMetricsList", ri.namespace, func(ns string, cur cluster.Cursor, fn func(any)) (cluster.Cursor, bool) {
		return s.c.Pods.Iterate(ns, cur, listChunk, func(p *cluster.Pod) bool {
			if p.State.Ready() {
				fn(renderPodMetrics(p))
			}
			return true
		})
	})
}

func (s *Server) nodeMetrics(w *capture, r *http.Request, ri reqInfo) {
	if ri.name != "" {
		s.c.RLock()
		n, ok := s.c.Nodes.Get("", ri.name)
		var out any
		if ok {
			out = renderNodeMetrics(n)
		}
		s.c.RUnlock()
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", "node metrics not found")
			return
		}
		writeJSON(w, out)
		return
	}
	s.streamMetricsList(w, r, "NodeMetricsList", "", func(ns string, cur cluster.Cursor, fn func(any)) (cluster.Cursor, bool) {
		return s.c.Nodes.Iterate("", cur, listChunk, func(n *cluster.Node) bool {
			fn(renderNodeMetrics(n))
			return true
		})
	})
}

type metricsIter func(ns string, cur cluster.Cursor, emit func(any)) (cluster.Cursor, bool)

func (s *Server) streamMetricsList(w *capture, r *http.Request, listKind, ns string, iter metricsIter) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"kind":"` + listKind + `","apiVersion":"metrics.k8s.io/v1beta1","items":[`))

	var (
		buf   bytes.Buffer
		enc   = json.NewEncoder(&buf)
		cur   cluster.Cursor
		first = true
		n     int
	)
	for {
		buf.Reset()
		s.c.RLock()
		next, more := iter(ns, cur, func(obj any) {
			if first {
				first = false
			} else {
				buf.WriteByte(',')
			}
			_ = enc.Encode(obj)
			n++
		})
		s.c.RUnlock()
		if buf.Len() > 0 {
			w.Write(buf.Bytes())
		}
		cur = next
		if !more {
			break
		}
	}
	w.Write([]byte(`],"metadata":{"resourceVersion":"` + s.c.RVString() + `"}}` + "\n"))
	w.items = n
}

// scaled turns a request value into a plausible usage value using the pod's
// own deterministic jitter, so repeated scrapes of the same pod move around a
// realistic amount instead of being constant or pure noise.
func scaled(base int64, ratio float64, jitter uint16) int64 {
	f := ratio * (0.6 + 0.8*float64(jitter)/65535)
	v := int64(float64(base) * f)
	if v < 1 {
		v = 1
	}
	return v
}

func renderPodMetrics(p *cluster.Pod) any {
	prof := p.Tmpl.Profile
	now := time.Now().UTC().Format(time.RFC3339)
	containers := make([]kapi.ContainerMetrics, 0, len(p.Tmpl.Containers))
	for i, c := range p.Tmpl.Containers {
		j := p.Jitter + uint16(i*7919)
		cpuNano := scaled(prof.CPUReqM*1_000_000, prof.UsageRatio, j)
		memKi := scaled(prof.MemReqMi*1024, prof.UsageRatio, j)
		containers = append(containers, kapi.ContainerMetrics{
			Name: c.Name,
			Usage: kapi.ResourceList{
				"cpu":    strconv.FormatInt(cpuNano, 10) + "n",
				"memory": strconv.FormatInt(memKi, 10) + "Ki",
			},
		})
	}
	return &kapi.PodMetrics{
		TypeMeta: kapi.TypeMeta{Kind: "PodMetrics", APIVersion: "metrics.k8s.io/v1beta1"},
		Metadata: kapi.ObjectMeta{
			Name:              p.Name,
			Namespace:         p.Tmpl.Namespace,
			CreationTimestamp: now,
			Labels:            p.Tmpl.Labels,
		},
		Timestamp:  now,
		Window:     "30s",
		Containers: containers,
	}
}

func renderNodeMetrics(n *cluster.Node) any {
	now := time.Now().UTC().Format(time.RFC3339)
	// Derive node usage from what is actually scheduled on it.
	cpuNano := int64(n.Pods)*180_000_000 + 250_000_000
	memKi := int64(n.Pods)*180*1024 + 2*1024*1024
	if maxCPU := int64(n.CPUCores) * 1_000_000_000; cpuNano > maxCPU {
		cpuNano = maxCPU
	}
	if maxMem := int64(n.MemGi) * 1024 * 1024; memKi > maxMem {
		memKi = maxMem
	}
	return &kapi.NodeMetrics{
		TypeMeta: kapi.TypeMeta{Kind: "NodeMetrics", APIVersion: "metrics.k8s.io/v1beta1"},
		Metadata: kapi.ObjectMeta{
			Name:              n.Name,
			CreationTimestamp: now,
			Labels:            n.NodeLabels(),
		},
		Timestamp: now,
		Window:    "30s",
		Usage: kapi.ResourceList{
			"cpu":    strconv.FormatInt(cpuNano, 10) + "n",
			"memory": strconv.FormatInt(memKi, 10) + "Ki",
		},
	}
}
