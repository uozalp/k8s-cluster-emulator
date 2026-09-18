package apiserver

import (
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
)

func jitterInt64(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return rand.Int63n(n)
}

// ---------------------------------------------------------------------------
// Scale subresource
// ---------------------------------------------------------------------------

func (s *Server) handleScale(w *capture, r *http.Request, ri reqInfo) {
	if r.Method == http.MethodGet {
		s.c.RLock()
		d, ok := s.c.Deployments.Get(ri.namespace, ri.name)
		var out any
		if ok {
			out = d.RenderScale()
		}
		s.c.RUnlock()
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", `deployments "`+ri.name+`" not found`)
			return
		}
		writeJSON(w, out)
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var patch map[string]any
	if err := json.Unmarshal(body, &patch); err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid body")
		return
	}
	replicas, ok := intAt(patch, "spec", "replicas")
	if !ok {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "spec.replicas missing")
		return
	}
	s.applyScale(w, ri.namespace, ri.name, int32(replicas), true)
}

// applyScale writes the new desired replica count. The actual pods are
// created or removed by the reconciler, exactly as in a real cluster.
func (s *Server) applyScale(w *capture, ns, name string, replicas int32, scaleResponse bool) {
	if replicas < 0 {
		replicas = 0
	}
	var (
		target *cluster.Deployment
		from   int32
		out    any
	)
	s.c.Write(func(tx *cluster.Tx) {
		d, ok := s.c.Deployments.Get(ns, name)
		if !ok {
			return
		}
		target, from = d, d.SpecReplicas
		d.SpecReplicas = replicas
		d.Generation++
		d.LastScale = tx.Now.Unix()
		tx.TouchDeployment(d)
		if scaleResponse {
			out = d.RenderScale()
		} else {
			out = d.Render()
		}
	})
	if target == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", `deployments "`+name+`" not found`)
		return
	}
	s.eng.WakeDeployment(target, from, replicas)
	writeJSON(w, out)
}

// ---------------------------------------------------------------------------
// Update / patch
// ---------------------------------------------------------------------------

func (s *Server) handleUpdate(w *capture, r *http.Request, ri reqInfo, v *cluster.View) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	var patch map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &patch); err != nil {
			writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid patch body")
			return
		}
	}

	switch ri.resource {
	case "deployments":
		if n, ok := intAt(patch, "spec", "replicas"); ok {
			s.applyScale(w, ri.namespace, ri.name, int32(n), false)
			return
		}
		if restartRequested(patch) {
			s.restartDeployment(w, ri)
			return
		}
	case "nodes":
		if b, ok := boolAt(patch, "spec", "unschedulable"); ok {
			s.setUnschedulable(w, ri.name, b)
			return
		}
	}

	// Anything else is accepted and echoed back unchanged; the emulator does
	// not model arbitrary mutation, but clients should not see an error.
	s.c.RLock()
	o, ok := v.Get(nsFor(v, ri), ri.name)
	var out any
	if ok {
		out = o.Render()
	}
	s.c.RUnlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", v.Resource+` "`+ri.name+`" not found`)
		return
	}
	writeJSON(w, out)
}

func nsFor(v *cluster.View, ri reqInfo) string {
	if v.Namespaced {
		return ri.namespace
	}
	return ""
}

func restartRequested(patch map[string]any) bool {
	meta, _ := patch["spec"].(map[string]any)
	if meta == nil {
		return false
	}
	tmpl, _ := meta["template"].(map[string]any)
	if tmpl == nil {
		return false
	}
	md, _ := tmpl["metadata"].(map[string]any)
	if md == nil {
		return false
	}
	ann, _ := md["annotations"].(map[string]any)
	if ann == nil {
		return false
	}
	_, ok := ann["kubectl.kubernetes.io/restartedAt"]
	return ok
}

// restartDeployment performs a real rollout: a fresh ReplicaSet takes over
// the replica count and the previous one is scaled to zero.
func (s *Server) restartDeployment(w *capture, ri reqInfo) {
	var (
		target *cluster.Deployment
		out    any
	)
	s.c.Write(func(tx *cluster.Tx) {
		d, ok := s.c.Deployments.Get(ri.namespace, ri.name)
		if !ok {
			return
		}
		target = d
		s.factory.NewRevision(tx, d, tx.Now)
		tx.TouchDeployment(d)
		out = d.Render()
	})
	if target == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", `deployments "`+ri.name+`" not found`)
		return
	}
	s.eng.WakeDeployment(target, target.SpecReplicas, target.SpecReplicas)
	writeJSON(w, out)
}

func (s *Server) setUnschedulable(w *capture, name string, val bool) {
	var out any
	s.c.Write(func(tx *cluster.Tx) {
		n, ok := s.c.Nodes.Get("", name)
		if !ok {
			return
		}
		n.Unschedulable = val
		tx.TouchNode(n)
		out = n.Render()
	})
	if out == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", `nodes "`+name+`" not found`)
		return
	}
	writeJSON(w, out)
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func (s *Server) handleDelete(w *capture, ri reqInfo, v *cluster.View) {
	switch ri.resource {
	case "pods":
		s.deletePod(w, ri)
	case "deployments":
		s.deleteDeployment(w, ri)
	default:
		s.deleteGeneric(w, ri, v)
	}
}

func (s *Server) deletePod(w *capture, ri reqInfo) {
	var (
		target *cluster.Pod
		out    any
	)
	s.c.Write(func(tx *cluster.Tx) {
		p, ok := s.c.Pods.Get(ri.namespace, ri.name)
		if !ok {
			return
		}
		target = p
		tx.MarkPodTerminating(p, s.factory.Duration(s.cfg.Lifecycle.Termination))
		out = p.Render()
	})
	if target == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", `pods "`+ri.name+`" not found`)
		return
	}
	s.eng.WakePod(target)
	writeJSON(w, out)
}

// deleteDeployment scales the workload to zero and lets the reconciler tear
// it down, so deleting a 60,000-replica Deployment does not stall the API.
func (s *Server) deleteDeployment(w *capture, ri reqInfo) {
	var (
		target *cluster.Deployment
		from   int32
	)
	s.c.Write(func(tx *cluster.Tx) {
		d, ok := s.c.Deployments.Get(ri.namespace, ri.name)
		if !ok {
			return
		}
		target, from = d, d.SpecReplicas
		d.Deleting = true
		d.SpecReplicas = 0
		d.Generation++
		tx.RemoveDeployment(d)
	})
	if target == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", `deployments "`+ri.name+`" not found`)
		return
	}
	s.eng.WakeDeployment(target, from, 0)
	writeJSON(w, okStatus(ri.name, "Deployment"))
}

func (s *Server) deleteGeneric(w *capture, ri reqInfo, v *cluster.View) {
	var out any
	s.c.Write(func(tx *cluster.Tx) {
		o, ok := v.Delete(nsFor(v, ri), ri.name)
		if !ok {
			return
		}
		tx.EmitDeleted(v.Resource, o)
		out = o.Render()
	})
	if out == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", v.Resource+` "`+ri.name+`" not found`)
		return
	}
	writeJSON(w, out)
}

func okStatus(name, kind string) map[string]any {
	return map[string]any{
		"kind": "Status", "apiVersion": "v1",
		"metadata": map[string]any{},
		"status":   "Success",
		"details":  map[string]any{"name": name, "kind": kind},
	}
}

// ---------------------------------------------------------------------------
// Pod logs
// ---------------------------------------------------------------------------

var logLines = []string{
	`{"level":"info","ts":"%s","caller":"server/http.go:142","msg":"request completed","method":"GET","path":"/healthz","status":200,"duration":"1.2ms"}`,
	`{"level":"info","ts":"%s","caller":"worker/pool.go:88","msg":"processed batch","items":128,"lag":"12ms"}`,
	`{"level":"warn","ts":"%s","caller":"cache/redis.go:51","msg":"cache miss ratio elevated","ratio":0.42}`,
	`{"level":"error","ts":"%s","caller":"db/pool.go:203","msg":"connection reset by peer","retry":3}`,
	`{"level":"info","ts":"%s","caller":"metrics/exporter.go:64","msg":"scrape served","series":4821}`,
	`{"level":"debug","ts":"%s","caller":"queue/consumer.go:117","msg":"ack","partition":3,"offset":918273}`,
}

func (s *Server) handlePodLog(w *capture, r *http.Request, ri reqInfo) {
	s.c.RLock()
	_, exists := s.c.Pods.Get(ri.namespace, ri.name)
	s.c.RUnlock()
	if !exists {
		writeStatus(w, http.StatusNotFound, "NotFound", `pods "`+ri.name+`" not found`)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.ResponseWriter.(http.Flusher)

	tail := 100
	if v := r.URL.Query().Get("tailLines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n < 10000 {
			tail = n
		}
	}
	base := time.Now().Add(-time.Duration(tail) * time.Second)
	for i := 0; i < tail; i++ {
		line := logLines[rand.Intn(len(logLines))]
		ts := base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano)
		w.Write([]byte(ts + " " + sprintf(line, ts) + "\n"))
	}
	if flusher != nil {
		flusher.Flush()
	}

	if r.URL.Query().Get("follow") != "true" {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case t := <-ticker.C:
			ts := t.UTC().Format(time.RFC3339Nano)
			line := logLines[rand.Intn(len(logLines))]
			if _, err := w.Write([]byte(ts + " " + sprintf(line, ts) + "\n")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

func sprintf(format, arg string) string {
	// The log templates contain exactly one %s.
	for i := 0; i+1 < len(format); i++ {
		if format[i] == '%' && format[i+1] == 's' {
			return format[:i] + arg + format[i+2:]
		}
	}
	return format
}

// ---------------------------------------------------------------------------
// Small JSON helpers
// ---------------------------------------------------------------------------

func intAt(m map[string]any, path ...string) (int64, bool) {
	v, ok := valueAt(m, path...)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}

func boolAt(m map[string]any, path ...string) (bool, bool) {
	v, ok := valueAt(m, path...)
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

func valueAt(m map[string]any, path ...string) (any, bool) {
	cur := any(m)
	for _, p := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = obj[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
