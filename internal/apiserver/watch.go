package apiserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
	"github.com/danske-spil/k8s-cluster-emulator/internal/metrics"
)

const (
	watchQueue       = 2048
	bookmarkInterval = 30 * time.Second
)

func metaOf(obj any) *kapi.ObjectMeta {
	if m, ok := obj.(kapi.MetaAccessor); ok {
		return m.GetObjectMeta()
	}
	return nil
}

// renderedField pulls a field-selector path out of an already-serialized
// object, so watch filtering costs nothing unless a selector is present.
func renderedField(obj any, meta *kapi.ObjectMeta, path string) (string, bool) {
	switch path {
	case "metadata.name":
		if meta != nil {
			return meta.Name, true
		}
	case "metadata.namespace":
		if meta != nil {
			return meta.Namespace, true
		}
	}
	switch o := obj.(type) {
	case *kapi.Pod:
		switch path {
		case "spec.nodeName":
			return o.Spec.NodeName, true
		case "status.phase":
			return o.Status.Phase, true
		}
	case *kapi.Event:
		switch path {
		case "type":
			return o.Type, true
		case "reason":
			return o.Reason, true
		case "involvedObject.kind":
			return o.InvolvedObject.Kind, true
		case "involvedObject.name":
			return o.InvolvedObject.Name, true
		case "involvedObject.uid":
			return o.InvolvedObject.UID, true
		}
	case *kapi.Node:
		if path == "spec.unschedulable" {
			return strconv.FormatBool(o.Spec.Unschedulable), true
		}
	}
	return "", false
}

func noticeMatches(obj any, ns string, f listFilter) bool {
	meta := metaOf(obj)
	if ns != "" && (meta == nil || meta.Namespace != ns) {
		return false
	}
	if !f.labels.Empty() {
		if meta == nil || !f.labels.Matches(meta.Labels) {
			return false
		}
	}
	if !f.fields.Empty() {
		if !f.fields.Matches(func(p string) (string, bool) { return renderedField(obj, meta, p) }) {
			return false
		}
	}
	return true
}

func (s *Server) handleWatch(w *capture, r *http.Request, ri reqInfo, v *cluster.View, rec *metrics.Request) {
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if !ok {
		writeStatus(w, http.StatusInternalServerError, "InternalError", "streaming unsupported")
		return
	}
	q := r.URL.Query()
	filter, err := parseFilter(q)
	if err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid selector: "+err.Error())
		return
	}
	ns := ri.namespace
	if !v.Namespaced {
		ns = ""
	}

	var since uint64
	if rv := q.Get("resourceVersion"); rv != "" && rv != "0" {
		if n, err := strconv.ParseUint(rv, 10, 64); err == nil {
			since = n
		}
	}
	if since > s.c.RV() {
		since = 0 // client is ahead of us; just stream from now
	}

	sendInitial := q.Get("sendInitialEvents") == "true"
	if sendInitial {
		since = 0
	}

	replay, sub, err := v.Hub.Subscribe(since, watchQueue)
	if err != nil {
		if errors.Is(err, cluster.ErrTooOld) {
			s.m.Gone410.Add(1)
			writeStatus(w, http.StatusGone, "Expired",
				"too old resource version: "+q.Get("resourceVersion"))
			return
		}
		writeStatus(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer sub.Close()

	s.delay(metrics.KindWatch)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	s.m.ActiveWA.Add(1)
	defer s.m.ActiveWA.Add(-1)

	bw := bufio.NewWriterSize(w, 32<<10)
	enc := json.NewEncoder(bw)
	write := func(typ string, obj any) bool {
		return enc.Encode(kapi.WatchEvent{Type: typ, Object: obj}) == nil
	}

	if sendInitial {
		if !s.streamInitial(bw, enc, flusher, v, ns, filter, rec) {
			return
		}
		if !write("BOOKMARK", bookmarkObject(v, s.c.RVString(), true)) {
			return
		}
	}
	for _, n := range replay {
		if !noticeMatches(n.Object, ns, filter) {
			continue
		}
		if !write(n.Type, n.Object) {
			return
		}
		rec.Items++
	}
	if bw.Flush() != nil {
		return
	}
	flusher.Flush()

	ctx := r.Context()
	if secs := q.Get("timeoutSeconds"); secs != "" {
		if n, err := strconv.Atoi(secs); err == nil && n > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(n)*time.Second)
			defer cancel()
		}
	}

	bookmarks := q.Get("allowWatchBookmarks") == "true"
	ticker := time.NewTicker(bookmarkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if bookmarks {
				if !write("BOOKMARK", bookmarkObject(v, s.c.RVString(), false)) {
					return
				}
			}
			if bw.Flush() != nil {
				return
			}
			flusher.Flush()
		case n, open := <-sub.C():
			if !open {
				// Subscription was torn down: either the watcher fell behind
				// or watch churn cut it. Ending the stream forces a re-LIST,
				// which is exactly what a real apiserver would cause.
				return
			}
			if noticeMatches(n.Object, ns, filter) {
				if !write(n.Type, n.Object) {
					return
				}
				rec.Items++
			}
			// Drain whatever else is buffered before paying for a flush.
			drained := true
			for drained {
				select {
				case n2, open2 := <-sub.C():
					if !open2 {
						bw.Flush()
						flusher.Flush()
						return
					}
					if noticeMatches(n2.Object, ns, filter) {
						if !write(n2.Type, n2.Object) {
							return
						}
						rec.Items++
					}
				default:
					drained = false
				}
			}
			if bw.Flush() != nil {
				return
			}
			flusher.Flush()
			if sub.Overflowed() {
				return
			}
		}
	}
}

// streamInitial replays current state as ADDED events for clients that use
// sendInitialEvents instead of a separate LIST.
func (s *Server) streamInitial(bw *bufio.Writer, enc *json.Encoder, flusher http.Flusher, v *cluster.View, ns string, filter listFilter, rec *metrics.Request) bool {
	cur := cluster.Cursor{}
	for {
		var batch []any
		s.c.RLock()
		next, more := v.Iterate(ns, cur, listChunk, func(o cluster.Object) bool {
			if filter.matches(o) {
				batch = append(batch, o.Render())
			}
			return true
		})
		s.c.RUnlock()

		for _, obj := range batch {
			if enc.Encode(kapi.WatchEvent{Type: "ADDED", Object: obj}) != nil {
				return false
			}
			rec.Items++
		}
		if bw.Flush() != nil {
			return false
		}
		flusher.Flush()
		cur = next
		if !more {
			return true
		}
	}
}

func bookmarkObject(v *cluster.View, rv string, initialEnd bool) any {
	meta := map[string]any{"resourceVersion": rv, "creationTimestamp": nil}
	if initialEnd {
		meta["annotations"] = map[string]string{"k8s.io/initial-events-end": "true"}
	}
	return map[string]any{
		"kind":       v.Kind,
		"apiVersion": v.GroupVersion,
		"metadata":   meta,
	}
}

// StartWatchChurn periodically severs every watch so clients are forced back
// through a full LIST, which is the single most expensive thing k9s does.
func (s *Server) StartWatchChurn(ctx context.Context) {
	if !s.cfg.API.WatchChurn {
		return
	}
	go func() {
		interval := s.cfg.API.WatchChurnInterval.D()
		t := time.NewTimer(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for _, v := range s.c.Views() {
					v.Hub.DisconnectAll()
				}
				t.Reset(interval/2 + time.Duration(jitterInt64(int64(interval))))
			}
		}
	}()
}
