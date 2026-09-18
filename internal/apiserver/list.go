package apiserver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
	"github.com/danske-spil/k8s-cluster-emulator/internal/metrics"
)

// listChunk is how many stored objects are rendered per acquisition of the
// cluster read lock. Small enough that the simulation engine is never blocked
// for long, large enough that a 100k-pod LIST is not lock-bound.
const listChunk = 500

type listFilter struct {
	labels *kapi.LabelSelectorExpr
	fields *kapi.FieldSelectorExpr
}

func parseFilter(q map[string][]string) (listFilter, error) {
	var f listFilter
	var err error
	if v := first(q, "labelSelector"); v != "" {
		if f.labels, err = kapi.ParseLabelSelector(v); err != nil {
			return f, err
		}
	}
	if v := first(q, "fieldSelector"); v != "" {
		if f.fields, err = kapi.ParseFieldSelector(v); err != nil {
			return f, err
		}
	}
	return f, nil
}

func (f listFilter) empty() bool { return f.labels.Empty() && f.fields.Empty() }

func (f listFilter) matches(o cluster.Object) bool {
	if !f.labels.Matches(o.ObjLabels()) {
		return false
	}
	return f.fields.Matches(o.FieldValue)
}

func first(q map[string][]string, key string) string {
	if v, ok := q[key]; ok && len(v) > 0 {
		return v[0]
	}
	return ""
}

func encodeContinue(cur cluster.Cursor) string {
	raw := strconv.Itoa(cur.NSIdx) + "." + strconv.Itoa(cur.Slot)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeContinue(tok string) (cluster.Cursor, bool) {
	if tok == "" {
		return cluster.Cursor{}, true
	}
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return cluster.Cursor{}, false
	}
	parts := strings.SplitN(string(raw), ".", 2)
	if len(parts) != 2 {
		return cluster.Cursor{}, false
	}
	ns, err1 := strconv.Atoi(parts[0])
	slot, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return cluster.Cursor{}, false
	}
	return cluster.Cursor{NSIdx: ns, Slot: slot}, true
}

// handleList streams a collection. Items are emitted before the list
// metadata so the continue token can be computed while streaming instead of
// buffering the entire response.
func (s *Server) handleList(w *capture, r *http.Request, ri reqInfo, v *cluster.View, rec *metrics.Request) {
	q := r.URL.Query()
	filter, err := parseFilter(q)
	if err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid selector: "+err.Error())
		return
	}

	limit := s.cfg.API.DefaultListLimit
	if lv := q.Get("limit"); lv != "" {
		if n, err := strconv.Atoi(lv); err == nil && n > 0 {
			limit = n
		}
	}
	cur, ok := decodeContinue(q.Get("continue"))
	if !ok {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid continue token")
		return
	}

	ns := ri.namespace
	if !v.Namespaced {
		ns = ""
	}
	listRV := s.c.RVString()
	s.delay(metrics.KindList)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")

	prefix := `{"kind":"` + v.ListKind + `","apiVersion":"` + v.GroupVersion + `","items":[`
	w.Write([]byte(prefix))

	var (
		buf      bytes.Buffer
		enc      = json.NewEncoder(&buf)
		sent     int
		firstOut = true
		contTok  string
	)
	for {
		buf.Reset()
		stop := false
		s.c.RLock()
		next, more := v.Iterate(ns, cur, listChunk, func(o cluster.Object) bool {
			if !filter.matches(o) {
				return true
			}
			if firstOut {
				firstOut = false
			} else {
				buf.WriteByte(',')
			}
			_ = enc.Encode(o.Render())
			sent++
			if limit > 0 && sent >= limit {
				stop = true
				return false
			}
			return true
		})
		s.c.RUnlock()

		if buf.Len() > 0 {
			w.Write(buf.Bytes())
		}
		s.serializeCost(listChunk)
		cur = next
		if stop && more {
			contTok = encodeContinue(cur)
			break
		}
		if !more {
			break
		}
	}

	w.Write([]byte(`],"metadata":{"resourceVersion":"` + listRV + `"`))
	if contTok != "" {
		s.c.RLock()
		remaining := int64(v.Count(ns) - sent)
		s.c.RUnlock()
		if remaining < 0 {
			remaining = 0
		}
		w.Write([]byte(`,"continue":"` + contTok + `","remainingItemCount":` + strconv.FormatInt(remaining, 10)))
	}
	w.Write([]byte("}}\n"))

	rec.Items = sent
	w.items = sent
}

func (s *Server) handleGet(w *capture, ri reqInfo, v *cluster.View) {
	ns := ri.namespace
	if !v.Namespaced {
		ns = ""
	}
	s.c.RLock()
	o, ok := v.Get(ns, ri.name)
	var rendered any
	if ok {
		rendered = o.Render()
	}
	s.c.RUnlock()

	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound",
			v.Resource+` "`+ri.name+`" not found`)
		return
	}
	writeJSON(w, rendered)
}
