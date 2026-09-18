package cluster

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Notice is a single watch event with its object already rendered, so the
// fan-out to N watchers costs one serialization, not N.
type Notice struct {
	Type   string // ADDED | MODIFIED | DELETED | BOOKMARK
	RV     uint64
	Object any
}

// ErrTooOld mirrors the Kubernetes 410 Gone response for a watch that tries
// to resume from a resourceVersion that has fallen out of the replay ring.
var ErrTooOld = errors.New("resource version too old")

// Subscription is a live watch stream.
type Subscription struct {
	ch     chan Notice
	hub    *Hub
	closed atomic.Bool
	// overflow is set when the subscriber could not keep up; the stream is
	// then terminated so the client re-LISTs, exactly like a real apiserver.
	overflow atomic.Bool
}

func (s *Subscription) C() <-chan Notice { return s.ch }
func (s *Subscription) Overflowed() bool { return s.overflow.Load() }
func (s *Subscription) Close()           { s.hub.unsubscribe(s) }

// Hub fans out notices for one resource kind and keeps a bounded replay ring
// so a watch established just after a LIST does not miss intervening changes.
type Hub struct {
	mu   sync.RWMutex
	ring []Notice
	next int  // next write slot
	full bool // ring has wrapped

	// floor is the oldest resourceVersion this hub can honestly resume from.
	// It starts at the version the cluster was sealed at, because nothing
	// before that was ever recorded.
	floor uint64

	subs map[*Subscription]struct{}

	Sent    atomic.Int64
	Dropped atomic.Int64
}

func NewHub(size int) *Hub {
	if size < 64 {
		size = 64
	}
	return &Hub{
		ring: make([]Notice, size),
		subs: make(map[*Subscription]struct{}),
	}
}

// SetFloor records the resourceVersion from which this hub keeps history.
func (h *Hub) SetFloor(rv uint64) {
	h.mu.Lock()
	h.floor = rv
	h.mu.Unlock()
}

// Subscribe registers a watcher. When sinceRV > 0 the buffered notices newer
// than it are returned for replay before the live stream begins.
func (h *Hub) Subscribe(sinceRV uint64, bufSize int) ([]Notice, *Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var replay []Notice
	if sinceRV > 0 {
		if sinceRV < h.floorLocked() {
			return nil, nil, ErrTooOld
		}
		replay = h.replayLocked(sinceRV)
	}

	if bufSize < 64 {
		bufSize = 64
	}
	sub := &Subscription{ch: make(chan Notice, bufSize), hub: h}
	h.subs[sub] = struct{}{}
	return replay, sub, nil
}

func (h *Hub) span() (start, count int) {
	if h.full {
		return h.next, len(h.ring)
	}
	return 0, h.next
}

// floorLocked is the oldest resourceVersion the hub can serve. Once the ring
// has wrapped, that is one below its oldest surviving notice.
func (h *Hub) floorLocked() uint64 {
	start, count := h.span()
	if count == 0 || !h.full {
		return h.floor
	}
	oldest := h.ring[start%len(h.ring)].RV
	if oldest == 0 {
		return h.floor
	}
	return oldest - 1
}

func (h *Hub) replayLocked(sinceRV uint64) []Notice {
	start, count := h.span()
	n := len(h.ring)
	out := make([]Notice, 0, 16)
	for i := 0; i < count; i++ {
		nt := h.ring[(start+i)%n]
		if nt.RV > sinceRV {
			out = append(out, nt)
		}
	}
	return out
}

func (h *Hub) unsubscribe(s *Subscription) {
	if s.closed.Swap(true) {
		return
	}
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
	close(s.ch)
}

// Broadcast records the notice in the replay ring and delivers it to every
// live subscriber.
func (h *Hub) Broadcast(n Notice) {
	h.mu.Lock()
	h.ring[h.next] = n
	h.next++
	if h.next == len(h.ring) {
		h.next = 0
		h.full = true
	}
	for s := range h.subs {
		select {
		case s.ch <- n:
			h.Sent.Add(1)
		default:
			s.overflow.Store(true)
			h.Dropped.Add(1)
		}
	}
	h.mu.Unlock()
}

// Watchers is the number of live subscriptions.
func (h *Hub) Watchers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// DisconnectAll terminates every live watch, used by the watch-churn chaos
// mode to force clients back through a full LIST.
func (h *Hub) DisconnectAll() int {
	h.mu.Lock()
	subs := make([]*Subscription, 0, len(h.subs))
	for s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.Unlock()
	for _, s := range subs {
		s.overflow.Store(true)
		h.unsubscribe(s)
	}
	return len(subs)
}
