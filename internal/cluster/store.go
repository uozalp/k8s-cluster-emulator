package cluster

import "sort"

// Cursor is an opaque position inside a Store used to resume paginated LIST
// requests. NSIdx indexes the sorted namespace list, Slot indexes that
// namespace's insertion-ordered bucket.
type Cursor struct {
	NSIdx int
	Slot  int
}

// Stored is implemented by every resource the emulator keeps in memory.
// Implementations must be pointer types so tombstones can be represented as
// the nil value.
type Stored interface {
	comparable
	GetNamespace() string
	GetName() string
}

type bucket[T Stored] struct {
	order []T
	index map[string]int
	dead  int
}

// Store is an insertion-ordered, namespace-partitioned object table.
//
// It is deliberately not safe for concurrent use on its own: callers go
// through Cluster.Read / Cluster.Write, which owns the lock. That keeps the
// hot LIST path free of per-object locking at 500k+ objects.
type Store[T Stored] struct {
	buckets map[string]*bucket[T]
	order   []string // sorted namespace names
	total   int
}

func NewStore[T Stored]() *Store[T] {
	return &Store[T]{buckets: make(map[string]*bucket[T])}
}

func (s *Store[T]) bucketFor(ns string, create bool) *bucket[T] {
	b, ok := s.buckets[ns]
	if ok || !create {
		return b
	}
	b = &bucket[T]{index: make(map[string]int)}
	s.buckets[ns] = b
	i := sort.SearchStrings(s.order, ns)
	s.order = append(s.order, "")
	copy(s.order[i+1:], s.order[i:])
	s.order[i] = ns
	return b
}

// Put inserts or replaces an object. Replacing keeps the original slot so
// existing pagination cursors stay meaningful.
func (s *Store[T]) Put(obj T) {
	b := s.bucketFor(obj.GetNamespace(), true)
	if i, ok := b.index[obj.GetName()]; ok {
		b.order[i] = obj
		return
	}
	b.index[obj.GetName()] = len(b.order)
	b.order = append(b.order, obj)
	s.total++
}

func (s *Store[T]) Get(ns, name string) (T, bool) {
	var zero T
	b := s.buckets[ns]
	if b == nil {
		return zero, false
	}
	i, ok := b.index[name]
	if !ok {
		return zero, false
	}
	return b.order[i], true
}

func (s *Store[T]) Delete(ns, name string) (T, bool) {
	var zero T
	b := s.buckets[ns]
	if b == nil {
		return zero, false
	}
	i, ok := b.index[name]
	if !ok {
		return zero, false
	}
	obj := b.order[i]
	b.order[i] = zero
	delete(b.index, name)
	b.dead++
	s.total--
	b.maybeCompact()
	return obj, true
}

// maybeCompact reclaims tombstones once they dominate the bucket. This
// invalidates outstanding cursors for that namespace, which matches the way
// real continue tokens expire.
func (b *bucket[T]) maybeCompact() {
	if b.dead < 1024 || b.dead*2 < len(b.order) {
		return
	}
	var zero T
	out := b.order[:0]
	for _, o := range b.order {
		if o == zero {
			continue
		}
		b.index[o.GetName()] = len(out)
		out = append(out, o)
	}
	b.order = out
	b.dead = 0
}

func (s *Store[T]) Len() int { return s.total }

func (s *Store[T]) LenNS(ns string) int {
	b := s.buckets[ns]
	if b == nil {
		return 0
	}
	return len(b.order) - b.dead
}

// Namespaces returns the sorted namespace names that hold objects.
func (s *Store[T]) Namespaces() []string { return s.order }

// Iterate walks up to limit live objects starting at cur. It returns the
// cursor to resume from and whether more objects remain. A limit <= 0 walks
// everything. Iteration stops early if fn returns false.
func (s *Store[T]) Iterate(ns string, cur Cursor, limit int, fn func(T) bool) (Cursor, bool) {
	var zero T
	nsList := s.order
	if ns != "" {
		if _, ok := s.buckets[ns]; !ok {
			return Cursor{}, false
		}
		nsList = []string{ns}
	}
	if cur.NSIdx >= len(nsList) {
		return Cursor{}, false
	}
	n := 0
	for i := cur.NSIdx; i < len(nsList); i++ {
		b := s.buckets[nsList[i]]
		slot := 0
		if i == cur.NSIdx {
			slot = cur.Slot
		}
		for ; slot < len(b.order); slot++ {
			obj := b.order[slot]
			if obj == zero {
				continue
			}
			if limit > 0 && n >= limit {
				return Cursor{NSIdx: i, Slot: slot}, true
			}
			if !fn(obj) {
				return Cursor{NSIdx: i, Slot: slot + 1}, true
			}
			n++
		}
	}
	return Cursor{}, false
}

// All is a convenience full scan.
func (s *Store[T]) All(fn func(T) bool) {
	s.Iterate("", Cursor{}, 0, fn)
}
