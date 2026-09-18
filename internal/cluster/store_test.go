package cluster

import (
	"fmt"
	"testing"
)

type testObj struct {
	ns, name string
}

func (o *testObj) GetNamespace() string { return o.ns }
func (o *testObj) GetName() string      { return o.name }

func newTestStore(namespaces, per int) *Store[*testObj] {
	s := NewStore[*testObj]()
	for n := 0; n < namespaces; n++ {
		ns := fmt.Sprintf("ns-%02d", n)
		for i := 0; i < per; i++ {
			s.Put(&testObj{ns: ns, name: fmt.Sprintf("obj-%03d", i)})
		}
	}
	return s
}

func collect(s *Store[*testObj], ns string, chunk int) []string {
	var out []string
	cur := Cursor{}
	for {
		next, more := s.Iterate(ns, cur, chunk, func(o *testObj) bool {
			out = append(out, o.ns+"/"+o.name)
			return true
		})
		cur = next
		if !more {
			return out
		}
	}
}

func TestStoreIterateAcrossNamespaces(t *testing.T) {
	s := newTestStore(4, 10)
	if s.Len() != 40 {
		t.Fatalf("Len = %d, want 40", s.Len())
	}
	all := collect(s, "", 7) // chunk size deliberately not a divisor
	if len(all) != 40 {
		t.Fatalf("iterated %d objects, want 40", len(all))
	}
	seen := map[string]bool{}
	for _, k := range all {
		if seen[k] {
			t.Fatalf("duplicate %s across pagination chunks", k)
		}
		seen[k] = true
	}
}

func TestStoreIterateSingleNamespace(t *testing.T) {
	s := newTestStore(4, 10)
	got := collect(s, "ns-02", 3)
	if len(got) != 10 {
		t.Fatalf("got %d objects, want 10", len(got))
	}
	for _, k := range got {
		if k[:5] != "ns-02" {
			t.Fatalf("namespace filter leaked %s", k)
		}
	}
	if _, more := s.Iterate("missing", Cursor{}, 10, func(*testObj) bool { return true }); more {
		t.Fatal("unknown namespace should yield nothing")
	}
}

func TestStoreDeleteLeavesNoGaps(t *testing.T) {
	s := newTestStore(2, 10)
	if _, ok := s.Delete("ns-00", "obj-004"); !ok {
		t.Fatal("delete failed")
	}
	if _, ok := s.Delete("ns-00", "obj-004"); ok {
		t.Fatal("second delete should report missing")
	}
	if s.Len() != 19 {
		t.Fatalf("Len = %d, want 19", s.Len())
	}
	if s.LenNS("ns-00") != 9 {
		t.Fatalf("LenNS = %d, want 9", s.LenNS("ns-00"))
	}
	for _, k := range collect(s, "", 4) {
		if k == "ns-00/obj-004" {
			t.Fatal("deleted object still iterated")
		}
	}
}

func TestStorePutReplacesInPlace(t *testing.T) {
	s := newTestStore(1, 3)
	before := collect(s, "", 10)
	s.Put(&testObj{ns: "ns-00", name: "obj-001"})
	after := collect(s, "", 10)
	if len(after) != len(before) {
		t.Fatalf("replace changed length: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("replace reordered: %v vs %v", before, after)
		}
	}
}

func TestStoreCompaction(t *testing.T) {
	s := newTestStore(1, 4000)
	for i := 0; i < 3000; i++ {
		s.Delete("ns-00", fmt.Sprintf("obj-%03d", i))
	}
	if s.Len() != 1000 {
		t.Fatalf("Len = %d, want 1000", s.Len())
	}
	if got := len(collect(s, "", 100)); got != 1000 {
		t.Fatalf("iterated %d, want 1000", got)
	}
}
