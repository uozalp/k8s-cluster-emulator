package cluster

import (
	"errors"
	"testing"
)

func TestHubReplaysMissedNotices(t *testing.T) {
	h := NewHub(64)
	for rv := uint64(1); rv <= 5; rv++ {
		h.Broadcast(Notice{Type: "ADDED", RV: rv})
	}

	replay, sub, err := h.Subscribe(3, 8)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	if len(replay) != 2 {
		t.Fatalf("replayed %d notices, want 2", len(replay))
	}
	if replay[0].RV != 4 || replay[1].RV != 5 {
		t.Fatalf("replayed %v, want RVs 4 and 5", replay)
	}

	h.Broadcast(Notice{Type: "MODIFIED", RV: 6})
	if got := <-sub.C(); got.RV != 6 {
		t.Fatalf("live notice RV = %d, want 6", got.RV)
	}
}

func TestHubRejectsExpiredResourceVersion(t *testing.T) {
	h := NewHub(64) // minimum ring size
	for rv := uint64(1); rv <= 200; rv++ {
		h.Broadcast(Notice{Type: "ADDED", RV: rv})
	}
	if _, _, err := h.Subscribe(5, 8); !errors.Is(err, ErrTooOld) {
		t.Fatalf("expected ErrTooOld, got %v", err)
	}
	if _, sub, err := h.Subscribe(190, 8); err != nil {
		t.Fatalf("recent resourceVersion should be served: %v", err)
	} else {
		sub.Close()
	}
}

func TestHubRejectsVersionsBelowItsFloor(t *testing.T) {
	h := NewHub(64)
	h.SetFloor(1000)
	if _, _, err := h.Subscribe(999, 8); !errors.Is(err, ErrTooOld) {
		t.Fatalf("expected ErrTooOld below the floor, got %v", err)
	}
	if _, sub, err := h.Subscribe(1000, 8); err != nil {
		t.Fatalf("the floor itself must be serveable: %v", err)
	} else {
		sub.Close()
	}
}

func TestHubMarksSlowWatchersOverflowed(t *testing.T) {
	h := NewHub(512)
	_, sub, err := h.Subscribe(0, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	for rv := uint64(1); rv <= 300; rv++ {
		h.Broadcast(Notice{Type: "ADDED", RV: rv})
	}
	if !sub.Overflowed() {
		t.Fatal("watcher that never reads should be marked overflowed")
	}
	if h.Dropped.Load() == 0 {
		t.Fatal("dropped counter should be non-zero")
	}
}

func TestHubDisconnectAll(t *testing.T) {
	h := NewHub(64)
	_, sub, err := h.Subscribe(0, 8)
	if err != nil {
		t.Fatal(err)
	}
	if n := h.DisconnectAll(); n != 1 {
		t.Fatalf("disconnected %d, want 1", n)
	}
	if _, open := <-sub.C(); open {
		t.Fatal("channel should be closed after disconnect")
	}
	if h.Watchers() != 0 {
		t.Fatalf("watchers = %d, want 0", h.Watchers())
	}
}
