package kapi

import "testing"

func TestLabelSelector(t *testing.T) {
	labels := map[string]string{
		"app.kubernetes.io/name": "frontend",
		"tier":                   "backend",
		"pod-template-hash":      "7f8c9d",
	}
	cases := []struct {
		sel  string
		want bool
	}{
		{"", true},
		{"app.kubernetes.io/name=frontend", true},
		{"app.kubernetes.io/name==frontend", true},
		{"app.kubernetes.io/name=checkout", false},
		{"app.kubernetes.io/name!=checkout", true},
		{"app.kubernetes.io/name=frontend,tier=backend", true},
		{"app.kubernetes.io/name=frontend,tier=web", false},
		{"tier in (backend,web)", true},
		{"tier in (web,data)", false},
		{"tier notin (web,data)", true},
		{"pod-template-hash", true},
		{"!pod-template-hash", false},
		{"!missing", true},
		{"missing", false},
	}
	for _, c := range cases {
		sel, err := ParseLabelSelector(c.sel)
		if err != nil {
			t.Fatalf("parse %q: %v", c.sel, err)
		}
		if got := sel.Matches(labels); got != c.want {
			t.Errorf("selector %q matched=%v, want %v", c.sel, got, c.want)
		}
	}
}

func TestFieldSelector(t *testing.T) {
	fields := map[string]string{
		"spec.nodeName": "node-00007",
		"status.phase":  "Running",
	}
	lookup := func(p string) (string, bool) { v, ok := fields[p]; return v, ok }

	cases := []struct {
		sel  string
		want bool
	}{
		{"", true},
		{"spec.nodeName=node-00007", true},
		{"spec.nodeName=node-00008", false},
		{"spec.nodeName!=node-00008", true},
		{"spec.nodeName=node-00007,status.phase=Running", true},
		{"spec.nodeName=node-00007,status.phase=Failed", false},
		// Unmodelled paths must not filter everything out.
		{"metadata.uid=abc", true},
	}
	for _, c := range cases {
		sel, err := ParseFieldSelector(c.sel)
		if err != nil {
			t.Fatalf("parse %q: %v", c.sel, err)
		}
		if got := sel.Matches(lookup); got != c.want {
			t.Errorf("selector %q matched=%v, want %v", c.sel, got, c.want)
		}
	}
}

func TestFieldSelectorGet(t *testing.T) {
	sel, _ := ParseFieldSelector("spec.nodeName=node-1,status.phase!=Failed")
	if v, ok := sel.Get("spec.nodeName"); !ok || v != "node-1" {
		t.Fatalf("Get(spec.nodeName) = %q,%v", v, ok)
	}
	if _, ok := sel.Get("status.phase"); ok {
		t.Fatal("negated requirement should not be reported as an exact match")
	}
}
