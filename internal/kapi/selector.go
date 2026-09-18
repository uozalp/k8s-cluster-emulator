package kapi

import (
	"strings"
)

// ---------------------------------------------------------------------------
// Label selectors
// ---------------------------------------------------------------------------

type selOp int

const (
	opEquals selOp = iota
	opNotEquals
	opIn
	opNotIn
	opExists
	opNotExists
)

type requirement struct {
	key    string
	op     selOp
	values []string
}

// LabelSelectorExpr is a parsed `labelSelector` query parameter. A nil
// selector matches everything.
type LabelSelectorExpr struct {
	reqs []requirement
}

// ParseLabelSelector parses the subset of the label selector grammar that
// real clients actually emit: `k=v`, `k==v`, `k!=v`, `k in (a,b)`,
// `k notin (a,b)`, `k` (exists) and `!k` (not exists).
func ParseLabelSelector(s string) (*LabelSelectorExpr, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	sel := &LabelSelectorExpr{}
	for _, part := range splitTopLevel(s) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		r, err := parseRequirement(part)
		if err != nil {
			return nil, err
		}
		sel.reqs = append(sel.reqs, r)
	}
	return sel, nil
}

// splitTopLevel splits on commas that are not inside parentheses.
func splitTopLevel(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

func parseRequirement(part string) (requirement, error) {
	lower := strings.ToLower(part)
	switch {
	case strings.Contains(lower, " notin "):
		i := strings.Index(lower, " notin ")
		return requirement{
			key:    strings.TrimSpace(part[:i]),
			op:     opNotIn,
			values: parseSet(part[i+len(" notin "):]),
		}, nil
	case strings.Contains(lower, " in "):
		i := strings.Index(lower, " in ")
		return requirement{
			key:    strings.TrimSpace(part[:i]),
			op:     opIn,
			values: parseSet(part[i+len(" in "):]),
		}, nil
	case strings.Contains(part, "!="):
		kv := strings.SplitN(part, "!=", 2)
		return requirement{key: strings.TrimSpace(kv[0]), op: opNotEquals, values: []string{strings.TrimSpace(kv[1])}}, nil
	case strings.Contains(part, "=="):
		kv := strings.SplitN(part, "==", 2)
		return requirement{key: strings.TrimSpace(kv[0]), op: opEquals, values: []string{strings.TrimSpace(kv[1])}}, nil
	case strings.Contains(part, "="):
		kv := strings.SplitN(part, "=", 2)
		return requirement{key: strings.TrimSpace(kv[0]), op: opEquals, values: []string{strings.TrimSpace(kv[1])}}, nil
	case strings.HasPrefix(part, "!"):
		return requirement{key: strings.TrimSpace(part[1:]), op: opNotExists}, nil
	default:
		return requirement{key: strings.TrimSpace(part), op: opExists}, nil
	}
}

func parseSet(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	raw := strings.Split(s, ",")
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		out = append(out, strings.TrimSpace(v))
	}
	return out
}

// Matches reports whether labels satisfy every requirement.
func (e *LabelSelectorExpr) Matches(labels map[string]string) bool {
	if e == nil || len(e.reqs) == 0 {
		return true
	}
	for _, r := range e.reqs {
		v, ok := labels[r.key]
		switch r.op {
		case opEquals:
			if !ok || v != r.values[0] {
				return false
			}
		case opNotEquals:
			if ok && v == r.values[0] {
				return false
			}
		case opExists:
			if !ok {
				return false
			}
		case opNotExists:
			if ok {
				return false
			}
		case opIn:
			if !ok || !contains(r.values, v) {
				return false
			}
		case opNotIn:
			if ok && contains(r.values, v) {
				return false
			}
		}
	}
	return true
}

// Empty reports whether the selector filters nothing out.
func (e *LabelSelectorExpr) Empty() bool { return e == nil || len(e.reqs) == 0 }

func contains(vs []string, v string) bool {
	for _, x := range vs {
		if x == v {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Field selectors
// ---------------------------------------------------------------------------

type fieldReq struct {
	path   string
	value  string
	negate bool
}

// FieldSelectorExpr is a parsed `fieldSelector` query parameter. Only
// equality and inequality are valid in the Kubernetes field selector grammar.
type FieldSelectorExpr struct {
	reqs []fieldReq
}

func ParseFieldSelector(s string) (*FieldSelectorExpr, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	sel := &FieldSelectorExpr{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if i := strings.Index(part, "!="); i >= 0 {
			sel.reqs = append(sel.reqs, fieldReq{path: strings.TrimSpace(part[:i]), value: strings.TrimSpace(part[i+2:]), negate: true})
			continue
		}
		if i := strings.Index(part, "="); i >= 0 {
			sel.reqs = append(sel.reqs, fieldReq{path: strings.TrimSpace(part[:i]), value: strings.TrimSpace(strings.TrimPrefix(part[i+1:], "="))})
		}
	}
	return sel, nil
}

// Matches evaluates the selector against a lookup function. Unknown field
// paths match, mirroring how the emulator degrades gracefully rather than
// rejecting requests it does not fully model.
func (e *FieldSelectorExpr) Matches(lookup func(path string) (string, bool)) bool {
	if e == nil || len(e.reqs) == 0 {
		return true
	}
	for _, r := range e.reqs {
		actual, known := lookup(r.path)
		if !known {
			continue
		}
		if r.negate == (actual == r.value) {
			return false
		}
	}
	return true
}

func (e *FieldSelectorExpr) Empty() bool { return e == nil || len(e.reqs) == 0 }

// Get returns the value required for an exact-match requirement on path.
// It lets callers short-circuit to an index instead of a full scan.
func (e *FieldSelectorExpr) Get(path string) (string, bool) {
	if e == nil {
		return "", false
	}
	for _, r := range e.reqs {
		if r.path == path && !r.negate {
			return r.value, true
		}
	}
	return "", false
}
