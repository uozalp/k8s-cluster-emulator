package cluster

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Object is the read-only view the API layer needs of any stored resource.
type Object interface {
	GetName() string
	GetNamespace() string
	ObjLabels() map[string]string
	FieldValue(path string) (string, bool)
	Render() any
}

type storedObject interface {
	comparable
	Object
}

func (p *Pod) ObjLabels() map[string]string        { return p.Tmpl.Labels }
func (r *ReplicaSet) ObjLabels() map[string]string { return r.Tmpl.Labels }
func (d *Deployment) ObjLabels() map[string]string { return d.Selector }
func (n *Node) ObjLabels() map[string]string       { return n.NodeLabels() }
func (n *Namespace) ObjLabels() map[string]string  { return n.Labels }
func (s *Service) ObjLabels() map[string]string    { return s.Labels }
func (c *ConfigMap) ObjLabels() map[string]string  { return c.Labels }
func (s *Secret) ObjLabels() map[string]string     { return s.Labels }
func (j *Job) ObjLabels() map[string]string        { return j.Labels }
func (c *CronJob) ObjLabels() map[string]string    { return c.Labels }
func (e *Event) ObjLabels() map[string]string      { return nil }

// View is a kind-agnostic handle the API server uses to serve LIST, GET and
// WATCH without knowing anything about the underlying resource type.
type View struct {
	Resource     string
	Singular     string
	Kind         string
	ListKind     string
	Group        string
	Version      string
	GroupVersion string
	Namespaced   bool
	Verbs        []string
	ShortNames   []string
	Categories   []string
	Hub          *Hub

	Count   func(ns string) int
	Iterate func(ns string, cur Cursor, limit int, fn func(Object) bool) (Cursor, bool)
	Get     func(ns, name string) (Object, bool)
	Delete  func(ns, name string) (Object, bool)
}

func bindView[T storedObject](st *Store[T], v *View) *View {
	v.Count = func(ns string) int {
		if ns == "" {
			return st.Len()
		}
		return st.LenNS(ns)
	}
	v.Iterate = func(ns string, cur Cursor, limit int, fn func(Object) bool) (Cursor, bool) {
		return st.Iterate(ns, cur, limit, func(o T) bool { return fn(o) })
	}
	v.Get = func(ns, name string) (Object, bool) {
		o, ok := st.Get(ns, name)
		if !ok {
			return nil, false
		}
		return o, true
	}
	v.Delete = func(ns, name string) (Object, bool) {
		o, ok := st.Delete(ns, name)
		if !ok {
			return nil, false
		}
		return o, true
	}
	return v
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

// Cluster is the stateful in-memory fake. A single RWMutex guards every
// store: readers (LIST/GET/WATCH bootstrap) run concurrently, and the
// simulation engine batches its mutations into short write transactions.
type Cluster struct {
	mu sync.RWMutex
	rv atomic.Uint64

	Namespaces  *Store[*Namespace]
	Nodes       *Store[*Node]
	Pods        *Store[*Pod]
	Deployments *Store[*Deployment]
	ReplicaSets *Store[*ReplicaSet]
	Services    *Store[*Service]
	ConfigMaps  *Store[*ConfigMap]
	Secrets     *Store[*Secret]
	Jobs        *Store[*Job]
	CronJobs    *Store[*CronJob]
	Events      *Store[*Event]

	nodeList []*Node

	hubs  map[string]*Hub
	views map[string]*View
	order []string

	// Aggregate live counters, kept current by the write path so the UI
	// never has to scan hundreds of thousands of objects.
	stateCounts [podStateCount]atomic.Int64
	terminating atomic.Int64
	podsCreated atomic.Int64
	podsDeleted atomic.Int64
	podSeq      atomic.Uint32
	eventSeq    atomic.Uint64

	eventQueue []*Event
	eventMax   int

	StartedAt time.Time
}

func New(watchBuffer, eventMax int) *Cluster {
	c := &Cluster{
		Namespaces:  NewStore[*Namespace](),
		Nodes:       NewStore[*Node](),
		Pods:        NewStore[*Pod](),
		Deployments: NewStore[*Deployment](),
		ReplicaSets: NewStore[*ReplicaSet](),
		Services:    NewStore[*Service](),
		ConfigMaps:  NewStore[*ConfigMap](),
		Secrets:     NewStore[*Secret](),
		Jobs:        NewStore[*Job](),
		CronJobs:    NewStore[*CronJob](),
		Events:      NewStore[*Event](),
		hubs:        make(map[string]*Hub),
		views:       make(map[string]*View),
		eventMax:    eventMax,
		StartedAt:   time.Now(),
	}
	c.rv.Store(1000)
	c.registerViews(watchBuffer)
	return c
}

func (c *Cluster) registerViews(watchBuffer int) {
	add := func(v *View) {
		v.Hub = NewHub(watchBuffer)
		if v.Group == "" {
			v.GroupVersion = v.Version
		} else {
			v.GroupVersion = v.Group + "/" + v.Version
		}
		c.hubs[v.Resource] = v.Hub
		c.views[v.Resource] = v
		c.order = append(c.order, v.Resource)
	}

	rw := []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}
	ro := []string{"get", "list", "watch"}

	add(bindView(c.Pods, &View{
		Resource: "pods", Singular: "pod", Kind: "Pod", ListKind: "PodList",
		Version: "v1", Namespaced: true, Verbs: rw, ShortNames: []string{"po"},
		Categories: []string{"all"},
	}))
	add(bindView(c.Nodes, &View{
		Resource: "nodes", Singular: "node", Kind: "Node", ListKind: "NodeList",
		Version: "v1", Namespaced: false, Verbs: rw, ShortNames: []string{"no"},
	}))
	add(bindView(c.Namespaces, &View{
		Resource: "namespaces", Singular: "namespace", Kind: "Namespace", ListKind: "NamespaceList",
		Version: "v1", Namespaced: false, Verbs: rw, ShortNames: []string{"ns"},
	}))
	add(bindView(c.Services, &View{
		Resource: "services", Singular: "service", Kind: "Service", ListKind: "ServiceList",
		Version: "v1", Namespaced: true, Verbs: rw, ShortNames: []string{"svc"},
		Categories: []string{"all"},
	}))
	add(bindView(c.ConfigMaps, &View{
		Resource: "configmaps", Singular: "configmap", Kind: "ConfigMap", ListKind: "ConfigMapList",
		Version: "v1", Namespaced: true, Verbs: rw, ShortNames: []string{"cm"},
	}))
	add(bindView(c.Secrets, &View{
		Resource: "secrets", Singular: "secret", Kind: "Secret", ListKind: "SecretList",
		Version: "v1", Namespaced: true, Verbs: rw,
	}))
	add(bindView(c.Events, &View{
		Resource: "events", Singular: "event", Kind: "Event", ListKind: "EventList",
		Version: "v1", Namespaced: true, Verbs: ro, ShortNames: []string{"ev"},
	}))
	add(bindView(c.Deployments, &View{
		Resource: "deployments", Singular: "deployment", Kind: "Deployment", ListKind: "DeploymentList",
		Group: "apps", Version: "v1", Namespaced: true, Verbs: rw, ShortNames: []string{"deploy"},
		Categories: []string{"all"},
	}))
	add(bindView(c.ReplicaSets, &View{
		Resource: "replicasets", Singular: "replicaset", Kind: "ReplicaSet", ListKind: "ReplicaSetList",
		Group: "apps", Version: "v1", Namespaced: true, Verbs: rw, ShortNames: []string{"rs"},
		Categories: []string{"all"},
	}))
	add(bindView(c.Jobs, &View{
		Resource: "jobs", Singular: "job", Kind: "Job", ListKind: "JobList",
		Group: "batch", Version: "v1", Namespaced: true, Verbs: rw,
		Categories: []string{"all"},
	}))
	add(bindView(c.CronJobs, &View{
		Resource: "cronjobs", Singular: "cronjob", Kind: "CronJob", ListKind: "CronJobList",
		Group: "batch", Version: "v1", Namespaced: true, Verbs: rw, ShortNames: []string{"cj"},
		Categories: []string{"all"},
	}))
}

// Seal marks the end of the initial build. Watch hubs start keeping history
// from this point, so a client asking to resume from an earlier version is
// correctly told its resourceVersion is too old.
func (c *Cluster) Seal() {
	rv := c.rv.Load()
	for _, h := range c.hubs {
		h.SetFloor(rv)
	}
}

// LookupView resolves a (group, version, resource) triple.
func (c *Cluster) LookupView(group, version, resource string) (*View, bool) {
	v, ok := c.views[resource]
	if !ok || v.Group != group || v.Version != version {
		return nil, false
	}
	return v, true
}

// Views returns every registered view in registration order.
func (c *Cluster) Views() []*View {
	out := make([]*View, 0, len(c.order))
	for _, r := range c.order {
		out = append(out, c.views[r])
	}
	return out
}

func (c *Cluster) Hub(resource string) *Hub { return c.hubs[resource] }

func (c *Cluster) RLock()   { c.mu.RLock() }
func (c *Cluster) RUnlock() { c.mu.RUnlock() }

// RV is the cluster's current resourceVersion.
func (c *Cluster) RV() uint64       { return c.rv.Load() }
func (c *Cluster) RVString() string { return strconv.FormatUint(c.rv.Load(), 10) }

// NodeAt returns the node with the given index.
func (c *Cluster) NodeAt(i int32) *Node {
	if int(i) < len(c.nodeList) {
		return c.nodeList[i]
	}
	return nil
}

func (c *Cluster) NodeCount() int { return len(c.nodeList) }

// ---------------------------------------------------------------------------
// Write transactions
// ---------------------------------------------------------------------------

type queuedNotice struct {
	hub    *Hub
	notice Notice
}

// Tx is a batch of mutations. Notices are rendered while the write lock is
// held and broadcast after it is released, so watchers never observe a
// half-applied change and never block the simulation.
type Tx struct {
	c       *Cluster
	notices []queuedNotice
	silent  bool
	Now     time.Time
}

// Bootstrap applies mutations without emitting watch notices. It is used for
// the initial cluster build, where rendering an event per object would cost
// more than generating the cluster itself and no watcher exists yet.
func (c *Cluster) Bootstrap(fn func(tx *Tx)) {
	tx := &Tx{c: c, Now: time.Now(), silent: true}
	c.mu.Lock()
	fn(tx)
	c.mu.Unlock()
}

// Write runs fn under the cluster write lock and then publishes its notices.
func (c *Cluster) Write(fn func(tx *Tx)) {
	tx := txPool.Get().(*Tx)
	tx.c = c
	tx.notices = tx.notices[:0]
	tx.silent = false
	tx.Now = time.Now()

	c.mu.Lock()
	fn(tx)
	c.mu.Unlock()

	for _, q := range tx.notices {
		q.hub.Broadcast(q.notice)
	}
	tx.c = nil
	txPool.Put(tx)
}

var txPool = sync.Pool{New: func() any { return &Tx{notices: make([]queuedNotice, 0, 64)} }}

func (t *Tx) nextRV() uint64 { return t.c.rv.Add(1) }

func (t *Tx) emit(resource, typ string, obj Object, rv uint64) {
	if t.silent {
		return
	}
	hub := t.c.hubs[resource]
	if hub == nil {
		return
	}
	t.notices = append(t.notices, queuedNotice{hub: hub, notice: Notice{Type: typ, RV: rv, Object: obj.Render()}})
}

// --- Pods ---

// AddPod inserts a new pod and emits ADDED.
func (t *Tx) AddPod(p *Pod) {
	p.RV = t.nextRV()
	t.c.Pods.Put(p)
	if rs := p.Tmpl.RS; rs != nil {
		rs.addPod(p)
		if p.State.Ready() {
			rs.ReadyCnt++
		}
		if p.Avail {
			rs.AvailCnt++
		}
	}
	t.c.stateCounts[p.State].Add(1)
	t.c.podsCreated.Add(1)
	if p.Scheduled != 0 {
		if n := t.c.NodeAt(p.NodeIdx); n != nil {
			n.Pods++
		}
	}
	t.emit("pods", "ADDED", p, p.RV)
}

// TouchPod bumps the pod's resourceVersion and emits MODIFIED.
func (t *Tx) TouchPod(p *Pod) {
	p.RV = t.nextRV()
	t.emit("pods", "MODIFIED", p, p.RV)
}

// SetPodState transitions a pod, keeping cluster-wide and ReplicaSet
// counters in sync so no status calculation ever needs a scan.
func (t *Tx) SetPodState(p *Pod, s PodState) {
	if p.State == s {
		return
	}
	wasReady := p.State.Ready()
	t.c.stateCounts[p.State].Add(-1)
	t.c.stateCounts[s].Add(1)
	p.State = s

	rs := p.Tmpl.RS
	if rs == nil {
		return
	}
	switch nowReady := s.Ready(); {
	case wasReady && !nowReady:
		rs.ReadyCnt--
		if p.Avail {
			rs.AvailCnt--
			p.Avail = false
		}
	case !wasReady && nowReady:
		rs.ReadyCnt++
	}
}

// MarkPodAvailable promotes a ready pod past minReadySeconds.
func (t *Tx) MarkPodAvailable(p *Pod) {
	if p.Avail || !p.State.Ready() {
		return
	}
	p.Avail = true
	if rs := p.Tmpl.RS; rs != nil {
		rs.AvailCnt++
	}
}

// SchedulePod binds a pending pod to its node.
func (t *Tx) SchedulePod(p *Pod, now time.Time) {
	if p.Scheduled != 0 {
		return
	}
	p.Scheduled = now.Unix()
	if n := t.c.NodeAt(p.NodeIdx); n != nil {
		n.Pods++
	}
}

// MarkPodTerminating sets a deletionTimestamp without removing the object.
func (t *Tx) MarkPodTerminating(p *Pod, grace time.Duration) {
	if p.Deleted != 0 {
		return
	}
	p.Deleted = t.Now.Unix()
	p.NextStep = t.Now.Add(grace).UnixNano()
	t.c.terminating.Add(1)
	if rs := p.Tmpl.RS; rs != nil {
		rs.Terminating++
	}
	t.TouchPod(p)
}

// RemovePod deletes the pod and emits DELETED.
func (t *Tx) RemovePod(p *Pod) {
	if _, ok := t.c.Pods.Delete(p.Tmpl.Namespace, p.Name); !ok {
		return
	}
	p.Dead = true
	if rs := p.Tmpl.RS; rs != nil {
		rs.removePod(p)
		if p.State.Ready() {
			rs.ReadyCnt--
		}
		if p.Avail {
			rs.AvailCnt--
		}
		if p.Deleted != 0 {
			rs.Terminating--
		}
	}
	t.c.stateCounts[p.State].Add(-1)
	if p.Deleted != 0 {
		t.c.terminating.Add(-1)
	}
	if p.Scheduled != 0 {
		if n := t.c.NodeAt(p.NodeIdx); n != nil && n.Pods > 0 {
			n.Pods--
		}
	}
	t.c.podsDeleted.Add(1)
	p.RV = t.nextRV()
	t.emit("pods", "DELETED", p, p.RV)
}

// NextPodOrdinal hands out the monotonic counter used for pod IPs and names.
func (c *Cluster) NextPodOrdinal() uint32 { return c.podSeq.Add(1) }

// --- Workloads ---

func (t *Tx) PutReplicaSet(rs *ReplicaSet) {
	rs.RV = t.nextRV()
	t.c.ReplicaSets.Put(rs)
	t.emit("replicasets", "ADDED", rs, rs.RV)
}

func (t *Tx) TouchReplicaSet(rs *ReplicaSet) {
	rs.RV = t.nextRV()
	rs.Observed = rs.Generation
	t.emit("replicasets", "MODIFIED", rs, rs.RV)
}

// RemoveReplicaSet drops an empty ReplicaSet once its Deployment is gone.
func (t *Tx) RemoveReplicaSet(rs *ReplicaSet) {
	if _, ok := t.c.ReplicaSets.Delete(rs.Namespace, rs.Name); !ok {
		return
	}
	rs.RV = t.nextRV()
	t.emit("replicasets", "DELETED", rs, rs.RV)
}

func (t *Tx) PutDeployment(d *Deployment) {
	d.RV = t.nextRV()
	t.c.Deployments.Put(d)
	t.emit("deployments", "ADDED", d, d.RV)
}

func (t *Tx) TouchDeployment(d *Deployment) {
	d.RV = t.nextRV()
	t.emit("deployments", "MODIFIED", d, d.RV)
}

func (t *Tx) RemoveDeployment(d *Deployment) {
	if _, ok := t.c.Deployments.Delete(d.Namespace, d.Name); !ok {
		return
	}
	d.RV = t.nextRV()
	t.emit("deployments", "DELETED", d, d.RV)
}

// --- Nodes & namespaces ---

func (t *Tx) PutNode(n *Node) {
	n.RV = t.nextRV()
	t.c.Nodes.Put(n)
	if int(n.Idx) >= len(t.c.nodeList) {
		grown := make([]*Node, n.Idx+1)
		copy(grown, t.c.nodeList)
		t.c.nodeList = grown
	}
	t.c.nodeList[n.Idx] = n
	t.emit("nodes", "ADDED", n, n.RV)
}

func (t *Tx) TouchNode(n *Node) {
	n.RV = t.nextRV()
	t.emit("nodes", "MODIFIED", n, n.RV)
}

func (t *Tx) PutNamespace(n *Namespace) {
	n.RV = t.nextRV()
	t.c.Namespaces.Put(n)
	t.emit("namespaces", "ADDED", n, n.RV)
}

// --- Simple namespaced resources ---

func (t *Tx) PutService(s *Service) {
	s.RV = t.nextRV()
	t.c.Services.Put(s)
	t.emit("services", "ADDED", s, s.RV)
}

func (t *Tx) PutConfigMap(cm *ConfigMap) {
	cm.RV = t.nextRV()
	t.c.ConfigMaps.Put(cm)
	t.emit("configmaps", "ADDED", cm, cm.RV)
}

func (t *Tx) PutSecret(s *Secret) {
	s.RV = t.nextRV()
	t.c.Secrets.Put(s)
	t.emit("secrets", "ADDED", s, s.RV)
}

func (t *Tx) PutJob(j *Job) {
	j.RV = t.nextRV()
	t.c.Jobs.Put(j)
	t.emit("jobs", "ADDED", j, j.RV)
}

func (t *Tx) PutCronJob(cj *CronJob) {
	cj.RV = t.nextRV()
	t.c.CronJobs.Put(cj)
	t.emit("cronjobs", "ADDED", cj, cj.RV)
}

// EmitDeleted publishes a DELETED notice for an object already removed from
// its store by a generic delete.
func (t *Tx) EmitDeleted(resource string, o Object) {
	t.emit(resource, "DELETED", o, t.nextRV())
}

// --- Events ---

// RecordEvent stores an Event, evicting the oldest once retention is hit.
func (t *Tx) RecordEvent(e *Event) {
	c := t.c
	if c.eventMax <= 0 {
		return
	}
	seq := c.eventSeq.Add(1)
	if e.Name == "" {
		e.Name = e.RefName + "." + strconv.FormatUint(seq, 16)
	}
	e.UID = UID{seq * 0x9e3779b97f4a7c15, seq ^ 0xc2b2ae3d27d4eb4f}
	e.RV = t.nextRV()
	c.Events.Put(e)
	c.eventQueue = append(c.eventQueue, e)
	t.emit("events", "ADDED", e, e.RV)

	for len(c.eventQueue) > c.eventMax {
		old := c.eventQueue[0]
		c.eventQueue = c.eventQueue[1:]
		if _, ok := c.Events.Delete(old.Namespace, old.Name); ok {
			old.RV = t.nextRV()
			t.emit("events", "DELETED", old, old.RV)
		}
	}
	if cap(c.eventQueue) > 4*c.eventMax && c.eventMax > 0 {
		trimmed := make([]*Event, len(c.eventQueue))
		copy(trimmed, c.eventQueue)
		c.eventQueue = trimmed
	}
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// Stats is a cheap snapshot for the dashboard.
type Stats struct {
	Pods        int
	PodStates   [podStateCount]int64
	Terminating int64
	Created     int64
	Deleted     int64
	Deployments int
	ReplicaSets int
	Namespaces  int
	Nodes       int
	Services    int
	Events      int
	Jobs        int
	CronJobs    int
	ConfigMaps  int
	Secrets     int
	RV          uint64
}

func (c *Cluster) Stats() Stats {
	s := Stats{
		Terminating: c.terminating.Load(),
		Created:     c.podsCreated.Load(),
		Deleted:     c.podsDeleted.Load(),
		RV:          c.rv.Load(),
	}
	for i := range s.PodStates {
		s.PodStates[i] = c.stateCounts[i].Load()
	}
	c.mu.RLock()
	s.Pods = c.Pods.Len()
	s.Deployments = c.Deployments.Len()
	s.ReplicaSets = c.ReplicaSets.Len()
	s.Namespaces = c.Namespaces.Len()
	s.Nodes = c.Nodes.Len()
	s.Services = c.Services.Len()
	s.Events = c.Events.Len()
	s.Jobs = c.Jobs.Len()
	s.CronJobs = c.CronJobs.Len()
	s.ConfigMaps = c.ConfigMaps.Len()
	s.Secrets = c.Secrets.Len()
	c.mu.RUnlock()
	return s
}

// PodStateName exposes the display name of a pod state index.
func PodStateName(i int) string { return PodState(i).String() }

// PodStateCount is the number of distinct pod states.
const PodStateCount = int(podStateCount)
