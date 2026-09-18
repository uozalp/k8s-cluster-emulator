// Package simulate contains the controllers that make the fake cluster move:
// the pod lifecycle stepper, the ReplicaSet/Deployment reconcilers, churn,
// and the event recorder.
package simulate

import (
	"container/heap"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/config"
	"github.com/danske-spil/k8s-cluster-emulator/internal/generate"
)

// minReadySeconds is the delay between a pod becoming Ready and counting as
// Available, which is what keeps readyReplicas and availableReplicas apart.
const minReadySeconds = 10 * time.Second

// tickInterval bounds how long a single write transaction holds the cluster
// lock; everything the engine does per tick is budgeted against it.
const tickInterval = 100 * time.Millisecond

type timer struct {
	at int64 // unix nanos
	p  *cluster.Pod
}

type podHeap []timer

func (h podHeap) Len() int           { return len(h) }
func (h podHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h podHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *podHeap) Push(x any)        { *h = append(*h, x.(timer)) }
func (h *podHeap) Pop() any          { old := *h; n := len(old); v := old[n-1]; *h = old[:n-1]; return v }

// Engine owns all mutation of the cluster. Everything it does happens on one
// goroutine inside short write transactions, so the controllers need no
// locking of their own.
type Engine struct {
	cfg config.Scenario
	c   *cluster.Cluster
	f   *generate.Factory
	rnd *rand.Rand

	pending podHeap
	deploys []*cluster.Deployment

	dirtyRS map[*cluster.ReplicaSet]struct{}
	dirtyD  map[*cluster.Deployment]struct{}

	wakeMu    sync.Mutex
	wakeD     []*cluster.Deployment
	wakePods  []*cluster.Pod
	scaleNote []scaleNote

	// Fractional accumulators for sub-tick churn rates.
	accDelete, accRestart, accScale, accNode, accEvent float64

	Transitions atomic.Int64
	EventsMade  atomic.Int64
	PodsCreated atomic.Int64
	PodsDeleted atomic.Int64
}

type scaleNote struct {
	d        *cluster.Deployment
	from, to int32
}

func NewEngine(cfg config.Scenario, c *cluster.Cluster, f *generate.Factory) *Engine {
	return &Engine{
		cfg:     cfg,
		c:       c,
		f:       f,
		rnd:     rand.New(rand.NewSource(cfg.Seed ^ 0x5eed)),
		dirtyRS: make(map[*cluster.ReplicaSet]struct{}, 256),
		dirtyD:  make(map[*cluster.Deployment]struct{}, 256),
	}
}

// Prime loads the work queue from the already-generated cluster.
func (e *Engine) Prime() {
	e.c.RLock()
	e.c.Pods.All(func(p *cluster.Pod) bool {
		if p.NextStep != 0 {
			e.pending = append(e.pending, timer{p.NextStep, p})
		}
		return true
	})
	e.c.Deployments.All(func(d *cluster.Deployment) bool {
		e.deploys = append(e.deploys, d)
		return true
	})
	e.c.RUnlock()
	heap.Init(&e.pending)
}

// Run drives the simulation until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.tick(now)
		}
	}
}

func (e *Engine) tick(now time.Time) {
	budget := e.cfg.Lifecycle.TransitionsPerSecond / int(time.Second/tickInterval)
	if budget < 1 {
		budget = 1
	}
	e.c.Write(func(tx *cluster.Tx) {
		e.drainWakeups(tx, now)
		e.stepLifecycle(tx, now, budget)
		e.churn(tx, now)
		e.reconcile(tx, now)
		e.backgroundEvents(tx, now)
		e.flushStatus(tx)
	})
}

// ---------------------------------------------------------------------------
// External wakeups (API writes)
// ---------------------------------------------------------------------------

// WakeDeployment asks the engine to reconcile a Deployment whose spec was
// changed by an API request.
func (e *Engine) WakeDeployment(d *cluster.Deployment, from, to int32) {
	e.wakeMu.Lock()
	e.wakeD = append(e.wakeD, d)
	if from != to {
		e.scaleNote = append(e.scaleNote, scaleNote{d, from, to})
	}
	e.wakeMu.Unlock()
}

// WakePod queues a pod whose deletion was requested through the API.
func (e *Engine) WakePod(p *cluster.Pod) {
	e.wakeMu.Lock()
	e.wakePods = append(e.wakePods, p)
	e.wakeMu.Unlock()
}

// TrackDeployment registers a Deployment created after the initial build.
func (e *Engine) TrackDeployment(d *cluster.Deployment) {
	e.wakeMu.Lock()
	e.wakeD = append(e.wakeD, d)
	e.wakeMu.Unlock()
}

func (e *Engine) drainWakeups(tx *cluster.Tx, now time.Time) {
	e.wakeMu.Lock()
	ds, ps, notes := e.wakeD, e.wakePods, e.scaleNote
	e.wakeD, e.wakePods, e.scaleNote = nil, nil, nil
	e.wakeMu.Unlock()

	for _, d := range ds {
		// A rollout or a delete changes more than the current ReplicaSet, so
		// every generation of the Deployment needs reconciling.
		for _, rs := range d.AllReplicaSets() {
			if rs == d.Current {
				rs.SpecReplicas = d.SpecReplicas
			}
			rs.Generation++
			e.dirtyRS[rs] = struct{}{}
		}
		e.dirtyD[d] = struct{}{}
	}
	for _, p := range ps {
		if !p.Dead {
			e.push(p)
		}
	}
	for _, n := range notes {
		verb := "up"
		if n.to < n.from {
			verb = "down"
		}
		name := n.d.Name
		if n.d.Current != nil {
			name = n.d.Current.Name
		}
		e.recordEvent(tx, n.d.Namespace, "Normal", "ScalingReplicaSet",
			"Scaled "+verb+" replica set "+name+" to "+itoa(n.to)+" from "+itoa(n.from),
			"deployment-controller", "Deployment", n.d.Name, n.d.UID.String(), "apps/v1", true)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func (e *Engine) push(p *cluster.Pod) {
	if p.NextStep == 0 {
		return
	}
	heap.Push(&e.pending, timer{p.NextStep, p})
}

func (e *Engine) stepLifecycle(tx *cluster.Tx, now time.Time, budget int) {
	nowNano := now.UnixNano()
	for budget > 0 && len(e.pending) > 0 && e.pending[0].at <= nowNano {
		t := heap.Pop(&e.pending).(timer)
		p := t.p
		if p.Dead || p.NextStep != t.at {
			continue // stale queue entry
		}
		e.stepPod(tx, p, now)
		e.Transitions.Add(1)
		budget--
	}
}

func (e *Engine) stepPod(tx *cluster.Tx, p *cluster.Pod, now time.Time) {
	rs := p.Tmpl.RS
	if rs != nil {
		e.dirtyRS[rs] = struct{}{}
	}

	if p.Deleted != 0 {
		e.recordPodEvent(tx, p, "Normal", "Killing", "Stopping container "+p.Tmpl.Containers[0].Name, "kubelet")
		tx.RemovePod(p)
		e.PodsDeleted.Add(1)
		return
	}

	switch p.State {
	case cluster.StPending:
		tx.SchedulePod(p, now)
		tx.SetPodState(p, cluster.StContainerCreating)
		p.NextStep = now.Add(e.f.Duration(e.cfg.Lifecycle.Startup)).UnixNano()
		e.recordPodEvent(tx, p, "Normal", "Scheduled",
			"Successfully assigned "+p.Tmpl.Namespace+"/"+p.Name+" to "+cluster.NodeName(p.NodeIdx),
			"default-scheduler")
		tx.TouchPod(p)
		e.push(p)

	case cluster.StContainerCreating:
		p.Started = now.Unix()
		target := p.Target
		if target == cluster.StPending || target == cluster.StContainerCreating {
			target = cluster.StRunning
		}
		tx.SetPodState(p, target)
		switch target {
		case cluster.StRunning:
			p.NextStep = now.Add(minReadySeconds).UnixNano()
			e.recordPodEvent(tx, p, "Normal", "Pulled", `Successfully pulled image "`+p.Tmpl.Containers[0].Image+`"`, "kubelet")
			e.recordPodEvent(tx, p, "Normal", "Started", "Started container "+p.Tmpl.Containers[0].Name, "kubelet")
		case cluster.StCrashLoopBackOff, cluster.StOOMKilled:
			p.Restarts++
			p.NextStep = now.Add(e.f.Backoff(p.Restarts)).UnixNano()
			e.recordPodEvent(tx, p, "Warning", "BackOff", "Back-off restarting failed container "+p.Tmpl.Containers[0].Name, "kubelet")
		case cluster.StImagePullBackOff, cluster.StErrImagePull:
			p.NextStep = 0
			e.recordPodEvent(tx, p, "Warning", "Failed", `Failed to pull image "`+p.Tmpl.Containers[0].Image+`": not found`, "kubelet")
		default:
			p.NextStep = 0
		}
		tx.TouchPod(p)
		e.push(p)

	case cluster.StRunning:
		// minReadySeconds elapsed.
		tx.MarkPodAvailable(p)
		p.NextStep = 0
		tx.TouchPod(p)

	case cluster.StCrashLoopBackOff, cluster.StOOMKilled:
		p.Restarts++
		// A small share of crash-looping pods eventually recover.
		if e.rnd.Float64() < 0.02 {
			tx.SetPodState(p, cluster.StRunning)
			p.Started = now.Unix()
			p.NextStep = now.Add(minReadySeconds).UnixNano()
			e.recordPodEvent(tx, p, "Normal", "Started", "Started container "+p.Tmpl.Containers[0].Name, "kubelet")
		} else {
			p.NextStep = now.Add(e.f.Backoff(p.Restarts)).UnixNano()
			e.recordPodEvent(tx, p, "Warning", "BackOff", "Back-off restarting failed container "+p.Tmpl.Containers[0].Name, "kubelet")
		}
		tx.TouchPod(p)
		e.push(p)

	default:
		p.NextStep = 0
	}
}

// ---------------------------------------------------------------------------
// Churn
// ---------------------------------------------------------------------------

func (e *Engine) churn(tx *cluster.Tx, now time.Time) {
	secs := tickInterval.Seconds()
	ch := e.cfg.Churn

	e.accDelete += ch.PodsPerSecond * secs
	for ; e.accDelete >= 1; e.accDelete-- {
		e.deleteRandomPod(tx, now)
	}
	e.accRestart += ch.RestartsPerSecond * secs
	for ; e.accRestart >= 1; e.accRestart-- {
		e.restartRandomPod(tx, now)
	}
	e.accScale += ch.ScalesPerMinute * secs / 60
	for ; e.accScale >= 1; e.accScale-- {
		e.scaleRandomDeployment(tx, now)
	}
	e.accNode += ch.NodesPerMinute * secs / 60
	for ; e.accNode >= 1; e.accNode-- {
		e.flipRandomNode(tx, now)
	}
}

func (e *Engine) randomDeployment() *cluster.Deployment {
	for i := 0; i < 8 && len(e.deploys) > 0; i++ {
		d := e.deploys[e.rnd.Intn(len(e.deploys))]
		if d.Current != nil && !d.Deleting {
			return d
		}
	}
	return nil
}

func (e *Engine) randomPod(d *cluster.Deployment) *cluster.Pod {
	pods := d.Current.Pods()
	for i := 0; i < 8 && len(pods) > 0; i++ {
		p := pods[e.rnd.Intn(len(pods))]
		if p != nil && !p.Dead && p.Deleted == 0 {
			return p
		}
	}
	return nil
}

func (e *Engine) deleteRandomPod(tx *cluster.Tx, now time.Time) {
	d := e.randomDeployment()
	if d == nil {
		return
	}
	p := e.randomPod(d)
	if p == nil {
		return
	}
	tx.MarkPodTerminating(p, e.f.Duration(e.cfg.Lifecycle.Termination))
	e.push(p)
	e.dirtyRS[d.Current] = struct{}{}
	e.recordPodEvent(tx, p, "Normal", "Killing", "Stopping container "+p.Tmpl.Containers[0].Name, "kubelet")
}

func (e *Engine) restartRandomPod(tx *cluster.Tx, now time.Time) {
	d := e.randomDeployment()
	if d == nil {
		return
	}
	p := e.randomPod(d)
	if p == nil || !p.State.Ready() {
		return
	}
	p.Restarts++
	if e.rnd.Float64() < 0.3 {
		tx.SetPodState(p, cluster.StCrashLoopBackOff)
		p.NextStep = now.Add(e.f.Backoff(p.Restarts)).UnixNano()
		e.push(p)
		e.recordPodEvent(tx, p, "Warning", "BackOff", "Back-off restarting failed container "+p.Tmpl.Containers[0].Name, "kubelet")
	} else {
		p.Started = now.Unix()
		e.recordPodEvent(tx, p, "Warning", "Unhealthy", "Liveness probe failed: HTTP probe failed with statuscode: 503", "kubelet")
	}
	tx.TouchPod(p)
	e.dirtyRS[d.Current] = struct{}{}
}

func (e *Engine) scaleRandomDeployment(tx *cluster.Tx, now time.Time) {
	d := e.randomDeployment()
	if d == nil {
		return
	}
	from := d.SpecReplicas
	delta := int32(float64(from) * e.cfg.Churn.ScaleFactor)
	if delta < 1 {
		delta = 1
	}
	to := from + int32(e.rnd.Intn(int(2*delta)+1)) - delta
	if to < 0 {
		to = 0
	}
	if to == from {
		return
	}
	d.SpecReplicas = to
	d.Generation++
	d.LastScale = now.Unix()
	d.Current.SpecReplicas = to
	d.Current.Generation++
	e.dirtyRS[d.Current] = struct{}{}
	e.dirtyD[d] = struct{}{}

	verb := "up"
	if to < from {
		verb = "down"
	}
	e.recordEvent(tx, d.Namespace, "Normal", "ScalingReplicaSet",
		"Scaled "+verb+" replica set "+d.Current.Name+" to "+itoa(to)+" from "+itoa(from),
		"deployment-controller", "Deployment", d.Name, d.UID.String(), "apps/v1", true)
}

func (e *Engine) flipRandomNode(tx *cluster.Tx, now time.Time) {
	if e.c.NodeCount() == 0 {
		return
	}
	n := e.c.NodeAt(int32(e.rnd.Intn(e.c.NodeCount())))
	if n == nil {
		return
	}
	if n.Ready == "True" {
		n.Ready = "False"
		e.recordEvent(tx, "default", "Warning", "NodeNotReady", "Node "+n.Name+" status is now: NodeNotReady",
			"node-controller", "Node", n.Name, n.UID.String(), "v1", true)
	} else {
		n.Ready = "True"
		e.recordEvent(tx, "default", "Normal", "NodeReady", "Node "+n.Name+" status is now: NodeReady",
			"node-controller", "Node", n.Name, n.UID.String(), "v1", true)
	}
	tx.TouchNode(n)
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

// maxCreatesPerTick bounds how many pods a single transaction materializes so
// a scale from 100 to 100,000 ramps up instead of freezing every reader.
const maxCreatesPerTick = 2000

func (e *Engine) reconcile(tx *cluster.Tx, now time.Time) {
	budget := maxCreatesPerTick
	for rs := range e.dirtyRS {
		if rs.Deploy != nil {
			e.dirtyD[rs.Deploy] = struct{}{}
		}
		// A deleted Deployment's ReplicaSets disappear once they are empty.
		if rs.Deploy != nil && rs.Deploy.Deleting {
			rs.SpecReplicas = 0
			if rs.Total == 0 {
				tx.RemoveReplicaSet(rs)
				delete(e.dirtyRS, rs)
				delete(e.dirtyD, rs.Deploy)
				continue
			}
		}
		active := rs.Total - rs.Terminating
		switch {
		case active < rs.SpecReplicas:
			want := int(rs.SpecReplicas - active)
			if want > budget {
				want = budget
			}
			for i := 0; i < want; i++ {
				p := e.f.NewPod(tx, rs, now, false)
				e.push(p)
				e.PodsCreated.Add(1)
			}
			budget -= want
			if want > 0 {
				e.recordEvent(tx, rs.Namespace, "Normal", "SuccessfulCreate",
					"Created pod: "+rs.Tmpl.NamePrefix+"*", "replicaset-controller",
					"ReplicaSet", rs.Name, rs.UID.String(), "apps/v1", false)
			}
		case active > rs.SpecReplicas:
			e.scaleDown(tx, rs, int(active-rs.SpecReplicas), now)
		}
	}
}

// scaleDown terminates surplus pods, preferring the least useful ones first,
// which is roughly what the real ReplicaSet controller does.
func (e *Engine) scaleDown(tx *cluster.Tx, rs *cluster.ReplicaSet, n int, now time.Time) {
	pods := rs.Pods()
	victims := make([]*cluster.Pod, 0, n)
	for pass := 0; pass < 2 && len(victims) < n; pass++ {
		for _, p := range pods {
			if len(victims) >= n {
				break
			}
			if p == nil || p.Dead || p.Deleted != 0 {
				continue
			}
			unready := !p.State.Ready()
			if (pass == 0) == unready {
				victims = append(victims, p)
			}
		}
	}
	for _, p := range victims {
		tx.MarkPodTerminating(p, e.f.Duration(e.cfg.Lifecycle.Termination))
		e.push(p)
	}
	if len(victims) > 0 {
		e.recordEvent(tx, rs.Namespace, "Normal", "SuccessfulDelete",
			"Deleted pod: "+victims[0].Name, "replicaset-controller",
			"ReplicaSet", rs.Name, rs.UID.String(), "apps/v1", false)
	}
}

func (e *Engine) flushStatus(tx *cluster.Tx) {
	for rs := range e.dirtyRS {
		tx.TouchReplicaSet(rs)
		delete(e.dirtyRS, rs)
	}
	for d := range e.dirtyD {
		d.ObservedGeneration = d.Generation
		tx.TouchDeployment(d)
		delete(e.dirtyD, d)
	}
}
