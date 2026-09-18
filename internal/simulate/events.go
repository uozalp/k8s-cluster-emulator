package simulate

import (
	"strconv"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
)

func itoa(v int32) string { return strconv.FormatInt(int64(v), 10) }

// recordEvent stores an Event unless sampling says to skip it. force is used
// for controller-level events (scaling, node flips) that are rare enough to
// always be interesting.
func (e *Engine) recordEvent(tx *cluster.Tx, ns, typ, reason, msg, component, refKind, refName, refUID, refAPI string, force bool) {
	if e.cfg.Events.Max <= 0 {
		return
	}
	if !force && e.rnd.Float64() > e.cfg.Events.Sample {
		return
	}
	now := tx.Now.Unix()
	tx.RecordEvent(&cluster.Event{
		Namespace: ns,
		First:     now,
		Last:      now,
		Count:     1,
		Type:      typ,
		Reason:    reason,
		Message:   msg,
		Component: component,
		Host:      "",
		RefKind:   refKind, RefName: refName, RefUID: refUID, RefAPIVers: refAPI,
	})
	e.EventsMade.Add(1)
}

func (e *Engine) recordPodEvent(tx *cluster.Tx, p *cluster.Pod, typ, reason, msg, component string) {
	if e.cfg.Events.Max <= 0 {
		return
	}
	if e.rnd.Float64() > e.cfg.Events.Sample {
		return
	}
	now := tx.Now.Unix()
	tx.RecordEvent(&cluster.Event{
		Namespace: p.Tmpl.Namespace,
		First:     now,
		Last:      now,
		Count:     1,
		Type:      typ,
		Reason:    reason,
		Message:   msg,
		Component: component,
		Host:      cluster.NodeName(p.NodeIdx),
		RefKind:   "Pod", RefName: p.Name, RefUID: p.UID.String(), RefAPIVers: "v1",
	})
	e.EventsMade.Add(1)
}

var backgroundReasons = []struct{ typ, reason, msg, component string }{
	{"Warning", "FailedScheduling", "0/500 nodes are available: insufficient cpu, insufficient memory.", "default-scheduler"},
	{"Warning", "Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 503", "kubelet"},
	{"Warning", "FailedMount", "MountVolume.SetUp failed for volume \"config\": timed out waiting for the condition", "kubelet"},
	{"Normal", "Pulling", "Pulling image", "kubelet"},
	{"Normal", "Created", "Created container", "kubelet"},
	{"Warning", "Evicted", "The node was low on resource: memory.", "kubelet"},
	{"Warning", "NodeHasDiskPressure", "Node has disk pressure", "kubelet"},
}

// backgroundEvents adds synthetic events on top of lifecycle ones so a
// scenario can flood k9s with events without also flooding it with pods.
func (e *Engine) backgroundEvents(tx *cluster.Tx, now time.Time) {
	rate := e.cfg.Events.ExtraPerSecond
	if rate <= 0 || e.cfg.Events.Max <= 0 {
		return
	}
	e.accEvent += rate * tickInterval.Seconds()
	for ; e.accEvent >= 1; e.accEvent-- {
		d := e.randomDeployment()
		if d == nil {
			return
		}
		r := backgroundReasons[e.rnd.Intn(len(backgroundReasons))]
		refKind, refName, refUID, refAPI := "Deployment", d.Name, d.UID.String(), "apps/v1"
		if p := e.randomPod(d); p != nil {
			refKind, refName, refUID, refAPI = "Pod", p.Name, p.UID.String(), "v1"
		}
		e.recordEvent(tx, d.Namespace, r.typ, r.reason, r.msg, r.component, refKind, refName, refUID, refAPI, true)
	}
}
