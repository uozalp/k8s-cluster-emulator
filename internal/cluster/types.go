package cluster

import (
	"strconv"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
)

// ---------------------------------------------------------------------------
// UID
// ---------------------------------------------------------------------------

// UID is a 128-bit identifier stored unformatted. At half a million pods the
// 36-byte string form costs more than the objects themselves, so it is
// rendered on demand.
type UID [2]uint64

const hexDigits = "0123456789abcdef"

func (u UID) String() string {
	var b [36]byte
	pos := 0
	write := func(v uint64, nibbles int) {
		for i := nibbles - 1; i >= 0; i-- {
			b[pos] = hexDigits[(v>>(uint(i)*4))&0xf]
			pos++
		}
	}
	write(u[0]>>32, 8)
	b[pos] = '-'
	pos++
	write(u[0]>>16, 4)
	b[pos] = '-'
	pos++
	write((u[0]&0xffff)|0x4000, 4) // version 4 nibble
	b[pos] = '-'
	pos++
	write((u[1]>>48)|0x8000, 4) // RFC 4122 variant
	b[pos] = '-'
	pos++
	write(u[1]&0xffffffffffff, 12)
	return string(b[:])
}

// ---------------------------------------------------------------------------
// Pod lifecycle
// ---------------------------------------------------------------------------

type PodState uint8

const (
	StPending PodState = iota
	StContainerCreating
	StRunning
	StCrashLoopBackOff
	StImagePullBackOff
	StErrImagePull
	StOOMKilled
	StError
	StEvicted
	StSucceeded
	StTerminating
	StUnknown
	podStateCount
)

var podStateNames = [podStateCount]string{
	StPending:           "Pending",
	StContainerCreating: "ContainerCreating",
	StRunning:           "Running",
	StCrashLoopBackOff:  "CrashLoopBackOff",
	StImagePullBackOff:  "ImagePullBackOff",
	StErrImagePull:      "ErrImagePull",
	StOOMKilled:         "OOMKilled",
	StError:             "Error",
	StEvicted:           "Evicted",
	StSucceeded:         "Completed",
	StTerminating:       "Terminating",
	StUnknown:           "Unknown",
}

func (s PodState) String() string {
	if int(s) < len(podStateNames) {
		return podStateNames[s]
	}
	return "Unknown"
}

// Phase is the status.phase a real kubelet would report for this state.
func (s PodState) Phase() string {
	switch s {
	case StPending, StContainerCreating, StImagePullBackOff, StErrImagePull:
		return "Pending"
	case StRunning, StCrashLoopBackOff, StOOMKilled, StTerminating:
		return "Running"
	case StSucceeded:
		return "Succeeded"
	case StError, StEvicted:
		return "Failed"
	default:
		return "Unknown"
	}
}

// Settled reports whether the state needs no further lifecycle work.
func (s PodState) Settled() bool {
	switch s {
	case StRunning, StSucceeded, StError, StEvicted, StImagePullBackOff, StUnknown:
		return true
	default:
		return false
	}
}

func (s PodState) Ready() bool { return s == StRunning }

// ---------------------------------------------------------------------------
// Resource profiles
// ---------------------------------------------------------------------------

// ResourceProfile is a named request/limit shape shared by every pod of a
// workload, the way a real pod template works.
type ResourceProfile struct {
	Name       string
	CPUReqM    int64 // millicores
	CPULimM    int64
	MemReqMi   int64
	MemLimMi   int64
	UsageRatio float64 // fraction of the request actually "used"
}

var Profiles = []ResourceProfile{
	{"small", 50, 200, 64, 128, 0.7},
	{"medium", 250, 1000, 256, 512, 0.6},
	{"large", 1000, 2000, 1024, 2048, 0.55},
	{"extreme", 4000, 8000, 8192, 16384, 0.5},
	{"wasteful", 8000, 16000, 16384, 32768, 0.05},
}

func (p *ResourceProfile) Requirements() kapi.ResourceRequirements {
	return kapi.ResourceRequirements{
		Requests: kapi.ResourceList{
			"cpu":    strconv.FormatInt(p.CPUReqM, 10) + "m",
			"memory": strconv.FormatInt(p.MemReqMi, 10) + "Mi",
		},
		Limits: kapi.ResourceList{
			"cpu":    strconv.FormatInt(p.CPULimM, 10) + "m",
			"memory": strconv.FormatInt(p.MemLimMi, 10) + "Mi",
		},
	}
}

// ---------------------------------------------------------------------------
// Pod template — shared by every pod of a ReplicaSet
// ---------------------------------------------------------------------------

// PodTemplate holds everything identical across the pods of one ReplicaSet.
// Sharing it by pointer is what keeps a 500k-pod cluster in a few hundred MB.
type PodTemplate struct {
	RS *ReplicaSet // owning ReplicaSet; nil for standalone pods

	Namespace     string
	NamePrefix    string // "frontend-7f8c9d-"
	App           string
	Labels        map[string]string
	Annotations   map[string]string
	Containers    []kapi.Container
	InitContainer *kapi.Container
	Volumes       []kapi.Volume
	NodeSelector  map[string]string
	Tolerations   []kapi.Toleration
	ServiceAcct   string
	PriorityClass string
	Priority      int32
	RestartPolicy string
	TermGrace     int64
	Profile       *ResourceProfile
}

// ---------------------------------------------------------------------------
// Pod
// ---------------------------------------------------------------------------

// Pod is the compact in-memory form. The wire representation is built on
// demand by Render.
type Pod struct {
	Name string
	Tmpl *PodTemplate
	UID  UID
	RV   uint64

	Created   int64 // unix seconds
	Scheduled int64
	Started   int64
	Deleted   int64 // non-zero once a deletionTimestamp is set
	NextStep  int64 // unix nanos; when the next lifecycle transition is due

	NodeIdx  int32
	IPSuffix uint32
	Restarts int32
	State    PodState
	Target   PodState // state the pod is progressing toward
	Jitter   uint16   // per-pod deterministic noise for metrics
	Avail    bool     // has been ready for longer than minReadySeconds
	Dead     bool     // removed from the store; queued work must skip it
}

func (p *Pod) GetNamespace() string { return p.Tmpl.Namespace }
func (p *Pod) GetName() string      { return p.Name }

// ---------------------------------------------------------------------------
// ReplicaSet
// ---------------------------------------------------------------------------

type ReplicaSet struct {
	Name       string
	Namespace  string
	UID        UID
	RV         uint64
	Created    int64
	Generation int64
	Revision   int

	Deploy *Deployment
	Tmpl   *PodTemplate
	Hash   string

	SpecReplicas int32

	// Live counters maintained by the controllers so status never requires a
	// scan over the pod table.
	Total       int32
	ReadyCnt    int32
	AvailCnt    int32
	Terminating int32
	Observed    int64
	pods        []*Pod
	deadSlots   int

	nextOrdinal uint32
}

func (r *ReplicaSet) GetNamespace() string { return r.Namespace }
func (r *ReplicaSet) GetName() string      { return r.Name }

// Pods returns the live pods owned by this ReplicaSet.
func (r *ReplicaSet) Pods() []*Pod { return r.pods }

func (r *ReplicaSet) addPod(p *Pod) {
	r.pods = append(r.pods, p)
	r.Total++
}

func (r *ReplicaSet) removePod(p *Pod) {
	for i, x := range r.pods {
		if x == p {
			r.pods[i] = nil
			r.deadSlots++
			r.Total--
			break
		}
	}
	if r.deadSlots > 64 && r.deadSlots*2 > len(r.pods) {
		out := r.pods[:0]
		for _, x := range r.pods {
			if x != nil {
				out = append(out, x)
			}
		}
		r.pods = out
		r.deadSlots = 0
	}
}

// ---------------------------------------------------------------------------
// Deployment
// ---------------------------------------------------------------------------

type DeployHealth uint8

const (
	HealthNormal DeployHealth = iota
	HealthDegraded
	HealthFailing
)

type Deployment struct {
	Name      string
	Namespace string
	UID       UID
	RV        uint64
	Created   int64

	Generation         int64
	ObservedGeneration int64
	SpecReplicas       int32

	Selector map[string]string
	Current  *ReplicaSet
	Old      []*ReplicaSet

	Health       DeployHealth
	AvailTarget  float64 // fraction of replicas allowed to become available
	LastScale    int64
	Deleting     bool
	MaxSurge     string
	MaxUnavail   string
	RevisionLast int
}

func (d *Deployment) GetNamespace() string { return d.Namespace }
func (d *Deployment) GetName() string      { return d.Name }

// AllReplicaSets returns the current ReplicaSet followed by historical ones.
func (d *Deployment) AllReplicaSets() []*ReplicaSet {
	out := make([]*ReplicaSet, 0, len(d.Old)+1)
	if d.Current != nil {
		out = append(out, d.Current)
	}
	return append(out, d.Old...)
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

type Node struct {
	Name    string
	UID     UID
	RV      uint64
	Created int64
	Idx     int32

	Ready         string // "True" | "False" | "Unknown"
	Unschedulable bool
	CPUCores      int32
	MemGi         int32
	PodCapacity   int32
	Pool          string
	Zone          string
	InstanceType  string

	Pods int32 // scheduled pod count, maintained incrementally
}

func (n *Node) GetNamespace() string { return "" }
func (n *Node) GetName() string      { return n.Name }

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

type Namespace struct {
	Name    string
	UID     UID
	RV      uint64
	Created int64
	Phase   string
	Labels  map[string]string
}

func (n *Namespace) GetNamespace() string { return "" }
func (n *Namespace) GetName() string      { return n.Name }

// ---------------------------------------------------------------------------
// Service / ConfigMap / Secret
// ---------------------------------------------------------------------------

type Service struct {
	Name      string
	Namespace string
	UID       UID
	RV        uint64
	Created   int64
	Type      string
	ClusterIP string
	ExtIP     string
	Ports     []kapi.ServicePort
	Selector  map[string]string
	Labels    map[string]string
}

func (s *Service) GetNamespace() string { return s.Namespace }
func (s *Service) GetName() string      { return s.Name }

type ConfigMap struct {
	Name      string
	Namespace string
	UID       UID
	RV        uint64
	Created   int64
	Labels    map[string]string
	Data      map[string]string
}

func (c *ConfigMap) GetNamespace() string { return c.Namespace }
func (c *ConfigMap) GetName() string      { return c.Name }

type Secret struct {
	Name      string
	Namespace string
	UID       UID
	RV        uint64
	Created   int64
	Type      string
	Labels    map[string]string
	Data      map[string]string
}

func (s *Secret) GetNamespace() string { return s.Namespace }
func (s *Secret) GetName() string      { return s.Name }

// ---------------------------------------------------------------------------
// Job / CronJob
// ---------------------------------------------------------------------------

type Job struct {
	Name        string
	Namespace   string
	UID         UID
	RV          uint64
	Created     int64
	Completions int32
	Parallelism int32
	Succeeded   int32
	Failed      int32
	Active      int32
	StartTime   int64
	Completion  int64
	Owner       *CronJob
	Tmpl        *PodTemplate
	Labels      map[string]string
}

func (j *Job) GetNamespace() string { return j.Namespace }
func (j *Job) GetName() string      { return j.Name }

type CronJob struct {
	Name         string
	Namespace    string
	UID          UID
	RV           uint64
	Created      int64
	Schedule     string
	Suspend      bool
	LastSchedule int64
	Tmpl         *PodTemplate
	Labels       map[string]string
}

func (c *CronJob) GetNamespace() string { return c.Namespace }
func (c *CronJob) GetName() string      { return c.Name }

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

type Event struct {
	Name       string
	Namespace  string
	UID        UID
	RV         uint64
	First      int64
	Last       int64
	Count      int32
	Type       string
	Reason     string
	Message    string
	Component  string
	Host       string
	RefKind    string
	RefName    string
	RefUID     string
	RefAPIVers string
}

func (e *Event) GetNamespace() string { return e.Namespace }
func (e *Event) GetName() string      { return e.Name }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func ts(sec int64) string {
	if sec == 0 {
		return ""
	}
	return time.Unix(sec, 0).UTC().Format(time.RFC3339)
}

func rvStr(rv uint64) string { return strconv.FormatUint(rv, 10) }
