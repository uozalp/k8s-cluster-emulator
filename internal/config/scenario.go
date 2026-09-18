// Package config defines the stress-test scenario description: how big the
// simulated cluster is, how its pods behave, and how the API server responds.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Range is an inclusive [Min,Max] integer range. A scenario uses it wherever
// a value should vary between generated objects.
type Range struct {
	Min int `yaml:"min"`
	Max int `yaml:"max"`
}

func (r Range) normalized() Range {
	if r.Max < r.Min {
		r.Max = r.Min
	}
	return r
}

// Duration is a time.Duration that round-trips through YAML as "150ms"
// rather than a raw nanosecond count.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	var n int64
	if err := value.Decode(&n); err != nil {
		return err
	}
	*d = Duration(time.Duration(n) * time.Millisecond)
	return nil
}

// DurationRange is the time-valued counterpart of Range.
type DurationRange struct {
	Min Duration `yaml:"min"`
	Max Duration `yaml:"max"`
}

func (r DurationRange) normalized() DurationRange {
	if r.Max < r.Min {
		r.Max = r.Min
	}
	return r
}

func durRange(min, max time.Duration) DurationRange {
	return DurationRange{Duration(min), Duration(max)}
}

// LifecycleMix is the steady-state distribution pods settle into, expressed
// as relative weights (percentages that need not sum to exactly 100).
type LifecycleMix struct {
	Running           float64 `yaml:"running"`
	Pending           float64 `yaml:"pending"`
	CrashLoopBackOff  float64 `yaml:"crashLoopBackOff"`
	ImagePullBackOff  float64 `yaml:"imagePullBackOff"`
	Failed            float64 `yaml:"failed"`
	Succeeded         float64 `yaml:"succeeded"`
	ContainerCreating float64 `yaml:"containerCreating"`
}

func (m LifecycleMix) Total() float64 {
	return m.Running + m.Pending + m.CrashLoopBackOff + m.ImagePullBackOff +
		m.Failed + m.Succeeded + m.ContainerCreating
}

// ResourceMix weights the per-workload resource request/limit profiles.
type ResourceMix struct {
	Small    float64 `yaml:"small"`
	Medium   float64 `yaml:"medium"`
	Large    float64 `yaml:"large"`
	Extreme  float64 `yaml:"extreme"`
	Wasteful float64 `yaml:"wasteful"`
}

func (m ResourceMix) Total() float64 {
	return m.Small + m.Medium + m.Large + m.Extreme + m.Wasteful
}

// Workloads describes how pods are distributed across deployments.
type Workloads struct {
	// Deployments is the total number of Deployments across all namespaces.
	Deployments int `yaml:"deployments"`
	// Replicas is the per-Deployment replica count range. When Pods is set
	// the range is used only to weight the distribution of the pod budget.
	Replicas Range `yaml:"replicas"`
	// Pods, when > 0, is a hard total pod budget spread across Deployments.
	Pods int `yaml:"pods"`
	// ExtraReplicaSets is the number of stale (scaled-to-zero) ReplicaSets
	// kept per Deployment to emulate rollout history.
	ExtraReplicaSets Range `yaml:"extraReplicaSets"`
	// Containers is the per-pod container count range.
	Containers Range `yaml:"containers"`
	// DegradedPercent of Deployments never reach full availability.
	DegradedPercent float64 `yaml:"degradedPercent"`
	// FailingPercent of Deployments report a ProgressDeadlineExceeded condition.
	FailingPercent float64 `yaml:"failingPercent"`
}

// Lifecycle controls how pods move through their states.
type Lifecycle struct {
	Mix LifecycleMix `yaml:"mix"`
	// Scheduling is how long a pod stays Pending before being scheduled.
	Scheduling DurationRange `yaml:"scheduling"`
	// Startup is how long ContainerCreating takes before Running.
	Startup DurationRange `yaml:"startup"`
	// Termination is how long a pod lingers in Terminating before deletion.
	Termination DurationRange `yaml:"termination"`
	// StartedRunning seeds the initial cluster with already-settled pods
	// instead of making every pod boot from Pending at startup.
	StartedRunning bool `yaml:"startedRunning"`
	// TransitionsPerSecond caps lifecycle work so huge clusters stay responsive.
	TransitionsPerSecond int `yaml:"transitionsPerSecond"`
}

// Churn controls ongoing mutation of the cluster.
type Churn struct {
	// PodsPerSecond is how many pods are deleted (and recreated by their
	// ReplicaSet) each second.
	PodsPerSecond float64 `yaml:"podsPerSecond"`
	// RestartsPerSecond is how many running pods get a container restart.
	RestartsPerSecond float64 `yaml:"restartsPerSecond"`
	// ScalesPerMinute is how many Deployments are randomly rescaled per minute.
	ScalesPerMinute float64 `yaml:"scalesPerMinute"`
	// ScaleFactor bounds the random rescale as a fraction of current replicas.
	ScaleFactor float64 `yaml:"scaleFactor"`
	// NodesPerMinute is how many nodes flip Ready condition per minute.
	NodesPerMinute float64 `yaml:"nodesPerMinute"`
}

// Events controls Event object generation.
type Events struct {
	// Max is the retention cap; oldest events are evicted beyond it.
	Max int `yaml:"max"`
	// Seed is how many historical events to pre-generate.
	Seed int `yaml:"seed"`
	// Sample is the fraction of lifecycle transitions that emit an Event.
	Sample float64 `yaml:"sample"`
	// ExtraPerSecond adds synthetic background events on top of transitions.
	ExtraPerSecond float64 `yaml:"extraPerSecond"`
}

// API controls how the HTTP layer behaves.
type API struct {
	Port int `yaml:"port"`
	// Latency is the base per-request delay, scaled per endpoint class.
	Latency Duration `yaml:"latency"`
	// Jitter is the fractional random variation applied to Latency.
	Jitter float64 `yaml:"jitter"`
	// PerObjectLatency is added once per serialized list item.
	PerObjectLatency Duration `yaml:"perObjectLatency"`
	// Chaos injects 500s, 429s and stalls.
	Chaos bool `yaml:"chaos"`
	// ChaosPercent is the combined error probability when Chaos is on.
	ChaosPercent float64 `yaml:"chaosPercent"`
	// WatchChurn periodically severs watch connections.
	WatchChurn bool `yaml:"watchChurn"`
	// WatchChurnInterval is the mean time between forced watch disconnects.
	WatchChurnInterval Duration `yaml:"watchChurnInterval"`
	// DefaultListLimit caps unbounded LIST requests (0 = unlimited).
	DefaultListLimit int `yaml:"defaultListLimit"`
	// WatchBuffer is the per-kind replay ring size used to serve watches
	// that resume from an older resourceVersion.
	WatchBuffer int `yaml:"watchBuffer"`
	// Metrics enables the metrics.k8s.io API group.
	Metrics bool `yaml:"metrics"`
}

// Objects controls per-object payload size, for testing serialization cost.
type Objects struct {
	// ExtraLabels adds N synthetic labels per pod template.
	ExtraLabels int `yaml:"extraLabels"`
	// ExtraAnnotations adds N synthetic annotations per pod template.
	ExtraAnnotations int `yaml:"extraAnnotations"`
	// AnnotationSize is the byte length of each synthetic annotation value.
	AnnotationSize int `yaml:"annotationSize"`
}

// Scenario is a complete, reproducible description of a simulated cluster.
type Scenario struct {
	Name string `yaml:"name"`
	Seed int64  `yaml:"seed"`

	Nodes         int    `yaml:"nodes"`
	Namespaces    int    `yaml:"namespaces"`
	SystemNS      bool   `yaml:"systemNamespaces"`
	NamespacePfx  string `yaml:"namespacePrefix"`
	ServicesPerNS Range  `yaml:"servicesPerNamespace"`
	ConfigMapsPer Range  `yaml:"configMapsPerNamespace"`
	SecretsPerNS  Range  `yaml:"secretsPerNamespace"`
	JobsPerNS     Range  `yaml:"jobsPerNamespace"`
	CronJobsPerNS Range  `yaml:"cronJobsPerNamespace"`

	Workloads Workloads   `yaml:"workloads"`
	Lifecycle Lifecycle   `yaml:"lifecycle"`
	Resources ResourceMix `yaml:"resources"`
	Churn     Churn       `yaml:"churn"`
	Events    Events      `yaml:"events"`
	API       API         `yaml:"api"`
	Objects   Objects     `yaml:"objects"`
}

// Default returns a moderate scenario that is a sane base for overrides.
func Default() Scenario {
	return Scenario{
		Name:          "default",
		Seed:          12345,
		Nodes:         500,
		Namespaces:    20,
		SystemNS:      true,
		NamespacePfx:  "team",
		ServicesPerNS: Range{1, 4},
		ConfigMapsPer: Range{1, 6},
		SecretsPerNS:  Range{1, 4},
		JobsPerNS:     Range{0, 3},
		CronJobsPerNS: Range{0, 2},
		Workloads: Workloads{
			Deployments:      200,
			Replicas:         Range{1, 40},
			Pods:             20000,
			ExtraReplicaSets: Range{0, 2},
			Containers:       Range{1, 3},
			DegradedPercent:  6,
			FailingPercent:   2,
		},
		Lifecycle: Lifecycle{
			Mix: LifecycleMix{
				Running:          90,
				Pending:          3,
				CrashLoopBackOff: 2,
				ImagePullBackOff: 1,
				Failed:           2,
				Succeeded:        2,
			},
			Scheduling:           durRange(500*time.Millisecond, 4*time.Second),
			Startup:              durRange(2*time.Second, 20*time.Second),
			Termination:          durRange(2*time.Second, 30*time.Second),
			StartedRunning:       true,
			TransitionsPerSecond: 2000,
		},
		Resources: ResourceMix{Small: 50, Medium: 30, Large: 14, Extreme: 3, Wasteful: 3},
		Churn: Churn{
			PodsPerSecond:     2,
			RestartsPerSecond: 1,
			ScalesPerMinute:   2,
			ScaleFactor:       0.25,
			NodesPerMinute:    1,
		},
		Events: Events{Max: 20000, Seed: 2000, Sample: 0.25, ExtraPerSecond: 0},
		API: API{
			Port:               6443,
			Latency:            0,
			Jitter:             0.5,
			Chaos:              false,
			ChaosPercent:       5,
			WatchChurn:         false,
			WatchChurnInterval: Duration(30 * time.Second),
			WatchBuffer:        8192,
			Metrics:            true,
		},
	}
}

// Normalize fills in derived defaults and clamps nonsense values so the rest
// of the emulator can assume the scenario is well-formed.
func (s *Scenario) Normalize() {
	if s.Nodes < 1 {
		s.Nodes = 1
	}
	if s.Namespaces < 1 {
		s.Namespaces = 1
	}
	if s.NamespacePfx == "" {
		s.NamespacePfx = "team"
	}
	if s.Workloads.Deployments < 1 {
		s.Workloads.Deployments = 1
	}
	if s.Workloads.Containers.Min < 1 {
		s.Workloads.Containers.Min = 1
	}
	s.Workloads.Containers = s.Workloads.Containers.normalized()
	s.Workloads.Replicas = s.Workloads.Replicas.normalized()
	s.Workloads.ExtraReplicaSets = s.Workloads.ExtraReplicaSets.normalized()
	s.ServicesPerNS = s.ServicesPerNS.normalized()
	s.ConfigMapsPer = s.ConfigMapsPer.normalized()
	s.SecretsPerNS = s.SecretsPerNS.normalized()
	s.JobsPerNS = s.JobsPerNS.normalized()
	s.CronJobsPerNS = s.CronJobsPerNS.normalized()
	s.Lifecycle.Scheduling = s.Lifecycle.Scheduling.normalized()
	s.Lifecycle.Startup = s.Lifecycle.Startup.normalized()
	s.Lifecycle.Termination = s.Lifecycle.Termination.normalized()

	if s.Lifecycle.Mix.Total() <= 0 {
		s.Lifecycle.Mix = LifecycleMix{Running: 100}
	}
	if s.Resources.Total() <= 0 {
		s.Resources = ResourceMix{Medium: 100}
	}
	if s.Lifecycle.TransitionsPerSecond < 1 {
		s.Lifecycle.TransitionsPerSecond = 1000
	}
	if s.Events.Max < 0 {
		s.Events.Max = 0
	}
	if s.Events.Seed > s.Events.Max {
		s.Events.Seed = s.Events.Max
	}
	if s.API.Port == 0 {
		s.API.Port = 6443
	}
	if s.API.Jitter < 0 {
		s.API.Jitter = 0
	}
	if s.API.WatchBuffer < 256 {
		s.API.WatchBuffer = 256
	}
	if s.API.WatchChurnInterval <= 0 {
		s.API.WatchChurnInterval = Duration(30 * time.Second)
	}
	if s.API.ChaosPercent <= 0 {
		s.API.ChaosPercent = 5
	}
	if s.Churn.ScaleFactor <= 0 {
		s.Churn.ScaleFactor = 0.25
	}
}

// Load reads a scenario from a YAML file, layered on top of a base preset
// named by the file's `name` field (or Default when absent).
func Load(path string) (Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, err
	}
	// Peek at the name so a file can extend a built-in preset.
	var head struct {
		Name string `yaml:"name"`
	}
	if err := yaml.Unmarshal(raw, &head); err != nil {
		return Scenario{}, fmt.Errorf("parse %s: %w", path, err)
	}
	base := Default()
	if head.Name != "" {
		if p, ok := Preset(head.Name); ok {
			base = p
		}
	}
	if err := yaml.Unmarshal(raw, &base); err != nil {
		return Scenario{}, fmt.Errorf("parse %s: %w", path, err)
	}
	base.Normalize()
	return base, nil
}

// TotalPods is the pod budget the generator will aim for.
func (s *Scenario) TotalPods() int {
	if s.Workloads.Pods > 0 {
		return s.Workloads.Pods
	}
	avg := (s.Workloads.Replicas.Min + s.Workloads.Replicas.Max) / 2
	return avg * s.Workloads.Deployments
}
