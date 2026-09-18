package config

import (
	"sort"
	"time"
)

type presetFn func(s *Scenario)

var presets = map[string]presetFn{
	"small": func(s *Scenario) {
		s.Nodes = 10
		s.Namespaces = 5
		s.Workloads.Deployments = 20
		s.Workloads.Pods = 500
		s.Events.Max = 2000
		s.Events.Seed = 200
	},
	"medium": func(s *Scenario) {
		s.Nodes = 200
		s.Namespaces = 20
		s.Workloads.Deployments = 300
		s.Workloads.Pods = 10000
	},
	"large": func(s *Scenario) {
		s.Nodes = 1500
		s.Namespaces = 60
		s.Workloads.Deployments = 1500
		s.Workloads.Pods = 60000
		s.Events.Max = 50000
		s.Events.Seed = 10000
		s.Churn.PodsPerSecond = 10
	},
	"huge": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 200
		s.Workloads.Deployments = 5000
		s.Workloads.Pods = 100000
		s.Workloads.Replicas = Range{1, 200}
		s.Events.Max = 100000
		s.Events.Seed = 25000
		s.Churn.PodsPerSecond = 25
		s.Churn.RestartsPerSecond = 10
		s.Lifecycle.TransitionsPerSecond = 5000
	},
	"insane": func(s *Scenario) {
		s.Nodes = 10000
		s.Namespaces = 500
		s.Workloads.Deployments = 20000
		s.Workloads.Pods = 500000
		s.Workloads.Replicas = Range{1, 500}
		s.Events.Max = 200000
		s.Events.Seed = 50000
		s.Churn.PodsPerSecond = 50
		s.Churn.RestartsPerSecond = 25
		s.Lifecycle.TransitionsPerSecond = 10000
		s.API.DefaultListLimit = 0
	},

	// ---- Shape-specific stress scenarios -------------------------------

	"huge-deployment": func(s *Scenario) {
		s.Nodes = 2000
		s.Namespaces = 1
		s.SystemNS = false
		s.Workloads.Deployments = 1
		s.Workloads.Pods = 60000
		s.Workloads.ExtraReplicaSets = Range{0, 0}
		s.Events.Max = 50000
		s.Events.Seed = 5000
	},
	"many-deployments": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 50
		s.Workloads.Deployments = 10000
		s.Workloads.Replicas = Range{10, 10}
		s.Workloads.Pods = 100000
		s.Workloads.ExtraReplicaSets = Range{0, 3}
		s.Events.Max = 100000
		s.Events.Seed = 20000
	},
	"one-deployment-per-pod": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 100
		s.Workloads.Deployments = 60000
		s.Workloads.Replicas = Range{1, 1}
		s.Workloads.Pods = 60000
		s.Workloads.ExtraReplicaSets = Range{0, 0}
	},
	"huge-namespace": func(s *Scenario) {
		s.Nodes = 2000
		s.Namespaces = 1
		s.SystemNS = false
		s.Workloads.Deployments = 500
		s.Workloads.Pods = 100000
		s.ServicesPerNS = Range{200, 200}
	},
	"many-namespaces": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 5000
		s.Workloads.Deployments = 15000
		s.Workloads.Pods = 100000
		s.ServicesPerNS = Range{1, 2}
		s.ConfigMapsPer = Range{1, 2}
		s.SecretsPerNS = Range{1, 1}
		s.JobsPerNS = Range{0, 1}
		s.CronJobsPerNS = Range{0, 1}
	},
	"high-churn": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 100
		s.Workloads.Deployments = 3000
		s.Workloads.Pods = 100000
		s.Churn.PodsPerSecond = 200
		s.Churn.RestartsPerSecond = 100
		s.Churn.ScalesPerMinute = 60
		s.Churn.NodesPerMinute = 30
		s.Lifecycle.TransitionsPerSecond = 20000
		s.Lifecycle.Startup = durRange(500*time.Millisecond, 5*time.Second)
		s.Lifecycle.Termination = durRange(time.Second, 8*time.Second)
		s.Events.Max = 200000
		s.Events.Sample = 1
	},
	"failure-storm": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 100
		s.Workloads.Deployments = 3000
		s.Workloads.Pods = 100000
		s.Workloads.DegradedPercent = 40
		s.Workloads.FailingPercent = 15
		s.Lifecycle.Mix = LifecycleMix{
			Running:          40,
			Pending:          12,
			CrashLoopBackOff: 20,
			ImagePullBackOff: 12,
			Failed:           12,
			Succeeded:        4,
		}
		s.Churn.RestartsPerSecond = 50
		s.Events.Max = 200000
		s.Events.Sample = 1
		s.Events.ExtraPerSecond = 100
	},
	"large-objects": func(s *Scenario) {
		s.Nodes = 2000
		s.Namespaces = 40
		s.Workloads.Deployments = 1200
		s.Workloads.Pods = 60000
		s.Workloads.Containers = Range{3, 6}
		s.Objects.ExtraLabels = 25
		s.Objects.ExtraAnnotations = 20
		s.Objects.AnnotationSize = 512
	},
	"slow-api": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 60
		s.Workloads.Deployments = 1500
		s.Workloads.Pods = 60000
		s.API.Latency = Duration(150 * time.Millisecond)
		s.API.PerObjectLatency = Duration(2 * time.Microsecond)
		s.API.Jitter = 0.6
	},
	"chaos": func(s *Scenario) {
		s.Nodes = 3000
		s.Namespaces = 60
		s.Workloads.Deployments = 1500
		s.Workloads.Pods = 60000
		s.API.Chaos = true
		s.API.WatchChurn = true
		s.API.Latency = Duration(80 * time.Millisecond)
		s.Churn.PodsPerSecond = 30
	},
}

// Preset returns a normalized built-in scenario by name.
func Preset(name string) (Scenario, bool) {
	fn, ok := presets[name]
	if !ok {
		return Scenario{}, false
	}
	s := Default()
	s.Name = name
	fn(&s)
	s.Normalize()
	return s, true
}

// PresetNames lists the built-in scenarios in stable order.
func PresetNames() []string {
	names := make([]string, 0, len(presets))
	for k := range presets {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
