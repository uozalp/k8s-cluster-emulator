// Package generate builds a reproducible cluster from a scenario. Every
// random decision comes from the scenario seed, so the same scenario always
// produces the same logical cluster.
package generate

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/config"
	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
)

// Factory owns the seeded random source and knows how to mint new objects.
// The simulation engine keeps using it after the initial build so pods
// created by a scale-up look exactly like pods created at boot.
type Factory struct {
	mu  sync.Mutex
	cfg config.Scenario
	c   *cluster.Cluster
	rnd *rand.Rand

	uidSeq   uint64
	stateCDF []stateWeight
	profCDF  []profWeight
	nodeCnt  int32
	nsCfg    map[string]*nsConfig
}

type stateWeight struct {
	cum   float64
	state cluster.PodState
}

type profWeight struct {
	cum  float64
	prof *cluster.ResourceProfile
}

// New returns a factory bound to an (empty) cluster.
func New(cfg config.Scenario, c *cluster.Cluster) *Factory {
	f := &Factory{
		cfg: cfg,
		c:   c,
		rnd: rand.New(rand.NewSource(cfg.Seed)),
	}
	f.buildCDFs()
	return f
}

func (f *Factory) buildCDFs() {
	m := f.cfg.Lifecycle.Mix
	entries := []struct {
		w float64
		s cluster.PodState
	}{
		{m.Running, cluster.StRunning},
		{m.Pending, cluster.StPending},
		{m.ContainerCreating, cluster.StContainerCreating},
		{m.CrashLoopBackOff, cluster.StCrashLoopBackOff},
		{m.ImagePullBackOff, cluster.StImagePullBackOff},
		{m.Failed, cluster.StError},
		{m.Succeeded, cluster.StSucceeded},
	}
	total := m.Total()
	cum := 0.0
	for _, e := range entries {
		if e.w <= 0 {
			continue
		}
		cum += e.w / total
		f.stateCDF = append(f.stateCDF, stateWeight{cum, e.s})
	}

	r := f.cfg.Resources
	rw := []float64{r.Small, r.Medium, r.Large, r.Extreme, r.Wasteful}
	rtotal := r.Total()
	cum = 0
	for i := range rw {
		if rw[i] <= 0 {
			continue
		}
		cum += rw[i] / rtotal
		f.profCDF = append(f.profCDF, profWeight{cum, &cluster.Profiles[i]})
	}
}

func (f *Factory) uid() cluster.UID {
	f.uidSeq++
	a := (f.uidSeq + uint64(f.cfg.Seed)) * 0x9E3779B97F4A7C15
	a ^= a >> 29
	b := (f.uidSeq ^ 0x5DEECE66D) * 0xC2B2AE3D27D4EB4F
	b ^= b >> 32
	return cluster.UID{a, b}
}

func (f *Factory) pickProfile() *cluster.ResourceProfile {
	x := f.rnd.Float64()
	for _, p := range f.profCDF {
		if x <= p.cum {
			return p.prof
		}
	}
	return &cluster.Profiles[1]
}

// pickTarget chooses the state a new pod is heading for, biased by the
// health its Deployment was assigned.
func (f *Factory) pickTarget(d *cluster.Deployment) cluster.PodState {
	if d != nil && d.Health != cluster.HealthNormal && f.rnd.Float64() > d.AvailTarget {
		switch f.rnd.Intn(4) {
		case 0:
			return cluster.StCrashLoopBackOff
		case 1:
			return cluster.StImagePullBackOff
		case 2:
			return cluster.StPending
		default:
			return cluster.StError
		}
	}
	x := f.rnd.Float64()
	for _, s := range f.stateCDF {
		if x <= s.cum {
			return s.state
		}
	}
	return cluster.StRunning
}

func (f *Factory) dur(r config.DurationRange) time.Duration {
	if r.Max <= r.Min {
		return r.Min.D()
	}
	return r.Min.D() + time.Duration(f.rnd.Int63n(int64(r.Max-r.Min)))
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

// Progress reports build progress so the UI can show a splash instead of a
// frozen terminal while half a million pods are minted.
type Progress func(stage string, done, total int)

// Build populates the cluster. It runs before the API server starts serving.
func (f *Factory) Build(report Progress) {
	if report == nil {
		report = func(string, int, int) {}
	}
	now := time.Now()

	f.c.Bootstrap(func(tx *cluster.Tx) {
		f.buildNodes(tx, now, report)
		nss := f.buildNamespaces(tx, now, report)
		f.buildConfig(tx, now, nss, report)
		deploys := f.buildWorkloads(tx, now, nss, report)
		f.buildServices(tx, now, nss, deploys, report)
		f.buildBatch(tx, now, nss, report)
		f.buildSeedEvents(tx, now, deploys, report)
	})
	f.c.Seal()
}

func (f *Factory) buildNodes(tx *cluster.Tx, now time.Time, report Progress) {
	n := f.cfg.Nodes
	f.nodeCnt = int32(n)
	for i := 0; i < n; i++ {
		pool := nodePools[f.rnd.Intn(len(nodePools))]
		if i < 3 {
			pool = "system"
		}
		ready := "True"
		switch {
		case f.rnd.Float64() < 0.01:
			ready = "False"
		case f.rnd.Float64() < 0.005:
			ready = "Unknown"
		}
		node := &cluster.Node{
			Name:          cluster.NodeName(int32(i)),
			UID:           f.uid(),
			Created:       now.Add(-time.Duration(f.rnd.Intn(180*24)) * time.Hour).Unix(),
			Idx:           int32(i),
			Ready:         ready,
			Unschedulable: f.rnd.Float64() < 0.01,
			CPUCores:      int32(pick(f.rnd, []int{8, 16, 32, 64})),
			MemGi:         int32(pick(f.rnd, []int{32, 64, 128, 256})),
			PodCapacity:   int32(pick(f.rnd, []int{110, 110, 250})),
			Pool:          pool,
			Zone:          zones[i%len(zones)],
			InstanceType:  instanceTypes[f.rnd.Intn(len(instanceTypes))],
		}
		tx.PutNode(node)
		if i%5000 == 0 {
			report("nodes", i, n)
		}
	}
	report("nodes", n, n)
}

func (f *Factory) buildNamespaces(tx *cluster.Tx, now time.Time, report Progress) []string {
	var names []string
	if f.cfg.SystemNS {
		names = append(names, "default", "kube-system", "kube-public", "kube-node-lease")
	}
	width := len(strconv.Itoa(f.cfg.Namespaces))
	if width < 2 {
		width = 2
	}
	for i := 0; i < f.cfg.Namespaces; i++ {
		names = append(names, pad(f.cfg.NamespacePfx+"-", i, width))
	}

	for i, name := range names {
		labels := map[string]string{"kubernetes.io/metadata.name": name}
		if !isSystemNS(name) {
			labels["team"] = teamNames[i%len(teamNames)]
			labels["environment"] = envNames[i%len(envNames)]
			labels["app.kubernetes.io/managed-by"] = "k8s-cluster-emulator"
		}
		tx.PutNamespace(&cluster.Namespace{
			Name:    name,
			UID:     f.uid(),
			Created: now.Add(-time.Duration(f.rnd.Intn(365*24)) * time.Hour).Unix(),
			Phase:   "Active",
			Labels:  labels,
		})
		if i%5000 == 0 {
			report("namespaces", i, len(names))
		}
	}
	report("namespaces", len(names), len(names))
	return names
}

func isSystemNS(n string) bool {
	switch n {
	case "default", "kube-system", "kube-public", "kube-node-lease":
		return true
	}
	return false
}

// nsConfig tracks the ConfigMaps and Secrets available for pods to mount.
type nsConfig struct {
	configMaps []string
	secrets    []string
}

func (f *Factory) buildConfig(tx *cluster.Tx, now time.Time, nss []string, report Progress) {
	f.nsCfg = make(map[string]*nsConfig, len(nss))
	for i, ns := range nss {
		nc := &nsConfig{}
		for j := 0; j < intBetween(f.rnd, f.cfg.ConfigMapsPer.Min, f.cfg.ConfigMapsPer.Max); j++ {
			name := pad(ns+"-config-", j, 2)
			nc.configMaps = append(nc.configMaps, name)
			tx.PutConfigMap(&cluster.ConfigMap{
				Name: name, Namespace: ns, UID: f.uid(),
				Created: now.Add(-time.Duration(f.rnd.Intn(90*24)) * time.Hour).Unix(),
				Labels:  map[string]string{"app.kubernetes.io/managed-by": "k8s-cluster-emulator"},
				Data: map[string]string{
					"LOG_LEVEL":   pick(f.rnd, []string{"debug", "info", "warn"}),
					"REGION":      "eu-west-1",
					"FEATURE_SET": filler(f.rnd, 24),
				},
			})
		}
		for j := 0; j < intBetween(f.rnd, f.cfg.SecretsPerNS.Min, f.cfg.SecretsPerNS.Max); j++ {
			name := pad(ns+"-secret-", j, 2)
			nc.secrets = append(nc.secrets, name)
			tx.PutSecret(&cluster.Secret{
				Name: name, Namespace: ns, UID: f.uid(),
				Created: now.Add(-time.Duration(f.rnd.Intn(90*24)) * time.Hour).Unix(),
				Type:    "Opaque",
				Data:    map[string]string{"token": filler(f.rnd, 44)},
			})
		}
		f.nsCfg[ns] = nc
		if i%5000 == 0 {
			report("config", i, len(nss))
		}
	}
	report("config", len(nss), len(nss))
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

func (f *Factory) buildWorkloads(tx *cluster.Tx, now time.Time, nss []string, report Progress) []*cluster.Deployment {
	w := f.cfg.Workloads
	replicas := f.distribute(f.cfg.TotalPods(), w.Deployments, w.Replicas.Min, w.Replicas.Max)

	// Round-robin over a shuffled namespace list gives an even spread while
	// still depending only on the seed.
	nsOrder := append([]string(nil), nss...)
	f.rnd.Shuffle(len(nsOrder), func(i, j int) { nsOrder[i], nsOrder[j] = nsOrder[j], nsOrder[i] })

	used := make(map[string]int, w.Deployments)
	deploys := make([]*cluster.Deployment, 0, w.Deployments)
	podsDone := 0
	totalPods := f.cfg.TotalPods()

	for i := 0; i < w.Deployments; i++ {
		ns := nsOrder[i%len(nsOrder)]
		app := appNames[f.rnd.Intn(len(appNames))]
		key := ns + "/" + app
		name := app
		if n := used[key]; n > 0 {
			name = app + "-" + strconv.Itoa(n+1)
		}
		used[key]++

		d := f.newDeployment(tx, now, ns, name, app, replicas[i])
		deploys = append(deploys, d)
		podsDone += int(replicas[i])
		if i%200 == 0 {
			report("pods", podsDone, totalPods)
		}
	}
	report("pods", podsDone, totalPods)
	return deploys
}

// distribute spreads a pod budget over n deployments using seeded weights
// drawn from [lo,hi], so the shape of the cluster follows the scenario.
func (f *Factory) distribute(total, n, lo, hi int) []int32 {
	out := make([]int32, n)
	if total <= 0 {
		for i := range out {
			out[i] = int32(intBetween(f.rnd, lo, hi))
		}
		return out
	}
	weights := make([]float64, n)
	sum := 0.0
	for i := range weights {
		w := float64(intBetween(f.rnd, lo, hi))
		if w < 1 {
			w = 1
		}
		weights[i] = w
		sum += w
	}
	assigned := 0
	for i := range weights {
		v := int(float64(total) * weights[i] / sum)
		out[i] = int32(v)
		assigned += v
	}
	for i := 0; assigned < total; i++ {
		out[i%n]++
		assigned++
	}
	return out
}

func (f *Factory) newDeployment(tx *cluster.Tx, now time.Time, ns, name, app string, replicas int32) *cluster.Deployment {
	created := now.Add(-time.Duration(f.rnd.Intn(120*24)+1) * time.Hour)
	instance := name + "-" + envNames[f.rnd.Intn(len(envNames))]

	d := &cluster.Deployment{
		Name:         name,
		Namespace:    ns,
		UID:          f.uid(),
		Created:      created.Unix(),
		Generation:   1,
		SpecReplicas: replicas,
		Selector: map[string]string{
			"app.kubernetes.io/name":     app,
			"app.kubernetes.io/instance": instance,
		},
		Health:       cluster.HealthNormal,
		AvailTarget:  1,
		MaxSurge:     "25%",
		MaxUnavail:   "25%",
		RevisionLast: 1,
	}

	roll := f.rnd.Float64() * 100
	switch {
	case roll < f.cfg.Workloads.FailingPercent:
		d.Health = cluster.HealthFailing
		d.AvailTarget = f.rnd.Float64() * 0.4
	case roll < f.cfg.Workloads.FailingPercent+f.cfg.Workloads.DegradedPercent:
		d.Health = cluster.HealthDegraded
		d.AvailTarget = 0.5 + f.rnd.Float64()*0.45
	}

	// Historical, scaled-to-zero ReplicaSets so rollout history looks real.
	extra := intBetween(f.rnd, f.cfg.Workloads.ExtraReplicaSets.Min, f.cfg.Workloads.ExtraReplicaSets.Max)
	for r := 0; r < extra; r++ {
		old := f.newReplicaSet(tx, d, app, instance, created.Add(time.Duration(r)*time.Hour), 0)
		old.Revision = r + 1
		d.Old = append(d.Old, old)
	}
	d.RevisionLast = extra + 1

	cur := f.newReplicaSet(tx, d, app, instance, created, replicas)
	cur.Revision = d.RevisionLast
	d.Current = cur
	d.ObservedGeneration = d.Generation
	tx.PutDeployment(d)

	for i := int32(0); i < replicas; i++ {
		f.NewPod(tx, cur, now, f.cfg.Lifecycle.StartedRunning)
	}
	return d
}

func (f *Factory) newReplicaSet(tx *cluster.Tx, d *cluster.Deployment, app, instance string, created time.Time, replicas int32) *cluster.ReplicaSet {
	hash := templateHash(f.rnd)
	rs := &cluster.ReplicaSet{
		Name:         d.Name + "-" + hash,
		Namespace:    d.Namespace,
		UID:          f.uid(),
		Created:      created.Unix(),
		Generation:   1,
		Observed:     1,
		Deploy:       d,
		Hash:         hash,
		SpecReplicas: replicas,
	}
	rs.Tmpl = f.newTemplate(rs, app, instance)
	tx.PutReplicaSet(rs)
	return rs
}

func (f *Factory) newTemplate(rs *cluster.ReplicaSet, app, instance string) *cluster.PodTemplate {
	d := rs.Deploy
	profile := f.pickProfile()

	labels := map[string]string{
		"app.kubernetes.io/name":       app,
		"app.kubernetes.io/instance":   instance,
		"app.kubernetes.io/component":  componentFor(app),
		"app.kubernetes.io/version":    "1." + strconv.Itoa(f.rnd.Intn(30)) + "." + strconv.Itoa(f.rnd.Intn(10)),
		"app.kubernetes.io/part-of":    teamNames[f.rnd.Intn(len(teamNames))],
		"app.kubernetes.io/managed-by": "k8s-cluster-emulator",
		"pod-template-hash":            rs.Hash,
		"tier":                         pick(f.rnd, []string{"frontend", "backend", "data"}),
	}
	for i := 0; i < f.cfg.Objects.ExtraLabels; i++ {
		labels[pad("emulator.io/label-", i, 2)] = filler(f.rnd, 12)
	}

	annotations := map[string]string{
		"prometheus.io/scrape":              "true",
		"prometheus.io/port":                "9090",
		"checksum/config":                   randomHex(f.rnd, 64),
		"deployment.kubernetes.io/revision": strconv.Itoa(rs.Revision + 1),
	}
	size := f.cfg.Objects.AnnotationSize
	if size <= 0 {
		size = 128
	}
	for i := 0; i < f.cfg.Objects.ExtraAnnotations; i++ {
		annotations[pad("emulator.io/annotation-", i, 2)] = filler(f.rnd, size)
	}

	nc := f.nsCfg[d.Namespace]
	var volumes []kapi.Volume
	if nc != nil && len(nc.configMaps) > 0 {
		volumes = append(volumes, kapi.Volume{Name: "config", ConfigMap: &kapi.ConfigMapVolume{Name: nc.configMaps[f.rnd.Intn(len(nc.configMaps))]}})
	}
	if nc != nil && len(nc.secrets) > 0 {
		volumes = append(volumes, kapi.Volume{Name: "secrets", Secret: &kapi.SecretVolume{SecretName: nc.secrets[f.rnd.Intn(len(nc.secrets))]}})
	}
	volumes = append(volumes, kapi.Volume{Name: "tmp", EmptyDir: &map[string]string{}})

	count := intBetween(f.rnd, f.cfg.Workloads.Containers.Min, f.cfg.Workloads.Containers.Max)
	containers := make([]kapi.Container, 0, count)
	containers = append(containers, f.newContainer(app, images[f.rnd.Intn(len(images))], profile, volumes, true))
	for i := 1; i < count; i++ {
		side := sidecarImages[f.rnd.Intn(len(sidecarImages))]
		containers = append(containers, f.newContainer(pad("sidecar-", i, 1), side, &cluster.Profiles[0], volumes, false))
	}

	tmpl := &cluster.PodTemplate{
		RS:            rs,
		Namespace:     d.Namespace,
		NamePrefix:    rs.Name + "-",
		App:           app,
		Labels:        labels,
		Annotations:   annotations,
		Containers:    containers,
		Volumes:       volumes,
		ServiceAcct:   app,
		RestartPolicy: "Always",
		TermGrace:     30,
		Profile:       profile,
		NodeSelector:  map[string]string{"kubernetes.io/os": "linux"},
		Tolerations: []kapi.Toleration{
			{Key: "node.kubernetes.io/not-ready", Operator: "Exists", Effect: "NoExecute"},
		},
	}
	if f.rnd.Float64() < 0.15 {
		tmpl.PriorityClass = "high-priority"
		tmpl.Priority = 1000
	}
	if f.rnd.Float64() < 0.25 {
		init := f.newContainer("init-migrate", "registry.internal/busybox:1.36", &cluster.Profiles[0], nil, false)
		init.Command = []string{"sh", "-c", "echo migrating && sleep 1"}
		tmpl.InitContainer = &init
	}
	return tmpl
}

func componentFor(app string) string {
	if c, ok := componentByApp[app]; ok {
		return c
	}
	return "service"
}

func (f *Factory) newContainer(name, image string, p *cluster.ResourceProfile, volumes []kapi.Volume, primary bool) kapi.Container {
	c := kapi.Container{
		Name:            name,
		Image:           image,
		Resources:       p.Requirements(),
		ImagePullPolicy: "IfNotPresent",
		Env: []kapi.EnvVar{
			{Name: "POD_NAMESPACE", Value: ""},
			{Name: "LOG_LEVEL", Value: pick(f.rnd, []string{"debug", "info", "warn"})},
			{Name: "GOMAXPROCS", Value: strconv.FormatInt(max64(1, p.CPULimM/1000), 10)},
		},
	}
	if primary {
		port := int32(8080 + f.rnd.Intn(100))
		c.Ports = []kapi.ContainerPort{
			{Name: "http", ContainerPort: port, Protocol: "TCP"},
			{Name: "metrics", ContainerPort: 9090, Protocol: "TCP"},
		}
		c.LivenessProbe = &kapi.Probe{
			HTTPGet:             &kapi.HTTPGetAction{Path: "/healthz", Port: port, Scheme: "HTTP"},
			InitialDelaySeconds: 10, PeriodSeconds: 10, TimeoutSeconds: 1, FailureThreshold: 3,
		}
		c.ReadinessProbe = &kapi.Probe{
			HTTPGet:             &kapi.HTTPGetAction{Path: "/readyz", Port: port, Scheme: "HTTP"},
			InitialDelaySeconds: 5, PeriodSeconds: 5, TimeoutSeconds: 1, FailureThreshold: 3,
		}
	}
	for _, v := range volumes {
		c.VolumeMounts = append(c.VolumeMounts, kapi.VolumeMount{Name: v.Name, MountPath: "/etc/" + v.Name, ReadOnly: v.EmptyDir == nil})
	}
	return c
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Pods
// ---------------------------------------------------------------------------

// NewPod mints a pod for rs and adds it to the cluster. When settled is true
// the pod is created already in its steady state, which is how the initial
// cluster is built; otherwise it starts Pending and boots over time.
func (f *Factory) NewPod(tx *cluster.Tx, rs *cluster.ReplicaSet, now time.Time, settled bool) *cluster.Pod {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.newPodLocked(tx, rs, now, settled)
}

func (f *Factory) newPodLocked(tx *cluster.Tx, rs *cluster.ReplicaSet, now time.Time, settled bool) *cluster.Pod {
	ordinal := f.c.NextPodOrdinal()
	target := f.pickTarget(rs.Deploy)

	p := &cluster.Pod{
		Name:     rs.Tmpl.NamePrefix + podSuffix(ordinal),
		Tmpl:     rs.Tmpl,
		UID:      f.uid(),
		IPSuffix: ordinal,
		NodeIdx:  f.rnd.Int31n(maxInt32(f.nodeCnt, 1)),
		Target:   target,
		Jitter:   uint16(f.rnd.Intn(1 << 16)),
	}

	if settled {
		age := time.Duration(f.rnd.Intn(72*3600)+60) * time.Second
		p.Created = now.Add(-age).Unix()
		p.State = target
		if target != cluster.StPending {
			p.Scheduled = p.Created + 2
			p.Started = p.Created + 5
		}
		switch target {
		case cluster.StCrashLoopBackOff, cluster.StOOMKilled:
			p.Restarts = int32(f.rnd.Intn(300) + 1)
			p.NextStep = now.Add(f.backoff(p.Restarts)).UnixNano()
		case cluster.StRunning:
			// A few running pods are ready but not yet available, which is
			// what gives ReplicaSets distinct ready/available counts.
			p.Avail = f.rnd.Float64() > 0.02
			if f.rnd.Float64() < 0.1 {
				p.Restarts = int32(f.rnd.Intn(5))
			}
		case cluster.StContainerCreating:
			p.NextStep = now.Add(f.dur(f.cfg.Lifecycle.Startup)).UnixNano()
			p.Target = cluster.StRunning
		}
	} else {
		p.Created = now.Unix()
		p.State = cluster.StPending
		p.NextStep = now.Add(f.dur(f.cfg.Lifecycle.Scheduling)).UnixNano()
	}
	if p.State == cluster.StPending && p.Target == cluster.StPending {
		p.NextStep = 0 // intentionally stuck Pending
	}

	tx.AddPod(p)
	return p
}

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// backoff mirrors the kubelet's exponential CrashLoopBackOff, capped at 5m.
func (f *Factory) backoff(restarts int32) time.Duration {
	d := 10 * time.Second
	for i := int32(0); i < restarts && d < 5*time.Minute; i++ {
		d *= 2
	}
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d + time.Duration(f.rnd.Intn(5000))*time.Millisecond
}

// Backoff exposes the kubelet backoff curve to the simulation engine.
func (f *Factory) Backoff(restarts int32) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.backoff(restarts)
}

// Rand gives the simulation engine access to the seeded source.
func (f *Factory) Rand() (*rand.Rand, *sync.Mutex) { return f.rnd, &f.mu }

// Duration draws from a scenario duration range using the seeded source.
func (f *Factory) Duration(r config.DurationRange) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dur(r)
}

// NewRevision starts a fresh rollout: a new ReplicaSet takes the replica
// count and the previous one is scaled to zero, which is what a real
// `rollout restart` does.
func (f *Factory) NewRevision(tx *cluster.Tx, d *cluster.Deployment, now time.Time) *cluster.ReplicaSet {
	f.mu.Lock()
	defer f.mu.Unlock()

	app := d.Selector["app.kubernetes.io/name"]
	instance := d.Selector["app.kubernetes.io/instance"]
	if old := d.Current; old != nil {
		old.SpecReplicas = 0
		old.Generation++
		d.Old = append([]*cluster.ReplicaSet{old}, d.Old...)
	}
	d.RevisionLast++
	rs := f.newReplicaSet(tx, d, app, instance, now, d.SpecReplicas)
	rs.Revision = d.RevisionLast
	d.Current = rs
	d.Generation++
	return rs
}

// ---------------------------------------------------------------------------
// Services / batch / events
// ---------------------------------------------------------------------------

func (f *Factory) buildServices(tx *cluster.Tx, now time.Time, nss []string, deploys []*cluster.Deployment, report Progress) {
	byNS := make(map[string][]*cluster.Deployment, len(nss))
	for _, d := range deploys {
		byNS[d.Namespace] = append(byNS[d.Namespace], d)
	}
	total := 0
	for i, ns := range nss {
		want := intBetween(f.rnd, f.cfg.ServicesPerNS.Min, f.cfg.ServicesPerNS.Max)
		local := byNS[ns]
		for j := 0; j < want; j++ {
			var sel map[string]string
			name := pad(ns+"-svc-", j, 2)
			if len(local) > 0 {
				d := local[(j+i)%len(local)]
				sel = d.Selector
				name = d.Name
				if j >= len(local) {
					name = d.Name + "-" + strconv.Itoa(j)
				}
			}
			if _, exists := f.c.Services.Get(ns, name); exists {
				name = name + "-" + strconv.Itoa(j)
			}
			typ := "ClusterIP"
			switch {
			case f.rnd.Float64() < 0.05:
				typ = "LoadBalancer"
			case f.rnd.Float64() < 0.1:
				typ = "NodePort"
			}
			svc := &cluster.Service{
				Name: name, Namespace: ns, UID: f.uid(),
				Created:   now.Add(-time.Duration(f.rnd.Intn(120*24)) * time.Hour).Unix(),
				Type:      typ,
				ClusterIP: cluster.PodIP(uint32(f.rnd.Int31n(1 << 20))),
				ExtIP:     cluster.NodeIP(f.rnd.Int31n(1 << 12)),
				Selector:  sel,
				Labels:    map[string]string{"app.kubernetes.io/managed-by": "k8s-cluster-emulator"},
				Ports: []kapi.ServicePort{
					{Name: "http", Protocol: "TCP", Port: 80, TargetPort: 8080},
				},
			}
			if typ == "NodePort" {
				svc.Ports[0].NodePort = int32(30000 + f.rnd.Intn(2767))
			}
			tx.PutService(svc)
			total++
		}
		if i%5000 == 0 {
			report("services", i, len(nss))
		}
	}
	report("services", len(nss), len(nss))
}

func (f *Factory) buildBatch(tx *cluster.Tx, now time.Time, nss []string, report Progress) {
	for i, ns := range nss {
		nJobs := intBetween(f.rnd, f.cfg.JobsPerNS.Min, f.cfg.JobsPerNS.Max)
		nCron := intBetween(f.rnd, f.cfg.CronJobsPerNS.Min, f.cfg.CronJobsPerNS.Max)

		for j := 0; j < nCron; j++ {
			app := appNames[f.rnd.Intn(len(appNames))]
			cj := &cluster.CronJob{
				Name: pad(app+"-cron-", j, 2), Namespace: ns, UID: f.uid(),
				Created:      now.Add(-time.Duration(f.rnd.Intn(200*24)) * time.Hour).Unix(),
				Schedule:     pick(f.rnd, []string{"*/5 * * * *", "0 * * * *", "0 2 * * *", "*/15 * * * *"}),
				Suspend:      f.rnd.Float64() < 0.1,
				LastSchedule: now.Add(-time.Duration(f.rnd.Intn(3600)) * time.Second).Unix(),
				Labels:       map[string]string{"app.kubernetes.io/name": app},
				Tmpl:         f.standaloneTemplate(ns, app),
			}
			tx.PutCronJob(cj)
		}
		for j := 0; j < nJobs; j++ {
			app := appNames[f.rnd.Intn(len(appNames))]
			completions := int32(intBetween(f.rnd, 1, 5))
			job := &cluster.Job{
				Name: pad(app+"-job-", j, 2), Namespace: ns, UID: f.uid(),
				Created:     now.Add(-time.Duration(f.rnd.Intn(48)) * time.Hour).Unix(),
				Completions: completions,
				Parallelism: 1,
				StartTime:   now.Add(-time.Duration(f.rnd.Intn(48)) * time.Hour).Unix(),
				Labels:      map[string]string{"app.kubernetes.io/name": app},
				Tmpl:        f.standaloneTemplate(ns, app),
			}
			switch {
			case f.rnd.Float64() < 0.7:
				job.Succeeded = completions
				job.Completion = job.StartTime + int64(f.rnd.Intn(600)+10)
			case f.rnd.Float64() < 0.5:
				job.Active = 1
			default:
				job.Failed = int32(f.rnd.Intn(6) + 1)
			}
			tx.PutJob(job)
		}
		if i%5000 == 0 {
			report("batch", i, len(nss))
		}
	}
	report("batch", len(nss), len(nss))
}

// standaloneTemplate builds a pod template not backed by a ReplicaSet, used
// by Jobs and CronJobs whose pods the emulator does not materialize.
func (f *Factory) standaloneTemplate(ns, app string) *cluster.PodTemplate {
	p := f.pickProfile()
	return &cluster.PodTemplate{
		Namespace:     ns,
		App:           app,
		Labels:        map[string]string{"app.kubernetes.io/name": app},
		Containers:    []kapi.Container{f.newContainer(app, images[f.rnd.Intn(len(images))], p, nil, true)},
		RestartPolicy: "OnFailure",
		TermGrace:     30,
		Profile:       p,
	}
}

var seedEventReasons = []struct {
	reason, typ, msg string
}{
	{"Scheduled", "Normal", "Successfully assigned pod to node"},
	{"Pulling", "Normal", "Pulling image"},
	{"Pulled", "Normal", "Successfully pulled image"},
	{"Created", "Normal", "Created container"},
	{"Started", "Normal", "Started container"},
	{"Killing", "Normal", "Stopping container"},
	{"BackOff", "Warning", "Back-off restarting failed container"},
	{"Failed", "Warning", "Error: ImagePullBackOff"},
	{"Unhealthy", "Warning", "Readiness probe failed: HTTP probe failed with statuscode: 503"},
	{"FailedScheduling", "Warning", "0/10 nodes are available: insufficient cpu"},
	{"ScalingReplicaSet", "Normal", "Scaled up replica set"},
	{"SuccessfulCreate", "Normal", "Created pod"},
	{"SuccessfulDelete", "Normal", "Deleted pod"},
	{"NodeNotReady", "Warning", "Node is not ready"},
}

func (f *Factory) buildSeedEvents(tx *cluster.Tx, now time.Time, deploys []*cluster.Deployment, report Progress) {
	n := f.cfg.Events.Seed
	if n <= 0 || len(deploys) == 0 {
		return
	}
	for i := 0; i < n; i++ {
		d := deploys[f.rnd.Intn(len(deploys))]
		r := seedEventReasons[f.rnd.Intn(len(seedEventReasons))]
		when := now.Add(-time.Duration(f.rnd.Intn(3600)) * time.Second).Unix()

		refKind, refName, refUID, refAPI := "Deployment", d.Name, d.UID.String(), "apps/v1"
		if d.Current != nil && len(d.Current.Pods()) > 0 && f.rnd.Float64() < 0.8 {
			p := d.Current.Pods()[f.rnd.Intn(len(d.Current.Pods()))]
			if p != nil {
				refKind, refName, refUID, refAPI = "Pod", p.Name, p.UID.String(), "v1"
			}
		}
		tx.RecordEvent(&cluster.Event{
			Namespace: d.Namespace,
			First:     when,
			Last:      when,
			Count:     int32(f.rnd.Intn(8) + 1),
			Type:      r.typ,
			Reason:    r.reason,
			Message:   r.msg,
			Component: pick(f.rnd, []string{"kubelet", "default-scheduler", "deployment-controller", "replicaset-controller"}),
			Host:      cluster.NodeName(f.rnd.Int31n(maxInt32(f.nodeCnt, 1))),
			RefKind:   refKind, RefName: refName, RefUID: refUID, RefAPIVers: refAPI,
		})
		if i%10000 == 0 {
			report("events", i, n)
		}
	}
	report("events", n, n)
}
