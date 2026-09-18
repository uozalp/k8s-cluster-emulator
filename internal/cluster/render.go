package cluster

import (
	"strconv"
	"strings"
	"time"

	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
)

var (
	trueP  = kapi.BoolPtr(true)
	falseP = kapi.BoolPtr(false)
)

// NodeName is the canonical name for the node at index i. Pods store only
// the index, so this is the single source of truth for both sides.
func NodeName(i int32) string {
	var b [10]byte
	copy(b[:], "node-")
	n := int(i)
	for p := 9; p >= 5; p-- {
		b[p] = byte('0' + n%10)
		n /= 10
	}
	return string(b[:])
}

func ipv4(a, b, c, d uint32) string {
	var s strings.Builder
	s.Grow(15)
	s.WriteString(strconv.FormatUint(uint64(a), 10))
	s.WriteByte('.')
	s.WriteString(strconv.FormatUint(uint64(b), 10))
	s.WriteByte('.')
	s.WriteString(strconv.FormatUint(uint64(c), 10))
	s.WriteByte('.')
	s.WriteString(strconv.FormatUint(uint64(d), 10))
	return s.String()
}

// NodeIP is the InternalIP of node i, inside 10.128.0.0/12.
func NodeIP(i int32) string {
	n := uint32(i)
	return ipv4(10, 128+(n>>16)&0x0f, (n>>8)&0xff, n&0xff)
}

// PodIP derives an address in 10.32.0.0/11 from the pod's ordinal.
func PodIP(n uint32) string {
	return ipv4(10, 32+((n>>16)&0x1f), (n>>8)&0xff, n&0xff)
}

// ---------------------------------------------------------------------------
// Pod
// ---------------------------------------------------------------------------

func (p *Pod) ownerRefs() []kapi.OwnerReference {
	rs := p.Tmpl.RS
	if rs == nil {
		return nil
	}
	return []kapi.OwnerReference{{
		APIVersion:         "apps/v1",
		Kind:               "ReplicaSet",
		Name:               rs.Name,
		UID:                rs.UID.String(),
		Controller:         trueP,
		BlockOwnerDeletion: trueP,
	}}
}

func (p *Pod) containerID() string {
	return "containerd://" + strconv.FormatUint(p.UID[1], 16) + strconv.FormatUint(p.UID[0], 16)
}

func (p *Pod) Render() any {
	t := p.Tmpl
	spec := kapi.PodSpec{
		Volumes:                       t.Volumes,
		Containers:                    t.Containers,
		RestartPolicy:                 t.RestartPolicy,
		TerminationGracePeriodSeconds: kapi.Int64Ptr(t.TermGrace),
		DNSPolicy:                     "ClusterFirst",
		NodeSelector:                  t.NodeSelector,
		ServiceAccountName:            t.ServiceAcct,
		SchedulerName:                 "default-scheduler",
		Tolerations:                   t.Tolerations,
	}
	if t.InitContainer != nil {
		spec.InitContainers = []kapi.Container{*t.InitContainer}
	}
	if t.PriorityClass != "" {
		spec.PriorityClassName = t.PriorityClass
		spec.Priority = kapi.Int32Ptr(t.Priority)
	}
	if p.Scheduled != 0 {
		spec.NodeName = NodeName(p.NodeIdx)
	}

	meta := kapi.ObjectMeta{
		Name:              p.Name,
		GenerateName:      t.NamePrefix,
		Namespace:         t.Namespace,
		UID:               p.UID.String(),
		ResourceVersion:   rvStr(p.RV),
		CreationTimestamp: ts(p.Created),
		Labels:            t.Labels,
		Annotations:       t.Annotations,
		OwnerReferences:   p.ownerRefs(),
	}
	if p.Deleted != 0 {
		meta.DeletionTimestamp = ts(p.Deleted)
		meta.DeletionGracePeriodSeconds = kapi.Int64Ptr(t.TermGrace)
	}

	return &kapi.Pod{
		TypeMeta: kapi.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		Metadata: meta,
		Spec:     spec,
		Status:   p.renderStatus(),
	}
}

func (p *Pod) renderStatus() kapi.PodStatus {
	t := p.Tmpl
	st := kapi.PodStatus{
		Phase:    p.State.Phase(),
		QOSClass: "Burstable",
	}

	scheduled := p.Scheduled != 0
	if scheduled {
		st.HostIP = NodeIP(p.NodeIdx)
		st.PodIP = PodIP(p.IPSuffix)
		st.PodIPs = []kapi.PodIP{{IP: st.PodIP}}
		st.StartTime = ts(p.Scheduled)
	}

	ready := p.State.Ready()
	readyStr := "False"
	if ready {
		readyStr = "True"
	}
	schedStr := "False"
	schedReason := "Unschedulable"
	if scheduled {
		schedStr, schedReason = "True", ""
	}
	initStr := "False"
	if p.State != StPending {
		initStr = "True"
	}

	st.Conditions = []kapi.PodCondition{
		{Type: "PodScheduled", Status: schedStr, Reason: schedReason, LastTransitionTime: ts(p.Created)},
		{Type: "Initialized", Status: initStr, LastTransitionTime: ts(p.Scheduled)},
		{Type: "ContainersReady", Status: readyStr, LastTransitionTime: ts(p.Started)},
		{Type: "Ready", Status: readyStr, LastTransitionTime: ts(p.Started)},
	}

	switch p.State {
	case StPending:
		st.Conditions[0].Message = "0/1 nodes are available: insufficient cpu."
		return st
	case StEvicted:
		st.Reason = "Evicted"
		st.Message = "The node was low on resource: memory."
	}

	st.ContainerStatuses = make([]kapi.ContainerStatus, len(t.Containers))
	for i, c := range t.Containers {
		cs := kapi.ContainerStatus{
			Name:    c.Name,
			Image:   c.Image,
			ImageID: "docker-pullable://" + c.Image,
			Ready:   ready,
		}
		if i == 0 {
			cs.RestartCount = p.Restarts
		}
		cs.State, cs.LastTerminationState = p.containerState(c)
		if ready {
			cs.Started = trueP
			cs.ContainerID = p.containerID()
		}
		st.ContainerStatuses[i] = cs
	}
	return st
}

func (p *Pod) containerState(c kapi.Container) (state, last kapi.ContainerState) {
	switch p.State {
	case StContainerCreating:
		state.Waiting = &kapi.ContainerStateWaiting{Reason: "ContainerCreating"}
	case StRunning, StTerminating:
		state.Running = &kapi.ContainerStateRunning{StartedAt: ts(p.Started)}
	case StCrashLoopBackOff:
		state.Waiting = &kapi.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off 5m0s restarting failed container=" + c.Name,
		}
		last.Terminated = &kapi.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error",
			StartedAt: ts(p.Started), FinishedAt: ts(p.Started + 5),
		}
	case StOOMKilled:
		state.Waiting = &kapi.ContainerStateWaiting{Reason: "CrashLoopBackOff"}
		last.Terminated = &kapi.ContainerStateTerminated{
			ExitCode: 137, Reason: "OOMKilled",
			StartedAt: ts(p.Started), FinishedAt: ts(p.Started + 30),
		}
	case StImagePullBackOff:
		state.Waiting = &kapi.ContainerStateWaiting{
			Reason:  "ImagePullBackOff",
			Message: `Back-off pulling image "` + c.Image + `"`,
		}
	case StErrImagePull:
		state.Waiting = &kapi.ContainerStateWaiting{
			Reason:  "ErrImagePull",
			Message: `failed to pull image "` + c.Image + `": not found`,
		}
	case StSucceeded:
		state.Terminated = &kapi.ContainerStateTerminated{
			ExitCode: 0, Reason: "Completed",
			StartedAt: ts(p.Started), FinishedAt: ts(p.Started + 60),
		}
	case StError, StEvicted:
		state.Terminated = &kapi.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error",
			StartedAt: ts(p.Started), FinishedAt: ts(p.Started + 10),
		}
	default:
		state.Waiting = &kapi.ContainerStateWaiting{Reason: "Unknown"}
	}
	return state, last
}

// DisplayState is the status column k9s/kubectl would compute for this pod.
func (p *Pod) DisplayState() PodState {
	if p.Deleted != 0 {
		return StTerminating
	}
	return p.State
}

// FieldValue implements the pod field-selector paths real clients send.
func (p *Pod) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return p.Name, true
	case "metadata.namespace":
		return p.Tmpl.Namespace, true
	case "spec.nodeName":
		if p.Scheduled == 0 {
			return "", true
		}
		return NodeName(p.NodeIdx), true
	case "status.phase":
		return p.State.Phase(), true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// PodTemplate
// ---------------------------------------------------------------------------

func (t *PodTemplate) renderSpec() kapi.PodTemplateSpec {
	spec := kapi.PodSpec{
		Volumes:                       t.Volumes,
		Containers:                    t.Containers,
		RestartPolicy:                 t.RestartPolicy,
		TerminationGracePeriodSeconds: kapi.Int64Ptr(t.TermGrace),
		DNSPolicy:                     "ClusterFirst",
		NodeSelector:                  t.NodeSelector,
		ServiceAccountName:            t.ServiceAcct,
		SchedulerName:                 "default-scheduler",
		Tolerations:                   t.Tolerations,
	}
	if t.InitContainer != nil {
		spec.InitContainers = []kapi.Container{*t.InitContainer}
	}
	return kapi.PodTemplateSpec{
		Metadata: kapi.ObjectMeta{
			CreationTimestamp: "",
			Labels:            t.Labels,
			Annotations:       t.Annotations,
		},
		Spec: spec,
	}
}

// ---------------------------------------------------------------------------
// ReplicaSet
// ---------------------------------------------------------------------------

func (r *ReplicaSet) selector() *kapi.LabelSelector {
	sel := map[string]string{"pod-template-hash": r.Hash}
	if r.Deploy != nil {
		for k, v := range r.Deploy.Selector {
			sel[k] = v
		}
	}
	return &kapi.LabelSelector{MatchLabels: sel}
}

// SelectorString is the encoded selector used by the scale subresource.
func (r *ReplicaSet) SelectorString() string {
	parts := make([]string, 0, 4)
	for k, v := range r.selector().MatchLabels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func (r *ReplicaSet) Render() any {
	meta := kapi.ObjectMeta{
		Name:              r.Name,
		Namespace:         r.Namespace,
		UID:               r.UID.String(),
		ResourceVersion:   rvStr(r.RV),
		Generation:        r.Generation,
		CreationTimestamp: ts(r.Created),
		Labels:            r.Tmpl.Labels,
		Annotations: map[string]string{
			"deployment.kubernetes.io/desired-replicas": strconv.FormatInt(int64(r.SpecReplicas), 10),
			"deployment.kubernetes.io/max-replicas":     strconv.FormatInt(int64(r.SpecReplicas)+1, 10),
			"deployment.kubernetes.io/revision":         strconv.Itoa(r.Revision),
		},
	}
	if r.Deploy != nil {
		meta.OwnerReferences = []kapi.OwnerReference{{
			APIVersion:         "apps/v1",
			Kind:               "Deployment",
			Name:               r.Deploy.Name,
			UID:                r.Deploy.UID.String(),
			Controller:         trueP,
			BlockOwnerDeletion: trueP,
		}}
	}

	status := kapi.ReplicaSetStatus{
		Replicas:             r.Total,
		FullyLabeledReplicas: r.Total,
		ReadyReplicas:        r.ReadyCnt,
		AvailableReplicas:    r.AvailCnt,
		ObservedGeneration:   r.Observed,
	}
	if r.SpecReplicas > 0 && r.Total < r.SpecReplicas {
		status.Conditions = []kapi.ReplicaSetCondition{{
			Type: "ReplicaFailure", Status: "True",
			Reason:             "FailedCreate",
			Message:            "pods are pending creation",
			LastTransitionTime: ts(r.Created),
		}}
	}

	return &kapi.ReplicaSet{
		TypeMeta: kapi.TypeMeta{Kind: "ReplicaSet", APIVersion: "apps/v1"},
		Metadata: meta,
		Spec: kapi.ReplicaSetSpec{
			Replicas: kapi.Int32Ptr(r.SpecReplicas),
			Selector: r.selector(),
			Template: r.Tmpl.renderSpec(),
		},
		Status: status,
	}
}

func (r *ReplicaSet) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return r.Name, true
	case "metadata.namespace":
		return r.Namespace, true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Deployment
// ---------------------------------------------------------------------------

func (d *Deployment) status() kapi.DeploymentStatus {
	var total, ready, avail, updated int32
	for _, rs := range d.AllReplicaSets() {
		total += rs.Total
		ready += rs.ReadyCnt
		avail += rs.AvailCnt
	}
	if d.Current != nil {
		updated = d.Current.Total
	}
	unavail := d.SpecReplicas - avail
	if unavail < 0 {
		unavail = 0
	}

	// Condition timestamps must not move on every render, or clients see the
	// object change when nothing actually happened.
	changed := ts(d.Created)
	if d.LastScale != 0 {
		changed = ts(d.LastScale)
	}
	st := kapi.DeploymentStatus{
		ObservedGeneration:  d.ObservedGeneration,
		Replicas:            total,
		UpdatedReplicas:     updated,
		ReadyReplicas:       ready,
		AvailableReplicas:   avail,
		UnavailableReplicas: unavail,
	}

	availCond := kapi.DeploymentCondition{
		Type: "Available", Status: "True",
		Reason: "MinimumReplicasAvailable", Message: "Deployment has minimum availability.",
		LastUpdateTime: changed, LastTransitionTime: changed,
	}
	if avail == 0 && d.SpecReplicas > 0 {
		availCond.Status = "False"
		availCond.Reason = "MinimumReplicasUnavailable"
		availCond.Message = "Deployment does not have minimum availability."
	}

	progCond := kapi.DeploymentCondition{
		Type: "Progressing", Status: "True",
		Reason:         "NewReplicaSetAvailable",
		LastUpdateTime: changed, LastTransitionTime: changed,
	}
	switch {
	case d.Health == HealthFailing:
		progCond.Status = "False"
		progCond.Reason = "ProgressDeadlineExceeded"
		progCond.Message = `ReplicaSet "` + rsName(d) + `" has timed out progressing.`
	case avail < d.SpecReplicas:
		progCond.Reason = "ReplicaSetUpdated"
		progCond.Message = `ReplicaSet "` + rsName(d) + `" is progressing.`
	default:
		progCond.Message = `ReplicaSet "` + rsName(d) + `" has successfully progressed.`
	}

	st.Conditions = []kapi.DeploymentCondition{availCond, progCond}
	if d.Health == HealthDegraded && avail < d.SpecReplicas {
		st.Conditions = append(st.Conditions, kapi.DeploymentCondition{
			Type: "ReplicaFailure", Status: "True",
			Reason:         "FailedCreate",
			Message:        "some replicas are unavailable",
			LastUpdateTime: changed, LastTransitionTime: changed,
		})
	}
	return st
}

func rsName(d *Deployment) string {
	if d.Current == nil {
		return d.Name
	}
	return d.Current.Name
}

func (d *Deployment) Render() any {
	var tmpl kapi.PodTemplateSpec
	if d.Current != nil {
		tmpl = d.Current.Tmpl.renderSpec()
		// The Deployment template has no pod-template-hash; that is added by
		// the ReplicaSet controller.
		labels := make(map[string]string, len(tmpl.Metadata.Labels))
		for k, v := range tmpl.Metadata.Labels {
			if k == "pod-template-hash" {
				continue
			}
			labels[k] = v
		}
		tmpl.Metadata.Labels = labels
	}

	return &kapi.Deployment{
		TypeMeta: kapi.TypeMeta{Kind: "Deployment", APIVersion: "apps/v1"},
		Metadata: kapi.ObjectMeta{
			Name:              d.Name,
			Namespace:         d.Namespace,
			UID:               d.UID.String(),
			ResourceVersion:   rvStr(d.RV),
			Generation:        d.Generation,
			CreationTimestamp: ts(d.Created),
			Labels:            d.Selector,
			Annotations: map[string]string{
				"deployment.kubernetes.io/revision": strconv.Itoa(d.RevisionLast),
			},
		},
		Spec: kapi.DeploymentSpec{
			Replicas: kapi.Int32Ptr(d.SpecReplicas),
			Selector: &kapi.LabelSelector{MatchLabels: d.Selector},
			Template: tmpl,
			Strategy: kapi.DeploymentStrategy{
				Type: "RollingUpdate",
				RollingUpdate: &kapi.RollingUpdateDeployment{
					MaxUnavailable: d.MaxUnavail,
					MaxSurge:       d.MaxSurge,
				},
			},
			RevisionHistoryLimit:    kapi.Int32Ptr(10),
			ProgressDeadlineSeconds: kapi.Int32Ptr(600),
		},
		Status: d.status(),
	}
}

// RenderScale renders the autoscaling/v1 scale subresource.
func (d *Deployment) RenderScale() any {
	var sel string
	if d.Current != nil {
		sel = d.Current.SelectorString()
	}
	var cur int32
	for _, rs := range d.AllReplicaSets() {
		cur += rs.Total
	}
	return &kapi.Scale{
		TypeMeta: kapi.TypeMeta{Kind: "Scale", APIVersion: "autoscaling/v1"},
		Metadata: kapi.ObjectMeta{
			Name:              d.Name,
			Namespace:         d.Namespace,
			UID:               d.UID.String(),
			ResourceVersion:   rvStr(d.RV),
			CreationTimestamp: ts(d.Created),
		},
		Spec:   kapi.ScaleSpec{Replicas: d.SpecReplicas},
		Status: kapi.ScaleStatus{Replicas: cur, Selector: sel},
	}
}

func (d *Deployment) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return d.Name, true
	case "metadata.namespace":
		return d.Namespace, true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

func (n *Node) NodeLabels() map[string]string {
	role := "worker"
	if n.Pool == "system" {
		role = "control-plane"
	}
	return map[string]string{
		"kubernetes.io/hostname":           n.Name,
		"kubernetes.io/os":                 "linux",
		"kubernetes.io/arch":               "amd64",
		"beta.kubernetes.io/instance-type": n.InstanceType,
		"node.kubernetes.io/instance-type": n.InstanceType,
		"topology.kubernetes.io/region":    "eu-west-1",
		"topology.kubernetes.io/zone":      n.Zone,
		"node-role.kubernetes.io/" + role:  "",
		"cloud.google.com/gke-nodepool":    n.Pool,
		"app.kubernetes.io/managed-by":     "k8s-cluster-emulator",
	}
}

func (n *Node) Render() any {
	cpu := strconv.FormatInt(int64(n.CPUCores), 10)
	mem := strconv.FormatInt(int64(n.MemGi)*1024*1024, 10) + "Ki"
	allocCPU := strconv.FormatInt(int64(n.CPUCores)*1000-200, 10) + "m"
	allocMem := strconv.FormatInt(int64(n.MemGi)*1024*1024-1500000, 10) + "Ki"
	now := ts(time.Now().Unix())

	conds := []kapi.NodeCondition{
		{Type: "MemoryPressure", Status: "False", Reason: "KubeletHasSufficientMemory", Message: "kubelet has sufficient memory available", LastHeartbeatTime: now, LastTransitionTime: ts(n.Created)},
		{Type: "DiskPressure", Status: "False", Reason: "KubeletHasNoDiskPressure", Message: "kubelet has no disk pressure", LastHeartbeatTime: now, LastTransitionTime: ts(n.Created)},
		{Type: "PIDPressure", Status: "False", Reason: "KubeletHasSufficientPID", Message: "kubelet has sufficient PID available", LastHeartbeatTime: now, LastTransitionTime: ts(n.Created)},
		{Type: "Ready", Status: n.Ready, Reason: "KubeletReady", Message: "kubelet is posting ready status", LastHeartbeatTime: now, LastTransitionTime: ts(n.Created)},
	}
	if n.Ready != "True" {
		conds[3].Reason = "KubeletNotReady"
		conds[3].Message = "node is not ready"
	}

	spec := kapi.NodeSpec{
		PodCIDR:       ipv4(10, 32+uint32(n.Idx)%32, uint32(n.Idx)%256, 0) + "/24",
		ProviderID:    "fake://" + n.Name,
		Unschedulable: n.Unschedulable,
	}
	if n.Unschedulable {
		spec.Taints = []kapi.Taint{{Key: "node.kubernetes.io/unschedulable", Effect: "NoSchedule"}}
	}
	if n.Pool == "system" {
		spec.Taints = append(spec.Taints, kapi.Taint{Key: "node-role.kubernetes.io/control-plane", Effect: "NoSchedule"})
	}

	return &kapi.Node{
		TypeMeta: kapi.TypeMeta{Kind: "Node", APIVersion: "v1"},
		Metadata: kapi.ObjectMeta{
			Name:              n.Name,
			UID:               n.UID.String(),
			ResourceVersion:   rvStr(n.RV),
			CreationTimestamp: ts(n.Created),
			Labels:            n.NodeLabels(),
			Annotations: map[string]string{
				"node.alpha.kubernetes.io/ttl":                           "0",
				"volumes.kubernetes.io/controller-managed-attach-detach": "true",
			},
		},
		Spec: spec,
		Status: kapi.NodeStatus{
			Capacity: kapi.ResourceList{
				"cpu": cpu, "memory": mem, "pods": strconv.FormatInt(int64(n.PodCapacity), 10),
				"ephemeral-storage": "100Gi",
			},
			Allocatable: kapi.ResourceList{
				"cpu": allocCPU, "memory": allocMem, "pods": strconv.FormatInt(int64(n.PodCapacity), 10),
				"ephemeral-storage": "95Gi",
			},
			Conditions: conds,
			Addresses: []kapi.NodeAddress{
				{Type: "InternalIP", Address: NodeIP(n.Idx)},
				{Type: "Hostname", Address: n.Name},
			},
			NodeInfo: kapi.NodeSystemInfo{
				MachineID: n.UID.String(), SystemUUID: n.UID.String(), BootID: n.UID.String(),
				KernelVersion:           "5.15.0-1052-gcp",
				OSImage:                 "Ubuntu 22.04.4 LTS",
				ContainerRuntimeVersion: "containerd://1.7.13",
				KubeletVersion:          "v1.30.2",
				KubeProxyVersion:        "v1.30.2",
				OperatingSystem:         "linux",
				Architecture:            "amd64",
			},
		},
	}
}

func (n *Node) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return n.Name, true
	case "spec.unschedulable":
		return strconv.FormatBool(n.Unschedulable), true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

func (n *Namespace) Render() any {
	return &kapi.Namespace{
		TypeMeta: kapi.TypeMeta{Kind: "Namespace", APIVersion: "v1"},
		Metadata: kapi.ObjectMeta{
			Name:              n.Name,
			UID:               n.UID.String(),
			ResourceVersion:   rvStr(n.RV),
			CreationTimestamp: ts(n.Created),
			Labels:            n.Labels,
		},
		Spec:   kapi.NamespaceSpec{Finalizers: []string{"kubernetes"}},
		Status: kapi.NamespaceStatus{Phase: n.Phase},
	}
}

func (n *Namespace) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return n.Name, true
	case "status.phase":
		return n.Phase, true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Service / ConfigMap / Secret
// ---------------------------------------------------------------------------

func (s *Service) Render() any {
	svc := &kapi.Service{
		TypeMeta: kapi.TypeMeta{Kind: "Service", APIVersion: "v1"},
		Metadata: kapi.ObjectMeta{
			Name:              s.Name,
			Namespace:         s.Namespace,
			UID:               s.UID.String(),
			ResourceVersion:   rvStr(s.RV),
			CreationTimestamp: ts(s.Created),
			Labels:            s.Labels,
		},
		Spec: kapi.ServiceSpec{
			Ports:           s.Ports,
			Selector:        s.Selector,
			ClusterIP:       s.ClusterIP,
			ClusterIPs:      []string{s.ClusterIP},
			Type:            s.Type,
			SessionAffinity: "None",
			IPFamilies:      []string{"IPv4"},
			IPFamilyPolicy:  "SingleStack",
		},
	}
	if s.Type == "LoadBalancer" {
		svc.Spec.ExternalTrafficPolicy = "Cluster"
		svc.Status.LoadBalancer.Ingress = []kapi.LoadBalancerIngress{{IP: s.ExtIP}}
	}
	return svc
}

func (s *Service) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return s.Name, true
	case "metadata.namespace":
		return s.Namespace, true
	}
	return "", false
}

func (c *ConfigMap) Render() any {
	return &kapi.ConfigMap{
		TypeMeta: kapi.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		Metadata: kapi.ObjectMeta{
			Name: c.Name, Namespace: c.Namespace, UID: c.UID.String(),
			ResourceVersion: rvStr(c.RV), CreationTimestamp: ts(c.Created), Labels: c.Labels,
		},
		Data: c.Data,
	}
}

func (c *ConfigMap) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return c.Name, true
	case "metadata.namespace":
		return c.Namespace, true
	}
	return "", false
}

func (s *Secret) Render() any {
	return &kapi.Secret{
		TypeMeta: kapi.TypeMeta{Kind: "Secret", APIVersion: "v1"},
		Metadata: kapi.ObjectMeta{
			Name: s.Name, Namespace: s.Namespace, UID: s.UID.String(),
			ResourceVersion: rvStr(s.RV), CreationTimestamp: ts(s.Created), Labels: s.Labels,
		},
		Type: s.Type,
		Data: s.Data,
	}
}

func (s *Secret) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return s.Name, true
	case "metadata.namespace":
		return s.Namespace, true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Job / CronJob
// ---------------------------------------------------------------------------

func (j *Job) Render() any {
	meta := kapi.ObjectMeta{
		Name: j.Name, Namespace: j.Namespace, UID: j.UID.String(),
		ResourceVersion: rvStr(j.RV), CreationTimestamp: ts(j.Created), Labels: j.Labels,
	}
	if j.Owner != nil {
		meta.OwnerReferences = []kapi.OwnerReference{{
			APIVersion: "batch/v1", Kind: "CronJob",
			Name: j.Owner.Name, UID: j.Owner.UID.String(),
			Controller: trueP, BlockOwnerDeletion: trueP,
		}}
	}
	status := kapi.JobStatus{
		StartTime: ts(j.StartTime),
		Active:    j.Active,
		Succeeded: j.Succeeded,
		Failed:    j.Failed,
		Ready:     kapi.Int32Ptr(j.Active),
	}
	if j.Completion != 0 {
		status.CompletionTime = ts(j.Completion)
		status.Conditions = []kapi.JobCondition{{
			Type: "Complete", Status: "True",
			LastTransitionTime: ts(j.Completion),
		}}
	} else if j.Failed > 0 && j.Active == 0 {
		status.Conditions = []kapi.JobCondition{{
			Type: "Failed", Status: "True", Reason: "BackoffLimitExceeded",
			Message: "Job has reached the specified backoff limit", LastTransitionTime: ts(j.StartTime),
		}}
	}
	return &kapi.Job{
		TypeMeta: kapi.TypeMeta{Kind: "Job", APIVersion: "batch/v1"},
		Metadata: meta,
		Spec: kapi.JobSpec{
			Parallelism:  kapi.Int32Ptr(j.Parallelism),
			Completions:  kapi.Int32Ptr(j.Completions),
			BackoffLimit: kapi.Int32Ptr(6),
			Selector:     &kapi.LabelSelector{MatchLabels: map[string]string{"batch.kubernetes.io/job-name": j.Name}},
			Template:     j.Tmpl.renderSpec(),
		},
		Status: status,
	}
}

func (j *Job) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return j.Name, true
	case "metadata.namespace":
		return j.Namespace, true
	}
	return "", false
}

func (c *CronJob) Render() any {
	return &kapi.CronJob{
		TypeMeta: kapi.TypeMeta{Kind: "CronJob", APIVersion: "batch/v1"},
		Metadata: kapi.ObjectMeta{
			Name: c.Name, Namespace: c.Namespace, UID: c.UID.String(),
			ResourceVersion: rvStr(c.RV), CreationTimestamp: ts(c.Created), Labels: c.Labels,
		},
		Spec: kapi.CronJobSpec{
			Schedule:                   c.Schedule,
			ConcurrencyPolicy:          "Allow",
			Suspend:                    kapi.BoolPtr(c.Suspend),
			JobTemplate:                kapi.JobTemplateSpec{Spec: kapi.JobSpec{Template: c.Tmpl.renderSpec()}},
			SuccessfulJobsHistoryLimit: kapi.Int32Ptr(3),
			FailedJobsHistoryLimit:     kapi.Int32Ptr(1),
		},
		Status: kapi.CronJobStatus{LastScheduleTime: ts(c.LastSchedule)},
	}
}

func (c *CronJob) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return c.Name, true
	case "metadata.namespace":
		return c.Namespace, true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

func (e *Event) Render() any {
	return &kapi.Event{
		TypeMeta: kapi.TypeMeta{Kind: "Event", APIVersion: "v1"},
		Metadata: kapi.ObjectMeta{
			Name: e.Name, Namespace: e.Namespace, UID: e.UID.String(),
			ResourceVersion: rvStr(e.RV), CreationTimestamp: ts(e.First),
		},
		InvolvedObject: kapi.ObjectReference{
			Kind: e.RefKind, Namespace: e.Namespace, Name: e.RefName,
			UID: e.RefUID, APIVersion: e.RefAPIVers,
		},
		Reason:         e.Reason,
		Message:        e.Message,
		Source:         kapi.EventSource{Component: e.Component, Host: e.Host},
		FirstTimestamp: ts(e.First),
		LastTimestamp:  ts(e.Last),
		Count:          e.Count,
		Type:           e.Type,
		ReportingComp:  e.Component,
		ReportingInst:  e.Host,
	}
}

func (e *Event) FieldValue(path string) (string, bool) {
	switch path {
	case "metadata.name":
		return e.Name, true
	case "metadata.namespace":
		return e.Namespace, true
	case "type":
		return e.Type, true
	case "reason":
		return e.Reason, true
	case "involvedObject.kind":
		return e.RefKind, true
	case "involvedObject.name":
		return e.RefName, true
	case "involvedObject.uid":
		return e.RefUID, true
	case "involvedObject.namespace":
		return e.Namespace, true
	}
	return "", false
}
