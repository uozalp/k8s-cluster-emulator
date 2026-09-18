// Package kapi contains the subset of Kubernetes API types the emulator
// serializes. They mirror the upstream JSON shapes closely enough that
// client-go (and therefore k9s) decodes them without complaint, but they are
// hand-written so the emulator stays dependency-free of k8s.io/*.
package kapi

// ---------------------------------------------------------------------------
// Meta
// ---------------------------------------------------------------------------

type TypeMeta struct {
	Kind       string `json:"kind,omitempty"`
	APIVersion string `json:"apiVersion,omitempty"`
}

type OwnerReference struct {
	APIVersion         string `json:"apiVersion"`
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Controller         *bool  `json:"controller,omitempty"`
	BlockOwnerDeletion *bool  `json:"blockOwnerDeletion,omitempty"`
}

type ObjectMeta struct {
	Name                       string            `json:"name"`
	GenerateName               string            `json:"generateName,omitempty"`
	Namespace                  string            `json:"namespace,omitempty"`
	UID                        string            `json:"uid"`
	ResourceVersion            string            `json:"resourceVersion"`
	Generation                 int64             `json:"generation,omitempty"`
	CreationTimestamp          string            `json:"creationTimestamp,omitempty"`
	DeletionTimestamp          string            `json:"deletionTimestamp,omitempty"`
	DeletionGracePeriodSeconds *int64            `json:"deletionGracePeriodSeconds,omitempty"`
	Labels                     map[string]string `json:"labels,omitempty"`
	Annotations                map[string]string `json:"annotations,omitempty"`
	OwnerReferences            []OwnerReference  `json:"ownerReferences,omitempty"`
	Finalizers                 []string          `json:"finalizers,omitempty"`
}

type ListMeta struct {
	ResourceVersion    string `json:"resourceVersion"`
	Continue           string `json:"continue,omitempty"`
	RemainingItemCount *int64 `json:"remainingItemCount,omitempty"`
}

type LabelSelector struct {
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// Status is the Kubernetes error envelope.
type Status struct {
	TypeMeta `json:",inline"`
	Metadata ListMeta       `json:"metadata"`
	Status   string         `json:"status"`
	Message  string         `json:"message,omitempty"`
	Reason   string         `json:"reason,omitempty"`
	Details  *StatusDetails `json:"details,omitempty"`
	Code     int            `json:"code"`
}

type StatusDetails struct {
	Name              string `json:"name,omitempty"`
	Group             string `json:"group,omitempty"`
	Kind              string `json:"kind,omitempty"`
	UID               string `json:"uid,omitempty"`
	RetryAfterSeconds int32  `json:"retryAfterSeconds,omitempty"`
}

func NewStatus(code int, reason, msg string) Status {
	return Status{
		TypeMeta: TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   "Failure",
		Message:  msg,
		Reason:   reason,
		Code:     code,
	}
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

type APIVersions struct {
	TypeMeta                   `json:",inline"`
	Versions                   []string                    `json:"versions"`
	ServerAddressByClientCIDRs []ServerAddressByClientCIDR `json:"serverAddressByClientCIDRs"`
}

type ServerAddressByClientCIDR struct {
	ClientCIDR    string `json:"clientCIDR"`
	ServerAddress string `json:"serverAddress"`
}

type GroupVersionForDiscovery struct {
	GroupVersion string `json:"groupVersion"`
	Version      string `json:"version"`
}

type APIGroup struct {
	TypeMeta         `json:",inline"`
	Name             string                     `json:"name"`
	Versions         []GroupVersionForDiscovery `json:"versions"`
	PreferredVersion GroupVersionForDiscovery   `json:"preferredVersion"`
}

type APIGroupList struct {
	TypeMeta `json:",inline"`
	Groups   []APIGroup `json:"groups"`
}

type APIResource struct {
	Name         string   `json:"name"`
	SingularName string   `json:"singularName"`
	Namespaced   bool     `json:"namespaced"`
	Kind         string   `json:"kind"`
	Verbs        []string `json:"verbs"`
	ShortNames   []string `json:"shortNames,omitempty"`
	Categories   []string `json:"categories,omitempty"`
	Group        string   `json:"group,omitempty"`
	Version      string   `json:"version,omitempty"`
}

type APIResourceList struct {
	TypeMeta     `json:",inline"`
	GroupVersion string        `json:"groupVersion"`
	APIResources []APIResource `json:"resources"`
}

type Info struct {
	Major        string `json:"major"`
	Minor        string `json:"minor"`
	GitVersion   string `json:"gitVersion"`
	GitCommit    string `json:"gitCommit"`
	GitTreeState string `json:"gitTreeState"`
	BuildDate    string `json:"buildDate"`
	GoVersion    string `json:"goVersion"`
	Compiler     string `json:"compiler"`
	Platform     string `json:"platform"`
}

// ---------------------------------------------------------------------------
// Core v1
// ---------------------------------------------------------------------------

type ResourceList map[string]string

type ResourceRequirements struct {
	Limits   ResourceList `json:"limits,omitempty"`
	Requests ResourceList `json:"requests,omitempty"`
}

type ContainerPort struct {
	Name          string `json:"name,omitempty"`
	ContainerPort int32  `json:"containerPort"`
	Protocol      string `json:"protocol,omitempty"`
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

type Probe struct {
	HTTPGet             *HTTPGetAction `json:"httpGet,omitempty"`
	InitialDelaySeconds int32          `json:"initialDelaySeconds,omitempty"`
	PeriodSeconds       int32          `json:"periodSeconds,omitempty"`
	TimeoutSeconds      int32          `json:"timeoutSeconds,omitempty"`
	FailureThreshold    int32          `json:"failureThreshold,omitempty"`
}

type HTTPGetAction struct {
	Path   string `json:"path,omitempty"`
	Port   int32  `json:"port"`
	Scheme string `json:"scheme,omitempty"`
}

type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type Container struct {
	Name            string               `json:"name"`
	Image           string               `json:"image"`
	Command         []string             `json:"command,omitempty"`
	Args            []string             `json:"args,omitempty"`
	Ports           []ContainerPort      `json:"ports,omitempty"`
	Env             []EnvVar             `json:"env,omitempty"`
	Resources       ResourceRequirements `json:"resources,omitempty"`
	VolumeMounts    []VolumeMount        `json:"volumeMounts,omitempty"`
	LivenessProbe   *Probe               `json:"livenessProbe,omitempty"`
	ReadinessProbe  *Probe               `json:"readinessProbe,omitempty"`
	ImagePullPolicy string               `json:"imagePullPolicy,omitempty"`
}

type Volume struct {
	Name      string             `json:"name"`
	ConfigMap *ConfigMapVolume   `json:"configMap,omitempty"`
	Secret    *SecretVolume      `json:"secret,omitempty"`
	EmptyDir  *map[string]string `json:"emptyDir,omitempty"`
}

type ConfigMapVolume struct {
	Name string `json:"name"`
}

type SecretVolume struct {
	SecretName string `json:"secretName"`
}

type Toleration struct {
	Key      string `json:"key,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
	Effect   string `json:"effect,omitempty"`
}

type PodSpec struct {
	Volumes                       []Volume          `json:"volumes,omitempty"`
	InitContainers                []Container       `json:"initContainers,omitempty"`
	Containers                    []Container       `json:"containers"`
	RestartPolicy                 string            `json:"restartPolicy,omitempty"`
	TerminationGracePeriodSeconds *int64            `json:"terminationGracePeriodSeconds,omitempty"`
	DNSPolicy                     string            `json:"dnsPolicy,omitempty"`
	NodeSelector                  map[string]string `json:"nodeSelector,omitempty"`
	ServiceAccountName            string            `json:"serviceAccountName,omitempty"`
	NodeName                      string            `json:"nodeName,omitempty"`
	SchedulerName                 string            `json:"schedulerName,omitempty"`
	Tolerations                   []Toleration      `json:"tolerations,omitempty"`
	PriorityClassName             string            `json:"priorityClassName,omitempty"`
	Priority                      *int32            `json:"priority,omitempty"`
}

type PodCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastProbeTime      string `json:"lastProbeTime,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

type ContainerStateWaiting struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type ContainerStateRunning struct {
	StartedAt string `json:"startedAt,omitempty"`
}

type ContainerStateTerminated struct {
	ExitCode    int32  `json:"exitCode"`
	Signal      int32  `json:"signal,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Message     string `json:"message,omitempty"`
	StartedAt   string `json:"startedAt,omitempty"`
	FinishedAt  string `json:"finishedAt,omitempty"`
	ContainerID string `json:"containerID,omitempty"`
}

type ContainerState struct {
	Waiting    *ContainerStateWaiting    `json:"waiting,omitempty"`
	Running    *ContainerStateRunning    `json:"running,omitempty"`
	Terminated *ContainerStateTerminated `json:"terminated,omitempty"`
}

type ContainerStatus struct {
	Name                 string         `json:"name"`
	State                ContainerState `json:"state"`
	LastTerminationState ContainerState `json:"lastState"`
	Ready                bool           `json:"ready"`
	RestartCount         int32          `json:"restartCount"`
	Image                string         `json:"image"`
	ImageID              string         `json:"imageID"`
	ContainerID          string         `json:"containerID,omitempty"`
	Started              *bool          `json:"started,omitempty"`
}

type PodIP struct {
	IP string `json:"ip"`
}

type PodStatus struct {
	Phase             string            `json:"phase,omitempty"`
	Conditions        []PodCondition    `json:"conditions,omitempty"`
	Message           string            `json:"message,omitempty"`
	Reason            string            `json:"reason,omitempty"`
	HostIP            string            `json:"hostIP,omitempty"`
	PodIP             string            `json:"podIP,omitempty"`
	PodIPs            []PodIP           `json:"podIPs,omitempty"`
	StartTime         string            `json:"startTime,omitempty"`
	InitContainerSt   []ContainerStatus `json:"initContainerStatuses,omitempty"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
	QOSClass          string            `json:"qosClass,omitempty"`
}

type Pod struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec"`
	Status   PodStatus  `json:"status"`
}

type PodTemplateSpec struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec"`
}

// --- Node ---

type NodeAddress struct {
	Type    string `json:"type"`
	Address string `json:"address"`
}

type NodeCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastHeartbeatTime  string `json:"lastHeartbeatTime,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

type NodeSystemInfo struct {
	MachineID               string `json:"machineID"`
	SystemUUID              string `json:"systemUUID"`
	BootID                  string `json:"bootID"`
	KernelVersion           string `json:"kernelVersion"`
	OSImage                 string `json:"osImage"`
	ContainerRuntimeVersion string `json:"containerRuntimeVersion"`
	KubeletVersion          string `json:"kubeletVersion"`
	KubeProxyVersion        string `json:"kubeProxyVersion"`
	OperatingSystem         string `json:"operatingSystem"`
	Architecture            string `json:"architecture"`
}

type Taint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

type NodeSpec struct {
	PodCIDR       string   `json:"podCIDR,omitempty"`
	PodCIDRs      []string `json:"podCIDRs,omitempty"`
	ProviderID    string   `json:"providerID,omitempty"`
	Unschedulable bool     `json:"unschedulable,omitempty"`
	Taints        []Taint  `json:"taints,omitempty"`
}

type NodeStatus struct {
	Capacity    ResourceList    `json:"capacity,omitempty"`
	Allocatable ResourceList    `json:"allocatable,omitempty"`
	Phase       string          `json:"phase,omitempty"`
	Conditions  []NodeCondition `json:"conditions,omitempty"`
	Addresses   []NodeAddress   `json:"addresses,omitempty"`
	NodeInfo    NodeSystemInfo  `json:"nodeInfo"`
}

type Node struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta `json:"metadata"`
	Spec     NodeSpec   `json:"spec"`
	Status   NodeStatus `json:"status"`
}

// --- Namespace ---

type NamespaceSpec struct {
	Finalizers []string `json:"finalizers,omitempty"`
}

type NamespaceStatus struct {
	Phase string `json:"phase,omitempty"`
}

type Namespace struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta      `json:"metadata"`
	Spec     NamespaceSpec   `json:"spec"`
	Status   NamespaceStatus `json:"status"`
}

// --- Service ---

type ServicePort struct {
	Name       string `json:"name,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	Port       int32  `json:"port"`
	TargetPort int32  `json:"targetPort,omitempty"`
	NodePort   int32  `json:"nodePort,omitempty"`
}

type ServiceSpec struct {
	Ports                 []ServicePort     `json:"ports,omitempty"`
	Selector              map[string]string `json:"selector,omitempty"`
	ClusterIP             string            `json:"clusterIP,omitempty"`
	ClusterIPs            []string          `json:"clusterIPs,omitempty"`
	Type                  string            `json:"type,omitempty"`
	SessionAffinity       string            `json:"sessionAffinity,omitempty"`
	ExternalTrafficPolicy string            `json:"externalTrafficPolicy,omitempty"`
	IPFamilies            []string          `json:"ipFamilies,omitempty"`
	IPFamilyPolicy        string            `json:"ipFamilyPolicy,omitempty"`
}

type LoadBalancerIngress struct {
	IP       string `json:"ip,omitempty"`
	Hostname string `json:"hostname,omitempty"`
}

type LoadBalancerStatus struct {
	Ingress []LoadBalancerIngress `json:"ingress,omitempty"`
}

type ServiceStatus struct {
	LoadBalancer LoadBalancerStatus `json:"loadBalancer"`
}

type Service struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta    `json:"metadata"`
	Spec     ServiceSpec   `json:"spec"`
	Status   ServiceStatus `json:"status"`
}

// --- ConfigMap / Secret ---

type ConfigMap struct {
	TypeMeta  `json:",inline"`
	Metadata  ObjectMeta        `json:"metadata"`
	Immutable *bool             `json:"immutable,omitempty"`
	Data      map[string]string `json:"data,omitempty"`
}

type Secret struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta        `json:"metadata"`
	Type     string            `json:"type,omitempty"`
	Data     map[string]string `json:"data,omitempty"`
}

// --- ServiceAccount ---

type ServiceAccount struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta `json:"metadata"`
}

// --- Event ---

type ObjectReference struct {
	Kind            string `json:"kind,omitempty"`
	Namespace       string `json:"namespace,omitempty"`
	Name            string `json:"name,omitempty"`
	UID             string `json:"uid,omitempty"`
	APIVersion      string `json:"apiVersion,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	FieldPath       string `json:"fieldPath,omitempty"`
}

type EventSource struct {
	Component string `json:"component,omitempty"`
	Host      string `json:"host,omitempty"`
}

type EventSeries struct {
	Count            int32  `json:"count,omitempty"`
	LastObservedTime string `json:"lastObservedTime,omitempty"`
}

type Event struct {
	TypeMeta       `json:",inline"`
	Metadata       ObjectMeta      `json:"metadata"`
	InvolvedObject ObjectReference `json:"involvedObject"`
	Reason         string          `json:"reason,omitempty"`
	Message        string          `json:"message,omitempty"`
	Source         EventSource     `json:"source,omitempty"`
	FirstTimestamp string          `json:"firstTimestamp,omitempty"`
	LastTimestamp  string          `json:"lastTimestamp,omitempty"`
	Count          int32           `json:"count,omitempty"`
	Type           string          `json:"type,omitempty"`
	EventTime      *string         `json:"eventTime,omitempty"`
	ReportingComp  string          `json:"reportingComponent,omitempty"`
	ReportingInst  string          `json:"reportingInstance,omitempty"`
	Action         string          `json:"action,omitempty"`
}

// ---------------------------------------------------------------------------
// apps/v1
// ---------------------------------------------------------------------------

type RollingUpdateDeployment struct {
	MaxUnavailable string `json:"maxUnavailable,omitempty"`
	MaxSurge       string `json:"maxSurge,omitempty"`
}

type DeploymentStrategy struct {
	Type          string                   `json:"type,omitempty"`
	RollingUpdate *RollingUpdateDeployment `json:"rollingUpdate,omitempty"`
}

type DeploymentSpec struct {
	Replicas                *int32             `json:"replicas,omitempty"`
	Selector                *LabelSelector     `json:"selector"`
	Template                PodTemplateSpec    `json:"template"`
	Strategy                DeploymentStrategy `json:"strategy,omitempty"`
	MinReadySeconds         int32              `json:"minReadySeconds,omitempty"`
	RevisionHistoryLimit    *int32             `json:"revisionHistoryLimit,omitempty"`
	Paused                  bool               `json:"paused,omitempty"`
	ProgressDeadlineSeconds *int32             `json:"progressDeadlineSeconds,omitempty"`
}

type DeploymentCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastUpdateTime     string `json:"lastUpdateTime,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

type DeploymentStatus struct {
	ObservedGeneration  int64                 `json:"observedGeneration,omitempty"`
	Replicas            int32                 `json:"replicas,omitempty"`
	UpdatedReplicas     int32                 `json:"updatedReplicas,omitempty"`
	ReadyReplicas       int32                 `json:"readyReplicas,omitempty"`
	AvailableReplicas   int32                 `json:"availableReplicas,omitempty"`
	UnavailableReplicas int32                 `json:"unavailableReplicas,omitempty"`
	Conditions          []DeploymentCondition `json:"conditions,omitempty"`
}

type Deployment struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta       `json:"metadata"`
	Spec     DeploymentSpec   `json:"spec"`
	Status   DeploymentStatus `json:"status"`
}

type ReplicaSetSpec struct {
	Replicas        *int32          `json:"replicas,omitempty"`
	MinReadySeconds int32           `json:"minReadySeconds,omitempty"`
	Selector        *LabelSelector  `json:"selector"`
	Template        PodTemplateSpec `json:"template"`
}

type ReplicaSetCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

type ReplicaSetStatus struct {
	Replicas             int32                 `json:"replicas"`
	FullyLabeledReplicas int32                 `json:"fullyLabeledReplicas,omitempty"`
	ReadyReplicas        int32                 `json:"readyReplicas,omitempty"`
	AvailableReplicas    int32                 `json:"availableReplicas,omitempty"`
	ObservedGeneration   int64                 `json:"observedGeneration,omitempty"`
	Conditions           []ReplicaSetCondition `json:"conditions,omitempty"`
}

type ReplicaSet struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta       `json:"metadata"`
	Spec     ReplicaSetSpec   `json:"spec"`
	Status   ReplicaSetStatus `json:"status"`
}

type DaemonSetSpec struct {
	Selector        *LabelSelector  `json:"selector"`
	Template        PodTemplateSpec `json:"template"`
	MinReadySeconds int32           `json:"minReadySeconds,omitempty"`
}

type DaemonSetStatus struct {
	CurrentNumberScheduled int32 `json:"currentNumberScheduled"`
	NumberMisscheduled     int32 `json:"numberMisscheduled"`
	DesiredNumberScheduled int32 `json:"desiredNumberScheduled"`
	NumberReady            int32 `json:"numberReady"`
	ObservedGeneration     int64 `json:"observedGeneration,omitempty"`
	UpdatedNumberScheduled int32 `json:"updatedNumberScheduled,omitempty"`
	NumberAvailable        int32 `json:"numberAvailable,omitempty"`
	NumberUnavailable      int32 `json:"numberUnavailable,omitempty"`
}

type DaemonSet struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta      `json:"metadata"`
	Spec     DaemonSetSpec   `json:"spec"`
	Status   DaemonSetStatus `json:"status"`
}

type StatefulSetSpec struct {
	Replicas            *int32          `json:"replicas,omitempty"`
	Selector            *LabelSelector  `json:"selector"`
	Template            PodTemplateSpec `json:"template"`
	ServiceName         string          `json:"serviceName,omitempty"`
	PodManagementPolicy string          `json:"podManagementPolicy,omitempty"`
}

type StatefulSetStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Replicas           int32  `json:"replicas"`
	ReadyReplicas      int32  `json:"readyReplicas,omitempty"`
	CurrentReplicas    int32  `json:"currentReplicas,omitempty"`
	UpdatedReplicas    int32  `json:"updatedReplicas,omitempty"`
	AvailableReplicas  int32  `json:"availableReplicas,omitempty"`
	CurrentRevision    string `json:"currentRevision,omitempty"`
	UpdateRevision     string `json:"updateRevision,omitempty"`
}

type StatefulSet struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta        `json:"metadata"`
	Spec     StatefulSetSpec   `json:"spec"`
	Status   StatefulSetStatus `json:"status"`
}

// ---------------------------------------------------------------------------
// batch/v1
// ---------------------------------------------------------------------------

type JobSpec struct {
	Parallelism  *int32          `json:"parallelism,omitempty"`
	Completions  *int32          `json:"completions,omitempty"`
	BackoffLimit *int32          `json:"backoffLimit,omitempty"`
	Selector     *LabelSelector  `json:"selector,omitempty"`
	Template     PodTemplateSpec `json:"template"`
}

type JobCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	LastProbeTime      string `json:"lastProbeTime,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
}

type JobStatus struct {
	Conditions     []JobCondition `json:"conditions,omitempty"`
	StartTime      string         `json:"startTime,omitempty"`
	CompletionTime string         `json:"completionTime,omitempty"`
	Active         int32          `json:"active,omitempty"`
	Succeeded      int32          `json:"succeeded,omitempty"`
	Failed         int32          `json:"failed,omitempty"`
	Ready          *int32         `json:"ready,omitempty"`
}

type Job struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta `json:"metadata"`
	Spec     JobSpec    `json:"spec"`
	Status   JobStatus  `json:"status"`
}

type JobTemplateSpec struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     JobSpec    `json:"spec"`
}

type CronJobSpec struct {
	Schedule                   string          `json:"schedule"`
	ConcurrencyPolicy          string          `json:"concurrencyPolicy,omitempty"`
	Suspend                    *bool           `json:"suspend,omitempty"`
	JobTemplate                JobTemplateSpec `json:"jobTemplate"`
	SuccessfulJobsHistoryLimit *int32          `json:"successfulJobsHistoryLimit,omitempty"`
	FailedJobsHistoryLimit     *int32          `json:"failedJobsHistoryLimit,omitempty"`
}

type CronJobStatus struct {
	Active             []ObjectReference `json:"active,omitempty"`
	LastScheduleTime   string            `json:"lastScheduleTime,omitempty"`
	LastSuccessfulTime string            `json:"lastSuccessfulTime,omitempty"`
}

type CronJob struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta    `json:"metadata"`
	Spec     CronJobSpec   `json:"spec"`
	Status   CronJobStatus `json:"status"`
}

// ---------------------------------------------------------------------------
// autoscaling/v1 Scale (deployment scale subresource)
// ---------------------------------------------------------------------------

type ScaleSpec struct {
	Replicas int32 `json:"replicas"`
}

type ScaleStatus struct {
	Replicas int32  `json:"replicas"`
	Selector string `json:"selector,omitempty"`
}

type Scale struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta  `json:"metadata"`
	Spec     ScaleSpec   `json:"spec"`
	Status   ScaleStatus `json:"status"`
}

// ---------------------------------------------------------------------------
// metrics.k8s.io/v1beta1
// ---------------------------------------------------------------------------

type ContainerMetrics struct {
	Name  string       `json:"name"`
	Usage ResourceList `json:"usage"`
}

type PodMetrics struct {
	TypeMeta   `json:",inline"`
	Metadata   ObjectMeta         `json:"metadata"`
	Timestamp  string             `json:"timestamp"`
	Window     string             `json:"window"`
	Containers []ContainerMetrics `json:"containers"`
}

type NodeMetrics struct {
	TypeMeta  `json:",inline"`
	Metadata  ObjectMeta   `json:"metadata"`
	Timestamp string       `json:"timestamp"`
	Window    string       `json:"window"`
	Usage     ResourceList `json:"usage"`
}

// ---------------------------------------------------------------------------
// authorization.k8s.io/v1
// ---------------------------------------------------------------------------

type SelfSubjectAccessReview struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta     `json:"metadata"`
	Spec     map[string]any `json:"spec"`
	Status   map[string]any `json:"status"`
}

type SelfSubjectRulesReview struct {
	TypeMeta `json:",inline"`
	Metadata ObjectMeta     `json:"metadata"`
	Spec     map[string]any `json:"spec"`
	Status   map[string]any `json:"status"`
}

// ---------------------------------------------------------------------------
// Watch
// ---------------------------------------------------------------------------

type WatchEvent struct {
	Type   string `json:"type"`
	Object any    `json:"object"`
}

func BoolPtr(b bool) *bool    { return &b }
func Int32Ptr(i int32) *int32 { return &i }
func Int64Ptr(i int64) *int64 { return &i }
