package kapi

// GetObjectMeta lets the watch layer filter rendered objects without a type
// switch per event.
type MetaAccessor interface{ GetObjectMeta() *ObjectMeta }

func (o *Pod) GetObjectMeta() *ObjectMeta         { return &o.Metadata }
func (o *Node) GetObjectMeta() *ObjectMeta        { return &o.Metadata }
func (o *Namespace) GetObjectMeta() *ObjectMeta   { return &o.Metadata }
func (o *Service) GetObjectMeta() *ObjectMeta     { return &o.Metadata }
func (o *ConfigMap) GetObjectMeta() *ObjectMeta   { return &o.Metadata }
func (o *Secret) GetObjectMeta() *ObjectMeta      { return &o.Metadata }
func (o *Event) GetObjectMeta() *ObjectMeta       { return &o.Metadata }
func (o *Deployment) GetObjectMeta() *ObjectMeta  { return &o.Metadata }
func (o *ReplicaSet) GetObjectMeta() *ObjectMeta  { return &o.Metadata }
func (o *DaemonSet) GetObjectMeta() *ObjectMeta   { return &o.Metadata }
func (o *StatefulSet) GetObjectMeta() *ObjectMeta { return &o.Metadata }
func (o *Job) GetObjectMeta() *ObjectMeta         { return &o.Metadata }
func (o *CronJob) GetObjectMeta() *ObjectMeta     { return &o.Metadata }
func (o *PodMetrics) GetObjectMeta() *ObjectMeta  { return &o.Metadata }
func (o *NodeMetrics) GetObjectMeta() *ObjectMeta { return &o.Metadata }
