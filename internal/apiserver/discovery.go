package apiserver

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/danske-spil/k8s-cluster-emulator/internal/kapi"
)

// extraRes is a kind the emulator advertises and serves as an empty list.
// Advertising them keeps k9s's resource catalogue realistic: the views exist
// and navigate correctly, they simply contain nothing.
type extraRes struct {
	group, version           string
	resource, singular, kind string
	namespaced               bool
	shortNames               []string
}

var extras = []extraRes{
	{"", "v1", "serviceaccounts", "serviceaccount", "ServiceAccount", true, []string{"sa"}},
	{"", "v1", "endpoints", "endpoints", "Endpoints", true, []string{"ep"}},
	{"", "v1", "persistentvolumes", "persistentvolume", "PersistentVolume", false, []string{"pv"}},
	{"", "v1", "persistentvolumeclaims", "persistentvolumeclaim", "PersistentVolumeClaim", true, []string{"pvc"}},
	{"", "v1", "resourcequotas", "resourcequota", "ResourceQuota", true, []string{"quota"}},
	{"", "v1", "limitranges", "limitrange", "LimitRange", true, []string{"limits"}},
	{"apps", "v1", "daemonsets", "daemonset", "DaemonSet", true, []string{"ds"}},
	{"apps", "v1", "statefulsets", "statefulset", "StatefulSet", true, []string{"sts"}},
	{"apps", "v1", "controllerrevisions", "controllerrevision", "ControllerRevision", true, nil},
	{"networking.k8s.io", "v1", "ingresses", "ingress", "Ingress", true, []string{"ing"}},
	{"networking.k8s.io", "v1", "networkpolicies", "networkpolicy", "NetworkPolicy", true, []string{"netpol"}},
	{"networking.k8s.io", "v1", "ingressclasses", "ingressclass", "IngressClass", false, nil},
	{"rbac.authorization.k8s.io", "v1", "roles", "role", "Role", true, nil},
	{"rbac.authorization.k8s.io", "v1", "rolebindings", "rolebinding", "RoleBinding", true, nil},
	{"rbac.authorization.k8s.io", "v1", "clusterroles", "clusterrole", "ClusterRole", false, nil},
	{"rbac.authorization.k8s.io", "v1", "clusterrolebindings", "clusterrolebinding", "ClusterRoleBinding", false, nil},
	{"storage.k8s.io", "v1", "storageclasses", "storageclass", "StorageClass", false, []string{"sc"}},
	{"policy", "v1", "poddisruptionbudgets", "poddisruptionbudget", "PodDisruptionBudget", true, []string{"pdb"}},
	{"autoscaling", "v2", "horizontalpodautoscalers", "horizontalpodautoscaler", "HorizontalPodAutoscaler", true, []string{"hpa"}},
	{"apiextensions.k8s.io", "v1", "customresourcedefinitions", "customresourcedefinition", "CustomResourceDefinition", false, []string{"crd"}},
	{"authorization.k8s.io", "v1", "selfsubjectaccessreviews", "", "SelfSubjectAccessReview", false, nil},
	{"authorization.k8s.io", "v1", "selfsubjectrulesreviews", "", "SelfSubjectRulesReview", false, nil},
	{"metrics.k8s.io", "v1beta1", "pods", "pod", "PodMetrics", true, nil},
	{"metrics.k8s.io", "v1beta1", "nodes", "node", "NodeMetrics", false, nil},
}

func emptyKind(group, version, resource string) (kind, groupVersion string, ok bool) {
	for _, e := range extras {
		if e.group == group && e.version == version && e.resource == resource {
			gv := e.version
			if e.group != "" {
				gv = e.group + "/" + e.version
			}
			return e.kind + "List", gv, true
		}
	}
	return "", "", false
}

func (s *Server) apiVersions() kapi.APIVersions {
	return kapi.APIVersions{
		TypeMeta: kapi.TypeMeta{Kind: "APIVersions"},
		Versions: []string{"v1"},
		ServerAddressByClientCIDRs: []kapi.ServerAddressByClientCIDR{
			{ClientCIDR: "0.0.0.0/0", ServerAddress: "localhost"},
		},
	}
}

func (s *Server) apiGroups() kapi.APIGroupList {
	seen := map[string]string{}
	for _, v := range s.c.Views() {
		if v.Group != "" {
			seen[v.Group] = v.Version
		}
	}
	for _, e := range extras {
		if e.group == "" {
			continue
		}
		if e.group == "metrics.k8s.io" && !s.cfg.API.Metrics {
			continue
		}
		if _, ok := seen[e.group]; !ok {
			seen[e.group] = e.version
		}
	}
	names := make([]string, 0, len(seen))
	for g := range seen {
		names = append(names, g)
	}
	sort.Strings(names)

	groups := make([]kapi.APIGroup, 0, len(names))
	for _, g := range names {
		gv := kapi.GroupVersionForDiscovery{GroupVersion: g + "/" + seen[g], Version: seen[g]}
		groups = append(groups, kapi.APIGroup{
			Name:             g,
			Versions:         []kapi.GroupVersionForDiscovery{gv},
			PreferredVersion: gv,
		})
	}
	return kapi.APIGroupList{
		TypeMeta: kapi.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
		Groups:   groups,
	}
}

// resourceList serves group/version discovery for /api/v1 and /apis/<g>/<v>.
func (s *Server) resourceList(path string) (kapi.APIResourceList, bool) {
	var group, version string
	switch {
	case path == "/api/v1":
		version = "v1"
	case strings.HasPrefix(path, "/apis/"):
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) != 3 {
			return kapi.APIResourceList{}, false
		}
		group, version = parts[1], parts[2]
	default:
		return kapi.APIResourceList{}, false
	}
	if group == "metrics.k8s.io" && !s.cfg.API.Metrics {
		return kapi.APIResourceList{}, false
	}

	gv := version
	if group != "" {
		gv = group + "/" + version
	}
	out := kapi.APIResourceList{
		TypeMeta:     kapi.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
		GroupVersion: gv,
	}

	for _, v := range s.c.Views() {
		if v.Group != group || v.Version != version {
			continue
		}
		out.APIResources = append(out.APIResources, kapi.APIResource{
			Name: v.Resource, SingularName: v.Singular, Namespaced: v.Namespaced,
			Kind: v.Kind, Verbs: v.Verbs, ShortNames: v.ShortNames, Categories: v.Categories,
		})
		// Status and scale subresources k9s probes for.
		out.APIResources = append(out.APIResources, kapi.APIResource{
			Name: v.Resource + "/status", Namespaced: v.Namespaced, Kind: v.Kind,
			Verbs: []string{"get", "patch", "update"},
		})
		switch v.Resource {
		case "pods":
			out.APIResources = append(out.APIResources,
				kapi.APIResource{Name: "pods/log", Namespaced: true, Kind: "Pod", Verbs: []string{"get"}},
				kapi.APIResource{Name: "pods/exec", Namespaced: true, Kind: "PodExecOptions", Verbs: []string{"create", "get"}},
				kapi.APIResource{Name: "pods/portforward", Namespaced: true, Kind: "PodPortForwardOptions", Verbs: []string{"create", "get"}},
			)
		case "deployments":
			out.APIResources = append(out.APIResources, kapi.APIResource{
				Name: "deployments/scale", Namespaced: true, Kind: "Scale",
				Group: "autoscaling", Version: "v1", Verbs: []string{"get", "patch", "update"},
			})
		case "replicasets":
			out.APIResources = append(out.APIResources, kapi.APIResource{
				Name: "replicasets/scale", Namespaced: true, Kind: "Scale",
				Group: "autoscaling", Version: "v1", Verbs: []string{"get", "patch", "update"},
			})
		}
	}

	for _, e := range extras {
		if e.group != group || e.version != version {
			continue
		}
		verbs := []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}
		if e.group == "authorization.k8s.io" {
			verbs = []string{"create"}
		}
		out.APIResources = append(out.APIResources, kapi.APIResource{
			Name: e.resource, SingularName: e.singular, Namespaced: e.namespaced,
			Kind: e.kind, Verbs: verbs, ShortNames: e.shortNames,
		})
	}

	if len(out.APIResources) == 0 {
		return out, false
	}
	return out, true
}

// emptyList answers with a well-formed empty collection, or an idle watch.
func (s *Server) emptyList(w *capture, listKind, groupVersion string, watch bool, r *http.Request) {
	if watch {
		s.idleWatch(w, r)
		return
	}
	writeJSON(w, map[string]any{
		"apiVersion": groupVersion,
		"kind":       listKind,
		"metadata":   map[string]any{"resourceVersion": s.c.RVString()},
		"items":      []any{},
	})
}

// idleWatch holds a watch open with no events, which is what a real API
// server does for a resource type that never changes.
func (s *Server) idleWatch(w *capture, r *http.Request) {
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if !ok {
		writeStatus(w, http.StatusInternalServerError, "InternalError", "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	s.m.ActiveWA.Add(1)
	defer s.m.ActiveWA.Add(-1)
	<-r.Context().Done()
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

func (s *Server) handleAuthz(w *capture, r *http.Request, ri reqInfo) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var req map[string]any
	_ = json.Unmarshal(body, &req)
	spec, _ := req["spec"].(map[string]any)
	if spec == nil {
		spec = map[string]any{}
	}

	switch ri.resource {
	case "selfsubjectaccessreviews":
		writeJSON(w, kapi.SelfSubjectAccessReview{
			TypeMeta: kapi.TypeMeta{Kind: "SelfSubjectAccessReview", APIVersion: "authorization.k8s.io/v1"},
			Spec:     spec,
			Status:   map[string]any{"allowed": true, "reason": "emulator allows all"},
		})
	case "selfsubjectrulesreviews":
		writeJSON(w, kapi.SelfSubjectRulesReview{
			TypeMeta: kapi.TypeMeta{Kind: "SelfSubjectRulesReview", APIVersion: "authorization.k8s.io/v1"},
			Spec:     spec,
			Status: map[string]any{
				"incomplete": false,
				"resourceRules": []map[string]any{
					{"verbs": []string{"*"}, "apiGroups": []string{"*"}, "resources": []string{"*"}},
				},
				"nonResourceRules": []map[string]any{
					{"verbs": []string{"*"}, "nonResourceURLs": []string{"*"}},
				},
			},
		})
	default:
		writeStatus(w, http.StatusNotFound, "NotFound", "unknown authorization resource")
	}
}
