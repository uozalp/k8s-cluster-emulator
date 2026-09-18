package apiserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/config"
	"github.com/danske-spil/k8s-cluster-emulator/internal/generate"
	"github.com/danske-spil/k8s-cluster-emulator/internal/metrics"
	"github.com/danske-spil/k8s-cluster-emulator/internal/simulate"
)

type listResponse struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Items      []struct {
		Metadata struct {
			Name            string            `json:"name"`
			Namespace       string            `json:"namespace"`
			Labels          map[string]string `json:"labels"`
			OwnerReferences []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
		Spec struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
	} `json:"items"`
	Metadata struct {
		ResourceVersion    string `json:"resourceVersion"`
		Continue           string `json:"continue"`
		RemainingItemCount int64  `json:"remainingItemCount"`
	} `json:"metadata"`
}

func newTestServer(t *testing.T) (*httptest.Server, *cluster.Cluster) {
	t.Helper()
	cfg, _ := config.Preset("small")
	cfg.Churn = config.Churn{}
	c := cluster.New(cfg.API.WatchBuffer, cfg.Events.Max)
	f := generate.New(cfg, c)
	f.Build(nil)
	eng := simulate.NewEngine(cfg, c, f)
	eng.Prime()
	srv := httptest.NewServer(New(cfg, c, eng, f, metrics.New()).Handler())
	t.Cleanup(srv.Close)
	return srv, c
}

func getJSON(t *testing.T, srv *httptest.Server, path string, out any) *http.Response {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
	}
	return resp
}

func TestListPaginationCoversEveryPod(t *testing.T) {
	srv, c := newTestServer(t)
	total := c.Pods.Len()

	seen := map[string]bool{}
	path := "/api/v1/pods?limit=37"
	for {
		var list listResponse
		getJSON(t, srv, path, &list)
		if list.Kind != "PodList" || list.APIVersion != "v1" {
			t.Fatalf("unexpected list envelope %q/%q", list.Kind, list.APIVersion)
		}
		for _, it := range list.Items {
			key := it.Metadata.Namespace + "/" + it.Metadata.Name
			if seen[key] {
				t.Fatalf("pod %s returned twice across pages", key)
			}
			seen[key] = true
		}
		if list.Metadata.Continue == "" {
			break
		}
		if len(list.Items) != 37 {
			t.Fatalf("page returned %d items with a continue token, want 37", len(list.Items))
		}
		path = "/api/v1/pods?limit=37&continue=" + url.QueryEscape(list.Metadata.Continue)
	}
	if len(seen) != total {
		t.Fatalf("paged through %d pods, store holds %d", len(seen), total)
	}
}

func TestListSelectors(t *testing.T) {
	srv, _ := newTestServer(t)

	var all listResponse
	getJSON(t, srv, "/api/v1/pods", &all)
	if len(all.Items) == 0 {
		t.Fatal("no pods")
	}
	sample := all.Items[0]

	var byLabel listResponse
	app := sample.Metadata.Labels["app.kubernetes.io/name"]
	inst := sample.Metadata.Labels["app.kubernetes.io/instance"]
	getJSON(t, srv, "/api/v1/pods?labelSelector="+url.QueryEscape(
		"app.kubernetes.io/name="+app+",app.kubernetes.io/instance="+inst), &byLabel)
	if len(byLabel.Items) == 0 || len(byLabel.Items) >= len(all.Items) {
		t.Fatalf("label selector returned %d of %d pods", len(byLabel.Items), len(all.Items))
	}
	for _, it := range byLabel.Items {
		if it.Metadata.Labels["app.kubernetes.io/instance"] != inst {
			t.Fatalf("label selector returned non-matching pod %s", it.Metadata.Name)
		}
	}

	var byNode listResponse
	getJSON(t, srv, "/api/v1/pods?fieldSelector="+url.QueryEscape("spec.nodeName="+sample.Spec.NodeName), &byNode)
	if len(byNode.Items) == 0 {
		t.Fatal("field selector on spec.nodeName returned nothing")
	}
	for _, it := range byNode.Items {
		if it.Spec.NodeName != sample.Spec.NodeName {
			t.Fatalf("field selector returned pod on %s, want %s", it.Spec.NodeName, sample.Spec.NodeName)
		}
	}
}

func TestNamespaceScopedList(t *testing.T) {
	srv, c := newTestServer(t)
	ns := c.Pods.Namespaces()[0]

	var list listResponse
	getJSON(t, srv, "/api/v1/namespaces/"+ns+"/pods", &list)
	if len(list.Items) != c.Pods.LenNS(ns) {
		t.Fatalf("namespaced list returned %d, want %d", len(list.Items), c.Pods.LenNS(ns))
	}
	for _, it := range list.Items {
		if it.Metadata.Namespace != ns {
			t.Fatalf("namespaced list leaked %s", it.Metadata.Namespace)
		}
	}
}

func TestPodOwnerReferencesSurviveSerialization(t *testing.T) {
	srv, _ := newTestServer(t)
	var list listResponse
	getJSON(t, srv, "/api/v1/pods?limit=5", &list)
	for _, it := range list.Items {
		if len(it.Metadata.OwnerReferences) != 1 || it.Metadata.OwnerReferences[0].Kind != "ReplicaSet" {
			t.Fatalf("pod %s is missing its ReplicaSet owner reference", it.Metadata.Name)
		}
	}
}

func TestGetAndNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	var list listResponse
	getJSON(t, srv, "/api/v1/pods?limit=1", &list)
	p := list.Items[0].Metadata

	var pod map[string]any
	getJSON(t, srv, "/api/v1/namespaces/"+p.Namespace+"/pods/"+p.Name, &pod)
	if pod["kind"] != "Pod" {
		t.Fatalf("GET returned kind %v", pod["kind"])
	}

	resp := getJSON(t, srv, "/api/v1/namespaces/"+p.Namespace+"/pods/does-not-exist", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing pod returned %d, want 404", resp.StatusCode)
	}
}

func TestScaleSubresource(t *testing.T) {
	srv, c := newTestServer(t)
	var deploy *cluster.Deployment
	c.Deployments.All(func(d *cluster.Deployment) bool { deploy = d; return false })
	if deploy == nil {
		t.Fatal("no deployments")
	}
	path := srv.URL + "/apis/apps/v1/namespaces/" + deploy.Namespace + "/deployments/" + deploy.Name + "/scale"

	var scale struct {
		Kind string `json:"kind"`
		Spec struct {
			Replicas int32 `json:"replicas"`
		} `json:"spec"`
	}
	getJSON(t, srv, "/apis/apps/v1/namespaces/"+deploy.Namespace+"/deployments/"+deploy.Name+"/scale", &scale)
	if scale.Kind != "Scale" {
		t.Fatalf("scale GET returned kind %q", scale.Kind)
	}

	req, _ := http.NewRequest(http.MethodPut, path, jsonBody(`{"spec":{"replicas":123}}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT scale: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT scale returned %d", resp.StatusCode)
	}
	if deploy.SpecReplicas != 123 {
		t.Fatalf("spec.replicas = %d, want 123", deploy.SpecReplicas)
	}
}

func TestWatchRejectsExpiredResourceVersion(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := getJSON(t, srv, "/api/v1/pods?watch=true&resourceVersion=1", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("expired watch returned %d, want 410", resp.StatusCode)
	}
}

func TestDiscovery(t *testing.T) {
	srv, _ := newTestServer(t)

	var core struct {
		GroupVersion string `json:"groupVersion"`
		Resources    []struct {
			Name string `json:"name"`
			Kind string `json:"kind"`
		} `json:"resources"`
	}
	getJSON(t, srv, "/api/v1", &core)
	if core.GroupVersion != "v1" {
		t.Fatalf("core discovery groupVersion = %q", core.GroupVersion)
	}
	want := map[string]bool{"pods": false, "nodes": false, "namespaces": false, "pods/log": false}
	for _, r := range core.Resources {
		if _, ok := want[r.Name]; ok {
			want[r.Name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Fatalf("core discovery is missing %q", name)
		}
	}

	var groups struct {
		Groups []struct {
			Name string `json:"name"`
		} `json:"groups"`
	}
	getJSON(t, srv, "/apis", &groups)
	seen := map[string]bool{}
	for _, g := range groups.Groups {
		seen[g.Name] = true
	}
	for _, g := range []string{"apps", "batch", "metrics.k8s.io", "authorization.k8s.io"} {
		if !seen[g] {
			t.Fatalf("api group %q not advertised", g)
		}
	}
}

func TestEmptyKindsStillListCleanly(t *testing.T) {
	srv, _ := newTestServer(t)
	var list listResponse
	getJSON(t, srv, "/apis/apps/v1/daemonsets", &list)
	if list.Kind != "DaemonSetList" || len(list.Items) != 0 {
		t.Fatalf("daemonsets returned %q with %d items", list.Kind, len(list.Items))
	}
}

func TestParsePath(t *testing.T) {
	cases := []struct {
		path string
		want reqInfo
	}{
		{"/api/v1/pods", reqInfo{version: "v1", resource: "pods", ok: true}},
		{"/api/v1/namespaces", reqInfo{version: "v1", resource: "namespaces", ok: true}},
		{"/api/v1/namespaces/team-01", reqInfo{version: "v1", resource: "namespaces", name: "team-01", ok: true}},
		{"/api/v1/namespaces/team-01/pods", reqInfo{version: "v1", namespace: "team-01", resource: "pods", ok: true}},
		{"/api/v1/namespaces/team-01/pods/web-1/log", reqInfo{version: "v1", namespace: "team-01", resource: "pods", name: "web-1", subresource: "log", ok: true}},
		{"/apis/apps/v1/namespaces/team-01/deployments/web/scale", reqInfo{group: "apps", version: "v1", namespace: "team-01", resource: "deployments", name: "web", subresource: "scale", ok: true}},
		{"/apis/apps/v1", reqInfo{group: "apps", version: "v1"}},
		{"/version", reqInfo{}},
	}
	for _, c := range cases {
		if got := parsePath(c.path); got != c.want {
			t.Errorf("parsePath(%q) = %+v, want %+v", c.path, got, c.want)
		}
	}
}

func jsonBody(s string) *strings.Reader { return strings.NewReader(s) }
