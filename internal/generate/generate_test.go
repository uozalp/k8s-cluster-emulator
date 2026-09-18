package generate

import (
	"testing"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/config"
)

func buildFixture(t *testing.T, seed int64) *cluster.Cluster {
	t.Helper()
	cfg, _ := config.Preset("small")
	cfg.Seed = seed
	cfg.Churn = config.Churn{}
	c := cluster.New(cfg.API.WatchBuffer, cfg.Events.Max)
	New(cfg, c).Build(nil)
	return c
}

func podNames(c *cluster.Cluster) []string {
	var out []string
	c.Pods.All(func(p *cluster.Pod) bool {
		out = append(out, p.Tmpl.Namespace+"/"+p.Name)
		return true
	})
	return out
}

func TestSameSeedProducesSameCluster(t *testing.T) {
	a, b := buildFixture(t, 4242), buildFixture(t, 4242)
	na, nb := podNames(a), podNames(b)
	if len(na) == 0 {
		t.Fatal("no pods generated")
	}
	if len(na) != len(nb) {
		t.Fatalf("pod counts differ: %d vs %d", len(na), len(nb))
	}
	for i := range na {
		if na[i] != nb[i] {
			t.Fatalf("pod %d differs: %s vs %s", i, na[i], nb[i])
		}
	}
}

func TestDifferentSeedProducesDifferentCluster(t *testing.T) {
	na := podNames(buildFixture(t, 1))
	nb := podNames(buildFixture(t, 2))
	same := len(na) == len(nb)
	if same {
		for i := range na {
			if na[i] != nb[i] {
				same = false
				break
			}
		}
	}
	if same {
		t.Fatal("different seeds produced an identical cluster")
	}
}

func TestOwnershipChain(t *testing.T) {
	c := buildFixture(t, 7)
	checked := 0
	c.Pods.All(func(p *cluster.Pod) bool {
		rs := p.Tmpl.RS
		if rs == nil {
			t.Fatalf("pod %s has no owning ReplicaSet", p.Name)
		}
		if rs.Deploy == nil {
			t.Fatalf("replicaset %s has no owning Deployment", rs.Name)
		}
		if rs.Namespace != p.Tmpl.Namespace || rs.Deploy.Namespace != rs.Namespace {
			t.Fatalf("namespace mismatch in ownership chain for %s", p.Name)
		}
		// The Deployment selector must select its own pods.
		for k, v := range rs.Deploy.Selector {
			if p.Tmpl.Labels[k] != v {
				t.Fatalf("pod %s label %s=%q does not match deployment selector %q",
					p.Name, k, p.Tmpl.Labels[k], v)
			}
		}
		if p.Tmpl.Labels["pod-template-hash"] != rs.Hash {
			t.Fatalf("pod %s has wrong pod-template-hash", p.Name)
		}
		checked++
		return true
	})
	if checked == 0 {
		t.Fatal("no pods to check")
	}
}

func TestReplicaSetCountersMatchPods(t *testing.T) {
	c := buildFixture(t, 11)
	c.ReplicaSets.All(func(rs *cluster.ReplicaSet) bool {
		var live, ready, avail int32
		for _, p := range rs.Pods() {
			if p == nil {
				continue
			}
			live++
			if p.State.Ready() {
				ready++
			}
			if p.Avail {
				avail++
			}
		}
		if rs.Total != live || rs.ReadyCnt != ready || rs.AvailCnt != avail {
			t.Fatalf("%s counters total/ready/avail = %d/%d/%d, want %d/%d/%d",
				rs.Name, rs.Total, rs.ReadyCnt, rs.AvailCnt, live, ready, avail)
		}
		if avail > ready {
			t.Fatalf("%s reports more available than ready", rs.Name)
		}
		return true
	})
}

func TestPodSuffixIsUnique(t *testing.T) {
	seen := make(map[string]uint32, 100000)
	for i := uint32(1); i <= 100000; i++ {
		s := podSuffix(i)
		if len(s) != 5 {
			t.Fatalf("suffix %q is not 5 characters", s)
		}
		if prev, dup := seen[s]; dup {
			t.Fatalf("suffix collision between ordinals %d and %d (%q)", prev, i, s)
		}
		seen[s] = i
	}
}
