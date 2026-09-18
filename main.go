// Command k8s-cluster-emulator serves a stateful fake Kubernetes API built
// for pushing k9s into extreme-scale scenarios.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"gopkg.in/yaml.v3"

	"github.com/danske-spil/k8s-cluster-emulator/internal/apiserver"
	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/config"
	"github.com/danske-spil/k8s-cluster-emulator/internal/generate"
	"github.com/danske-spil/k8s-cluster-emulator/internal/metrics"
	"github.com/danske-spil/k8s-cluster-emulator/internal/simulate"
	"github.com/danske-spil/k8s-cluster-emulator/internal/ui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type flags struct {
	scenario    string
	file        string
	seed        int64
	port        int
	nodes       int
	namespaces  int
	deployments int
	pods        int
	replicasMin int
	replicasMax int
	containers  int
	latency     time.Duration
	perObject   time.Duration
	jitter      float64
	chaos       bool
	watchChurn  bool
	churnPods   float64
	churnRest   float64
	churnScale  float64
	eventsMax   int
	eventsRate  float64
	listLimit   int
	noMetrics   bool
	headless    bool
	listPresets bool
	dumpConfig  bool
}

func run() error {
	var f flags
	flag.StringVar(&f.scenario, "scenario", "medium", "built-in scenario preset (see --list-scenarios)")
	flag.StringVar(&f.file, "config", "", "path to a scenario YAML file")
	flag.Int64Var(&f.seed, "seed", 0, "random seed; the same seed and scenario rebuild the same cluster")
	flag.IntVar(&f.port, "port", 0, "listen port")
	flag.IntVar(&f.nodes, "nodes", 0, "number of nodes")
	flag.IntVar(&f.namespaces, "namespaces", 0, "number of namespaces")
	flag.IntVar(&f.deployments, "deployments", 0, "number of deployments")
	flag.IntVar(&f.pods, "pods", 0, "total pod budget spread across deployments")
	flag.IntVar(&f.replicasMin, "replicas-min", 0, "minimum replicas per deployment")
	flag.IntVar(&f.replicasMax, "replicas-max", 0, "maximum replicas per deployment")
	flag.IntVar(&f.containers, "containers", 0, "maximum containers per pod")
	flag.DurationVar(&f.latency, "latency", 0, "base API latency, scaled per endpoint class")
	flag.DurationVar(&f.perObject, "per-object-latency", 0, "extra latency per serialized list item")
	flag.Float64Var(&f.jitter, "latency-jitter", 0, "fractional latency jitter (0.5 = +/-50%)")
	flag.BoolVar(&f.chaos, "chaos", false, "inject 500s, 429s and stalls")
	flag.BoolVar(&f.watchChurn, "watch-churn", false, "periodically sever all watch connections")
	flag.Float64Var(&f.churnPods, "churn-pods", 0, "pods deleted per second")
	flag.Float64Var(&f.churnRest, "churn-restarts", 0, "container restarts per second")
	flag.Float64Var(&f.churnScale, "churn-scales", 0, "deployment rescales per minute")
	flag.IntVar(&f.eventsMax, "events-max", 0, "maximum retained events")
	flag.Float64Var(&f.eventsRate, "events-rate", 0, "extra background events per second")
	flag.IntVar(&f.listLimit, "list-limit", 0, "force a server-side page size on unbounded LISTs")
	flag.BoolVar(&f.noMetrics, "no-metrics", false, "disable the metrics.k8s.io API")
	flag.BoolVar(&f.headless, "headless", false, "log to stdout instead of running the dashboard")
	flag.BoolVar(&f.listPresets, "list-scenarios", false, "print the built-in scenarios and exit")
	flag.BoolVar(&f.dumpConfig, "dump-config", false, "print the resolved scenario as YAML and exit")
	flag.Parse()

	if f.listPresets {
		fmt.Println("built-in scenarios:")
		for _, n := range config.PresetNames() {
			p, _ := config.Preset(n)
			fmt.Printf("  %-22s %9s pods  %6d deployments  %5d namespaces  %6d nodes\n",
				n, thousands(p.TotalPods()), p.Workloads.Deployments, p.Namespaces, p.Nodes)
		}
		return nil
	}

	cfg, err := resolve(f)
	if err != nil {
		return err
	}
	if f.dumpConfig {
		out, err := yaml.Marshal(cfg)
		if err != nil {
			return err
		}
		fmt.Print(string(out))
		return nil
	}

	c := cluster.New(cfg.API.WatchBuffer, cfg.Events.Max)
	factory := generate.New(cfg, c)
	build(cfg, factory)

	coll := metrics.New()
	engine := simulate.NewEngine(cfg, c, factory)
	engine.Prime()

	server := apiserver.New(cfg, c, engine, factory, coll)
	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.API.Port),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      0, // watches and log streams are long-lived
		IdleTimeout:       300 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go engine.Run(ctx)
	server.StartWatchChurn(ctx)

	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	endpoint := fmt.Sprintf("http://localhost:%d", cfg.API.Port)
	if f.headless || !isTerminal() {
		runHeadless(ctx, cfg, c, coll, endpoint, errCh)
	} else if err := runDashboard(ctx, cancel, cfg, c, engine, coll, endpoint, errCh); err != nil {
		return err
	}

	shutdown, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	_ = httpSrv.Shutdown(shutdown)
	return nil
}

// resolve layers explicitly-passed flags over the chosen preset or file.
func resolve(f flags) (config.Scenario, error) {
	var cfg config.Scenario
	if f.file != "" {
		var err error
		if cfg, err = config.Load(f.file); err != nil {
			return cfg, err
		}
	} else {
		p, ok := config.Preset(f.scenario)
		if !ok {
			return cfg, fmt.Errorf("unknown scenario %q (try --list-scenarios)", f.scenario)
		}
		cfg = p
	}

	set := map[string]bool{}
	flag.Visit(func(fl *flag.Flag) { set[fl.Name] = true })
	apply := func(name string, fn func()) {
		if set[name] {
			fn()
		}
	}
	apply("seed", func() { cfg.Seed = f.seed })
	apply("port", func() { cfg.API.Port = f.port })
	apply("nodes", func() { cfg.Nodes = f.nodes })
	apply("namespaces", func() { cfg.Namespaces = f.namespaces })
	apply("deployments", func() { cfg.Workloads.Deployments = f.deployments })
	apply("pods", func() { cfg.Workloads.Pods = f.pods })
	apply("replicas-min", func() { cfg.Workloads.Replicas.Min = f.replicasMin })
	apply("replicas-max", func() { cfg.Workloads.Replicas.Max = f.replicasMax })
	apply("containers", func() { cfg.Workloads.Containers.Max = f.containers })
	apply("latency", func() { cfg.API.Latency = config.Duration(f.latency) })
	apply("per-object-latency", func() { cfg.API.PerObjectLatency = config.Duration(f.perObject) })
	apply("latency-jitter", func() { cfg.API.Jitter = f.jitter })
	apply("chaos", func() { cfg.API.Chaos = f.chaos })
	apply("watch-churn", func() { cfg.API.WatchChurn = f.watchChurn })
	apply("churn-pods", func() { cfg.Churn.PodsPerSecond = f.churnPods })
	apply("churn-restarts", func() { cfg.Churn.RestartsPerSecond = f.churnRest })
	apply("churn-scales", func() { cfg.Churn.ScalesPerMinute = f.churnScale })
	apply("events-max", func() { cfg.Events.Max = f.eventsMax })
	apply("events-rate", func() { cfg.Events.ExtraPerSecond = f.eventsRate })
	apply("list-limit", func() { cfg.API.DefaultListLimit = f.listLimit })
	apply("no-metrics", func() { cfg.API.Metrics = !f.noMetrics })

	cfg.Normalize()
	return cfg, nil
}

// build generates the cluster, reporting progress on stderr because the
// dashboard cannot start until there is something to show.
func build(cfg config.Scenario, factory *generate.Factory) {
	start := time.Now()
	var last time.Time
	fmt.Fprintf(os.Stderr, "building %q: %s pods, %s deployments, %s namespaces, %s nodes (seed %d)\n",
		cfg.Name, thousands(cfg.TotalPods()), thousands(cfg.Workloads.Deployments),
		thousands(cfg.Namespaces), thousands(cfg.Nodes), cfg.Seed)

	factory.Build(func(stage string, done, total int) {
		if done != total && time.Since(last) < 100*time.Millisecond {
			return
		}
		last = time.Now()
		pct := 0
		if total > 0 {
			pct = done * 100 / total
		}
		fmt.Fprintf(os.Stderr, "\r  %-12s %3d%% (%s/%s)%s", stage, pct, thousands(done), thousands(total), strings.Repeat(" ", 12))
	})
	fmt.Fprintf(os.Stderr, "\r  ready in %s%s\n", time.Since(start).Truncate(time.Millisecond), strings.Repeat(" ", 40))
}

func runDashboard(ctx context.Context, cancel context.CancelFunc, cfg config.Scenario, c *cluster.Cluster, e *simulate.Engine, m *metrics.Collector, addr string, errCh chan error) error {
	p := tea.NewProgram(ui.NewModel(cfg, c, e, m, addr), tea.WithAltScreen(), tea.WithContext(ctx))
	go func() {
		select {
		case err := <-errCh:
			fmt.Fprintln(os.Stderr, "server error:", err)
			p.Quit()
		case <-ctx.Done():
		}
	}()

	_, err := p.Run()
	cancel()
	if err != nil && err != tea.ErrProgramKilled && !strings.Contains(err.Error(), "context canceled") {
		return err
	}
	return nil
}

func runHeadless(ctx context.Context, cfg config.Scenario, c *cluster.Cluster, m *metrics.Collector, addr string, errCh chan error) {
	fmt.Printf("listening on %s (scenario %s, seed %d)\n", addr, cfg.Name, cfg.Seed)
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			fmt.Fprintln(os.Stderr, "server error:", err)
			return
		case <-t.C:
			s := m.Snapshot(0, 0)
			cs := c.Stats()
			fmt.Printf("pods=%d running=%d pending=%d failing=%d | rps=%.1f p95=%.1fms watches=%d | heap=%.0fMB cpu=%.2f\n",
				cs.Pods,
				cs.PodStates[cluster.StRunning],
				cs.PodStates[cluster.StPending]+cs.PodStates[cluster.StContainerCreating],
				cs.PodStates[cluster.StError]+cs.PodStates[cluster.StCrashLoopBackOff]+cs.PodStates[cluster.StImagePullBackOff],
				s.RPS, s.P95, s.WatchesLive, s.HeapMB, s.CPUCores)
		}
	}
}

func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func thousands(v int) string {
	s := fmt.Sprintf("%d", v)
	var out []byte
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}
