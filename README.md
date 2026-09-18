# K8s Cluster Emulator

A stateful, in-memory fake Kubernetes API server built for stress-testing
[k9s](https://k9scli.io) at extreme scale — hundreds of thousands of pods with
real ownership chains, a real lifecycle, and real LIST/WATCH behaviour.

It is not a Kubernetes implementation. It is a large, convincing Kubernetes
*dataset* that moves.

```bash
# Terminal 1 — start the emulator
go run . --scenario huge

# Terminal 2 — point k9s at it
KUBECONFIG=fake-kubeconfig.yaml k9s
```

## What it simulates

- **Real object graph.** `Deployment → ReplicaSet → Pod`, wired with
  `ownerReferences`, matching labels and selectors, and rollout history in the
  form of scaled-to-zero ReplicaSets. Pod → owner navigation, Deployment →
  Pods and ReplicaSet → Deployment all work in k9s.
- **A pod lifecycle.** Pods move `Pending → ContainerCreating → Running`, are
  promoted from ready to available after `minReadySeconds`, crash-loop with an
  exponential backoff, get evicted, terminate with a grace period, and are
  replaced by their ReplicaSet.
- **Working controllers.** Changing `spec.replicas` (via k9s, `kubectl scale`,
  or the `scale` subresource) makes the reconciler create or terminate pods
  over time, and Deployment/ReplicaSet status follows along with distinct
  `replicas` / `readyReplicas` / `availableReplicas` / `unavailableReplicas`
  values and realistic conditions. `rollout restart` starts a genuine new
  revision.
- **Events** generated from actual simulated activity — `Scheduled`, `Pulled`,
  `Started`, `BackOff`, `Killing`, `ScalingReplicaSet`, `SuccessfulCreate` —
  referencing the object they happened to, with bounded retention.
- **LIST and WATCH that behave.** Server-side pagination with `continue`
  tokens, label and field selectors, per-kind watch streams with a replay ring
  (so a watch that resumes from an old `resourceVersion` gets the events it
  missed, or a `410 Gone` if it is too far behind), bookmarks, and
  `sendInitialEvents`.
- **`metrics.k8s.io`**, so k9s's CPU/MEM columns are populated — and so you get
  another very large LIST to stress.
- **Reproducibility.** The same scenario and `--seed` always produce the same
  logical cluster.

## Scenarios

Everything about the cluster's size and shape comes from a scenario. There are
built-in presets for the shapes that break clients in different ways:

```bash
go run . --list-scenarios
```

| Scenario | Shape |
|---|---|
| `small` / `medium` / `large` | 500 / 10k / 60k pods — everyday testing |
| `huge` | 100k pods, 5,000 deployments, 200 namespaces |
| `insane` | 500k pods, 20,000 deployments, 10,000 nodes |
| `huge-deployment` | 1 Deployment, 1 ReplicaSet, 60k pods |
| `many-deployments` | 10,000 Deployments × 10 pods |
| `one-deployment-per-pod` | 60,000 Deployments × 1 pod |
| `huge-namespace` | 1 namespace, 100k pods |
| `many-namespaces` | 5,000 namespaces, 100k pods |
| `high-churn` | 100k pods, 200 pods/sec churn, constant rescaling |
| `failure-storm` | 100k pods, mostly CrashLoopBackOff / ImagePullBackOff / Failed |
| `large-objects` | 60k pods with 6 containers, 25 labels, 20 fat annotations |
| `slow-api` | 60k pods behind 150 ms of endpoint-scaled latency |
| `chaos` | 60k pods with 500s, 429s and watch disconnects |

A preset is just a starting point — override anything with flags:

```bash
go run . --scenario huge --pods 250000 --namespaces 2000 --seed 99
go run . --scenario huge-deployment --pods 60000 --latency 200ms
go run . --scenario high-churn --churn-pods 500 --events-rate 200
```

### Scenario files

For anything you want to keep, write a YAML file. The `name` field selects the
preset to layer on top of, so a file only needs to state its differences:

```yaml
name: huge
seed: 4242
nodes: 4000
namespaces: 400
workloads:
  deployments: 8000
  pods: 250000
  replicas: { min: 1, max: 400 }
  degradedPercent: 15
lifecycle:
  mix:
    running: 85
    pending: 4
    crashLoopBackOff: 5
    imagePullBackOff: 2
    failed: 2
    succeeded: 2
  startup: { min: 1s, max: 30s }
churn:
  podsPerSecond: 40
  scalesPerMinute: 10
events:
  max: 150000
  extraPerSecond: 50
api:
  latency: 120ms
  jitter: 0.6
  perObjectLatency: 2us
objects:
  extraLabels: 10
  extraAnnotations: 8
  annotationSize: 256
```

```bash
go run . --config my-scenario.yaml
```

`--dump-config` prints the fully resolved scenario (preset + file + flags) as
YAML, which is the easiest way to discover every available field.

## Flags

| Flag | Description |
|---|---|
| `--scenario` | built-in preset name (default `medium`) |
| `--config` | scenario YAML file |
| `--seed` | random seed; same seed + scenario = same cluster |
| `--port` | listen port (default 6443) |
| `--nodes`, `--namespaces`, `--deployments`, `--pods` | size overrides |
| `--replicas-min`, `--replicas-max`, `--containers` | shape overrides |
| `--latency`, `--per-object-latency`, `--latency-jitter` | API latency model |
| `--chaos`, `--watch-churn` | failure injection |
| `--churn-pods`, `--churn-restarts`, `--churn-scales` | cluster churn rates |
| `--events-max`, `--events-rate` | event retention and background rate |
| `--list-limit` | force a server-side page size on unbounded LISTs |
| `--no-metrics` | disable `metrics.k8s.io` |
| `--headless` | log a summary line instead of running the dashboard |
| `--list-scenarios`, `--dump-config` | inspect configuration and exit |

## Dashboard

Running in a terminal starts a Bubble Tea dashboard instead of printing to
stdout, so it can sit next to a k9s session without fighting over the screen.

```
k8s cluster emulator  http://localhost:6443 · scenario huge · seed 12345 · up 4m12s
▎1 overview   2 endpoints   3 requests

╭──────────────────────────────╮╭──────────────────────────────╮╭──────────────────────────────╮
│ cluster                      ││ pods                         ││ api                          │
│ namespaces    204            ││ total         100,412        ││ requests/sec  124.0          │
│ nodes         3,000          ││ running       90,180         ││ list          812            │
│ deployments   5,000          ││ pending       3,010          ││ get           4,209          │
│ replicasets   9,912          ││ crashloop     2,004          ││ watches live  8              │
```

- **overview** — cluster counts, pod state breakdown, API rates, latency
  percentiles, watch event rate, pod create/delete rate, lifecycle transition
  rate, heap, CPU and goroutines.
- **endpoints** — the hottest endpoints by total time served, with call counts,
  errors, average/max latency, bytes and items.
- **requests** — a live tail of recent requests.

Keys: `tab` / `1`–`3` switch view, `p` pause, `q` quit.

## Architecture

```
main.go                  flag parsing, wiring, lifecycle
internal/config          scenario definition, presets, YAML loading
internal/kapi            Kubernetes API JSON types, label/field selectors
internal/cluster         the stateful store: objects, indexes, watch hubs,
                         write transactions, aggregate counters
internal/generate        seeded factory that mints the cluster and new pods
internal/simulate        pod lifecycle stepper, ReplicaSet/Deployment
                         reconcilers, churn, event recorder
internal/apiserver       routing, discovery, LIST/WATCH/GET, writes, metrics API
internal/metrics         request telemetry for the dashboard
internal/ui              Bubble Tea dashboard
```

A few decisions worth knowing about:

- **Pods share their template by pointer.** Everything identical across the
  pods of a ReplicaSet — labels, annotations, containers, volumes, resource
  profile — lives in one `PodTemplate`. A pod itself is a compact struct of
  state and timestamps. 500,000 pods fit in roughly 400 MB.
- **One writer.** All mutation happens on the simulation goroutine inside short
  write transactions. Watch notices are rendered while the lock is held and
  broadcast after it is released, so watchers never see a half-applied change
  and never block the simulation. API writes only change *spec*; the
  reconciler does the rest, exactly like a real cluster.
- **LISTs stream in chunks.** A LIST renders 500 objects per acquisition of the
  read lock and writes them out before taking the lock again. Memory stays
  bounded and the simulation is never blocked for long, even while serving a
  2 GB pod list.
- **Status is counted, never scanned.** ReplicaSet and Deployment status comes
  from counters maintained on every transition, so rendering a Deployment does
  not walk 60,000 pods.

## Making it hurt

The emulator is deliberately fast by default. Use these to push k9s instead of
benchmarking the fake server:

```bash
# Very large initial LIST, then a huge steady watch stream
go run . --scenario huge --churn-pods 200 --churn-restarts 100

# Realistic API server latency that scales with response size
go run . --scenario large --latency 150ms --per-object-latency 3us

# Force k9s to re-LIST constantly
go run . --scenario large --watch-churn

# Errors, throttling and stalls on top
go run . --scenario chaos

# Fat objects: 6 containers, 25 labels, 20 x 512-byte annotations per pod
go run . --scenario large-objects
```

Worth watching in k9s under these: initial load time for `:pods` across all
namespaces, memory growth, responsiveness while sorting and filtering,
behaviour when a watch dies, and how it copes with the events view at 200k
events.

## k9s tuning for large clusters

```yaml
# ~/.config/k9s/config.yaml
k9s:
  refreshRate: 5      # default 2s generates a lot of load at 100k pods
  maxConnRetry: 3
  ui:
    logoless: true
```

## Docker

```bash
docker build -t k8s-emulator .
docker run -p 6443:6443 k8s-emulator --scenario huge --headless
```

## Notes and limits

- Table (server-side print) responses are not implemented, so `kubectl get`
  shows only NAME/AGE. k9s renders columns client-side from typed objects and
  is unaffected; `kubectl get -o yaml|json` and `kubectl describe` work fine.
- DaemonSets, StatefulSets, Ingresses, RBAC, PVs and friends are advertised in
  discovery and served as empty collections, so the k9s views exist and
  navigate but contain nothing.
- Deleting a Deployment scales it to zero and lets the reconciler tear it
  down, so removing a 60,000-replica workload does not stall the API.
- There is no kubelet, scheduler, container runtime, networking or etcd, and
  no attempt at complete controller semantics — only enough to be convincing.
