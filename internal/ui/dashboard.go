// Package ui renders the live dashboard. It replaces the previous approach
// of printing metrics to stdout, which fought with the terminal and made the
// emulator unusable next to a k9s session.
package ui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/danske-spil/k8s-cluster-emulator/internal/cluster"
	"github.com/danske-spil/k8s-cluster-emulator/internal/config"
	"github.com/danske-spil/k8s-cluster-emulator/internal/metrics"
	"github.com/danske-spil/k8s-cluster-emulator/internal/simulate"
)

const refresh = 500 * time.Millisecond

var (
	colAccent = lipgloss.Color("39")
	colDim    = lipgloss.Color("244")
	colGood   = lipgloss.Color("42")
	colWarn   = lipgloss.Color("214")
	colBad    = lipgloss.Color("203")
	colValue  = lipgloss.Color("255")

	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("231")).Background(colAccent).Padding(0, 1)
	tabActive   = lipgloss.NewStyle().Bold(true).Foreground(colAccent)
	tabIdle     = lipgloss.NewStyle().Foreground(colDim)
	sectionName = lipgloss.NewStyle().Bold(true).Foreground(colAccent)
	labelStyle  = lipgloss.NewStyle().Foreground(colDim)
	valueStyle  = lipgloss.NewStyle().Foreground(colValue)
	goodStyle   = lipgloss.NewStyle().Foreground(colGood)
	warnStyle   = lipgloss.NewStyle().Foreground(colWarn)
	badStyle    = lipgloss.NewStyle().Foreground(colBad)
	boxStyle    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colDim).Padding(0, 1)
	footerStyle = lipgloss.NewStyle().Foreground(colDim)
)

type tickMsg time.Time

type sample struct {
	at          time.Time
	watchEvents int64
	created     int64
	deleted     int64
	transitions int64
	events      int64
	requests    int64
}

// Model is the Bubble Tea dashboard.
type Model struct {
	cfg  config.Scenario
	c    *cluster.Cluster
	eng  *simulate.Engine
	m    *metrics.Collector
	addr string

	snap  metrics.Snapshot
	stats cluster.Stats
	prev  sample
	rates struct {
		watchEvents float64
		created     float64
		deleted     float64
		transitions float64
		events      float64
	}

	width, height int
	tab           int
	paused        bool
}

func NewModel(cfg config.Scenario, c *cluster.Cluster, eng *simulate.Engine, m *metrics.Collector, addr string) *Model {
	return &Model{cfg: cfg, c: c, eng: eng, m: m, addr: addr, width: 100, height: 40}
}

func (m *Model) Init() tea.Cmd {
	return tea.Batch(tick(), func() tea.Msg { return tickMsg(time.Now()) })
}

func tick() tea.Cmd {
	return tea.Tick(refresh, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			return m, tea.Quit
		case "tab", "right", "l":
			m.tab = (m.tab + 1) % 3
		case "shift+tab", "left", "h":
			m.tab = (m.tab + 2) % 3
		case "1":
			m.tab = 0
		case "2":
			m.tab = 1
		case "3":
			m.tab = 2
		case " ", "p":
			m.paused = !m.paused
		}
	case tickMsg:
		if !m.paused {
			m.refresh()
		}
		return m, tick()
	}
	return m, nil
}

func (m *Model) refresh() {
	m.snap = m.m.Snapshot(12, 24)
	m.stats = m.c.Stats()

	cur := sample{
		at:          time.Now(),
		created:     m.stats.Created,
		deleted:     m.stats.Deleted,
		transitions: m.eng.Transitions.Load(),
		events:      m.eng.EventsMade.Load(),
		requests:    m.snap.Total,
	}
	for _, v := range m.c.Views() {
		cur.watchEvents += v.Hub.Sent.Load()
	}
	if !m.prev.at.IsZero() {
		// Ignore very short intervals; they turn startup counters into
		// nonsense rates.
		if dt := cur.at.Sub(m.prev.at).Seconds(); dt > 0.2 {
			m.rates.watchEvents = float64(cur.watchEvents-m.prev.watchEvents) / dt
			m.rates.created = float64(cur.created-m.prev.created) / dt
			m.rates.deleted = float64(cur.deleted-m.prev.deleted) / dt
			m.rates.transitions = float64(cur.transitions-m.prev.transitions) / dt
			m.rates.events = float64(cur.events-m.prev.events) / dt
		}
	}
	m.prev = cur
}

// ---------------------------------------------------------------------------
// View
// ---------------------------------------------------------------------------

func (m *Model) View() string {
	var b strings.Builder
	b.WriteString(m.header())
	b.WriteString("\n")
	switch m.tab {
	case 0:
		b.WriteString(m.overview())
	case 1:
		b.WriteString(m.endpoints())
	case 2:
		b.WriteString(m.requests())
	}
	b.WriteString("\n")
	b.WriteString(m.footer())
	return b.String()
}

func (m *Model) header() string {
	title := titleStyle.Render("k8s cluster emulator")
	sub := labelStyle.Render(fmt.Sprintf(" %s  ·  scenario %s  ·  seed %d  ·  up %s",
		m.addr, m.cfg.Name, m.cfg.Seed, m.snap.Uptime.Truncate(time.Second)))

	tabs := []string{"overview", "endpoints", "requests"}
	rendered := make([]string, len(tabs))
	for i, t := range tabs {
		label := fmt.Sprintf("%d %s", i+1, t)
		if i == m.tab {
			rendered[i] = tabActive.Render("▎" + label)
		} else {
			rendered[i] = tabIdle.Render(" " + label)
		}
	}
	line := title + sub
	if m.paused {
		line += warnStyle.Render("  [paused]")
	}
	return line + "\n" + strings.Join(rendered, "  ")
}

func (m *Model) footer() string {
	return footerStyle.Render("tab/1-3 switch view · p pause · q quit")
}

// ---------------------------------------------------------------------------
// Overview
// ---------------------------------------------------------------------------

func (m *Model) overview() string {
	left := m.clusterBox()
	mid := m.podBox()
	right := m.apiBox()

	top := lipgloss.JoinHorizontal(lipgloss.Top, left, mid, right)
	bottom := lipgloss.JoinHorizontal(lipgloss.Top, m.activityBox(), m.runtimeBox())
	return top + "\n" + bottom
}

func kv(label, value string) string {
	return labelStyle.Render(fmt.Sprintf("%-14s", label)) + valueStyle.Render(value)
}

func kvc(label, value string, style lipgloss.Style) string {
	return labelStyle.Render(fmt.Sprintf("%-14s", label)) + style.Render(value)
}

func (m *Model) clusterBox() string {
	s := m.stats
	lines := []string{
		sectionName.Render("cluster"),
		kv("namespaces", num(s.Namespaces)),
		kv("nodes", num(s.Nodes)),
		kv("deployments", num(s.Deployments)),
		kv("replicasets", num(s.ReplicaSets)),
		kv("services", num(s.Services)),
		kv("configmaps", num(s.ConfigMaps)),
		kv("secrets", num(s.Secrets)),
		kv("jobs", num(s.Jobs+s.CronJobs)),
		kv("events", num(s.Events)),
		kv("resourceVer", strconv.FormatUint(s.RV, 10)),
	}
	return boxStyle.Width(30).Render(strings.Join(lines, "\n"))
}

func (m *Model) podBox() string {
	s := m.stats
	get := func(st cluster.PodState) int64 { return s.PodStates[st] }
	lines := []string{
		sectionName.Render("pods"),
		kv("total", num(s.Pods)),
		kvc("running", num64(get(cluster.StRunning)), goodStyle),
		kvc("pending", num64(get(cluster.StPending)), warnStyle),
		kvc("creating", num64(get(cluster.StContainerCreating)), warnStyle),
		kvc("crashloop", num64(get(cluster.StCrashLoopBackOff)), badStyle),
		kvc("imagepull", num64(get(cluster.StImagePullBackOff)+get(cluster.StErrImagePull)), badStyle),
		kvc("failed", num64(get(cluster.StError)+get(cluster.StEvicted)), badStyle),
		kv("succeeded", num64(get(cluster.StSucceeded))),
		kvc("terminating", num64(s.Terminating), warnStyle),
	}
	return boxStyle.Width(30).Render(strings.Join(lines, "\n"))
}

func (m *Model) apiBox() string {
	s := m.snap
	errStyle := goodStyle
	if s.Errors > 0 {
		errStyle = badStyle
	}
	lines := []string{
		sectionName.Render("api"),
		kv("requests/sec", fmt.Sprintf("%.1f", s.RPS)),
		kv("total", num64(s.Total)),
		kv("list", num64(s.Lists)),
		kv("get", num64(s.Gets)),
		kv("write", num64(s.Writes)),
		kvc("watches live", num64(s.WatchesLive), goodStyle),
		kv("watch total", num64(s.WatchesTotal)),
		kvc("errors", num64(s.Errors), errStyle),
		kvc("410 gone", num64(s.Gone410), warnStyle),
		kv("served", bytesHuman(s.Bytes)),
	}
	return boxStyle.Width(30).Render(strings.Join(lines, "\n"))
}

func (m *Model) activityBox() string {
	s := m.snap
	lines := []string{
		sectionName.Render("latency & activity"),
		kv("p50", fmt.Sprintf("%.1f ms", s.P50)) + "   " + kv("p95", fmt.Sprintf("%.1f ms", s.P95)),
		kv("p99", fmt.Sprintf("%.1f ms", s.P99)) + "   " + kv("max", fmt.Sprintf("%.1f ms", s.Max)),
		kv("watch ev/sec", fmt.Sprintf("%.0f", m.rates.watchEvents)) + "   " + kv("items sent", num64(s.Items)),
		kv("pods +/sec", fmt.Sprintf("%.1f", m.rates.created)) + "   " + kv("pods -/sec", fmt.Sprintf("%.1f", m.rates.deleted)),
		kv("transitions/s", fmt.Sprintf("%.0f", m.rates.transitions)) + "   " + kv("events/sec", fmt.Sprintf("%.1f", m.rates.events)),
	}
	return boxStyle.Width(52).Render(strings.Join(lines, "\n"))
}

func (m *Model) runtimeBox() string {
	s := m.snap
	lines := []string{
		sectionName.Render("runtime"),
		kv("heap", fmt.Sprintf("%.0f MB", s.HeapMB)) + "   " + kv("sys", fmt.Sprintf("%.0f MB", s.SysMB)),
		kv("cpu", fmt.Sprintf("%.2f cores", s.CPUCores)) + "   " + kv("goroutines", num(s.Goroutines)),
		kv("gc pause", s.GCPause.Truncate(time.Microsecond).String()) + "   " + kv("chaos 5xx", num64(s.Chaos500)),
		kv("chaos 429", num64(s.Chaos429)) + "   " + kv("pods created", num64(m.stats.Created)),
	}
	return boxStyle.Width(52).Render(strings.Join(lines, "\n"))
}

// ---------------------------------------------------------------------------
// Endpoints tab
// ---------------------------------------------------------------------------

func (m *Model) endpoints() string {
	rows := m.snap.HotPaths
	sort.Slice(rows, func(i, j int) bool { return rows[i].TotalMs > rows[j].TotalMs })

	var b strings.Builder
	b.WriteString(sectionName.Render("hottest endpoints by total time") + "\n")
	b.WriteString(labelStyle.Render(fmt.Sprintf("%-46s %8s %8s %9s %9s %10s %8s",
		"ENDPOINT", "CALLS", "ERRS", "AVG ms", "MAX ms", "BYTES", "ITEMS")) + "\n")
	for _, p := range rows {
		avg := 0.0
		if p.Count > 0 {
			avg = p.TotalMs / float64(p.Count)
		}
		errCol := valueStyle
		if p.Errors > 0 {
			errCol = badStyle
		}
		b.WriteString(fmt.Sprintf("%-46s %8s %s %9.1f %9.1f %10s %8s\n",
			truncate(p.Path, 46), num64(p.Count),
			errCol.Render(fmt.Sprintf("%8s", num64(p.Errors))),
			avg, p.MaxMs, bytesHuman(p.Bytes), num64(p.Items)))
	}
	if len(rows) == 0 {
		b.WriteString(labelStyle.Render("  no requests yet — point k9s at the emulator\n"))
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Requests tab
// ---------------------------------------------------------------------------

func (m *Model) requests() string {
	var b strings.Builder
	b.WriteString(sectionName.Render("recent requests") + "\n")
	b.WriteString(labelStyle.Render(fmt.Sprintf("%-12s %-5s %-6s %-52s %9s %9s %7s",
		"TIME", "CODE", "TYPE", "PATH", "TOOK", "BYTES", "ITEMS")) + "\n")

	limit := m.height - 12
	if limit < 5 {
		limit = 5
	}
	for i, r := range m.snap.Recent {
		if i >= limit {
			break
		}
		style := goodStyle
		switch {
		case r.Status >= 500:
			style = badStyle
		case r.Status >= 400:
			style = warnStyle
		}
		b.WriteString(fmt.Sprintf("%-12s %s %-6s %-52s %9s %9s %7s\n",
			r.Time.Format("15:04:05.00"),
			style.Render(fmt.Sprintf("%-5d", r.Status)),
			r.Kind.String(),
			truncate(r.Path, 52),
			fmt.Sprintf("%.1fms", float64(r.Duration.Microseconds())/1000),
			bytesHuman(r.Bytes),
			num(r.Items)))
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

func num(v int) string { return num64(int64(v)) }

func num64(v int64) string {
	s := strconv.FormatInt(v, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

func bytesHuman(b int64) string {
	const unit = 1024
	if b < unit {
		return strconv.FormatInt(b, 10) + " B"
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTP"[exp])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}
