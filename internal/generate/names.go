package generate

import (
	"math/rand"
	"strconv"
	"strings"
)

var appNames = []string{
	"frontend", "checkout", "payments", "catalog", "search", "recommender",
	"auth", "session", "cart", "orders", "shipping", "inventory", "pricing",
	"notifications", "email-worker", "sms-gateway", "webhooks", "scheduler",
	"reporting", "analytics", "etl", "ingestion", "streaming", "aggregator",
	"gateway", "proxy", "router", "cache", "queue-consumer", "queue-producer",
	"billing", "invoicing", "ledger", "reconciliation", "fraud-detection",
	"risk-engine", "kyc", "onboarding", "profile", "preferences", "feature-flags",
	"config-service", "audit-log", "metrics-relay", "trace-collector", "log-shipper",
	"image-resizer", "pdf-renderer", "media-encoder", "thumbnailer", "cdn-purge",
	"admin-api", "public-api", "graphql-gateway", "grpc-bridge", "websocket-hub",
	"cron-runner", "batch-processor", "data-export", "data-import", "backup-agent",
}

var componentByApp = map[string]string{
	"frontend": "web", "gateway": "web", "proxy": "web", "public-api": "api",
	"admin-api": "api", "graphql-gateway": "api", "grpc-bridge": "api",
	"cache": "cache", "queue-consumer": "worker", "queue-producer": "worker",
	"etl": "worker", "batch-processor": "worker", "cron-runner": "worker",
}

var images = []string{
	"registry.internal/nginx:1.25.4",
	"registry.internal/envoyproxy/envoy:v1.29.1",
	"registry.internal/redis:7.2-alpine",
	"registry.internal/postgres:16.2",
	"registry.internal/node:20.11-alpine",
	"registry.internal/python:3.12-slim",
	"registry.internal/golang-service:1.22.1",
	"registry.internal/openjdk:21-jre-slim",
	"registry.internal/dotnet/aspnet:8.0",
	"registry.internal/rust-service:1.76",
	"registry.internal/busybox:1.36",
	"registry.internal/otel/opentelemetry-collector:0.96.0",
	"registry.internal/fluent/fluent-bit:2.2.2",
	"registry.internal/grafana/agent:v0.40.2",
}

var sidecarImages = []string{
	"registry.internal/istio/proxyv2:1.21.0",
	"registry.internal/otel/opentelemetry-collector:0.96.0",
	"registry.internal/fluent/fluent-bit:2.2.2",
	"registry.internal/linkerd/proxy:stable-2.14.10",
}

var envNames = []string{"prod", "staging", "dev", "qa", "sandbox"}
var teamNames = []string{
	"platform", "payments", "growth", "data", "identity", "commerce",
	"infra", "sre", "mobile", "web", "ml", "security", "billing", "support",
}
var zones = []string{"eu-west-1a", "eu-west-1b", "eu-west-1c"}
var instanceTypes = []string{"m6i.xlarge", "m6i.2xlarge", "c6i.4xlarge", "r6i.2xlarge", "m7g.4xlarge"}
var nodePools = []string{"general", "compute", "memory", "spot", "system"}

const b36 = "0123456789abcdefghijklmnopqrstuvwxyz"

// podSuffix maps a monotonic ordinal to a unique 5-character suffix that
// still looks like the random string a real ReplicaSet controller produces.
// Multiplying by a constant coprime to 36^5 keeps the mapping bijective.
func podSuffix(ordinal uint32) string {
	const modulus = 60466176 // 36^5
	v := (uint64(ordinal) * 2654435761) % modulus
	var b [5]byte
	for i := 4; i >= 0; i-- {
		b[i] = b36[v%36]
		v /= 36
	}
	return string(b[:])
}

// hashChars matches the alphabet Kubernetes uses for pod-template-hash.
const hashChars = "bcdfghjklmnpqrstvwxz2456789"

func templateHash(rnd *rand.Rand) string {
	var b [9]byte
	for i := range b {
		b[i] = hashChars[rnd.Intn(len(hashChars))]
	}
	return string(b[:])
}

func randomHex(rnd *rand.Rand, n int) string {
	const hx = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = hx[rnd.Intn(16)]
	}
	return string(b)
}

func filler(rnd *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte(alphabet[rnd.Intn(len(alphabet))])
	}
	return sb.String()
}

func pick[T any](rnd *rand.Rand, xs []T) T { return xs[rnd.Intn(len(xs))] }

func intBetween(rnd *rand.Rand, lo, hi int) int {
	if hi <= lo {
		return lo
	}
	return lo + rnd.Intn(hi-lo+1)
}

func pad(prefix string, n, width int) string {
	s := strconv.Itoa(n)
	for len(s) < width {
		s = "0" + s
	}
	return prefix + s
}
