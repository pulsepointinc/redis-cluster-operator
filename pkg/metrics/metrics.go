package metrics

import (
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	prometheusMetricsServerReadTimeout    = 8 * time.Second
	prometheusMetricsServerWriteTimeout   = 8 * time.Second
	prometheusMetricsServerMaxHeaderBytes = 1 << 20 // 1 MiB
)

// Metrics is designed to be a shared object for updating the metrics exported
// by the Redis Cluster Operator.
type Metrics struct {
	registry *prometheus.Registry

	redisCLusterBackupStatus *prometheus.GaugeVec
}

// Creates a new Metrics instance.
func New() *Metrics {
	var (
		redisClusterBackupStatus = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "redis_cluster_backup_status",
				Help: "Redis cluster backup status (0=not present, 1=present)",
			},
			[]string{"name", "namespace", "cluster", "status"},
		)
	)

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)

	return &Metrics{
		registry:                 registry,
		redisCLusterBackupStatus: redisClusterBackupStatus,
	}
}

// Creates a new HTTP server to export the metrics.
func (m *Metrics) NewServer(ln net.Listener) *http.Server {
	m.registry.MustRegister(m.redisCLusterBackupStatus)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))

	return &http.Server{
		Addr:           ln.Addr().String(),
		ReadTimeout:    prometheusMetricsServerReadTimeout,
		WriteTimeout:   prometheusMetricsServerWriteTimeout,
		MaxHeaderBytes: prometheusMetricsServerMaxHeaderBytes,
		Handler:        mux,
	}
}
