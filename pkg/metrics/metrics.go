package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

func RegisterAllMetrics() *prometheus.Registry {
	registry := prometheus.NewRegistry()

	registry.MustRegister(
		ClusterBackupInfo,
	)

	return registry
}
