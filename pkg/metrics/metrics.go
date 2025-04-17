package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

func RegisterMetrics() {
	prometheus.MustRegister(
		ClusterBackupInfo,
	)
}
