package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ClusterBackupInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_backup_info",
			Help: "Information about a Redis cluster backup",
		},
		[]string{"namespace", "name", "phase"},
	)
)
