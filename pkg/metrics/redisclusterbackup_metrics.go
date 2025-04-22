package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ClusterBackupInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_backup_status",
			Help: "Redis cluster backup status",
		},
		[]string{"namespace", "cluster_name", "backup_name", "status"},
	)
)
