package redisclusterbackup

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	BackupStatusRunning   = "running"
	BackupStatusFailed    = "failed"
	BackupStatusSucceeded = "succeeded"
	BackupStatusUnknown   = "unknown"
	BackupStatusIgnored   = "ignored"
)

var (
	ClusterBackupStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_cluster_backup_status",
			Help: "Redis cluster backup status (0=not present, 1=present)",
		},
		[]string{"namespace", "cluster_name", "backup_name", "status"},
	)
)

func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(ClusterBackupStatus)
}

func DeleteMetrics(ns string, clusterName string) {
	ClusterBackupStatus.Delete(
		prometheus.Labels{
			"namespace":    ns,
			"cluster_name": clusterName,
			// "backup_name":  "*",
			// "status":       "*",
		},
	)
}

func SetBackupStatus(ns string, clusterName string, backupName string, status string) {
	for _, s := range []string{BackupStatusRunning, BackupStatusSucceeded, BackupStatusFailed, BackupStatusIgnored} {
		ClusterBackupStatus.WithLabelValues(ns, clusterName, backupName, s).Set(0)
	}

	ClusterBackupStatus.WithLabelValues(ns, clusterName, backupName, status).Set(1)
}
