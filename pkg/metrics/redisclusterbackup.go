package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
)

const (
	BackupStatusRunning   = "running"
	BackupStatusFailed    = "failed"
	BackupStatusSucceeded = "succeeded"
	BackupStatusIgnored   = "ignored"
	BackupStatusUnknown   = "unknown"
)

// Updates the given RedisClusterBackup's metrics.
func (m *Metrics) UpdateBackup(backup *redisv1alpha1.RedisClusterBackup) {
	m.UpdateRedisClusterBackupStatus(backup)
}

// Updates the given RedisClusterBackup's status metrics.
func (m *Metrics) UpdateRedisClusterBackupStatus(backup *redisv1alpha1.RedisClusterBackup) {
	m.resetRedisClusterBackupStatus(backup)
	m.redisCLusterBackupStatus.WithLabelValues(
		backup.Name,
		backup.Namespace,
		backup.ClusterName,
		getBackupStatusByPhase(backup.Status.Phase),
	).Set(1)
}

// Removes the given RedisClusterBackup's metrics.
func (m *Metrics) RemoveBackup(key types.NamespacedName) {
	m.redisCLusterBackupStatus.DeletePartialMatch(prometheus.Labels{"name": key.Name, "namespace": key.Namespace})
}

// Reset the given RedisClusterBackup's statuses to 0.
func (m *Metrics) resetRedisClusterBackupStatus(backup *redisv1alpha1.RedisClusterBackup) {
	var (
		backupStatuses = []string{
			BackupStatusRunning,
			BackupStatusFailed,
			BackupStatusSucceeded,
			BackupStatusIgnored,
			BackupStatusUnknown,
		}
	)

	for _, status := range backupStatuses {
		m.redisCLusterBackupStatus.WithLabelValues(backup.Name, backup.Namespace, backup.ClusterName, status).Set(0)
	}
}

// Maps the BackupPhase to a string status.
func getBackupStatusByPhase(phase redisv1alpha1.BackupPhase) string {
	backupPhaseToStatusMap := map[redisv1alpha1.BackupPhase]string{
		redisv1alpha1.BackupPhaseRunning:   BackupStatusRunning,
		redisv1alpha1.BackupPhaseSucceeded: BackupStatusSucceeded,
		redisv1alpha1.BackupPhaseFailed:    BackupStatusFailed,
		redisv1alpha1.BackupPhaseIgnored:   BackupStatusIgnored,
	}

	if status, exists := backupPhaseToStatusMap[phase]; exists {
		return status
	}
	return BackupStatusUnknown
}
