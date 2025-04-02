package v1alpha1

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	minMasterSize       = 3
	minClusterReplicas  = 1
	defaultRedisImage   = "redis:5.0.4-alpine"
	defaultMonitorImage = "oliver006/redis_exporter:latest"
)

func (in *DistributedRedisCluster) DefaultSpec(log logr.Logger) bool {
	update := false
	if in.Spec.MasterSize < minMasterSize {
		in.Spec.MasterSize = minMasterSize
		update = true
	}

	if in.Spec.Image == "" {
		in.Spec.Image = defaultRedisImage
		update = true
	}

	if in.Spec.ServiceName == "" {
		in.Spec.ServiceName = in.Name
		update = true
	}

	if in.Spec.Resources == nil || in.Spec.Resources.Size() == 0 {
		in.Spec.Resources = defaultResource()
		update = true
	}

	mon := in.Spec.Monitor
	if mon != nil {
		if mon.Image == "" {
			mon.Image = defaultMonitorImage
			update = true
		}

		if mon.Prometheus == nil {
			mon.Prometheus = &PrometheusSpec{}
			update = true
		}
		if mon.Prometheus.Port == 0 {
			mon.Prometheus.Port = PrometheusExporterPortNumber
			update = true
		}
		if in.Spec.Annotations == nil {
			in.Spec.Annotations = make(map[string]string)
			update = true
		}

		in.Spec.Annotations["prometheus.io/scrape"] = "true"
		in.Spec.Annotations["prometheus.io/path"] = PrometheusExporterTelemetryPath
		in.Spec.Annotations["prometheus.io/port"] = fmt.Sprintf("%d", mon.Prometheus.Port)
	}
	return update
}

func (in *DistributedRedisCluster) IsRestoreFromBackup() bool {
	initSpec := in.Spec.Init
	if initSpec != nil && initSpec.BackupSource != nil {
		return true
	}
	return false
}

func (in *DistributedRedisCluster) IsRestored() bool {
	return in.Status.Restore.Phase == RestorePhaseSucceeded
}

func (in *DistributedRedisCluster) ShouldInitRestorePhase() bool {
	return in.Status.Restore.Phase == ""
}

func (in *DistributedRedisCluster) IsRestoreRunning() bool {
	return in.Status.Restore.Phase == RestorePhaseRunning
}

func (in *DistributedRedisCluster) IsRestoreRestarting() bool {
	return in.Status.Restore.Phase == RestorePhaseRestart
}

func (in *DistributedRedisCluster) HasReplicas() bool {
	return in.Spec.ClusterReplicas > 0
}

func defaultResource() *v1.ResourceRequirements {
	return &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("200m"),
			v1.ResourceMemory: resource.MustParse("2Gi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000m"),
			v1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
}

func DefaultOwnerReferences(cluster *DistributedRedisCluster) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		*metav1.NewControllerRef(cluster, schema.GroupVersionKind{
			Group:   SchemeGroupVersion.Group,
			Version: SchemeGroupVersion.Version,
			Kind:    DistributedRedisClusterKind,
		}),
	}
}

// ValidateBackupSource validates that the BackupSource field has a valid value
func (in *RedisClusterBackup) ValidateBackupSource() error {
	//if in.Spec.BackupSource == "" {
	//	// Empty source is fine - will be determined at runtime
	//	return nil
	//}
	//
	//if in.Spec.BackupSource != string(BackupSourceMasters) && in.Spec.BackupSource != string(BackupSourceReplicas) {
	//	return fmt.Errorf("invalid backup source: %s. Must be either '%s' or '%s'",
	//		in.Spec.BackupSource, BackupSourceMasters, BackupSourceReplicas)
	//}

	return nil
}

// DetermineBackupSource determines the appropriate backup source based on the spec and cluster
func (in *RedisClusterBackup) DetermineBackupSource(cluster *DistributedRedisCluster) string {
	//// If explicitly set, use the specified source
	//if in.Spec.BackupSource != "" {
	//	return in.Spec.BackupSource
	//}
	//
	//// Otherwise, use replicas if available
	//if cluster != nil && cluster.Spec.ClusterReplicas > 0 {
	//	return string(BackupSourceReplicas)
	//}
	//
	//// Default to masters if no replicas or cluster info not available
	//return string(BackupSourceMasters)
	return "replicas"
}

func (in *RedisClusterBackup) Validate() error {
	clusterName := in.Spec.RedisClusterName
	if clusterName == "" {
		return fmt.Errorf("backup [RedisClusterName] is missing")
	}

	// Validate BackupSource field if present
	if err := in.ValidateBackupSource(); err != nil {
		return err
	}

	// BucketName can't be empty
	if in.Spec.S3 == nil && in.Spec.GCS == nil && in.Spec.Azure == nil && in.Spec.Swift == nil && in.Spec.Local == nil {
		return fmt.Errorf("no storage provider is configured")
	}

	if in.Spec.Azure != nil || in.Spec.Swift != nil {
		if in.Spec.StorageSecretName == "" {
			return fmt.Errorf("backup [SecretName] is missing")
		}
	}

	return nil
}

func (in *RedisClusterBackup) RemotePath() (string, error) {
	spec := in.Spec.Backend
	timePrefix := in.Status.StartTime.Format("20060102150405")

	// Get the base path
	var basePath string
	if spec.S3 != nil {
		basePath = filepath.Join(spec.S3.Prefix, DatabaseNamePrefix, in.Namespace, in.Spec.RedisClusterName, timePrefix)
	} else if spec.GCS != nil {
		basePath = filepath.Join(spec.GCS.Prefix, DatabaseNamePrefix, in.Namespace, in.Spec.RedisClusterName, timePrefix)
	} else if spec.Azure != nil {
		basePath = filepath.Join(spec.Azure.Prefix, DatabaseNamePrefix, in.Namespace, in.Spec.RedisClusterName, timePrefix)
	} else if spec.Local != nil {
		basePath = filepath.Join(DatabaseNamePrefix, in.Namespace, in.Spec.RedisClusterName, timePrefix)
	} else if spec.Swift != nil {
		basePath = filepath.Join(spec.Swift.Prefix, DatabaseNamePrefix, in.Namespace, in.Spec.RedisClusterName, timePrefix)
	} else {
		return "", fmt.Errorf("no storage provider is configured")
	}

	return basePath, nil
}

func (in *RedisClusterBackup) RCloneSecretName() string {
	return fmt.Sprintf("rcloneconfig-%v", in.Name)
}

func (in *RedisClusterBackup) JobName() string {
	return fmt.Sprintf("redisbackup-%v", in.Name)
}

func (in *RedisClusterBackup) IsRefLocalPVC() bool {
	return in.Spec.Local != nil && in.Spec.Local.PersistentVolumeClaim != nil
}

// Helper functions for working with backup sources

// IsBackupFromReplicas checks if this backup is/will be from replicas
func (in *RedisClusterBackup) IsBackupFromReplicas() bool {
	return true
	//return in.Spec.BackupSource == string(BackupSourceReplicas) ||
	//	(in.Status.BackupSource != "" && in.Status.BackupSource == string(BackupSourceReplicas))
}

// IsBackupFromMasters checks if this backup is/will be from masters
func (in *RedisClusterBackup) IsBackupFromMasters() bool {
	return false
	//return in.Spec.BackupSource == string(BackupSourceMasters) ||
	//	(in.Status.BackupSource != "" && in.Status.BackupSource == string(BackupSourceMasters)) ||
	//	(in.Spec.BackupSource == "" && in.Status.BackupSource == "") // Default is masters
}

// GetBackupSourceLabel returns a label value for the backup source
func (in *RedisClusterBackup) GetBackupSourceLabel() string {
	//if in.Spec.BackupSource != "" {
	//	return in.Spec.BackupSource
	//}
	//if in.Status.BackupSource != "" {
	//	return in.Status.BackupSource
	//}
	//return string(BackupSourceMasters) // Default
	return "replicas"
}

// CreateReplicaBackupScript generates a shell script for backing up from a Redis replica
func CreateReplicaBackupScript(redisIP string, dataDir string) string {
	return strings.TrimSpace(`
#!/bin/bash
set -e

# Create backup directory
mkdir -p ` + dataDir + `

# Use redis-cli to create a dump.rdb file from the replica
redis-cli -h ` + redisIP + ` --rdb ` + dataDir + `/dump.rdb

# Copy configuration file if it exists
if [ -f /var/lib/redis-replica-data/redis.conf ]; then
  cp /var/lib/redis-replica-data/redis.conf ` + dataDir + `/
fi

# Copy any other important files
if [ -d /var/lib/redis-replica-data/appendonly ]; then
  cp -r /var/lib/redis-replica-data/appendonly ` + dataDir + `/
fi

echo "Backup from replica completed successfully"
`)
}

// GetReplicaBackupCommand returns the command to execute the replica backup
func GetReplicaBackupCommand(redisIP string, replicaData string, dataDir string, withPassword bool) []string {
	var cmd []string

	if withPassword {
		cmd = []string{
			"/bin/bash",
			"-c",
			fmt.Sprintf("redis-cli -h %s -a \"${REDIS_PASSWORD}\" --rdb %s/dump.rdb && "+
				"if [ -d \"%s\" ]; then cp -r %s/* %s/ || true; fi",
				redisIP, dataDir, replicaData, replicaData, dataDir),
		}
	} else {
		cmd = []string{
			"/bin/bash",
			"-c",
			fmt.Sprintf("redis-cli -h %s --rdb %s/dump.rdb && "+
				"if [ -d \"%s\" ]; then cp -r %s/* %s/ || true; fi",
				redisIP, dataDir, replicaData, replicaData, dataDir),
		}
	}

	return cmd
}
