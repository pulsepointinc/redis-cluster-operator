package v1alpha1

import (
	"fmt"
	"github.com/go-logr/logr"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"path/filepath"
)

const (
	minMasterSize       = 3
	minClusterReplicas  = 1
	defaultRedisImage   = "redis:7.2.3-alpine"
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
	if in.Spec.BackupSource == "" {
		// Empty source is fine - will be determined at runtime
		return nil
	}

	if in.Spec.BackupSource != BackupSourceMasters && in.Spec.BackupSource != BackupSourceReplicas {
		return fmt.Errorf("invalid backup source: %s. Must be either '%s' or '%s'",
			in.Spec.BackupSource, BackupSourceMasters, BackupSourceReplicas)
	}

	return nil
}

// DetermineBackupSource determines the appropriate backup source based on the spec and cluster
func (in *RedisClusterBackup) DetermineBackupSource(cluster *DistributedRedisCluster) string {
	// If explicitly set, use the specified source
	if in.Spec.BackupSource != "" {
		return in.Spec.BackupSource
	}

	// Otherwise, use replicas if available
	if cluster != nil && cluster.Spec.ClusterReplicas > 0 {
		return BackupSourceReplicas
	}

	// Default to replicas if no replicas or cluster info not available
	return BackupSourceReplicas
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
	return in.Spec.BackupSource == BackupSourceReplicas || in.Spec.BackupSource == ""
}

// IsBackupFromMasters checks if this backup is/will be from masters
func (in *RedisClusterBackup) IsBackupFromMasters() bool {
	return in.Spec.BackupSource == BackupSourceMasters
}
