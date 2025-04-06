package redisclusterbackup

import (
	"fmt"
	"github.com/go-logr/logr"
	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/event"
	"github.com/ucloud/redis-cluster-operator/pkg/k8sutil"
	"github.com/ucloud/redis-cluster-operator/pkg/osm"
	"github.com/ucloud/redis-cluster-operator/pkg/utils"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//Error Handling in Job Completion Logic: In handleBackupJobs, the function successfully marks backups as completed or failed, but doesn't properly handle partial failures. If some but not all jobs fail, it's marked as "Partially successful" but still sets the phase to BackupPhaseSucceeded. This could be misleading.

import (
	"context"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"os"
)

func (r *ReconcileRedisClusterBackup) create(reqLogger logr.Logger, backup *redisv1alpha1.RedisClusterBackup) error {
	if backup.Status.StartTime == nil {
		t := metav1.Now()
		backup.Status.StartTime = &t
		if err := r.crController.UpdateCRStatus(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}
	}

	// Do not process "completed", aka "failed" or "succeeded" or "ignored", backups.
	if backup.Status.Phase == redisv1alpha1.BackupPhaseFailed ||
		backup.Status.Phase == redisv1alpha1.BackupPhaseSucceeded ||
		backup.Status.Phase == redisv1alpha1.BackupPhaseIgnored {
		delete(backup.GetLabels(), redisv1alpha1.LabelBackupStatus)
		if err := r.crController.UpdateCR(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}
		return nil
	}

	if err := r.ValidateBackup(backup); err != nil {
		if k8sutil.IsRequestRetryable(err) {
			return err
		}
		r.markAsFailedBackup(backup, err.Error())
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupFailed,
			err.Error(),
		)
		return nil // stop retry
	}

	running, err := r.isBackupRunning(backup)
	if err != nil {
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupError,
			err.Error(),
		)
		return err
	}
	if running {
		return r.handleBackupJobs(reqLogger, backup)
	}

	cluster, err := r.crController.GetDistributedRedisCluster(backup.Namespace, backup.Spec.RedisClusterName)
	if err != nil {
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupError,
			err.Error(),
		)
		return err
	}

	secret, err := osm.NewRcloneSecret(r.client, backup.RCloneSecretName(), backup.Namespace, backup.Spec.Backend, []metav1.OwnerReference{
		{
			APIVersion: redisv1alpha1.SchemeGroupVersion.String(),
			Kind:       redisv1alpha1.RedisClusterBackupKind,
			Name:       backup.Name,
			UID:        backup.UID,
		},
	})
	if err != nil {
		msg := fmt.Sprintf("Failed to generate rclone secret. Reason: %v", err)
		r.markAsFailedBackup(backup, msg)
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupFailed,
			msg,
		)
		return nil // don't retry
	}

	if err := k8sutil.CreateSecret(r.client, secret, reqLogger); err != nil {
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupError,
			err.Error(),
		)
		return err
	}

	if backup.Spec.Local == nil {
		if err := osm.CheckBucketAccess(r.client, backup.Spec.Backend, backup.Namespace); err != nil {
			r.markAsFailedBackup(backup, err.Error())
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupFailed,
				err.Error(),
			)
			return nil
		}
	}

	// Create the backup jobs based on the backup source
	var nodesInfo []NodeInfo
	var sourceType string

	if backup.IsBackupFromReplicas() {
		// Get replica node info if backing up from replicas
		nodesInfo, err = r.getReplicaNodesInfo(backup.Namespace, cluster)
		if err != nil {
			message := fmt.Sprintf("Failed to get replica nodes info: %v", err)
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				message,
			)
			return r.markAsFailedBackup(backup, message)
		}
		if len(nodesInfo) == 0 {
			message := "No replica nodes found for backup"
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				message,
			)
			return r.markAsFailedBackup(backup, message)
		}
		sourceType = "replica"
	} else {
		// Get master node info if backing up from masters
		nodesInfo, err = r.getMasterNodesInfo(backup.Namespace, cluster)
		if err != nil {
			message := fmt.Sprintf("Failed to get master nodes info: %v", err)
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				message,
			)
			return r.markAsFailedBackup(backup, message)
		}
		if len(nodesInfo) == 0 {
			message := "No master nodes found for backup"
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				message,
			)
			return r.markAsFailedBackup(backup, message)
		}
		sourceType = "master"
	}

	// Create the backup jobs
	if err := r.createBackupJobs(reqLogger, backup, cluster, nodesInfo); err != nil {
		message := fmt.Sprintf("Failed to create backup jobs: %v", err)
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupError,
			message,
		)
		if k8sutil.IsRequestRetryable(err) {
			return err
		}
		return r.markAsFailedBackup(backup, message)
	}

	backup.Status.Phase = redisv1alpha1.BackupPhaseRunning
	backup.Status.MasterSize = cluster.Spec.MasterSize
	backup.Status.ClusterReplicas = cluster.Spec.ClusterReplicas
	backup.Status.ClusterImage = cluster.Spec.Image
	if err := r.crController.UpdateCRStatus(backup); err != nil {
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupFailed,
			err.Error(),
		)
		return err
	}

	backup.Labels[redisv1alpha1.LabelClusterName] = backup.Spec.RedisClusterName
	backup.Labels[redisv1alpha1.LabelBackupStatus] = string(redisv1alpha1.BackupPhaseRunning)
	backup.Labels[redisv1alpha1.LabelBackupSource] = "replicas"
	if err := r.crController.UpdateCR(backup); err != nil {
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupError,
			err.Error(),
		)
		return err
	}

	reqLogger.Info(fmt.Sprintf("%s backup running", sourceType))
	r.recorder.Event(
		backup,
		corev1.EventTypeNormal,
		event.Starting,
		fmt.Sprintf("%s backup running", sourceType),
	)

	return nil
}

// NodeInfo contains information about a Redis node and its associated pod
type NodeInfo struct {
	IP           string
	NodeName     string
	PodName      string
	PvcName      string
	Role         string
	MasterRef    string
	StatefulSet  string
	Index        int
	HostDataPath string // Host path of Redis data
}

// getMasterNodesInfo gets information about master nodes, including their pods, PVCs and host paths
func (r *ReconcileRedisClusterBackup) getMasterNodesInfo(namespace string, cluster *redisv1alpha1.DistributedRedisCluster) ([]NodeInfo, error) {
	var masterNodes []NodeInfo

	// Get all pods for this Redis cluster
	podList := &corev1.PodList{}
	err := r.client.List(context.TODO(), podList,
		client.InNamespace(namespace),
		client.MatchingLabels{redisv1alpha1.LabelClusterName: cluster.Name})
	if err != nil {
		return nil, err
	}

	// Create a map of pod IP to pod details
	podMap := make(map[string]*corev1.Pod)
	for i := range podList.Items {
		pod := &podList.Items[i]
		podMap[pod.Status.PodIP] = pod
	}

	// Get all PVs to find host paths
	pvList := &corev1.PersistentVolumeList{}
	if err := r.client.List(context.TODO(), pvList); err != nil {
		return nil, fmt.Errorf("failed to list PVs: %v", err)
	}

	// Create a map of PVC name to PV
	pvcToPV := make(map[string]string)
	hostPathMap := make(map[string]string)

	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == namespace {
			pvcKey := fmt.Sprintf("%s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
			pvcToPV[pvcKey] = pv.Name

			// Extract host path information
			if pv.Spec.HostPath != nil {
				hostPathMap[pv.Name] = pv.Spec.HostPath.Path
			} else if pv.Spec.Local != nil {
				hostPathMap[pv.Name] = pv.Spec.Local.Path
			} else if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeAttributes != nil {
				// For CSI drivers, try to get path from volume attributes
				if path, exists := pv.Spec.CSI.VolumeAttributes["path"]; exists {
					hostPathMap[pv.Name] = path
				}
			}
		}
	}

	// Find master nodes and their associated pods
	index := 0
	for _, node := range cluster.Status.Nodes {
		if node.Role == redisv1alpha1.RedisClusterNodeRoleMaster {
			info := NodeInfo{
				IP:          node.IP,
				Role:        string(node.Role),
				MasterRef:   node.MasterRef,
				StatefulSet: node.StatefulSet,
				Index:       index,
			}

			// Find the pod for this master node
			if pod, exists := podMap[node.IP]; exists {
				info.NodeName = pod.Spec.NodeName
				info.PodName = pod.Name

				// Find the PVC used by this pod and its host path
				for _, volume := range pod.Spec.Volumes {
					if volume.PersistentVolumeClaim != nil {
						info.PvcName = volume.PersistentVolumeClaim.ClaimName

						// Try to get the host path
						pvcKey := fmt.Sprintf("%s/%s", namespace, info.PvcName)
						if pvName, exists := pvcToPV[pvcKey]; exists {
							if hostPath, exists := hostPathMap[pvName]; exists {
								info.HostDataPath = hostPath
							}
						}

						break
					} else if volume.HostPath != nil {
						// Handle case where pod directly uses hostPath volumes
						info.HostDataPath = volume.HostPath.Path
						break
					} else if volume.EmptyDir != nil {
						// Handle emptyDir case
						podUID := string(pod.UID)
						info.HostDataPath = fmt.Sprintf("/var/lib/kubelet/pods/%s/volumes/kubernetes.io~empty-dir/%s",
							podUID, volume.Name)
						break
					}
				}
			}

			masterNodes = append(masterNodes, info)
			index++
		}
	}

	return masterNodes, nil
}

// getReplicaNodesInfo gets information about replica nodes, including their pods, PVCs and host paths
func (r *ReconcileRedisClusterBackup) getReplicaNodesInfo(namespace string, cluster *redisv1alpha1.DistributedRedisCluster) ([]NodeInfo, error) {
	var replicaNodes []NodeInfo

	// Get all pods for this Redis cluster
	podList := &corev1.PodList{}
	err := r.client.List(context.TODO(), podList,
		client.InNamespace(namespace),
		client.MatchingLabels{redisv1alpha1.LabelClusterName: cluster.Name})
	if err != nil {
		return nil, err
	}

	// Create a map of pod IP to pod details
	podMap := make(map[string]*corev1.Pod)
	for i := range podList.Items {
		pod := &podList.Items[i]
		podMap[pod.Status.PodIP] = pod
	}

	// Get all PVs to find host paths
	pvList := &corev1.PersistentVolumeList{}
	if err := r.client.List(context.TODO(), pvList); err != nil {
		return nil, fmt.Errorf("failed to list PVs: %v", err)
	}

	// Create a map of PVC name to PV
	pvcToPV := make(map[string]string)
	hostPathMap := make(map[string]string)

	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == namespace {
			pvcKey := fmt.Sprintf("%s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
			pvcToPV[pvcKey] = pv.Name

			// Extract host path information
			if pv.Spec.HostPath != nil {
				hostPathMap[pv.Name] = pv.Spec.HostPath.Path
			} else if pv.Spec.Local != nil {
				hostPathMap[pv.Name] = pv.Spec.Local.Path
			} else if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeAttributes != nil {
				// For CSI drivers, try to get path from volume attributes
				if path, exists := pv.Spec.CSI.VolumeAttributes["path"]; exists {
					hostPathMap[pv.Name] = path
				}
			}
		}
	}

	// Find replica nodes and their associated pods
	index := 0
	for _, node := range cluster.Status.Nodes {
		if node.Role == redisv1alpha1.RedisClusterNodeRoleReplica {
			info := NodeInfo{
				IP:          node.IP,
				Role:        string(node.Role),
				MasterRef:   node.MasterRef,
				StatefulSet: node.StatefulSet,
				Index:       index,
			}

			// Find the pod for this replica node
			if pod, exists := podMap[node.IP]; exists {
				info.NodeName = pod.Spec.NodeName
				info.PodName = pod.Name

				// Find the PVC used by this pod and its host path
				for _, volume := range pod.Spec.Volumes {
					if volume.PersistentVolumeClaim != nil {
						info.PvcName = volume.PersistentVolumeClaim.ClaimName

						// Try to get the host path
						pvcKey := fmt.Sprintf("%s/%s", namespace, info.PvcName)
						if pvName, exists := pvcToPV[pvcKey]; exists {
							if hostPath, exists := hostPathMap[pvName]; exists {
								info.HostDataPath = hostPath
							}
						}

						break
					} else if volume.HostPath != nil {
						// Handle case where pod directly uses hostPath volumes
						info.HostDataPath = volume.HostPath.Path
						break
					} else if volume.EmptyDir != nil {
						// Handle emptyDir case
						podUID := string(pod.UID)
						info.HostDataPath = fmt.Sprintf("/var/lib/kubelet/pods/%s/volumes/kubernetes.io~empty-dir/%s",
							podUID, volume.Name)
						break
					}
				}
			}

			replicaNodes = append(replicaNodes, info)
			index++
		}
	}

	return replicaNodes, nil
}

// createBackupJobs creates separate backup jobs for each node
func (r *ReconcileRedisClusterBackup) createBackupJobs(reqLogger logr.Logger, backup *redisv1alpha1.RedisClusterBackup, cluster *redisv1alpha1.DistributedRedisCluster, nodesInfo []NodeInfo) error {
	// Create a separate job for each node
	for _, nodeInfo := range nodesInfo {
		// Create job name with node index
		jobName := fmt.Sprintf("%s-%d", backup.JobName(), nodeInfo.Index)

		// Create a job for this node
		job, err := r.createNodeBackupJob(reqLogger, backup, cluster, nodeInfo, jobName)
		if err != nil {
			return err
		}

		// Create the job
		if err := r.client.Create(context.TODO(), job); err != nil {
			return err
		}

		reqLogger.Info(fmt.Sprintf("Created backup job for %s node %s", nodeInfo.Role, nodeInfo.IP),
			"job", jobName, "node", nodeInfo.NodeName)
	}

	return nil
}

// createNodeBackupJob creates a backup job for a specific node with host path mounting
func (r *ReconcileRedisClusterBackup) createNodeBackupJob(reqLogger logr.Logger, backup *redisv1alpha1.RedisClusterBackup, cluster *redisv1alpha1.DistributedRedisCluster, nodeInfo NodeInfo, jobName string) (*batchv1.Job, error) {
	// Create labels for the job
	jobLabels := map[string]string{
		redisv1alpha1.LabelClusterName:  backup.Spec.RedisClusterName,
		redisv1alpha1.AnnotationJobType: redisv1alpha1.JobTypeBackup,
		redisv1alpha1.LabelBackupStatus: string(redisv1alpha1.BackupPhaseRunning),
		redisv1alpha1.LabelBackupSource: "replicas",
		"redis.kun/node-index":          fmt.Sprintf("%d", nodeInfo.Index),
		"redis.kun/node-role":           nodeInfo.Role,
		"redis.kun/backup-name":         backup.Name,
	}

	// Check if host path is available - required for this backup method
	if nodeInfo.HostDataPath == "" {
		return nil, fmt.Errorf("host path is not available for node %s (IP: %s) - cannot proceed with backup",
			nodeInfo.NodeName, nodeInfo.IP)
	}

	// Create persistent volume for the backup
	persistentVolume, err := r.GetVolumeForBackup(backup, jobName)
	if err != nil {
		return nil, err
	}

	// Create init containers for cleanup if retention policy exists
	initContainers, err := r.createCleanupInitContainers(backup, reqLogger)
	if err != nil {
		return nil, err
	}

	// Create the backup container
	container, err := r.createBackupContainerWithRclone(backup, cluster, nodeInfo, reqLogger)
	if err != nil {
		return nil, err
	}

	isController := true
	boLimit := int32(1)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: backup.Namespace,
			Labels:    jobLabels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: redisv1alpha1.SchemeGroupVersion.String(),
					Kind:       redisv1alpha1.RedisClusterBackupKind,
					Name:       backup.Name,
					UID:        backup.UID,
					Controller: &isController,
				},
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &boLimit,
			ActiveDeadlineSeconds: backup.Spec.ActiveDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: jobLabels,
				},
				Spec: corev1.PodSpec{
					InitContainers: initContainers,
					Containers:     []corev1.Container{container},
					Volumes: []corev1.Volume{
						{
							Name:         persistentVolume.Name,
							VolumeSource: persistentVolume.VolumeSource,
						},
						{
							Name: "rcloneconfig",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: backup.RCloneSecretName(),
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	// Add node affinity to schedule backup pod on the same node as the Redis pod
	if nodeInfo.NodeName != "" {
		if job.Spec.Template.Spec.Affinity == nil {
			job.Spec.Template.Spec.Affinity = &corev1.Affinity{}
		}

		if job.Spec.Template.Spec.Affinity.NodeAffinity == nil {
			job.Spec.Template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
		}

		job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{nodeInfo.NodeName},
						},
					},
				},
			},
		}
	}

	// Add direct host path volume
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "redis-data-hostpath",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: nodeInfo.HostDataPath,
				Type: func() *corev1.HostPathType {
					t := corev1.HostPathDirectory
					return &t
				}(),
			},
		},
	})

	// Add volume mount to container
	for i := range job.Spec.Template.Spec.Containers {
		job.Spec.Template.Spec.Containers[i].VolumeMounts = append(
			job.Spec.Template.Spec.Containers[i].VolumeMounts,
			corev1.VolumeMount{
				Name:      "redis-data-hostpath",
				MountPath: "/var/lib/redis-data",
				ReadOnly:  true,
			},
		)
	}

	// Add local volume if specified
	if backup.Spec.Backend.Local != nil {
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
			Name:         "local",
			VolumeSource: backup.Spec.Backend.Local.VolumeSource,
		})
	}

	// Add privileged security context as we're accessing host paths
	if job.Spec.Template.Spec.SecurityContext == nil {
		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}

	// Set privileged for hostPath access
	privileged := true
	runAsUser := int64(0) // Run as root
	for i := range job.Spec.Template.Spec.Containers {
		if job.Spec.Template.Spec.Containers[i].SecurityContext == nil {
			job.Spec.Template.Spec.Containers[i].SecurityContext = &corev1.SecurityContext{}
		}
		job.Spec.Template.Spec.Containers[i].SecurityContext.Privileged = &privileged
		job.Spec.Template.Spec.Containers[i].SecurityContext.RunAsUser = &runAsUser
	}

	// Add pod spec options if specified
	if backup.Spec.PodSpec != nil {
		job.Spec.Template.Spec.NodeSelector = backup.Spec.PodSpec.NodeSelector
		job.Spec.Template.Spec.SchedulerName = backup.Spec.PodSpec.SchedulerName
		job.Spec.Template.Spec.Tolerations = backup.Spec.PodSpec.Tolerations
		job.Spec.Template.Spec.PriorityClassName = backup.Spec.PodSpec.PriorityClassName
		job.Spec.Template.Spec.Priority = backup.Spec.PodSpec.Priority
		job.Spec.Template.Spec.ImagePullSecrets = backup.Spec.PodSpec.ImagePullSecrets

		// If custom affinity is specified, merge it with our node affinity
		if backup.Spec.PodSpec.Affinity != nil {
			if backup.Spec.PodSpec.Affinity.PodAffinity != nil {
				job.Spec.Template.Spec.Affinity.PodAffinity = backup.Spec.PodSpec.Affinity.PodAffinity
			}
			if backup.Spec.PodSpec.Affinity.PodAntiAffinity != nil {
				job.Spec.Template.Spec.Affinity.PodAntiAffinity = backup.Spec.PodSpec.Affinity.PodAntiAffinity
			}
		}
	}

	// Add cluster-scoped annotation if needed
	if utils.IsClusterScoped() {
		if job.Annotations == nil {
			job.Annotations = make(map[string]string)
		}
		job.Annotations[utils.AnnotationScope] = utils.AnnotationClusterScoped
	}

	return job, nil
}

// createBackupContainerWithRclone creates a backup container using BGSAVE
// to use Redis's native backup mechanism and then copy the resulting RDB file
func (r *ReconcileRedisClusterBackup) createBackupContainerWithRclone(backup *redisv1alpha1.RedisClusterBackup, cluster *redisv1alpha1.DistributedRedisCluster, nodeInfo NodeInfo, reqLogger logr.Logger) (corev1.Container, error) {
	backupSpec := backup.Spec.Backend
	location, err := backupSpec.Location()
	if err != nil {
		return corev1.Container{}, err
	}

	// Ensure we have a StartTime
	if backup.Status.StartTime == nil {
		t := metav1.Now()
		backup.Status.StartTime = &t
		if err := r.crController.UpdateCRStatus(backup); err != nil {
			reqLogger.Error(err, "Failed to update backup StartTime")
		}
	}

	// Format timestamp from StartTime in UTC for folder name
	timestamp := backup.Status.StartTime.UTC().Format("20060102150405")
	reqLogger.Info("Using UTC timestamp from StartTime", "timestamp", timestamp, "StartTime", backup.Status.StartTime.String())

	// Create folder path for this backup
	folder := fmt.Sprintf("%s/%s/%s", backup.Namespace, cluster.Name, timestamp)
	reqLogger.Info("Backup folder path", "folder", folder)

	// Create snapshot name for this node
	snapshotName := fmt.Sprintf("%s-%d", backup.Name, nodeInfo.Index)

	// Define rclone options for controlled copy speed
	rcloneOptions := os.Getenv("RCLONE_OPTIONS")

	// Create local rclone config for direct copying
	rcloneLocalConfig := `
cat > /tmp/rclone-local.conf << EOF
[local]
type = local
EOF
`

	// Command for backup using BGSAVE and copying the RDB file
	backupCommand := fmt.Sprintf(`
set -ex

echo "Starting backup from %s node %s (IP: %s)"

# Set up variables
IP="%s"
NAMESPACE="%s"
CLUSTER_NAME="%s"
TIMESTAMP="%s"
BACKUP_NAME="%s"
REDIS_DATA_PATH="/var/lib/redis-data"
RDB_FILENAME="dump.rdb"

# Create the correct directory structure expected by the restore process
# Format: /back/redis/[namespace]/[cluster-name]/[timestamp]/[backup-name]/
BACKUP_DIR="/back/redis/${NAMESPACE}/${CLUSTER_NAME}/${TIMESTAMP}/${BACKUP_NAME}"

# Ensure the directory exists
mkdir -p "${BACKUP_DIR}"

# Create a temporary directory for generating any additional files
TEMP_DIR="/tmp/redis-backup-temp"
mkdir -p "${TEMP_DIR}"

# Setup local rclone config
%s

# Create metadata file in temp dir
cat > "${TEMP_DIR}/backup-info.txt" << ENDOFINFO
Backup Source: Redis Node %s %s
Backup Time: $(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)
Snapshot Name: ${BACKUP_NAME}
Node Name: ${NODE_NAME:-unknown}
Node Role: %s
Master Reference: ${MASTER_REF:-none}
ENDOFINFO

# Trigger BGSAVE on the Redis instance
echo "Triggering BGSAVE on Redis instance ${IP}..."
redis-cli -h "${IP}" BGSAVE

# Wait for BGSAVE to complete (check lastbgsave_status)
echo "Waiting for BGSAVE to complete..."
while true; do
    BGSAVE_INFO=$(redis-cli -h "${IP}" INFO persistence)
    BGSAVE_IN_PROGRESS=$(echo "$BGSAVE_INFO" | grep "rdb_bgsave_in_progress" | cut -d: -f2 | tr -d '\r')
    BGSAVE_STATUS=$(echo "$BGSAVE_INFO" | grep "rdb_last_bgsave_status" | cut -d: -f2 | tr -d '\r')
    
    echo "BGSAVE in progress: ${BGSAVE_IN_PROGRESS}, status: ${BGSAVE_STATUS}"
    
    if [ "${BGSAVE_IN_PROGRESS}" = "0" ]; then
        if [ "${BGSAVE_STATUS}" = "ok" ]; then
            echo "BGSAVE completed successfully"
            break
        elif [ "${BGSAVE_STATUS}" = "err" ]; then
            echo "BGSAVE failed"
            exit 1
        fi
    fi
    
    echo "Waiting for BGSAVE to complete..."
    sleep 5
done

# Copy the RDB file from the Redis data directory
echo "Copying RDB file from Redis data directory to backup location..."
if [ -f "${REDIS_DATA_PATH}/${RDB_FILENAME}" ]; then
    # Use rclone to copy the RDB file to the backup directory
    rclone copy "${REDIS_DATA_PATH}/${RDB_FILENAME}" "${BACKUP_DIR}/" %s --config=/tmp/rclone-local.conf 2>&1
    echo "Successfully copied dump.rdb using rclone"
else
    echo "Failed to find dump.rdb at ${REDIS_DATA_PATH}/${RDB_FILENAME}"
    # Try to find the RDB file in the data directory
    echo "Searching for RDB files in ${REDIS_DATA_PATH}..."
    find "${REDIS_DATA_PATH}" -name "*.rdb" -type f
    exit 1
fi

# Copy the metadata file
rclone copy "${TEMP_DIR}/backup-info.txt" "${BACKUP_DIR}/" %s --config=/tmp/rclone-local.conf 2>&1

# Process nodes.conf to make it look like it came from the master
echo "Creating nodes.conf from cluster information..."

# First get myself line to find the master reference
MYSELF_LINE=$(redis-cli -h ${IP} CLUSTER NODES | grep "myself")
echo "Found myself line: ${MYSELF_LINE}"

# Extract master ID from the myself line (4th field)
MASTER_ID=$(echo "${MYSELF_LINE}" | awk '{print $4}')
echo "Extracted master ID: ${MASTER_ID}"

if [ -n "${MASTER_ID}" ]; then
    # Get the master's line from CLUSTER NODES output
    MASTER_LINE=$(redis-cli -h ${IP} CLUSTER NODES | grep "^${MASTER_ID}")
    echo "Found master line: ${MASTER_LINE}"
    
    if [ -n "${MASTER_LINE}" ]; then
        # Modify the master line to add "myself," before master role and write to nodes.conf
        echo "${MASTER_LINE}" | sed 's/\([^ ]*\) \([^ ]*\) \([^,]*\)\(.*\)/\1 \2 myself,\3\4/' > "${BACKUP_DIR}/nodes.conf"
        echo "Modified master info saved to nodes.conf:"
        cat "${BACKUP_DIR}/nodes.conf"
    else
        echo "Warning: Could not find master line in CLUSTER NODES output."
        # Create a fallback nodes.conf by modifying the myself line
        echo "${MYSELF_LINE}" | sed 's/myself,slave [^ ]*/myself,master -/' > "${BACKUP_DIR}/nodes.conf"
        echo "Created fallback nodes.conf from self info:"
        cat "${BACKUP_DIR}/nodes.conf"
    fi
else
    echo "Warning: Could not extract master ID from myself line."
    # Last resort fallback
    redis-cli -h ${IP} CLUSTER NODES | grep "myself" | sed 's/myself,slave [^ ]*/myself,master -/' > "${BACKUP_DIR}/nodes.conf"
fi

# Verify that backup was created successfully
if [ -f "${BACKUP_DIR}/dump.rdb" ] && [ -f "${BACKUP_DIR}/nodes.conf" ]; then
    echo "Backup files verified successfully"
else
    echo "Error: Required backup files missing"
    exit 1
fi

# If this is a replica node, sync the backup to the corresponding master
if [ "%s" = "replica" ] && [ -n "${MASTER_REF}" ]; then
    echo "This is a replica node. Preparing to sync backup to corresponding master..."
    
    # Find the master pod name and node by querying the Redis cluster
    MASTER_IP=$(redis-cli -h ${IP} CLUSTER NODES | grep "^${MASTER_REF}" | awk '{print $2}' | cut -d':' -f1)
    
    if [ -n "${MASTER_IP}" ]; then
        echo "Found master IP: ${MASTER_IP}"
        
        # Create a unique master backup name based on replica backup
        MASTER_BACKUP_NAME="$(echo ${BACKUP_NAME} | sed 's/-[0-9]*$/-m&/')"
        MASTER_BACKUP_DIR="/back/redis/${NAMESPACE}/${CLUSTER_NAME}/${TIMESTAMP}/${MASTER_BACKUP_NAME}"
        
        echo "Creating master backup directory: ${MASTER_BACKUP_DIR}"
        mkdir -p "${MASTER_BACKUP_DIR}"
        
        echo "Syncing backup from replica to master backup location..."
        rclone copy "${BACKUP_DIR}/" "${MASTER_BACKUP_DIR}/" %s --config=/tmp/rclone-local.conf 2>&1
        
        # Update metadata in master backup
        cat > "${TEMP_DIR}/master-backup-info.txt" << ENDOFINFO
Backup Source: Redis Replica Node ${IP} (Synced to Master ${MASTER_IP})
Original Replica: ${BACKUP_NAME}
Backup Time: $(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)
Snapshot Name: ${MASTER_BACKUP_NAME}
Node Role: master (from replica)
ENDOFINFO
        
        # Copy the updated metadata file to master backup
        rclone copy "${TEMP_DIR}/master-backup-info.txt" "${MASTER_BACKUP_DIR}/backup-info.txt" %s --config=/tmp/rclone-local.conf 2>&1
        
        echo "Successfully synced replica backup to master location: ${MASTER_BACKUP_NAME}"
    else
        echo "Warning: Could not find master IP. Skipping sync to master."
    fi
fi

echo "Backup from %s node %s completed successfully"
echo "Backup saved to ${NAMESPACE}/${CLUSTER_NAME}/${TIMESTAMP}/${BACKUP_NAME}"
exit 0
`,
		nodeInfo.Role,
		nodeInfo.NodeName,
		nodeInfo.IP,
		nodeInfo.IP,
		backup.Namespace,
		cluster.Name,
		timestamp,
		snapshotName,
		rcloneLocalConfig,
		nodeInfo.Role,
		nodeInfo.IP,
		nodeInfo.Role,
		rcloneOptions,
		rcloneOptions,
		nodeInfo.Role,
		rcloneOptions,
		rcloneOptions,
		nodeInfo.Role,
		nodeInfo.NodeName)

	container := corev1.Container{
		Name:            fmt.Sprintf("%s-%d", redisv1alpha1.JobTypeBackup, nodeInfo.Index),
		Image:           backup.Spec.Image,
		ImagePullPolicy: "Always",
		Args: []string{
			redisv1alpha1.JobTypeBackup,
			fmt.Sprintf(`--data-dir=%s`, redisv1alpha1.BackupDumpDir),
			fmt.Sprintf(`--location=%s`, location),
			fmt.Sprintf(`--host=%s`, nodeInfo.IP),
			// Use the folder path based on namespace, cluster name, and UTC timestamp
			fmt.Sprintf(`--folder=%s`, folder),
			fmt.Sprintf(`--snapshot=%s`, snapshotName),
			"--",
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      redisv1alpha1.UtilVolumeName,
				MountPath: redisv1alpha1.BackupDumpDir,
			},
			{
				Name:      "rcloneconfig",
				ReadOnly:  true,
				MountPath: osm.SecretMountPath,
			},
		},
		Command: []string{
			"sh",
			"-c",
			backupCommand,
		},
	}

	// Add node name environment variable
	if nodeInfo.NodeName != "" {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "NODE_NAME",
			Value: nodeInfo.NodeName,
		})
	}

	// Add master reference for replica nodes
	if nodeInfo.Role == string(redisv1alpha1.RedisClusterNodeRoleReplica) && nodeInfo.MasterRef != "" {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "MASTER_REF",
			Value: nodeInfo.MasterRef,
		})
	}

	// Add local volume mount if configured
	if backup.Spec.Backend.Local != nil {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      "local",
			MountPath: backup.Spec.Backend.Local.MountPath,
			SubPath:   backup.Spec.Backend.Local.SubPath,
		})
	}

	// Add pod spec resources and probes if configured
	if backup.Spec.PodSpec != nil {
		container.Resources = backup.Spec.PodSpec.Resources
		container.LivenessProbe = backup.Spec.PodSpec.LivenessProbe
		container.ReadinessProbe = backup.Spec.PodSpec.ReadinessProbe
		container.Lifecycle = backup.Spec.PodSpec.Lifecycle
	}

	return container, nil
}

// Legacy createBackupContainer method is now deprecated
// Use createBackupContainerWithRclone instead for all backups
func (r *ReconcileRedisClusterBackup) createBackupContainer(backup *redisv1alpha1.RedisClusterBackup, cluster *redisv1alpha1.DistributedRedisCluster, nodeInfo NodeInfo, reqLogger logr.Logger) (corev1.Container, error) {
	// Forward to the rclone implementation instead
	return r.createBackupContainerWithRclone(backup, cluster, nodeInfo, reqLogger)
}

// createCleanupInitContainers creates init containers for cleanup if retention policy exists
func (r *ReconcileRedisClusterBackup) createCleanupInitContainers(backup *redisv1alpha1.RedisClusterBackup, reqLogger logr.Logger) ([]corev1.Container, error) {
	var initContainers []corev1.Container

	// Use retention policy from the backup or from its owner schedule
	var retentionPolicy *redisv1alpha1.BackupRetentionPolicy

	// Check if this backup is created by a schedule with retention policy
	for _, ownerRef := range backup.OwnerReferences {
		if ownerRef.Kind == "RedisClusterBackupSchedule" {
			// Find the schedule
			scheduleTmp := &redisv1alpha1.RedisClusterBackupSchedule{}
			err := r.client.Get(context.TODO(), types.NamespacedName{
				Namespace: backup.Namespace,
				Name:      ownerRef.Name,
			}, scheduleTmp)
			if err == nil && scheduleTmp.Spec.RetentionPolicy != nil {
				retentionPolicy = scheduleTmp.Spec.RetentionPolicy
				break
			}
		}
	}

	// Add init containers for cleanup if retention policy exists
	if retentionPolicy != nil {
		if retentionPolicy.MaxCount != nil {
			reqLogger.Info("Adding cleanup init container with retention policy",
				"MaxCount", retentionPolicy.MaxCount)

			// Generate cleanup script
			cleanupScript := generateCleanupScript(retentionPolicy)

			initContainers = append(initContainers, corev1.Container{
				Name:            "backup-cleanup",
				Image:           "busybox:latest",
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{
					"sh",
					"-c",
					cleanupScript,
				},
				VolumeMounts: []corev1.VolumeMount{},
			})

			// Mount the local volume to the cleanup container
			if backup.Spec.Backend.Local != nil {
				// For local backups, mount the local PVC
				initContainers[0].VolumeMounts = append(initContainers[0].VolumeMounts, corev1.VolumeMount{
					Name:      "local",
					MountPath: "/data", // Mount at /data since that's the base path in our script
					SubPath:   backup.Spec.Backend.Local.SubPath,
				})
			}
		}
	}

	return initContainers, nil
}

// generateCleanupScript generates a shell script for backup retention cleanup
func generateCleanupScript(policy *redisv1alpha1.BackupRetentionPolicy) string {
	script := `
echo "Starting pre-backup cleanup process..."
cd /data || { echo "Data directory not found"; exit 1; }

# Path structure is: redis/[namespace]/[redisClusterName]/[timestamp]
if [ ! -d "redis" ]; then
    echo "Redis directory not found, nothing to clean up"
    exit 0
fi

cd redis || { echo "Cannot access redis directory"; exit 1; }
for NAMESPACE in */; do
    echo "Processing namespace: $NAMESPACE"
    cd "$NAMESPACE" || continue
    for CLUSTER_DIR in */; do
        echo "Processing cluster directory: $CLUSTER_DIR"
        cd "$CLUSTER_DIR" || continue
        # Find backup folders in YYYYMMDDHHmmss format (e.g., 20250326090000)
        BACKUP_DIRS=$(find . -maxdepth 1 -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]" | sort)
        
        if [ -z "$BACKUP_DIRS" ]; then
            echo "No backup directories found in $CLUSTER_DIR"
            cd ..
            continue
        fi
        
        echo "Found the following backup directories in $CLUSTER_DIR:"
        echo "$BACKUP_DIRS"

        TOTAL_BACKUPS=$(echo "$BACKUP_DIRS" | wc -l)
        echo "Total backup folders found: $TOTAL_BACKUPS"
`

	// If MaxCount is set, add script to keep only the newest N backups
	if policy.MaxCount != nil {
		maxCount := *policy.MaxCount
		script += fmt.Sprintf(`
        # Keep only the newest %d backups
        if [ "$TOTAL_BACKUPS" -gt %d ]; then
            echo "Removing old backups to keep only %d most recent..."
            TO_DELETE=$((TOTAL_BACKUPS - %d))
            echo "Will delete $TO_DELETE oldest backups"
            
            # Get the oldest backups to delete (sort by name, which is timestamp)
            DIRS_TO_DELETE=$(echo "$BACKUP_DIRS" | head -n $TO_DELETE)
            
            for DIR in $DIRS_TO_DELETE; do
                echo "Deleting old backup: $DIR"
                rm -rf "$DIR"
            done
        else
            echo "No need to clean up based on count, have $TOTAL_BACKUPS backups with limit %d"
        fi
`, maxCount, maxCount, maxCount, maxCount, maxCount)
	}

	script += `
        # Return to namespace directory
        cd ..
    done
    
    # Return to redis base directory
    cd ..
done

echo "Backup cleanup completed."
`

	return script
}

func (r *ReconcileRedisClusterBackup) ValidateBackup(backup *redisv1alpha1.RedisClusterBackup) error {
	if backup.Labels == nil {
		backup.Labels = make(map[string]string)
	}
	if err := backup.Validate(); err != nil {
		return err
	}

	if _, err := r.crController.GetDistributedRedisCluster(backup.Namespace, backup.Spec.RedisClusterName); err != nil {
		return err
	}

	return nil
}

func (r *ReconcileRedisClusterBackup) GetVolumeForBackup(backup *redisv1alpha1.RedisClusterBackup, jobName string) (*corev1.Volume, error) {
	storage := backup.Spec.Storage
	if storage == nil || storage.Type == redisv1alpha1.Ephemeral {
		ed := corev1.EmptyDirVolumeSource{}
		return &corev1.Volume{
			Name: redisv1alpha1.UtilVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &ed,
			},
		}, nil
	}

	volume := &corev1.Volume{
		Name: redisv1alpha1.UtilVolumeName,
	}

	if err := r.createPVCForBackup(backup, jobName); err != nil {
		return nil, err
	}

	volume.PersistentVolumeClaim = &corev1.PersistentVolumeClaimVolumeSource{
		ClaimName: jobName,
	}

	return volume, nil
}

func (r *ReconcileRedisClusterBackup) createPVCForBackup(backup *redisv1alpha1.RedisClusterBackup, jobName string) error {
	getClaim := &corev1.PersistentVolumeClaim{}
	err := r.client.Get(context.TODO(), types.NamespacedName{
		Namespace: backup.Namespace,
		Name:      jobName,
	}, getClaim)
	if err != nil {
		if errors.IsNotFound(err) {
			storage := backup.Spec.Storage
			mode := corev1.PersistentVolumeFilesystem
			pvcSpec := &corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storage.Size,
					},
				},
				StorageClassName: &storage.Class,
				VolumeMode:       &mode,
			}

			claim := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: backup.Namespace,
				},
				Spec: *pvcSpec,
			}
			if storage.DeleteClaim {
				claim.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: redisv1alpha1.SchemeGroupVersion.String(),
						Kind:       redisv1alpha1.RedisClusterBackupKind,
						Name:       backup.Name,
						UID:        backup.UID,
					},
				}
			}

			return r.client.Create(context.TODO(), claim)
		}
		return err
	}
	return nil
}

// handleBackupJobs checks the status of all backup jobs for a backup CR
func (r *ReconcileRedisClusterBackup) handleBackupJobs(reqLogger logr.Logger, backup *redisv1alpha1.RedisClusterBackup) error {
	reqLogger.Info("Handling backup jobs")

	// List all jobs associated with this backup
	jobList := &batchv1.JobList{}
	err := r.client.List(context.TODO(), jobList,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels{
			redisv1alpha1.LabelBackupStatus: string(redisv1alpha1.BackupPhaseRunning),
			"redis.kun/backup-name":         backup.Name,
		})

	if err != nil {
		return err
	}

	if len(jobList.Items) == 0 {
		// No jobs found, this could be a race condition or the jobs might have been deleted
		reqLogger.Info("No backup jobs found", "backup", backup.Name)
		return nil
	}

	// Check if all jobs are finished
	jobsCompleted := 0
	jobsFailed := 0

	for _, job := range jobList.Items {
		if isJobFinished(&job) {
			for _, condition := range job.Status.Conditions {
				if condition.Status == corev1.ConditionTrue {
					if condition.Type == batchv1.JobComplete {
						jobsCompleted++
					} else if condition.Type == batchv1.JobFailed {
						jobsFailed++
					}
				}
			}
		}
	}

	totalJobs := len(jobList.Items)
	finishedJobs := jobsCompleted + jobsFailed

	reqLogger.Info("Backup job status",
		"total", totalJobs,
		"completed", jobsCompleted,
		"failed", jobsFailed,
		"finished", finishedJobs)

	// If not all jobs are finished, wait for them to complete
	if finishedJobs < totalJobs {
		return fmt.Errorf("waiting for all backup jobs to finish: %d/%d completed", finishedJobs, totalJobs)
	}

	// Get Redis cluster for event recording
	cluster, err := r.crController.GetDistributedRedisCluster(backup.Namespace, backup.Spec.RedisClusterName)
	if err != nil {
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupError,
			err.Error(),
		)
		return err
	}

	// All jobs finished, determine overall status
	if jobsFailed == 0 {
		// All jobs succeeded
		backup.Status.Phase = redisv1alpha1.BackupPhaseSucceeded
		t := metav1.Now()
		backup.Status.CompletionTime = &t
		if err := r.crController.UpdateCRStatus(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}

		delete(backup.GetLabels(), redisv1alpha1.LabelBackupStatus)
		if err := r.crController.UpdateCR(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}

		msg := fmt.Sprintf("Successfully completed backup with %d jobs", totalJobs)
		reqLogger.Info(msg)
		r.recorder.Event(
			backup,
			corev1.EventTypeNormal,
			event.BackupSuccessful,
			msg,
		)
		r.recorder.Event(
			cluster,
			corev1.EventTypeNormal,
			event.BackupSuccessful,
			msg,
		)
	} else if jobsCompleted == 0 {
		// All jobs failed
		backup.Status.Phase = redisv1alpha1.BackupPhaseFailed
		backup.Status.Reason = fmt.Sprintf("All %d backup jobs failed", totalJobs)
		t := metav1.Now()
		backup.Status.CompletionTime = &t
		if err := r.crController.UpdateCRStatus(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}

		delete(backup.GetLabels(), redisv1alpha1.LabelBackupStatus)
		if err := r.crController.UpdateCR(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}

		msg := fmt.Sprintf("Failed to complete backup: all %d jobs failed", totalJobs)
		reqLogger.Info(msg)
		r.recorder.Event(
			backup,
			corev1.EventTypeWarning,
			event.BackupFailed,
			msg,
		)
		r.recorder.Event(
			cluster,
			corev1.EventTypeWarning,
			event.BackupFailed,
			msg,
		)
	} else {
		// Partial success
		backup.Status.Phase = redisv1alpha1.BackupPhaseSucceeded
		backup.Status.Reason = fmt.Sprintf("Partially successful backup: %d/%d jobs succeeded", jobsCompleted, totalJobs)
		t := metav1.Now()
		backup.Status.CompletionTime = &t
		if err := r.crController.UpdateCRStatus(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}

		delete(backup.GetLabels(), redisv1alpha1.LabelBackupStatus)
		if err := r.crController.UpdateCR(backup); err != nil {
			r.recorder.Event(
				backup,
				corev1.EventTypeWarning,
				event.BackupError,
				err.Error(),
			)
			return err
		}

		msg := fmt.Sprintf("Partially successful backup: %d of %d jobs succeeded", jobsCompleted, totalJobs)
		reqLogger.Info(msg)
		r.recorder.Event(
			backup,
			corev1.EventTypeNormal,
			event.BackupSuccessful,
			msg,
		)
		r.recorder.Event(
			cluster,
			corev1.EventTypeNormal,
			event.BackupSuccessful,
			msg,
		)
	}

	return nil
}
