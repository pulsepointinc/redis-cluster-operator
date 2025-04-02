package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ResourceSingularBackupSchedule = "backupschedule"
)

// BackupRetentionPolicy defines how long to retain backups
type BackupRetentionPolicy struct {
	// MaxCount specifies how many backups should be kept at most
	// +optional
	MaxCount *int32 `json:"maxCount,omitempty"`

	// MaxAge specifies how old backups can be before being deleted (in days)
	// +optional
	MaxAge *int32 `json:"maxAge,omitempty"`
}

// RedisClusterBackupScheduleSpec defines the desired state of RedisClusterBackupSchedule
// +k8s:openapi-gen=true
type RedisClusterBackupScheduleSpec struct {
	// Schedule in Cron format, see https://en.wikipedia.org/wiki/Cron.
	Schedule string `json:"schedule"`

	// RedisClusterName to backup
	RedisClusterName string `json:"redisClusterName"`

	// BackupTemplate defines the backup spec that will be created
	BackupTemplate RedisClusterBackupSpec `json:"backupTemplate"`

	// Paused indicates whether the backup schedule is paused
	// +optional
	Paused bool `json:"paused,omitempty"`

	// SuccessfulJobsHistoryLimit specifies how many completed jobs should be kept.
	// +optional
	//SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// FailedJobsHistoryLimit specifies how many failed jobs should be kept.
	// +optional
	//FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`

	// RetentionPolicy defines how long to keep backups in the PV
	// +optional
	RetentionPolicy *BackupRetentionPolicy `json:"retentionPolicy,omitempty"`
}

// RedisClusterBackupScheduleStatus defines the observed state of RedisClusterBackupSchedule
// +k8s:openapi-gen=true
type RedisClusterBackupScheduleStatus struct {
	// Last time the backup was successfully scheduled
	// +optional
	LastScheduled *metav1.Time `json:"lastScheduled,omitempty"`

	// Last time the backup was successfully completed
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// Last time a backup failed
	// +optional
	LastFailedTime *metav1.Time `json:"lastFailedTime,omitempty"`

	// Active holds pointers to currently running backup jobs
	// +optional
	Active []string `json:"active,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RedisClusterBackupSchedule is the Schema for scheduling redis cluster backups
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=redisclusterbackupschedules,scope=Namespaced
type RedisClusterBackupSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisClusterBackupScheduleSpec   `json:"spec,omitempty"`
	Status RedisClusterBackupScheduleStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RedisClusterBackupScheduleList contains a list of RedisClusterBackupSchedule
type RedisClusterBackupScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisClusterBackupSchedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisClusterBackupSchedule{}, &RedisClusterBackupScheduleList{})
}
