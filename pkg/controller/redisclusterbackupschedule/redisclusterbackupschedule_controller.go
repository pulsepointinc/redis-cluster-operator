package redisclusterbackupschedule

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/robfig/cron"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/utils"
)

var (
	log = logf.Log.WithName("controller_redisclusterbackupschedule")

	scheduleFlagSet *pflag.FlagSet
	// maxConcurrentReconciles is the maximum number of concurrent Reconciles which can be run. Defaults to 1.
	maxConcurrentSchedules int
)

const backupScheduleFinalizer = "finalizer.backupschedule.redis.kun"

func init() {
	scheduleFlagSet = pflag.NewFlagSet("backupschedule", pflag.ExitOnError)
	scheduleFlagSet.IntVar(&maxConcurrentSchedules, "schedulectr-maxconcurrent", 2, "the maximum number of concurrent Reconciles which can be run. Defaults to 1.")
}

// Add creates a new RedisClusterBackupSchedule Controller and adds it to the Manager.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	dc, _ := client.New(mgr.GetConfig(), client.Options{})
	r := &ReconcileRedisClusterBackupSchedule{
		client:       mgr.GetClient(),
		scheme:       mgr.GetScheme(),
		directClient: dc,
		recorder:     mgr.GetEventRecorderFor("redis-cluster-backup-schedule"),
	}
	return r
}

// FlagSet returns the FlagSet for this controller
func FlagSet() *pflag.FlagSet {
	return scheduleFlagSet
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("redisclusterbackupschedule-controller", mgr, controller.Options{
		Reconciler:              r,
		MaxConcurrentReconciles: maxConcurrentSchedules,
	})
	if err != nil {
		return err
	}

	pred := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			// returns false if RedisClusterBackupSchedule is ignored (not managed) by this operator.
			if !utils.ShoudManage(e.MetaNew) {
				return false
			}
			log.WithValues("namespace", e.MetaNew.GetNamespace(), "name", e.MetaNew.GetName()).V(5).Info("Call UpdateFunc")
			// Ignore updates to CR status in which case metadata.Generation does not change
			if e.MetaOld.GetGeneration() != e.MetaNew.GetGeneration() {
				log.WithValues("namespace", e.MetaNew.GetNamespace(), "name", e.MetaNew.GetName()).Info("Generation change return true",
					"old", e.ObjectOld, "new", e.ObjectNew)
				return true
			}
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// returns false if RedisClusterBackupSchedule is ignored (not managed) by this operator.
			if !utils.ShoudManage(e.Meta) {
				return false
			}
			log.WithValues("namespace", e.Meta.GetNamespace(), "name", e.Meta.GetName()).Info("Call DeleteFunc")
			// Evaluates to false if the object has been confirmed deleted.
			return !e.DeleteStateUnknown
		},
		CreateFunc: func(e event.CreateEvent) bool {
			// returns false if RedisClusterBackupSchedule is ignored (not managed) by this operator.
			if !utils.ShoudManage(e.Meta) {
				return false
			}
			log.WithValues("namespace", e.Meta.GetNamespace(), "name", e.Meta.GetName()).Info("Call CreateFunc")
			return true
		},
	}

	// Watch for changes to primary resource RedisClusterBackupSchedule
	err = c.Watch(&source.Kind{Type: &redisv1alpha1.RedisClusterBackupSchedule{}}, &handler.EnqueueRequestForObject{}, pred)
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Backups and requeue the owner BackupSchedule
	err = c.Watch(&source.Kind{Type: &redisv1alpha1.RedisClusterBackup{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &redisv1alpha1.RedisClusterBackupSchedule{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileRedisClusterBackupSchedule implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileRedisClusterBackupSchedule{}

// ReconcileRedisClusterBackupSchedule reconciles a RedisClusterBackupSchedule object
type ReconcileRedisClusterBackupSchedule struct {
	// Client will be used to Get and Update resources
	client client.Client
	// DirectClient will be used for creation of jobs to bypass caching
	directClient client.Client
	scheme       *runtime.Scheme
	recorder     record.EventRecorder
}

// Reconcile reads the state of the RedisClusterBackupSchedule and makes changes as needed
func (r *ReconcileRedisClusterBackupSchedule) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling RedisClusterBackupSchedule")

	// Fetch the RedisClusterBackupSchedule instance
	instance := &redisv1alpha1.RedisClusterBackupSchedule{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Check if the schedule instance is marked to be deleted
	isScheduleMarkedToBeDeleted := instance.GetDeletionTimestamp() != nil
	if isScheduleMarkedToBeDeleted {
		if contains(instance.GetFinalizers(), backupScheduleFinalizer) {
			// Run finalization logic for backupScheduleFinalizer. If the
			// finalization logic fails, don't remove the finalizer so
			// that we can retry during the next reconciliation.
			if err := r.finalizeBackupSchedule(reqLogger, instance); err != nil {
				return reconcile.Result{}, err
			}

			// Remove backupScheduleFinalizer. Once all finalizers have been
			// removed, the object will be deleted.
			instance.SetFinalizers(remove(instance.GetFinalizers(), backupScheduleFinalizer))
			err := r.client.Update(context.TODO(), instance)
			if err != nil {
				return reconcile.Result{}, err
			}
		}
		return reconcile.Result{}, nil
	}

	// Add finalizer for this CR
	if !contains(instance.GetFinalizers(), backupScheduleFinalizer) {
		if err := r.addFinalizer(reqLogger, instance); err != nil {
			return reconcile.Result{}, err
		}
	}

	// If the schedule is paused, don't do anything
	if instance.Spec.Paused {
		reqLogger.Info("Schedule is paused, skipping")
		return reconcile.Result{}, nil
	}

	// Get the Redis Cluster to ensure it exists
	redisCluster := &redisv1alpha1.DistributedRedisCluster{}
	err = r.client.Get(context.TODO(), client.ObjectKey{
		Namespace: request.Namespace,
		Name:      instance.Spec.RedisClusterName,
	}, redisCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			// Redis cluster doesn't exist yet - wait and requeue
			r.recorder.Event(instance, corev1.EventTypeWarning, "ClusterNotFound",
				fmt.Sprintf("Redis cluster %s not found", instance.Spec.RedisClusterName))
			return reconcile.Result{
				RequeueAfter: time.Minute,
			}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Clean up old completed and failed backup jobs
	if err := r.cleanupBackups(instance); err != nil {
		return reconcile.Result{}, err
	}

	// Check if it's time to run a new backup
	var requeueAfter time.Duration
	nextScheduledTime, err := getNextScheduleTime(instance.Spec.Schedule, instance.Status.LastScheduled)
	if err != nil {
		r.recorder.Event(instance, corev1.EventTypeWarning, "InvalidSchedule",
			fmt.Sprintf("Cannot parse schedule %q: %v", instance.Spec.Schedule, err))
		// Don't requeue immediately, wait to be requeued by a change to the resource
		return reconcile.Result{}, nil
	}

	now := time.Now()
	if instance.Status.LastScheduled == nil {
		// First time - create backup immediately
		reqLogger.Info("Initial execution of backup schedule")
	} else if nextScheduledTime.After(now) {
		// Not yet time for next backup, calculate how long to wait
		requeueAfter = nextScheduledTime.Sub(now)
		reqLogger.Info("Scheduled next backup", "nextRun", nextScheduledTime, "requeueAfter", requeueAfter)
		return reconcile.Result{RequeueAfter: requeueAfter}, nil
	}

	// Check if any backup is already in progress
	if r.isBackupInProgress(instance, reqLogger) {
		reqLogger.Info("A backup is already in progress, will retry later")
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}

	// First update the LastScheduled time to prevent race conditions between concurrent reconciliations
	instance.Status.LastScheduled = &metav1.Time{Time: now}
	err = r.client.Status().Update(context.TODO(), instance)
	if err != nil {
		if errors.IsConflict(err) {
			// Resource was modified, we should fetch the latest version and retry
			reqLogger.Info("Conflict detected, will retry reconciliation")
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}

	// After successfully updating the status, create a new backup
	err = r.createBackupJob(instance, reqLogger)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Calculate the next run time for requeue
	nextScheduledTime, err = getNextScheduleTime(instance.Spec.Schedule, instance.Status.LastScheduled)
	if err != nil {
		r.recorder.Event(instance, corev1.EventTypeWarning, "InvalidSchedule",
			fmt.Sprintf("Cannot parse schedule %q: %v", instance.Spec.Schedule, err))
		return reconcile.Result{}, nil
	}
	requeueAfter = nextScheduledTime.Sub(now)
	reqLogger.Info("Scheduled next backup after creating job", "nextRun", nextScheduledTime, "requeueAfter", requeueAfter)

	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

// isBackupInProgress checks if any backup from this schedule is currently running
func (r *ReconcileRedisClusterBackupSchedule) isBackupInProgress(schedule *redisv1alpha1.RedisClusterBackupSchedule, logger logr.Logger) bool {
	// Get all backups owned by this schedule
	backupList := &redisv1alpha1.RedisClusterBackupList{}
	listOpts := []client.ListOption{
		client.InNamespace(schedule.Namespace),
		client.MatchingLabels{
			"schedule": schedule.Name,
		},
	}

	if err := r.client.List(context.TODO(), backupList, listOpts...); err != nil {
		logger.Error(err, "Failed to list backup jobs")
		return false // In case of error, proceed with caution (allow backup creation)
	}

	// Check if any backup is in progress
	for _, backup := range backupList.Items {
		if backup.Status.Phase == redisv1alpha1.BackupPhaseRunning ||
			backup.Status.Phase == "" { // Empty phase means the backup is just created
			logger.Info("Found in-progress backup", "backup", backup.Name, "phase", backup.Status.Phase)
			return true
		}
	}

	return false
}

// finalizeBackupSchedule handles any necessary cleanup when the schedule is being deleted
func (r *ReconcileRedisClusterBackupSchedule) finalizeBackupSchedule(reqLogger logr.Logger, schedule *redisv1alpha1.RedisClusterBackupSchedule) error {
	// No need to do anything special here, the owner reference on the backups will
	// ensure they get cleaned up by the garbage collector
	reqLogger.Info("Successfully finalized RedisClusterBackupSchedule")
	return nil
}

// addFinalizer adds a finalizer to the schedule CR
func (r *ReconcileRedisClusterBackupSchedule) addFinalizer(reqLogger logr.Logger, schedule *redisv1alpha1.RedisClusterBackupSchedule) error {
	reqLogger.Info("Adding Finalizer for the BackupSchedule")
	schedule.SetFinalizers(append(schedule.GetFinalizers(), backupScheduleFinalizer))

	// Update CR
	err := r.client.Update(context.TODO(), schedule)
	if err != nil {
		reqLogger.Error(err, "Failed to update RedisClusterBackupSchedule with finalizer")
		return err
	}
	return nil
}

// createBackupJob creates a new backup CR based on the schedule
func (r *ReconcileRedisClusterBackupSchedule) createBackupJob(schedule *redisv1alpha1.RedisClusterBackupSchedule, logger logr.Logger) error {
	// Create a new backup name based on schedule name and time
	backupName := generateBackupName(schedule.Name)

	logger.Info("Creating backup from schedule", "BackupName", backupName)

	// Create a new backup CR
	backup := &redisv1alpha1.RedisClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupName,
			Namespace: schedule.Namespace,
			Labels: map[string]string{
				"created-by": "backup-schedule",
				"schedule":   schedule.Name,
			},
			Annotations: map[string]string{
				"redis.kun/scope": "cluster-scoped",
			},
		},
		Spec: schedule.Spec.BackupTemplate,
	}

	// Set the owner reference so the backup is cleaned up when the schedule is deleted
	if err := controllerutil.SetControllerReference(schedule, backup, r.scheme); err != nil {
		return err
	}

	// Create the backup using direct client to avoid caching issues
	if err := r.directClient.Create(context.TODO(), backup); err != nil {
		r.recorder.Event(schedule, corev1.EventTypeWarning, "BackupCreationFailed",
			fmt.Sprintf("Failed to create backup %s: %v", backupName, err))
		return err
	}

	// Get a reference to the backup for events
	backupRef, err := reference.GetReference(r.scheme, backup)
	if err != nil {
		logger.Error(err, "Unable to get reference for backup")
	} else {
		r.recorder.Event(schedule, corev1.EventTypeNormal, "BackupCreated",
			fmt.Sprintf("Created backup %s", backupRef.Name))
	}

	return nil
}

const kubeCharset = "abcdefghijklmnopqrstuvwxyz0123456789"

func randomKubeSuffix(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(kubeCharset))))
		sb.WriteByte(kubeCharset[num.Int64()])
	}
	return sb.String()
}

func generateBackupName(scheduleName string) string {
	now := time.Now().UTC()
	timestamp := now.Format("02-01-2006-15-04")
	suffix := randomKubeSuffix(5)
	return fmt.Sprintf("%s-%s-%s", scheduleName, timestamp, suffix)
}

// cleanupBackups cleans up old backup jobs based on the history limits
func (r *ReconcileRedisClusterBackupSchedule) cleanupBackups(schedule *redisv1alpha1.RedisClusterBackupSchedule) error {
	// Don't do anything if history limits are not set
	if schedule.Spec.SuccessfulJobsHistoryLimit == nil && schedule.Spec.FailedJobsHistoryLimit == nil {
		return nil
	}

	// Get all backups owned by this schedule
	backupList := &redisv1alpha1.RedisClusterBackupList{}
	listOpts := []client.ListOption{
		client.InNamespace(schedule.Namespace),
		client.MatchingLabels{
			"schedule": schedule.Name,
		},
	}

	if err := r.client.List(context.TODO(), backupList, listOpts...); err != nil {
		return err
	}

	// Separate successful and failed backups
	var successfulBackups, failedBackups []redisv1alpha1.RedisClusterBackup

	for _, backup := range backupList.Items {
		switch backup.Status.Phase {
		case redisv1alpha1.BackupPhaseSucceeded:
			successfulBackups = append(successfulBackups, backup)
		case redisv1alpha1.BackupPhaseFailed:
			failedBackups = append(failedBackups, backup)
		}
	}

	// Clean up successful backups if limit is set
	if schedule.Spec.SuccessfulJobsHistoryLimit != nil {
		successLimit := int(*schedule.Spec.SuccessfulJobsHistoryLimit)
		if len(successfulBackups) > successLimit {
			// Sort backups by completion time (oldest first)
			sort.Slice(successfulBackups, func(i, j int) bool {
				if successfulBackups[i].Status.CompletionTime == nil {
					return true
				}
				if successfulBackups[j].Status.CompletionTime == nil {
					return false
				}
				return successfulBackups[i].Status.CompletionTime.Before(successfulBackups[j].Status.CompletionTime)
			})

			// Delete the oldest backups beyond the limit
			for i := 0; i < len(successfulBackups)-successLimit; i++ {
				if err := r.client.Delete(context.TODO(), &successfulBackups[i]); err != nil {
					return err
				}
				r.recorder.Event(schedule, corev1.EventTypeNormal, "BackupDeleted",
					fmt.Sprintf("Deleted old successful backup %s", successfulBackups[i].Name))
			}
		}
	}

	// Clean up failed backups if limit is set
	if schedule.Spec.FailedJobsHistoryLimit != nil {
		failLimit := int(*schedule.Spec.FailedJobsHistoryLimit)
		if len(failedBackups) > failLimit {
			// Sort backups by completion time (oldest first)
			sort.Slice(failedBackups, func(i, j int) bool {
				if failedBackups[i].Status.CompletionTime == nil {
					return true
				}
				if failedBackups[j].Status.CompletionTime == nil {
					return false
				}
				return failedBackups[i].Status.CompletionTime.Before(failedBackups[j].Status.CompletionTime)
			})

			// Delete the oldest backups beyond the limit
			for i := 0; i < len(failedBackups)-failLimit; i++ {
				if err := r.client.Delete(context.TODO(), &failedBackups[i]); err != nil {
					return err
				}
				r.recorder.Event(schedule, corev1.EventTypeNormal, "BackupDeleted",
					fmt.Sprintf("Deleted old failed backup %s", failedBackups[i].Name))
			}
		}
	}

	return nil
}

// getNextScheduleTime calculates the next scheduled time based on the cron schedule
func getNextScheduleTime(schedule string, lastScheduled *metav1.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		return time.Time{}, fmt.Errorf("unparseable schedule: %v", err)
	}

	var earliestTime time.Time
	if lastScheduled != nil {
		earliestTime = lastScheduled.Time
	} else {
		earliestTime = time.Now()
	}

	return sched.Next(earliestTime), nil
}

// Helper functions
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func remove(list []string, s string) []string {
	for i, v := range list {
		if v == s {
			list = append(list[:i], list[i+1:]...)
		}
	}
	return list
}
