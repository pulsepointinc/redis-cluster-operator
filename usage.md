## CRDs (Custom Resource Definitions)

The operator introduces the following Custom Resource Definitions:

- **DistributedRedisCluster**: Defines a Redis Cluster with its configuration
- **RedisClusterBackup**: Defines a single backup operation
- **RedisClusterBackupSchedule**: Defines a schedule for periodic backups

## Usage

### Creating a Redis Cluster

1. Create a file named `redis-cluster.yaml`:

```yaml
apiVersion: redis.kun/v1alpha1
kind: DistributedRedisCluster
metadata:
  name: example-redis
  namespace: default
spec:
  image: redis:7.2.3-alpine
  masterSize: 3
  clusterReplicas: 1
  serviceName: example-redis
  config:
    maxmemory: "4096mb"
    maxmemory-policy: allkeys-lru
  storage:
    class: standard
    size: 10Gi
    type: persistent-claim
    deleteClaim: false
  resources:
    limits:
      cpu: 1000m
      memory: 5Gi
    requests:
      cpu: 500m
      memory: 2Gi
```

### Creating a one-time backup

1. Create a file with the following content:

```yaml
apiVersion: redis.kun/v1alpha1
kind: RedisClusterBackup
metadata:
  name: example-backup
  namespace: default
spec:
  image: redis-tools:1.0.5
  redisClusterName: example-redis
  local:
    mountPath: /back
    persistentVolumeClaim:
      claimName: redis-backup-pvc
```

### Creating a backup schedule

Create a file with the following content:

```yaml
apiVersion: redis.kun/v1alpha1
kind: RedisClusterBackupSchedule
metadata:
  name: example-schedule
  namespace: default
spec:
  schedule: "0 2 * * *"  # Every day at 2 AM
  redisClusterName: example-redis
  retentionPolicy:
    maxCount: 3
  backupTemplate:
    image: redis-tools:1.0.5
    redisClusterName: example-redis
    local:
      mountPath: /back
      persistentVolumeClaim:
        claimName: redis-backups
```

## Backup Configuration

### Backup Sources

By default, backups are taken from replica nodes if available. This minimizes the impact on your production traffic
which is served by master nodes.

### Retention Policy

You can control how many backups to keep using the `retentionPolicy.maxCount` parameter in your
`RedisClusterBackupSchedule`.

## Monitoring Backups

### List all backups

```bash
kubectl get redisclusterbackup -n <namespace>
```

### Check backup status

```bash
kubectl describe redisclusterbackup <backup-name> -n <namespace>
```

### List backup schedules

```bash
kubectl get redisclusterbackupschedule -n <namespace>
```

## Configuration Reference

### DistributedRedisCluster

| Field             | Description                         | Default |
|-------------------|-------------------------------------|---------|
| `image`           | Redis image to use                  | -       |
| `masterSize`      | Number of master nodes              | 3       |
| `clusterReplicas` | Number of replicas per master       | 1       |
| `serviceName`     | Name of the service                 | -       |
| `config`          | Redis configuration parameters      | -       |
| `storage`         | Persistent storage configuration    | -       |
| `resources`       | CPU/Memory requests and limits      | -       |
| `nodeSelector`    | Node selection constraints          | -       |
| `toleRations`     | Pod tolerations                     | -       |
| `affinity`        | Pod affinity/anti-affinity rules    | -       |
| `monitor`         | Prometheus monitoring configuration | -       |

### RedisClusterBackup

| Field              | Description                           | Default |
|--------------------|---------------------------------------|---------|
| `image`            | Backup tool image                     | -       |
| `redisClusterName` | Name of the Redis cluster to backup   | -       |
| `local`            | Local backup configuration            | -       |
| `podSpec`          | Pod specifications for the backup job | -       |

### RedisClusterBackupSchedule

| Field              | Description                         | Default |
|--------------------|-------------------------------------|---------|
| `schedule`         | Cron expression for backup schedule | -       |
| `redisClusterName` | Name of the Redis cluster to backup | -       |
| `backupTemplate`   | Template for backup CR              | -       |
| `retentionPolicy`  | Policy for backup retention         | -       |
| `paused`           | Whether the schedule is paused      | false   |
