#!/bin/bash
set -eou pipefail

# Enhanced version of redis-tools.sh to support replica-based backups

show_help() {
  echo "redis-tools.sh - run tools"
  echo " "
  echo "redis-tools.sh COMMAND [options]"
  echo " "
  echo "options:"
  echo "-h, --help                         show brief help"
  echo "    --data-dir=DIR                 path to directory holding db data (default: /var/data)"
  echo "    --host=HOST                    database host"
  echo "    --user=USERNAME                database username"
  echo "    --bucket=BUCKET                name of bucket"
  echo "    --location=LOCATION            location of backend (<provider>:<bucket name>)"
  echo "    --folder=FOLDER                name of folder in bucket"
  echo "    --snapshot=SNAPSHOT            name of snapshot"
  echo "    --replica-data=DIR             path to mounted replica data (if available)"
  echo "    --node-name=NAME               Kubernetes node name (for replica co-location)"
}

RETVAL=0
DEBUG=${DEBUG:-}
REDIS_HOST=${REDIS_HOST:-}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_USER=${REDIS_USER:-}
REDIS_PASSWORD=${REDIS_PASSWORD:-}
REDIS_BUCKET=${REDIS_BUCKET:-}
REDIS_LOCATION=${REDIS_LOCATION:-}
REDIS_FOLDER=${REDIS_FOLDER:-}
REDIS_SNAPSHOT=${REDIS_SNAPSHOT:-}
REDIS_DATA_DIR=${REDIS_DATA_DIR:-/data}
REDIS_RESTORE_SUCCEEDED=${REDIS_RESTORE_SUCCEEDED:-0}
REDIS_REPLICA_DATA=${REDIS_REPLICA_DATA:-}
REDIS_NODE_NAME=${REDIS_NODE_NAME:-}
RCLONE_CONFIG_FILE=/etc/rclone/config

op=$1
shift

while test $# -gt 0; do
  case "$1" in
    -h | --help)
      show_help
      exit 0
      ;;
    --data-dir*)
      export REDIS_DATA_DIR=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --host*)
      export REDIS_HOST=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --user*)
      export REDIS_USER=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --bucket*)
      export REDIS_BUCKET=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --location*)
      export REDIS_LOCATION=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --folder*)
      export REDIS_FOLDER=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --snapshot*)
      export REDIS_SNAPSHOT=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --replica-data*)
      export REDIS_REPLICA_DATA=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --node-name*)
      export REDIS_NODE_NAME=$(echo $1 | sed -e 's/^[^=]*=//g')
      shift
      ;;
    --)
      shift
      break
      ;;
    *)
      show_help
      exit 1
      ;;
  esac
done

if [ -n "$DEBUG" ]; then
  env | sort | grep REDIS_*
  echo ""
fi

# cleanup data dump dir
mkdir -p "$REDIS_DATA_DIR"
cd "$REDIS_DATA_DIR"

case "$op" in
  backup)
    echo "Backing up Redis database from replica..."
    echo "DB Host ${REDIS_HOST}"
    SOURCE_DIR="$REDIS_DATA_DIR"/"$REDIS_SNAPSHOT"
    mkdir -p "$SOURCE_DIR"

    cd "$SOURCE_DIR"
    # cleanup data dump dir
    rm -rf *

    # Create a metadata file with backup information
    cat > "backup-info.txt" <<EOF
Backup Source: Redis Replica ${REDIS_HOST}
Backup Time: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
Snapshot Name: ${REDIS_SNAPSHOT}
Node Name: ${REDIS_NODE_NAME:-unknown}
EOF

    # Option 1: Use redis-cli to create RDB dump directly from running replica
    echo "Creating RDB dump from replica ${REDIS_HOST}..."
    if [ -n "$REDIS_PASSWORD" ]; then
      redis-cli --rdb dump.rdb -h "${REDIS_HOST}" -a "${REDIS_PASSWORD}"
    else
      redis-cli --rdb dump.rdb -h "${REDIS_HOST}"
    fi

    # Get cluster node information
    if [ -n "$REDIS_PASSWORD" ]; then
      redis-cli -h "${REDIS_HOST}" -a "${REDIS_PASSWORD}" CLUSTER NODES | grep myself > nodes.conf
    else
      redis-cli -h "${REDIS_HOST}" CLUSTER NODES | grep myself > nodes.conf
    fi

    # Option 2: Copy files from mounted replica data (if available)
    if [ -n "$REDIS_REPLICA_DATA" ] && [ -d "$REDIS_REPLICA_DATA" ]; then
      echo "Copying additional files from replica data volume..."

      # Copy configuration file if it exists
      if [ -f "$REDIS_REPLICA_DATA/redis.conf" ]; then
        cp "$REDIS_REPLICA_DATA/redis.conf" ./
      fi

      # Copy append-only files if present
      if [ -d "$REDIS_REPLICA_DATA/appendonlydir" ]; then
        mkdir -p ./appendonlydir
        cp -r "$REDIS_REPLICA_DATA/appendonlydir"/* ./appendonlydir/
      fi

      # Copy other important Redis files (if they exist)
      for file in appendonly.aof; do
        if [ -f "$REDIS_REPLICA_DATA/$file" ]; then
          cp "$REDIS_REPLICA_DATA/$file" ./
        fi
      done

      echo "Files copied from replica data volume."
    fi

    ls -lh "$SOURCE_DIR"
    echo "Uploading backup files to the backend..."
    echo "From $SOURCE_DIR"
    rclone --config "$RCLONE_CONFIG_FILE" copy "$SOURCE_DIR" "$REDIS_LOCATION"/"$REDIS_FOLDER/$REDIS_SNAPSHOT" -v

    echo "Backup from replica ${REDIS_HOST} completed successfully"
    ;;
  restore)
    echo "Pulling backup file from the backend"
    if [ "${REDIS_RESTORE_SUCCEEDED}" == "1" ];then
      echo "Has been restored successfully"
      exit 0
    fi
    index=$(echo "${POD_NAME}" | awk -F- '{print $(NF-1)}')
    REDIS_SNAPSHOT=${REDIS_SNAPSHOT}-${index}
    SOURCE_SNAPSHOT="$REDIS_LOCATION"/"$REDIS_FOLDER/$REDIS_SNAPSHOT"
    echo "From $SOURCE_SNAPSHOT"
    rclone --config "$RCLONE_CONFIG_FILE" sync "$SOURCE_SNAPSHOT" "$REDIS_DATA_DIR" -v

    echo "Recovery successful"
    ;;
  check-replica)
    # New operation to check if the current host is a replica
    echo "Checking if ${REDIS_HOST} is a replica..."

    role_info=$(redis-cli -h "${REDIS_HOST}" -a "${REDIS_PASSWORD}" INFO REPLICATION | grep role)

    if [[ "$role_info" == *"role:slave"* ]]; then
      echo "REPLICA_STATUS=true"
      echo "${REDIS_HOST} is a replica node"
      exit 0
    else
      echo "REPLICA_STATUS=false"
      echo "${REDIS_HOST} is not a replica node"
      exit 1
    fi
    ;;
  *)
    (10)
    echo $"Unknown op!"
    RETVAL=1
    ;;
esac
exit "$RETVAL"