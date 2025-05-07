FROM redis:5.0.4

RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    netcat \
    zip \
    procps \
    curl \
    jq \
    rsync \
    iproute2 \
  && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man /tmp/*

# Copy required tools and scripts
COPY rclone /usr/local/bin/rclone
COPY redis-tools.sh /usr/local/bin/redis-tools.sh
RUN chmod +x /usr/local/bin/redis-tools.sh

# Create a directory for mounting replica data
RUN mkdir -p /var/lib/redis-replica-data

ENTRYPOINT ["redis-tools.sh"]