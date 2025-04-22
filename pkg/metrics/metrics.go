package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	rcb "github.com/ucloud/redis-cluster-operator/pkg/metrics/redisclusterbackup"
)

func RegisterAllMetrics() *prometheus.Registry {
	registry := prometheus.NewRegistry()

	rcb.RegisterMetrics(registry)

	return registry
}
