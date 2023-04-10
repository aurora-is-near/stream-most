package monitoring

import (
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	AnnouncementsProcessed prometheus.Counter
	ShardsProcessed        prometheus.Counter
	ValidationPassed       prometheus.Counter
	ValidationError        prometheus.Counter
)

func Export(options *monitor_options.Options) {
	AnnouncementsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: options.Namespace,
		Subsystem: options.Subsystem,
		Name:      "announcements_processed",
		Help:      "Total number of block announcements processed by the block processor",
	})
	ShardsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: options.Namespace,
		Subsystem: options.Subsystem,
		Name:      "shards_processed",
		Help:      "Total number of block shards processed by the block processor",
	})
	ValidationPassed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: options.Namespace,
		Subsystem: options.Subsystem,
		Name:      "validation_passed",
		Help:      "Total number of messages validator has confirmed as good ones",
	})
	ValidationError = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: options.Namespace,
		Subsystem: options.Subsystem,
		Name:      "validation_errors",
		Help:      "Total number of messages validator has marked as bad ones",
	})
}
