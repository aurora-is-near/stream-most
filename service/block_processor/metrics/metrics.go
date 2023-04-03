package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	AnnouncementsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "announcements_processed",
		Help: "Total number of block announcements processed by the block processor",
	})
	ShardsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shards_processed",
		Help: "Total number of block shards processed by the block processor",
	})
)
