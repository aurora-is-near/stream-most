package monitoring

import (
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TipHeight       prometheus.Gauge
	LastWriteHeight prometheus.Gauge
)

func Export(opts *monitor_options.Options) {
	TipHeight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        "writer_tip_height",
		Help:        "Height of the last fully written block on the output",
		ConstLabels: nil,
	})
	LastWriteHeight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        "writer_last_height",
		Help:        "Height of the last block written by the writer",
		ConstLabels: nil,
	})
}
