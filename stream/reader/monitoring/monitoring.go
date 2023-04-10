package monitoring

import (
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	LastReadSequence prometheus.Gauge
)

func Export(options *monitor_options.Options) {
	LastReadSequence = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "reader_last_sequence",
		Help:      "Last sequence that was read by the reader",
		Namespace: options.Namespace,
		Subsystem: options.Subsystem,
	})
}
