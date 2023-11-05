package metrics

import "github.com/prometheus/client_golang/prometheus"

type Sink interface {
	AddGauge(name string, help string, labelNames []string, constLabels map[string]string) *prometheus.GaugeVec
	AddCounter(name string, help string, labelNames []string, constLabels map[string]string) *prometheus.CounterVec
	AddHistogram(name string, help string, buckets []float64, labelNames []string, constLabels map[string]string) *prometheus.HistogramVec
}

// Static assertion
var _ Sink = (*labeler)(nil)

type labeler struct {
	upstream    Sink
	constLabels map[string]string
}

func (l *labeler) extendConstLabels(otherLabels map[string]string) map[string]string {
	resultLabels := make(map[string]string)
	for k, v := range l.constLabels {
		resultLabels[k] = v
	}
	for k, v := range otherLabels {
		resultLabels[k] = v
	}
	return resultLabels
}

func (l *labeler) AddGauge(name string, help string, labelNames []string, constLabels map[string]string) *prometheus.GaugeVec {
	return l.upstream.AddGauge(name, help, labelNames, l.extendConstLabels(constLabels))
}

func (l *labeler) AddCounter(name string, help string, labelNames []string, constLabels map[string]string) *prometheus.CounterVec {
	return l.upstream.AddCounter(name, help, labelNames, l.extendConstLabels(constLabels))
}

func (l *labeler) AddHistogram(name string, help string, buckets []float64, labelNames []string, constLabels map[string]string) *prometheus.HistogramVec {
	return l.upstream.AddHistogram(name, help, buckets, labelNames, l.extendConstLabels(constLabels))
}

func WithConstLabels(upstream Sink, constLabels map[string]string) Sink {
	return &labeler{
		upstream:    upstream,
		constLabels: constLabels,
	}
}
