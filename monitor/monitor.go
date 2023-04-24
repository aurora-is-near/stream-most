package monitor

import (
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	blockProcessorMonitoring "github.com/aurora-is-near/stream-most/service/block_processor/monitoring"
	blockWriterMonitoring "github.com/aurora-is-near/stream-most/service/block_writer/monitoring"
	readerMonitoring "github.com/aurora-is-near/stream-most/stream/reader/monitoring"
	"github.com/olekukonko/tablewriter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"strings"
	"time"
)

type MetricsServer struct {
	Registry *prometheus.Registry
	options  *monitor_options.Options

	stdoutStop chan struct{}
	ticker     *time.Ticker
}

func (m *MetricsServer) registerExports() {
	readerMonitoring.Export(m.options)
	blockProcessorMonitoring.Export(m.options)
	blockWriterMonitoring.Export(m.options)
}

// Serve serves a server for Prometheus to scrape metrics from.
// If logMilestones is set to true, it periodically dumps metrics to stdout
func (m *MetricsServer) Serve(logMilestones bool) {
	defer func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(m.options.ListenAddress, nil)
		// I'm not completely sure if not having a graceful shutdown is safe. Are you?
		// In particular, that it won't panic, will release the socket, port binding etc
	}()

	if logMilestones {
		ticker := time.NewTicker(time.Duration(m.options.StdoutIntervalSeconds) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-m.stdoutStop:
				return
			case <-ticker.C:
				m.spewStdout()
			}
		}
	}
}

func (m *MetricsServer) Stop() {
	close(m.stdoutStop)
}

func (m *MetricsServer) spewStdout() {
	table := tablewriter.NewWriter(os.Stdout)

	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		panic(err)
	}

	for _, mf := range metricFamilies {
		if strings.HasPrefix(mf.GetName(), "go_") {
			// It's a default metric about go stuff, skip it.
			continue
		}
		for _, metric := range mf.GetMetric() {
			table.Append([]string{mf.GetName(), metric.String()})
		}
	}

	table.Render()
}

func NewMetricsServer(options *monitor_options.Options) *MetricsServer {
	m := &MetricsServer{
		options:  options,
		Registry: prometheus.NewRegistry(),
	}
	m.registerExports()
	return m
}
