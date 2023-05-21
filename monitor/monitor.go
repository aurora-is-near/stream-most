package monitor

import (
	"context"
	"github.com/aurora-is-near/stream-most/monitor/monitor_options"
	blockProcessorMonitoring "github.com/aurora-is-near/stream-most/service/block_processor/monitoring"
	blockWriterMonitoring "github.com/aurora-is-near/stream-most/service/block_writer/monitoring"
	readerMonitoring "github.com/aurora-is-near/stream-most/stream/reader/monitoring"
	"github.com/aurora-is-near/stream-most/support/when_interrupted"
	"github.com/olekukonko/tablewriter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"strings"
	"time"
)

// MetricsServer serves metrics for Prometheus to scrape.
// Metrics are pluggable: add your own at registerExports() function below
type MetricsServer struct {
	Registry *prometheus.Registry
	options  *monitor_options.Options

	stdoutStop chan struct{}
}

// registerExports is a place where you register all your additional metrics
func (m *MetricsServer) registerExports() {
	readerMonitoring.Export(m.options)
	blockProcessorMonitoring.Export(m.options)
	blockWriterMonitoring.Export(m.options)
}

// Serve serves a server for Prometheus to scrape metrics from.
// If logMilestones is set to true, it periodically dumps metrics to stdout
func (m *MetricsServer) Serve(ctx context.Context, logMilestones bool) {
	defer func() {
		server := &http.Server{Addr: m.options.ListenAddress}
		http.Handle("/metrics", promhttp.Handler())
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logrus.Fatalf("Monitoring server ListenAndServe(): %v", err)
		}
		when_interrupted.Call(func() {
			_ = server.Shutdown(ctx)
		})
	}()

	if logMilestones {
		go m.serveStdout(ctx)
	}
}

// serveStdout spits metrics to stdout every stdoutIntervalSeconds
func (m *MetricsServer) serveStdout(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.options.StdoutIntervalSeconds) * time.Second)

	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stdoutStop:
			return
		case <-ticker.C:
			m.spewStdout()
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
		logrus.Error(err)
		return
	}

	for _, mf := range metricFamilies {
		if strings.HasPrefix(mf.GetName(), "go_") {
			// It's a default metric about go stuff, skip it.
			continue
		}
		if strings.HasPrefix(mf.GetName(), "promhttp_") {
			// It's promhttp's own metrics, skip it.
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
