package monitor

import (
	"github.com/olekukonko/tablewriter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"strings"
	"time"
)

type MetricsServer struct {
	stdoutStop chan struct{}
	ticker     *time.Ticker
}

// Serve serves a server for Prometheus to scrape metrics from.
// If logMilestones is set to true, it periodically dumps metrics to stdout
func (m *MetricsServer) Serve(logMilestones bool) {
	defer func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(":7000", nil)
	}()

	if logMilestones {
		ticker := time.NewTicker(10 * time.Second)
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

func NewMetricsServer() *MetricsServer {
	return &MetricsServer{}
}
