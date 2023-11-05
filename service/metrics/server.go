package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	ListenAddress string
	Namespace     string
	Subsystem     string
}

// Static assertion
var _ Sink = (*Server)(nil)

type Server struct {
	config     *Config
	registry   *prometheus.Registry
	httpServer *http.Server
	stopped    chan struct{}
	err        error
}

func StartServer(config *Config) *Server {
	srv := &Server{
		config:   config,
		registry: prometheus.NewRegistry(),
		stopped:  make(chan struct{}),
	}

	if config.ListenAddress != "" {
		srv.httpServer = &http.Server{
			Addr:    config.ListenAddress,
			Handler: promhttp.HandlerFor(srv.registry, promhttp.HandlerOpts{}),
		}
		go func() {
			srv.err = srv.httpServer.ListenAndServe()
			close(srv.stopped)
		}()
	}

	return srv
}

func (srv *Server) Stop(wait time.Duration) error {
	if srv.httpServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	if err := srv.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-srv.stopped:
		return nil
	}
}

func (srv *Server) Error() error {
	select {
	case <-srv.stopped:
		return srv.err
	default:
		return nil
	}
}

func (srv *Server) AddGauge(name string, help string, labelNames []string, constLabels map[string]string) *prometheus.GaugeVec {
	gaugeVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   srv.config.Namespace,
			Subsystem:   srv.config.Subsystem,
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		},
		labelNames,
	)
	srv.registry.Register(gaugeVec)
	return gaugeVec
}

func (srv *Server) AddCounter(name string, help string, labelNames []string, constLabels map[string]string) *prometheus.CounterVec {
	counterVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   srv.config.Namespace,
			Subsystem:   srv.config.Subsystem,
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		},
		labelNames,
	)
	srv.registry.Register(counterVec)
	return counterVec
}

func (srv *Server) AddHistogram(name string, help string, buckets []float64, labelNames []string, constLabels map[string]string) *prometheus.HistogramVec {
	histogramVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   srv.config.Namespace,
			Subsystem:   srv.config.Subsystem,
			Name:        name,
			Buckets:     buckets,
			Help:        help,
			ConstLabels: constLabels,
		},
		labelNames,
	)
	srv.registry.Register(histogramVec)
	return histogramVec
}
