package streamoutput

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/metrics"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/support/msgwaiter"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
)

var (
	wstatusSuccess   = "success"
	wstatusError     = "error"
	wstatusDesync    = "desync"
	wstatusCollision = "collision"
	wstatusDuplicate = "duplicate"
)

type componentMetrics struct {
	logger *logrus.Entry

	connected prometheus.Gauge
	inBytes   prometheus.Gauge
	outBytes  prometheus.Gauge

	lastKnownDeletedSeq prometheus.Gauge
	lastKnownSeq        prometheus.Gauge
	lastKnownHeight     prometheus.Gauge

	lastWriteSeq       *prometheus.GaugeVec
	lastWriteSourceSeq *prometheus.GaugeVec
	lastWriteHeight    *prometheus.GaugeVec
	writesCount        *prometheus.CounterVec
	writeSize          *prometheus.HistogramVec

	// For logging
	lastWrittenSeq       atomic.Uint64
	lastWrittenSourceSeq atomic.Uint64
	lastWrittenHeight    atomic.Uint64

	connectionObservingJob *util.Job
	lastKnownMsgWaiter     *msgwaiter.MsgWaiter
	logJob                 *util.Job
}

func startComponentMetrics(sink metrics.Sink, logInterval time.Duration, logger *logrus.Entry) *componentMetrics {
	m := &componentMetrics{
		logger: logger,

		connected: sink.AddGauge("streamoutput_connected", "", nil, nil).WithLabelValues(),
		inBytes:   sink.AddGauge("streamoutput_in_bytes", "", nil, nil).WithLabelValues(),
		outBytes:  sink.AddGauge("streamoutput_out_bytes", "", nil, nil).WithLabelValues(),

		lastKnownDeletedSeq: sink.AddGauge("streamoutput_known_deleted_seq", "", nil, nil).WithLabelValues(),
		lastKnownSeq:        sink.AddGauge("streamoutput_last_known_seq", "", nil, nil).WithLabelValues(),
		lastKnownHeight:     sink.AddGauge("streamoutput_last_known_height", "", nil, nil).WithLabelValues(),

		lastWriteSeq:       sink.AddGauge("streamoutput_last_write_seq", "", []string{"blocktype", "wstatus"}, nil),
		lastWriteSourceSeq: sink.AddGauge("streamoutput_last_write_source_seq", "", []string{"blocktype", "wstatus"}, nil),
		lastWriteHeight:    sink.AddGauge("streamoutput_last_write_height", "", []string{"blocktype", "wstatus"}, nil),
		writesCount:        sink.AddCounter("streamoutput_writes_count", "", []string{"blocktype", "wstatus"}, nil),
		writeSize: sink.AddHistogram(
			"streamoutput_write_size",
			"",
			metrics.CreateHistogramBuckets(10, 128*1024*1024, 20, true),
			[]string{"blocktype", "wstatus"},
			nil,
		),
	}

	m.lastKnownMsgWaiter = msgwaiter.Start(context.Background(), func(msg blockio.Msg) {
		if dec, _ := msg.GetDecoded(context.Background()); dec != nil && dec.Block != nil {
			m.lastKnownHeight.Set(float64(dec.Block.GetHeight()))
		}
	})

	if logInterval > 0 {
		m.logJob = util.StartJobLoopWithInterval(context.Background(), logInterval, false, m.log)
	}

	return m
}

func (m *componentMetrics) stop() {
	m.lastKnownMsgWaiter.Stop(context.Background())
	if m.logJob != nil {
		m.logJob.Stop(context.Background())
	}
}

func (m *componentMetrics) observeConnection(sc *streamconnector.StreamConnector) {
	m.connected.Set(1)
	m.connectionObservingJob = util.StartJobLoopWithInterval(context.Background(), time.Second, true, func(ctx context.Context) error {
		stats := sc.NatsStats()
		m.inBytes.Set(float64(stats.InBytes))
		m.outBytes.Set(float64(stats.OutBytes))
		return nil
	})
}

func (m *componentMetrics) stopConnectionObserving() {
	m.connected.Set(0)
	m.connectionObservingJob.Stop(context.Background())
}

func (m *componentMetrics) log(ctx context.Context) error {
	var successWrites, duplicateWrites, errorWrites, desyncWrites, collisionWrites uint64

	for _, btype := range []string{"header", "shard"} {
		successWrites += m.readUint64Counter(m.writesCount.WithLabelValues(btype, wstatusSuccess))
		duplicateWrites += m.readUint64Counter(m.writesCount.WithLabelValues(btype, wstatusDuplicate))
		errorWrites += m.readUint64Counter(m.writesCount.WithLabelValues(btype, wstatusError))
		desyncWrites += m.readUint64Counter(m.writesCount.WithLabelValues(btype, wstatusDesync))
		collisionWrites += m.readUint64Counter(m.writesCount.WithLabelValues(btype, wstatusCollision))
	}

	m.logger.Infof(
		"connected=%d, lastKnownDeletedSeq=%d, lastKnownSeq=%d, lastKnownHeight=%d, lastWrittenSeq=%d, lastWrittenSourceSeq=%d, lastWrittenHeight=%d, successWrites=%d, duplicateWrites=%d, errorWrites=%d, desyncWrites=%d, collisionWrites=%d",
		m.readUint64Gauge(m.connected),
		m.readUint64Gauge(m.lastKnownDeletedSeq),
		m.readUint64Gauge(m.lastKnownSeq),
		m.readUint64Gauge(m.lastKnownHeight),
		m.lastWrittenSeq.Load(),
		m.lastWrittenSourceSeq.Load(),
		m.lastWrittenHeight.Load(),
		successWrites,
		duplicateWrites,
		errorWrites,
		desyncWrites,
		collisionWrites,
	)
	return nil
}

func (m *componentMetrics) readUint64Gauge(g prometheus.Gauge) uint64 {
	var out io_prometheus_client.Metric
	if err := g.Write(&out); err != nil {
		m.logger.Errorf("unable to extract gauge metric: %v", err)
		return 0
	}
	return uint64(out.Gauge.GetValue())
}

func (m *componentMetrics) readUint64Counter(g prometheus.Counter) uint64 {
	var out io_prometheus_client.Metric
	if err := g.Write(&out); err != nil {
		m.logger.Errorf("unable to extract counter metric: %v", err)
		return 0
	}
	return uint64(out.Counter.GetValue())
}

func (m *componentMetrics) acknowledgeWrite(seq uint64, msg *messages.BlockMessage, status string) {
	btype := "header"
	if msg.Block.GetBlockType() == blocks.Shard {
		btype = "shard"
	}

	m.lastWriteSeq.WithLabelValues(btype, status).Set(float64(seq))
	m.lastWriteSourceSeq.WithLabelValues(btype, status).Set(float64(msg.Msg.GetSequence()))
	m.lastWriteHeight.WithLabelValues(btype, status).Set(float64(msg.Block.GetHeight()))
	m.writesCount.WithLabelValues(btype, status).Inc()
	m.writeSize.WithLabelValues(btype, status).Observe(float64(len(msg.Msg.GetData())))

	if status == wstatusSuccess || status == wstatusDuplicate {
		m.lastWrittenSeq.Store(seq)
		m.lastWrittenSourceSeq.Store(msg.Msg.GetSequence())
		m.lastWrittenHeight.Store(msg.Block.GetHeight())
	}
}
