package streaminput

import (
	"context"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/metrics"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/support/msgwaiter"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
)

type componentMetrics struct {
	logger *logrus.Entry

	connected prometheus.Gauge
	inBytes   prometheus.Gauge
	outBytes  prometheus.Gauge

	lastKnownDeletedSeq prometheus.Gauge
	lastKnownSeq        prometheus.Gauge
	lastKnownHeight     prometheus.Gauge

	lastSeekSeq    prometheus.Gauge
	lastSeekHeight prometheus.Gauge

	readerSeq        prometheus.Gauge
	readerHeight     prometheus.Gauge
	readerHeaderSize prometheus.Observer
	readerShardSize  prometheus.Observer

	connectionObservingJob *util.Job
	lastKnownMsgWaiter     *msgwaiter.MsgWaiter
	readerMsgWaiter        *msgwaiter.MsgWaiter
	logJob                 *util.Job
}

func startComponentMetrics(sink metrics.Sink, logInterval time.Duration, logger *logrus.Entry) *componentMetrics {
	readerMsgSize := sink.AddHistogram(
		"streaminput_reader_msg_size",
		"",
		metrics.CreateHistogramBuckets(10, 128*1024*1024, 20, true),
		[]string{"blocktype"},
		nil,
	)

	m := &componentMetrics{
		logger: logger,

		connected: sink.AddGauge("streaminput_connected", "", nil, nil).WithLabelValues(),
		inBytes:   sink.AddGauge("streaminput_in_bytes", "", nil, nil).WithLabelValues(),
		outBytes:  sink.AddGauge("streaminput_out_bytes", "", nil, nil).WithLabelValues(),

		lastKnownDeletedSeq: sink.AddGauge("streaminput_known_deleted_seq", "", nil, nil).WithLabelValues(),
		lastKnownSeq:        sink.AddGauge("streaminput_last_known_seq", "", nil, nil).WithLabelValues(),
		lastKnownHeight:     sink.AddGauge("streaminput_last_known_height", "", nil, nil).WithLabelValues(),

		lastSeekSeq:    sink.AddGauge("streaminput_last_seek_seq", "", nil, nil).WithLabelValues(),
		lastSeekHeight: sink.AddGauge("streaminput_last_seek_height", "", nil, nil).WithLabelValues(),

		readerSeq:        sink.AddGauge("streaminput_reader_seq", "", nil, nil).WithLabelValues(),
		readerHeight:     sink.AddGauge("streaminput_reader_height", "", nil, nil).WithLabelValues(),
		readerHeaderSize: readerMsgSize.WithLabelValues("header"),
		readerShardSize:  readerMsgSize.WithLabelValues("shard"),
	}

	m.lastKnownMsgWaiter = msgwaiter.Start(context.Background(), func(msg blockio.Msg) {
		if dec, _ := msg.GetDecoded(context.Background()); dec != nil && dec.Block != nil {
			m.lastKnownHeight.Set(float64(dec.Block.GetHeight()))
		}
	})
	m.readerMsgWaiter = msgwaiter.Start(context.Background(), func(msg blockio.Msg) {
		if dec, _ := msg.GetDecoded(context.Background()); dec != nil && dec.Block != nil {
			m.readerSeq.Set(float64(msg.Get().GetSequence()))
			m.readerHeight.Set(float64(dec.Block.GetHeight()))
			if dec.Block.GetBlockType() == blocks.Shard {
				m.readerShardSize.Observe(float64(len(msg.Get().GetData())))
			} else {
				m.readerHeaderSize.Observe(float64(len(msg.Get().GetData())))
			}
		}
	})

	if logInterval > 0 {
		m.logJob = util.StartJobLoopWithInterval(context.Background(), logInterval, false, m.log)
	}

	return m
}

func (m *componentMetrics) stop() {
	m.lastKnownMsgWaiter.Stop(context.Background())
	m.readerMsgWaiter.Stop(context.Background())
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
	m.logger.Infof(
		"connected=%d, lastKnownDeletedSeq=%d, lastKnownSeq=%d, lastKnownHeight=%d, lastSeekSeq=%d, lastSeekHeight=%d, readerSeq=%d, readerHeight=%d",
		m.readUint64Gauge(m.connected),
		m.readUint64Gauge(m.lastKnownDeletedSeq),
		m.readUint64Gauge(m.lastKnownSeq),
		m.readUint64Gauge(m.lastKnownHeight),
		m.readUint64Gauge(m.lastSeekSeq),
		m.readUint64Gauge(m.lastSeekHeight),
		m.readUint64Gauge(m.readerSeq),
		m.readUint64Gauge(m.readerHeight),
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
