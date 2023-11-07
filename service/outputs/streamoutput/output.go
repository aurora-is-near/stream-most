package streamoutput

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/metrics"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/sirupsen/logrus"
)

var (
	ErrConnectionProblem = fmt.Errorf("connection problem")
	ErrInvalidCfg        = fmt.Errorf("invalid cfg")
	ErrStopped           = fmt.Errorf("stopped")
)

// Static assertion
var _ blockio.Output = (*Output)(nil)

type Output struct {
	logger  *logrus.Entry
	metrics *componentMetrics

	config *Config

	curConn atomic.Pointer[connection]

	ctx      context.Context
	cancel   func()
	finished chan struct{}
	finalErr error
}

func Start(config *Config, metricSink metrics.Sink) *Output {
	out := &Output{
		logger: logrus.
			WithField("component", "streamoutput").
			WithField("stream", config.Conn.Stream.Name).
			WithField("nats", config.Conn.Nats.LogTag),

		config:   config,
		finished: make(chan struct{}),
	}

	out.metrics = startComponentMetrics(metricSink, out.config.LogInterval, out.logger)

	out.ctx, out.cancel = context.WithCancel(context.Background())

	out.logger.Infof("Starting...")
	go out.run()

	return out
}

func (out *Output) Stop(wait bool) {
	out.logger.Infof("Stopping")
	out.cancel()
	if wait {
		<-out.finished
	}
}

func (out *Output) run() {
	defer out.metrics.stop()

	out.finalErr = out.runLoop()
	if errors.Is(out.finalErr, ErrStopped) {
		out.logger.Infof("Stopped externally")
	} else {
		out.logger.Errorf("Finished with error: %v", out.finalErr)
	}
	out.finalErr = fmt.Errorf("%w: %w", blockio.ErrCompletelyUnavailable, out.finalErr)
	close(out.finished)
}

func (out *Output) runLoop() error {
	for i := 0; out.ctx.Err() == nil; i++ {
		err := out.runConnection()
		if !errors.Is(err, ErrConnectionProblem) {
			return err
		}

		out.logger.Errorf("Got connection problem: %v", err)
		if out.config.MaxReconnects >= 0 && i >= out.config.MaxReconnects {
			return fmt.Errorf("max reconnects (%d) exceeded: %w", out.config.MaxReconnects, err)
		}

		out.logger.Infof("Sleeping for %s before next reconnection...", out.config.ReconnectDelay.String())
		if !util.CtxSleep(out.ctx, out.config.ReconnectDelay) {
			return ErrStopped
		}
	}
	return ErrStopped
}

func (out *Output) runConnection() error {
	conn, err := startConnection(out)
	if err != nil {
		return err
	}
	defer conn.stop(true)
	out.curConn.Store(conn)

	select {
	case <-conn.hasErr:
		return conn.err
	case <-out.ctx.Done():
		out.logger.Infof("Got stopped externally, stopping connection...")
		return ErrStopped
	}
}

func (out *Output) getConn() (*connection, error) {
	select {
	case <-out.finished:
		return nil, out.finalErr
	default:
	}

	conn := out.curConn.Load()
	if conn == nil {
		return nil, fmt.Errorf("%w: not started yet", blockio.ErrTemporarilyUnavailable)
	}
	return conn, nil
}

func (out *Output) LastKnownDeletedSeq() (uint64, error) {
	conn, err := out.getConn()
	if err != nil {
		return 0, err
	}
	return conn.getLastKnownDeletedSeq()
}

func (out *Output) LastKnownSeq() (uint64, error) {
	conn, err := out.getConn()
	if err != nil {
		return 0, err
	}
	return conn.getLastKnownSeq()
}

func (out *Output) LastKnownMessage() (blockio.Msg, uint64, error) {
	conn, err := out.getConn()
	if err != nil {
		return nil, 0, err
	}
	return conn.getLastKnownMsg()
}

func (out *Output) ProtectedWrite(ctx context.Context, predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage) error {
	conn, err := out.getConn()
	if err != nil {
		return err
	}
	return conn.protectedWrite(ctx, predecessorSeq, predecessorMsgID, msg)
}
