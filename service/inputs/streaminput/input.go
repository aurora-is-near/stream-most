package streaminput

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/metrics"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/sirupsen/logrus"
)

var (
	ErrConnectionProblem = fmt.Errorf("connection problem")
	ErrStopped           = fmt.Errorf("stopped")
)

// Static assertion
var _ blockio.Input = (*Input)(nil)

type Input struct {
	logger  *logrus.Entry
	metrics *componentMetrics

	config *Config

	curConn    atomic.Pointer[connection]
	curSession atomic.Pointer[session]

	ctx      context.Context
	cancel   func()
	finished chan struct{}
	finalErr error
}

func Start(config *Config, metricSink metrics.Sink) *Input {
	in := &Input{
		logger: logrus.
			WithField("component", "streaminput").
			WithField("stream", config.Conn.Stream.Name).
			WithField("nats", config.Conn.Nats.LogTag),

		config:   config,
		finished: make(chan struct{}),
	}

	in.metrics = startComponentMetrics(metricSink, in.config.LogInterval, in.logger)

	in.curSession.Store(newSession(nil, 0).finalize(nil))

	in.ctx, in.cancel = context.WithCancel(context.Background())

	in.logger.Infof("Starting...")
	go in.run()

	return in
}

func (in *Input) Stop(wait bool) {
	in.logger.Infof("Stopping")
	in.cancel()
	if wait {
		<-in.finished
	}
}

func (in *Input) run() {
	defer in.metrics.stop()

	in.finalErr = in.runLoop()
	if errors.Is(in.finalErr, ErrStopped) {
		in.logger.Infof("Stopped externally")
	} else {
		in.logger.Errorf("Finished with error: %v", in.finalErr)
	}
	in.finalErr = fmt.Errorf("%w: %w", blockio.ErrCompletelyUnavailable, in.finalErr)
	close(in.finished)
	in.curSession.Load().finalize(in.finalErr)
}

func (in *Input) runLoop() error {
	for i := 0; in.ctx.Err() == nil; i++ {
		err := in.runConnection()
		if !errors.Is(err, ErrConnectionProblem) {
			return err
		}

		in.logger.Errorf("Got connection problem: %v", err)
		if in.config.MaxReconnects >= 0 && i >= in.config.MaxReconnects {
			return fmt.Errorf("max reconnects (%d) exceeded: %w", in.config.MaxReconnects, err)
		}

		in.logger.Infof("Sleeping for %s before next reconnection...", in.config.ReconnectDelay.String())
		if !util.CtxSleep(in.ctx, in.config.ReconnectDelay) {
			return ErrStopped
		}
	}
	return ErrStopped
}

func (in *Input) runConnection() error {
	conn, err := startConnection(in)
	if err != nil {
		return err
	}
	defer conn.stop(true)
	in.curConn.Store(conn)

	select {
	case <-conn.hasErr:
		return conn.err
	case <-in.ctx.Done():
		in.logger.Infof("Got stopped externally, stopping connection...")
		return ErrStopped
	}
}

func (in *Input) getConn() (*connection, error) {
	select {
	case <-in.finished:
		return nil, in.finalErr
	default:
	}

	conn := in.curConn.Load()
	if conn == nil {
		return nil, fmt.Errorf("%w: not started yet", blockio.ErrTemporarilyUnavailable)
	}
	return conn, nil
}

func (in *Input) LastKnownDeletedSeq() (uint64, error) {
	conn, err := in.getConn()
	if err != nil {
		return 0, err
	}
	return conn.getLastKnownDeletedSeq()
}

func (in *Input) LastKnownSeq() (uint64, error) {
	conn, err := in.getConn()
	if err != nil {
		return 0, err
	}
	return conn.getLastKnownSeq()
}

func (in *Input) LastKnownMessage() (blockio.Msg, uint64, error) {
	conn, err := in.getConn()
	if err != nil {
		return nil, 0, err
	}
	return conn.getLastKnownMsg()
}

func (in *Input) CurrentSession() blockio.InputSession {
	return in.curSession.Load()
}

func (in *Input) SeekBlock(block blocks.Block, startSeq uint64, endSeq uint64, onlyGreater bool) blockio.InputSession {
	return in.seek(&seekOptions{seekBlock: block, startSeq: startSeq, endSeq: endSeq, seekOnlyGreaterBlocks: onlyGreater})
}

func (in *Input) SeekSeq(seq uint64, startSeq uint64, endSeq uint64) blockio.InputSession {
	return in.seek(&seekOptions{seekSeq: seq, startSeq: startSeq, endSeq: endSeq})
}

func (in *Input) seek(seekOpts *seekOptions) blockio.InputSession {
	s := newSession(seekOpts, in.config.BufferSize)

	oldSession := in.curSession.Swap(s)
	close(oldSession.outdated)
	oldSession.finalize(blockio.ErrReseeked)

	select {
	case <-in.finished:
		s.finalize(in.finalErr)
	default:
	}

	return s
}
