package streaminput

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/service/blockdecode"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/streamseek"
	"github.com/aurora-is-near/stream-most/service/streamstate"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/sirupsen/logrus"
)

var (
	ErrNotFetchedYet     = fmt.Errorf("not fetched yet")
	ErrConnectionProblem = fmt.Errorf("connection problem")
	ErrStopped           = fmt.Errorf("stopped")
)

// Static assertion
var _ blockio.Input = (*Input)(nil)

type Input struct {
	logger *logrus.Entry

	config *Config
	sc     *streamconnector.StreamConnector

	curSession atomic.Pointer[session]
	sessionMtx sync.Mutex // Prevents seeking after finish

	curState        atomic.Pointer[streamstate.State]
	stateFetchErrCh chan error

	ctx      context.Context
	cancel   func()
	finished chan struct{}
}

func Start(config *Config) *Input {
	blockdecode.EnsureDecodersRunning()

	in := &Input{
		logger: logrus.
			WithField("component", "streaminput").
			WithField("stream", config.Conn.Stream.Name).
			WithField("tag", config.Conn.Nats.LogTag),

		config:   config,
		finished: make(chan struct{}),
	}

	in.curSession.Store(newSession(nil, 0).close(nil))

	in.curState.Store(&streamstate.State{
		Err: fmt.Errorf("%w (%w)", ErrNotFetchedYet, blockio.ErrTemporarilyUnavailable),
	})

	in.ctx, in.cancel = context.WithCancel(context.Background())

	in.logger.Infof("Starting")
	go in.run()

	return in
}

func (in *Input) State() (blockio.State, error) {
	s := in.curState.Load()
	if s.Err != nil {
		return nil, s.Err
	}
	return s, nil
}

func (in *Input) Blocks() <-chan blockio.Msg {
	return in.curSession.Load().ch
}

func (in *Input) Error() error {
	return in.curSession.Load().getError()
}

func (in *Input) SeekNextBlock(block blocks.Block) {
	in.seek(&seekSettings{
		seekBlockAfter: block,
	})
}

func (in *Input) SeekSeq(seq uint64) {
	in.seek(&seekSettings{
		seekSeq: seq,
	})
}

func (in *Input) Stop(wait bool) {
	in.logger.Infof("Stopping")
	in.cancel()
	if wait {
		<-in.finished
	}
}

func (in *Input) seek(seekOpts *seekSettings) {
	in.sessionMtx.Lock()
	defer in.sessionMtx.Unlock()

	/*
		It's not allowed to initiate new sessions after input is finished,
		because last session always holds the final error.
	*/
	select {
	case <-in.finished:
		return
	default:
	}

	s := newSession(seekOpts, in.config.BufferSize)
	prev := in.curSession.Swap(s)
	close(prev.outdated)
}

func (in *Input) run() {
	err := in.runReconnectionLoop()

	if errors.Is(err, ErrStopped) {
		in.logger.Infof("Stopped externally")
	} else {
		in.logger.Errorf("Finished with error: %v", err)
	}

	in.sessionMtx.Lock()
	defer in.sessionMtx.Unlock()
	defer close(in.finished)

	if err == nil {
		// This should never happen
		err = fmt.Errorf("finished for unknown reason")
	}
	err = fmt.Errorf("%w (%w)", err, blockio.ErrCompletelyUnavailable)

	s := in.curSession.Load()
	if s.isClosed() {
		// If current session is already closed - create new dummy session with final error
		in.curSession.Store(newSession(nil, 0).close(err))
		close(s.outdated)
	} else {
		// Otherwise just close current session with final error
		s.close(err)
	}

	in.curState.Store(&streamstate.State{
		Err: err,
	})
}

func (in *Input) runReconnectionLoop() error {
	var lastErr error
	for i := 0; in.config.MaxReconnects < 0 || i <= in.config.MaxReconnects; i++ {
		select {
		case <-in.ctx.Done():
			return ErrStopped
		default:
		}

		if i > 0 {
			in.curState.Store(&streamstate.State{
				Err: fmt.Errorf("%w (%w)", lastErr, blockio.ErrTemporarilyUnavailable),
			})

			in.logger.Infof("Sleeping for %s before next reconnection...", in.config.ReconnectDelay.String())
			if !util.CtxSleep(in.ctx, in.config.ReconnectDelay) {
				return ErrStopped
			}
		}

		in.logger.Infof("Connecting stream...")
		if in.sc, lastErr = streamconnector.Connect(in.config.Conn); lastErr != nil {
			in.logger.Errorf("Unable to connect: %v", lastErr)
			lastErr = fmt.Errorf("%w: %w", ErrConnectionProblem, lastErr)
			continue
		}

		lastErr = in.runMultisessionLoop()

		if errors.Is(lastErr, ErrStopped) {
			in.logger.Infof("Got stopped externally, finishing...")
		} else {
			in.logger.Errorf("Got error: %v", lastErr)
		}

		in.logger.Infof("Disconnecting stream...")
		in.sc.Disconnect()
		in.sc = nil

		if !errors.Is(lastErr, ErrConnectionProblem) {
			return lastErr
		}
	}

	return fmt.Errorf("max reconnects (%d) exceeded: %w", in.config.MaxReconnects, lastErr)
}

func (in *Input) runMultisessionLoop() error {
	in.stateFetchErrCh = make(chan error, 1)
	stateFetcher := streamstate.StartFetcher(in.sc.Stream(), in.config.StateFetchInterval, true, func(s *streamstate.State) {
		if s.Err != nil {
			in.stateFetchErrCh <- fmt.Errorf("unable to fetch state: %w (%w)", s.Err, ErrConnectionProblem)
		} else {
			in.curState.Store(s)
		}
	})
	defer stateFetcher.Stop(true)

	for {
		select {
		case <-in.ctx.Done():
			return ErrStopped
		case err := <-in.stateFetchErrCh:
			return err
		default:
		}

		s := in.curSession.Load()

		if s.isClosed() {
			select {
			case <-in.ctx.Done():
				return ErrStopped
			case err := <-in.stateFetchErrCh:
				return err
			case <-s.outdated:
				continue
			}
		}

		if err := in.handleSession(s); err != nil {
			return err
		}
	}
}

func (in *Input) handleSession(s *session) error {
	if s.nextSeq == 0 {
		in.logger.Infof("Starting new reading session. Performing seek...")
		seekErrCh, cancelSeek := s.runSeek(in.sc.Stream(), in.config.StartSeq, in.config.EndSeq)
		select {
		case <-in.ctx.Done():
			cancelSeek(true)
			return ErrStopped
		case err := <-in.stateFetchErrCh:
			cancelSeek(true)
			return err
		case <-s.outdated:
			cancelSeek(true)
			return nil
		case err := <-seekErrCh:
			cancelSeek(false)
			if err != nil {
				if errors.Is(err, streamseek.ErrEmptyRange) {
					in.logger.Infof("Seek result: nothing to read, closing session immediately")
					s.close(nil)
					return nil
				}
				return fmt.Errorf("unable to perform seek: %w (%w)", err, ErrConnectionProblem)
			}
			in.logger.Infof("Seek done: nextSeq=%d", s.nextSeq)
		}
	} else {
		in.logger.Infof("Resuming reading session from nextSeq=%d", s.nextSeq)
	}

	rcv := newReceiver(s)

	in.logger.Infof("Starting reader from seq=%d", s.nextSeq)
	reader, err := reader.Start(
		in.sc.Stream(),
		&reader.Config{
			FilterSubjects: in.config.FilterSubjects,
			StartSeq:       s.nextSeq,
			EndSeq:         in.config.EndSeq,
			MaxSilence:     in.config.MaxSilence,
		},
		rcv,
	)
	if err != nil {
		return fmt.Errorf("unable to start reader: %w (%w)", err, ErrConnectionProblem)
	}
	defer reader.Stop(true)

	select {
	case <-in.ctx.Done():
		return ErrStopped
	case err := <-in.stateFetchErrCh:
		return err
	case <-s.outdated:
		return nil
	case err := <-rcv.errCh:
		if err == nil {
			s.close(nil)
			return nil
		}
		return fmt.Errorf("got reader error: %w (%w)", err, ErrConnectionProblem)
	}
}
