package streaminput

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockdecode"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/streamseek"
	"github.com/aurora-is-near/stream-most/service/streamstate"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
)

type connection struct {
	in *Input

	sc *streamconnector.StreamConnector

	lastKnownDeletedSeq atomic.Uint64
	lastKnownSeq        atomic.Uint64
	lastKnownMsg        atomic.Pointer[blockio.Msg]
	firstStateFetchDone atomic.Bool

	err      error
	hasErr   chan struct{}
	errOnce  sync.Once
	finished chan struct{}
}

func startConnection(in *Input) (*connection, error) {
	c := &connection{
		in:       in,
		hasErr:   make(chan struct{}),
		finished: make(chan struct{}),
	}

	in.logger.Infof("Connecting stream...")
	var err error
	if c.sc, err = streamconnector.Connect(in.config.Conn); err != nil {
		in.logger.Errorf("Unable to connect stream: %v", err)
		return nil, fmt.Errorf("unable to connect stream: %w (%w)", err, ErrConnectionProblem)
	}

	go c.run()

	return c, nil
}

func (c *connection) setError(err error) *connection {
	c.errOnce.Do(func() {
		c.err = err
		close(c.hasErr)
	})
	return c
}

func (c *connection) getError() error {
	select {
	case <-c.hasErr:
		return c.err
	default:
		return nil
	}
}

func (c *connection) stop(wait bool) {
	c.setError(ErrStopped)
	if wait {
		<-c.finished
	}
}

func (c *connection) run() {
	defer close(c.finished)
	defer c.sc.Disconnect()
	defer c.in.logger.Infof("Disconnecting stream...")

	stateFetcher := streamstate.StartFetcher(c.sc.Stream(), c.in.config.StateFetchInterval, true, func(s *streamstate.State) {
		if s.Err != nil {
			c.in.logger.Errorf("unable to fetch stream state: %v", s.Err)
			c.setError(fmt.Errorf("unable to fetch stream state: %w (%w)", s.Err, ErrConnectionProblem))
			return
		}
		if s.Info.State.FirstSeq > 0 {
			c.updateLastKnownDeletedSeq(s.Info.State.FirstSeq - 1)
		}
		c.updateLastKnownSeq(s.Info.State.LastSeq)
		if s.LastMsg != nil {
			c.updateLastKnownMsg(s.LastMsg)
		}
		c.firstStateFetchDone.Store(true)
	})
	defer stateFetcher.Stop(true)

	for {
		select {
		case <-c.hasErr:
			return
		default:
			c.handleSession(c.in.curSession.Load())
		}
	}
}

func (c *connection) handleSession(s *session) {
	select {
	case <-c.hasErr:
		return
	case <-s.outdated:
		return
	default:
	}

	if !s.acquire() {
		select {
		case <-c.hasErr:
		case <-s.outdated:
		}
		return
	}
	defer s.release()

	if s.nextSeq == 0 {
		c.in.logger.Infof("Starting new reading session. Performing seek...")
		if !c.seekSession(s) {
			return
		}
	} else {
		c.in.logger.Infof("Resuming reading session from nextSeq=%d", s.nextSeq)
	}

	rcv := reader.NewCbReceiver().WithHandleMsgCb(func(ctx context.Context, msg messages.NatsMessage) {
		m, ok := blockdecode.ScheduleBlockDecoding(ctx, msg)
		if !ok {
			return
		}

		c.updateLastKnownMsg(m)

		select {
		case <-ctx.Done():
			return
		case s.ch <- m:
		}

		s.nextSeq = msg.GetSequence() + 1
	})

	rcv = rcv.WithHandleNewKnownSeqCb(func(seq uint64) {
		c.updateLastKnownSeq(seq)
	})

	rcv = rcv.WithHandleFinishCb(func(err error) {
		if err == nil {
			s.finalize(nil)
			return
		}
		c.setError(fmt.Errorf("got reader error: %w (%w)", err, ErrConnectionProblem))
	})

	c.in.logger.Infof("Starting reader from seq=%d", s.nextSeq)
	reader, err := reader.Start(
		c.sc.Stream(),
		&reader.Config{
			FilterSubjects: c.in.config.FilterSubjects,
			StartSeq:       s.nextSeq,
			EndSeq:         s.seekOpts.endSeq,
			MaxSilence:     c.in.config.MaxSilence,
			LogTag:         c.in.config.Conn.Stream.LogTag,
		},
		rcv,
	)
	if err != nil {
		c.setError(fmt.Errorf("unable to start reader: %w (%w)", err, ErrConnectionProblem))
		return
	}
	defer reader.Stop(true)

	select {
	case <-c.hasErr:
	case <-s.finalized:
	case <-s.outdated:
	}
}

func (c *connection) seekSession(s *session) bool {
	seekCtx, cancelSeek := context.WithCancel(context.Background())
	seekResCh := s.seekOpts.seek(seekCtx, c.sc.Stream())
	defer func() {
		cancelSeek()
		<-seekResCh
	}()

	select {
	case <-c.hasErr:
		return false
	case <-s.finalized:
		return false
	case <-s.outdated:
		return false
	case seekRes := <-seekResCh:
		if seekRes.err != nil {
			if errors.Is(seekRes.err, streamseek.ErrEmptyRange) {
				c.in.logger.Infof("Seek result: nothing to read, closing session immediately")
				s.finalize(nil)
				return false
			}
			c.setError(fmt.Errorf("unable to perform seek: %w (%w)", seekRes.err, ErrConnectionProblem))
			return false
		}
		s.nextSeq = seekRes.seq
		return true
	}
}

func (c *connection) updateLastKnownDeletedSeq(seq uint64) {
	for { // CAS-based atomic maximum
		prev := c.lastKnownDeletedSeq.Load()
		if prev >= seq || c.lastKnownDeletedSeq.CompareAndSwap(prev, seq) {
			return
		}
	}
}

func (c *connection) updateLastKnownSeq(seq uint64) {
	for { // CAS-based atomic maximum
		prev := c.lastKnownSeq.Load()
		if prev >= seq || c.lastKnownSeq.CompareAndSwap(prev, seq) {
			return
		}
	}
}

func (c *connection) updateLastKnownMsg(msg blockio.Msg) {
	c.updateLastKnownSeq(msg.Get().GetSequence())
	for { // CAS-based atomic maximum
		prev := c.lastKnownMsg.Load()
		if prev != nil && *prev != nil && (*prev).Get().GetSequence() >= msg.Get().GetSequence() {
			return
		}
		if c.lastKnownMsg.CompareAndSwap(prev, &msg) {
			return
		}
	}
}

func (c *connection) getStateError() error {
	if err := c.getError(); err != nil {
		return fmt.Errorf("%w (%w)", blockio.ErrTemporarilyUnavailable, err)
	}
	if !c.firstStateFetchDone.Load() {
		return fmt.Errorf("not fetched yet (%w)", blockio.ErrTemporarilyUnavailable)
	}
	return nil
}

func (c *connection) getLastKnownDeletedSeq() (uint64, error) {
	if err := c.getStateError(); err != nil {
		return 0, err
	}
	return c.lastKnownDeletedSeq.Load(), nil
}

func (c *connection) getLastKnownSeq() (uint64, error) {
	if err := c.getStateError(); err != nil {
		return 0, err
	}
	return c.lastKnownSeq.Load(), nil
}

func (c *connection) getLastKnownMsg() (blockio.Msg, error) {
	if err := c.getStateError(); err != nil {
		return nil, err
	}
	msg := c.lastKnownMsg.Load()
	if msg == nil {
		return nil, nil
	}
	return *msg, nil
}
