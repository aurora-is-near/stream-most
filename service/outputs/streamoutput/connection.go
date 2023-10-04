package streamoutput

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/blockdecode"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/streamstate"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type connection struct {
	out *Output

	sc             *streamconnector.StreamConnector
	subjectPattern string

	lastKnownDeletedSeq atomic.Uint64
	lastKnownSeq        atomic.Uint64
	lastKnownMsg        atomic.Pointer[blockio.Msg]
	firstStateFetchDone atomic.Bool

	err       error
	hasErr    chan struct{}
	errOnce   sync.Once
	clientsWg sync.WaitGroup
	finished  chan struct{}
}

func startConnection(out *Output) (*connection, error) {
	c := &connection{
		out:      out,
		hasErr:   make(chan struct{}),
		finished: make(chan struct{}),
	}

	out.logger.Infof("Connecting stream...")
	var err error
	if c.sc, err = streamconnector.Connect(out.config.Conn); err != nil {
		out.logger.Errorf("Unable to connect stream: %v", err)
		return nil, fmt.Errorf("unable to connect stream: %w (%w)", err, ErrConnectionProblem)
	}

	if err := c.configureSubjectPattern(); err != nil {
		out.logger.Errorf("Can't configure subject pattern, disconnecting stream... (%v)", err)
		c.sc.Disconnect()
		return nil, err
	}

	go c.run()

	return c, nil
}

func (c *connection) configureSubjectPattern() error {
	c.subjectPattern = c.out.config.SubjectPattern
	if c.subjectPattern == "" {
		c.out.logger.Infof("Writing subject pattern not provided, figuring it out automatically...")
		subjects, err := c.sc.Stream().GetConfigSubjects(c.out.ctx)
		if err != nil {
			if c.out.ctx.Err() != nil && errors.Is(err, c.out.ctx.Err()) {
				return ErrStopped
			}
			return fmt.Errorf("%w: can't get stream subjects: %w", ErrConnectionProblem, err)
		}
		if len(subjects) != 1 {
			return fmt.Errorf(
				"unable to recognize subject automatically, expected just one subject, got [%s] (%w)",
				strings.Join(subjects, ", "),
				ErrInvalidCfg,
			)
		}
		c.subjectPattern = subjects[0]
		c.out.logger.Infof("Selected subject pattern '%s'", c.subjectPattern)
	}
	return nil
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
	defer c.out.logger.Infof("Disconnecting stream...")
	defer c.clientsWg.Wait()

	stateFetcher := streamstate.StartFetcher(c.sc.Stream(), c.out.config.StateFetchInterval, true, func(s *streamstate.State) {
		if s.Err != nil {
			c.out.logger.Errorf("unable to fetch stream state: %v", s.Err)
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

	<-c.hasErr
}

func (c *connection) acquire() error {
	if err := c.getError(); err != nil {
		return fmt.Errorf("%w (%w)", blockio.ErrTemporarilyUnavailable, err)
	}

	c.clientsWg.Add(1)

	if err := c.getError(); err != nil {
		c.clientsWg.Done()
		return fmt.Errorf("%w (%w)", blockio.ErrTemporarilyUnavailable, err)
	}

	return nil
}

func (c *connection) protectedWrite(ctx context.Context, predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage) error {
	if err := c.acquire(); err != nil {
		return err
	}
	defer c.clientsWg.Done()

	if err := c.doProtectedWrite(ctx, predecessorSeq, predecessorMsgID, msg); err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return fmt.Errorf("%w (%w)", ctx.Err(), blockio.ErrCanceled)
		}
		if errors.Is(err, ErrConnectionProblem) {
			c.out.logger.Errorf("unable to perform protected write: %v", err)
			c.setError(fmt.Errorf("unable to perform protected write: %w", err))
			return fmt.Errorf("%w (%w)", err, blockio.ErrTemporarilyUnavailable)
		}
		c.out.logger.Errorf("Got failed write attempt: %v", err)
		return err
	}

	return nil
}

func (c *connection) doProtectedWrite(ctx context.Context, predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage) error {
	wMsg := &nats.Msg{
		Header: make(nats.Header),
		Data:   msg.Msg.GetData(),
	}

	switch msg.Block.GetBlockType() {
	case blocks.Announcement:
		wMsg.Subject = strings.ReplaceAll(c.subjectPattern, "*", "header")
	case blocks.Shard:
		wMsg.Subject = strings.ReplaceAll(c.subjectPattern, "*", strconv.FormatUint(msg.Block.GetShardID(), 10))
	default:
		wMsg.Subject = strings.ReplaceAll(c.subjectPattern, "*", "unknown")
	}

	if c.out.config.PreserveCustomHeaders {
		for k, v := range msg.Msg.GetHeader() {
			if _, ok := serviceHeaders[k]; !ok {
				wMsg.Header[k] = v
			}
		}
	}

	msgID := blocks.ConstructMsgID(msg.Block)
	opts := []jetstream.PublishOpt{
		jetstream.WithMsgID(msgID),
		jetstream.WithRetryWait(c.out.config.WriteRetryWait),
		jetstream.WithRetryAttempts(c.out.config.WriteRetryAttempts),
		jetstream.WithExpectLastSequence(predecessorSeq),
	}
	if predecessorMsgID != "" {
		opts = append(opts, jetstream.WithExpectLastMsgID(predecessorMsgID))
	}

	ack, writeErr := c.sc.Stream().Write(ctx, wMsg, opts...)
	if writeErr != nil {
		if !isFailedExpectErr(writeErr) {
			return fmt.Errorf("unable to write msgid='%s' at seq=%d: %w (%w)", msgID, predecessorSeq+1, writeErr, ErrConnectionProblem)
		}

		c.out.logger.Warnf("Got failed expected-predecessor checks on seq=%d and msgid='%s' (%v), running analysis...", predecessorSeq+1, msgID, writeErr)
		if predecessorMsgID != "" {
			c.out.logger.Infof("Btw expected predecessor's msgid='%s'", predecessorMsgID)
		}

		analysis, err := c.analyzeVicinity(ctx, predecessorSeq+1)
		if err != nil {
			return err
		}

		if realMsg, realMsgPresent := analysis[predecessorSeq+1]; realMsgPresent {
			if realMsg == nil {
				return fmt.Errorf("can't check write on seq=%d (%w)", predecessorSeq+1, blockio.ErrRemovedPosition)
			}
			if realMsgID := realMsg.GetHeader().Get(jetstream.MsgIDHeader); realMsgID != msgID {
				return fmt.Errorf("expected msgid='%s' on seq=%d but already got '%s' (%w)", msgID, predecessorSeq+1, realMsgID, blockio.ErrCollision)
			}
			c.out.logger.Infof("msgid on this seq is actually right, we probably just fell out of dedup window, ignoring...")
			c.saveLastWrittenMsg(wMsg, msg.Block, predecessorSeq+1, msgID)
			return nil
		}

		if predecessorSeq == 0 {
			if predecessorMsgID != "" {
				return fmt.Errorf("it's not possible to have predesessor with msgid='%s' and seq=%d (%w)", predecessorMsgID, predecessorSeq, blockio.ErrWrongPredecessor)
			}
			return fmt.Errorf("can't write to empty stream for unknown reason: %w (%w)", writeErr, ErrConnectionProblem)
		}

		if realPredecessor, realPredecessorPresent := analysis[predecessorSeq]; realPredecessorPresent {
			if predecessorMsgID != "" {
				if realPredecessor == nil {
					return fmt.Errorf("can't check predecessor msgid on seq=%d (%w)", predecessorSeq, blockio.ErrRemovedPredecessor)
				}
				if realPredecessorMsgID := realPredecessor.GetHeader().Get(jetstream.MsgIDHeader); realPredecessorMsgID != predecessorMsgID {
					return fmt.Errorf("expected predecessor msgid='%s' on seq=%d but got '%s' (%w)", predecessorMsgID, predecessorSeq, realPredecessorMsgID, blockio.ErrWrongPredecessor)
				}
				c.out.logger.Infof("predecessor msgid is alright, but nats-server doesn't like our expect-last-msgid header, perhaps we fell out of dedup window, let's try removing this header...")
				return c.doProtectedWrite(ctx, predecessorSeq, "", msg)
			}
			return fmt.Errorf("expect-checks failed for unknown reason (%w), predecessor on seq=%d confirmed to be right, see logs (%w)", writeErr, predecessorSeq, ErrConnectionProblem)
		}

		return fmt.Errorf("expected predecessor on seq=%d, but it doesn't exist yet (%w)", predecessorSeq, blockio.ErrWrongPredecessor)
	}

	if ack.Sequence != predecessorSeq+1 {
		c.out.logger.Errorf("Wanted to write msgid='%s' on seq=%d, but it's already present on seq=%d, running analysis...", msgID, predecessorSeq+1, ack.Sequence)

		if _, err := c.analyzeVicinity(ctx, predecessorSeq+1, ack.Sequence); err != nil {
			return err
		}

		return fmt.Errorf("wanted to write msgid='%s' on seq=%d, but it's already present on seq=%d, see logs (%w)", msgID, predecessorSeq+1, ack.Sequence, blockio.ErrCollision)
	}

	c.saveLastWrittenMsg(wMsg, msg.Block, predecessorSeq+1, msgID)
	return nil
}

func (c *connection) analyzeVicinity(ctx context.Context, centers ...uint64) (map[uint64]messages.NatsMessage, error) {
	dedup := map[uint64]struct{}{}
	elements := []uint64{}
	for _, center := range centers {
		l, r := uint64(1), center+3
		if center > 3 {
			l = center - 3
		}
		for seq := l; seq <= r; seq++ {
			if _, ok := dedup[seq]; !ok {
				elements = append(elements, seq)
				dedup[seq] = struct{}{}
			}
		}
	}
	sort.Slice(elements, func(i, j int) bool {
		return elements[i] < elements[j]
	})

	info, infoErr := c.sc.Stream().GetInfo(ctx)
	if infoErr != nil {
		return nil, fmt.Errorf("unable to fetch stream info: %w (%w)", infoErr, ErrConnectionProblem)
	}
	c.out.logger.Infof("analysis: firstSeq=%d, lastSeq=%d", info.State.FirstSeq, info.State.LastSeq)

	res := make(map[uint64]messages.NatsMessage)

	for _, seq := range elements {
		if seq < info.State.FirstSeq {
			c.out.logger.Infof("analysis: seq=%d fell out from stream", seq)
			res[seq] = nil
			continue
		}
		if seq > info.State.LastSeq {
			c.out.logger.Infof("analysis: seq=%d is not yet in stream", seq)
			continue
		}
		msg, err := c.sc.Stream().Get(ctx, seq)
		if err != nil {
			info, infoErr = c.sc.Stream().GetInfo(ctx)
			if infoErr != nil {
				return nil, fmt.Errorf("unable to fetch stream info: %w (%w)", infoErr, ErrConnectionProblem)
			}
			if seq < info.State.FirstSeq {
				c.out.logger.Infof("analysis: seq=%d fell out from stream", seq)
				res[seq] = nil
				continue
			}
			if seq > info.State.LastSeq {
				c.out.logger.Infof("analysis: seq=%d is not yet in stream", seq)
				continue
			}
			return nil, fmt.Errorf("unable to fetch msg on seq=%d: %w (%w)", seq, err, ErrConnectionProblem)
		}
		c.out.logger.Infof("analysis: seq=%d has msgid='%s'", seq, msg.GetHeader().Get(jetstream.MsgIDHeader))
		res[seq] = msg
	}

	return res, nil
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

func (c *connection) saveLastWrittenMsg(msg *nats.Msg, block blocks.Block, seq uint64, msgid string) {
	msg.Header.Set(jetstream.MsgIDHeader, msgid)

	bmsg := &messages.BlockMessage{
		Block: block,
		Msg: &messages.RawStreamMessage{
			RawStreamMsg: &jetstream.RawStreamMsg{
				Subject:  msg.Subject,
				Sequence: seq,
				Header:   msg.Header,
				Data:     msg.Data,
				Time:     time.Now(),
			},
		},
	}

	c.updateLastKnownMsg(blockdecode.NewPredecodedMsg(bmsg))
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
