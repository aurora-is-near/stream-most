package streamoutput

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service2/blockio"
	"github.com/aurora-is-near/stream-most/service2/streamstate"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

var (
	ErrNotStartedYet     = fmt.Errorf("not started yet")
	ErrConnectionProblem = fmt.Errorf("connection problem")
	ErrInvalidCfg        = fmt.Errorf("invalid cfg")
	ErrStopped           = fmt.Errorf("stopped")
)

// Static assertion
var _ blockio.Output = (*Output)(nil)

type Output struct {
	logger *logrus.Entry

	config *Config

	curState       atomic.Pointer[streamstate.State]
	curConn        atomic.Pointer[connection]
	lastWrittenMsg atomic.Pointer[messages.BlockMessage]

	ctx      context.Context
	cancel   func()
	finished chan struct{}
}

func Start(config *Config) *Output {
	out := &Output{
		logger: logrus.
			WithField("component", "streamoutput").
			WithField("stream", config.Stream.Stream).
			WithField("nats", config.Stream.Nats.LogTag),

		config:   config,
		finished: make(chan struct{}),
	}

	out.putErr(fmt.Errorf("%w (%w)", ErrNotStartedYet, blockio.ErrTemporarilyUnavailable))

	out.ctx, out.cancel = context.WithCancel(context.Background())

	out.logger.Infof("Starting...")
	go out.run()

	return out
}

func (out *Output) State() (blockio.State, error) {
	s := out.curState.Load()
	if s.Err != nil {
		return nil, s.Err
	}
	lastWritten := out.lastWrittenMsg.Load()
	if lastWritten == nil || lastWritten.Msg.GetSequence() < s.LastSeq() {
		return s, nil
	}
	return stateWithTip{state: s, tip: lastWritten}, nil
}

func (out *Output) Stop(wait bool) {
	out.logger.Infof("Stopping")
	out.cancel()
	if wait {
		<-out.finished
	}
}

func (out *Output) putErr(err error) {
	out.curState.Store(&streamstate.State{Err: err})
	out.curConn.Store(newConnection(nil, "").setError(err).killQuota())
}

func (out *Output) run() {
	defer close(out.finished)

	err := out.runReconnectionLoop()

	if errors.Is(err, ErrStopped) {
		out.logger.Infof("Stopped externally")
	} else {
		out.logger.Errorf("Finished with error: %v", err)
	}

	if err == nil { // This should never happen
		err = fmt.Errorf("finished for unknown reason")
	}

	out.putErr(fmt.Errorf("%w (%w)", err, blockio.ErrCompletelyUnavailable))
}

func (out *Output) runReconnectionLoop() error {
	var lastErr error
	for i := 0; out.config.MaxReconnects < 0 || i <= out.config.MaxReconnects; i++ {
		if out.ctx.Err() != nil {
			return ErrStopped
		}

		if i > 0 {
			out.logger.Infof("Sleeping for %s before next reconnection...", out.config.ReconnectDelay.String())
			if !util.CtxSleep(out.ctx, out.config.ReconnectDelay) {
				return ErrStopped
			}
		}

		out.logger.Infof("Connecting stream...")
		s, err := stream.Connect(out.config.Stream)
		if err != nil {
			out.logger.Errorf("Unable to connect: %v", err)
			lastErr = fmt.Errorf("%w: %w", ErrConnectionProblem, err)
			out.putErr(fmt.Errorf("%w (%w)", lastErr, blockio.ErrTemporarilyUnavailable))
			continue
		}

		lastErr = out.runStreamConnection(s)

		if errors.Is(lastErr, ErrStopped) {
			out.logger.Infof("Got stopped externally, finishing...")
			out.putErr(fmt.Errorf("%w (%w)", lastErr, blockio.ErrCompletelyUnavailable))
		} else {
			out.logger.Errorf("Got error: %v", lastErr)
			out.putErr(fmt.Errorf("%w (%w)", lastErr, blockio.ErrTemporarilyUnavailable))
		}

		out.logger.Infof("Disconnecting stream...")
		s.Disconnect()

		if !errors.Is(lastErr, ErrConnectionProblem) {
			return lastErr
		}
	}

	return fmt.Errorf("max reconnects (%d) exceeded: %w", out.config.MaxReconnects, lastErr)
}

func (out *Output) runStreamConnection(s stream.Interface) error {
	subjectPattern := out.config.SubjectPattern
	if subjectPattern == "" {
		out.logger.Infof("Writing subject pattern not provided, figuring it out automatically...")
		subjects, err := s.GetConfigSubjects(out.ctx)
		if err != nil {
			if out.ctx.Err() != nil && errors.Is(err, out.ctx.Err()) {
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
		subjectPattern = subjects[0]
		out.logger.Infof("Selected subject pattern '%s'", subjectPattern)
	}

	out.curConn.Store(newConnection(s, subjectPattern))

	return out.handleConnection()
}

func (out *Output) handleConnection() error {
	conn := out.curConn.Load()
	stateFetcher := streamstate.StartFetcher(conn.stream, out.config.StateFetchInterval, true, func(s *streamstate.State) {
		if s.Err != nil {
			conn.setError(fmt.Errorf("unable to fetch state: %w (%w)", s.Err, ErrConnectionProblem))
		} else {
			out.curState.Store(s)
		}
	})
	defer stateFetcher.Stop(true)

	defer conn.killQuota()

	select {
	case <-conn.hasErr:
		return conn.err
	case <-out.ctx.Done():
		conn.setError(ErrStopped)
		return ErrStopped
	}
}

func (out *Output) WriteAfter(ctx context.Context, predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage) error {
	conn := out.curConn.Load()
	if err := conn.obtainQuota(); err != nil {
		if errors.Is(err, blockio.ErrTemporarilyUnavailable) || errors.Is(err, blockio.ErrCompletelyUnavailable) {
			return err
		}
		return fmt.Errorf("%w (%w)", err, blockio.ErrTemporarilyUnavailable)
	}
	defer conn.returnQuota()

	if err := out.writeAfter(ctx, predecessorSeq, predecessorMsgID, msg, conn); err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return fmt.Errorf("%w (%w)", ctx.Err(), blockio.ErrCanceled)
		}
		if errors.Is(err, ErrConnectionProblem) {
			conn.setError(err)
			return fmt.Errorf("%w (%w)", err, blockio.ErrTemporarilyUnavailable)
		}
		out.logger.Errorf("Got failed write attempt: %v", err)
		return err
	}

	return nil
}

func (out *Output) writeAfter(ctx context.Context, predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage, conn *connection) error {
	wMsg := &nats.Msg{
		Header: make(nats.Header),
		Data:   msg.Msg.GetData(),
	}

	switch msg.Block.GetBlockType() {
	case blocks.Announcement:
		wMsg.Subject = strings.ReplaceAll(conn.subjectPattern, "*", "header")
	case blocks.Shard:
		wMsg.Subject = strings.ReplaceAll(conn.subjectPattern, "*", strconv.FormatUint(msg.Block.GetShardID(), 10))
	default:
		wMsg.Subject = strings.ReplaceAll(conn.subjectPattern, "*", "unknown")
	}

	if out.config.PreserveCustomHeaders {
		for k, v := range msg.Msg.GetHeader() {
			if _, ok := serviceHeaders[k]; !ok {
				wMsg.Header[k] = v
			}
		}
	}

	msgID := blocks.ConstructMsgID(msg.Block)
	opts := []jetstream.PublishOpt{
		jetstream.WithMsgID(msgID),
		jetstream.WithRetryWait(out.config.WriteRetryWait),
		jetstream.WithRetryAttempts(out.config.WriteRetryAttempts),
		jetstream.WithExpectLastSequence(predecessorSeq),
	}
	if predecessorMsgID != "" {
		opts = append(opts, jetstream.WithExpectLastMsgID(predecessorMsgID))
	}

	ack, writeErr := conn.stream.Write(ctx, wMsg, opts...)
	if writeErr != nil {
		if !isFailedExpectErr(writeErr) {
			return fmt.Errorf("unable to write msgid='%s' at seq=%d: %w (%w)", msgID, predecessorSeq+1, writeErr, ErrConnectionProblem)
		}

		out.logger.Warnf("Got failed expected-predecessor checks on seq=%d and msgid='%s' (%v), running analysis...", predecessorSeq+1, msgID, writeErr)
		if predecessorMsgID != "" {
			out.logger.Infof("Btw expected predecessor's msgid='%s'", predecessorMsgID)
		}

		analysis, err := out.analyzeVicinity(ctx, conn, predecessorSeq+1)
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
			out.logger.Infof("msgid on this seq is actually right, we probably just fell out of dedup window, ignoring...")
			out.saveLastWrittenMsg(wMsg, msg.Block, predecessorSeq+1, msgID)
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
			}
			return fmt.Errorf("expect-checks failed for unknown reason (%w), predecessor on seq=%d confirmed to be right, see logs (%w)", writeErr, predecessorSeq, ErrConnectionProblem)
		}

		return fmt.Errorf("expected predecessor on seq=%d, but it doesn't exist yet (%w)", predecessorSeq, blockio.ErrWrongPredecessor)
	}

	if ack.Sequence != predecessorSeq+1 {
		out.logger.Errorf("Wanted to write msgid='%s' on seq=%d, but it's already present on seq=%d, running analysis...", msgID, predecessorSeq+1, ack.Sequence)

		if _, err := out.analyzeVicinity(ctx, conn, predecessorSeq+1, ack.Sequence); err != nil {
			return err
		}

		return fmt.Errorf("wanted to write msgid='%s' on seq=%d, but it's already present on seq=%d, see logs (%w)", msgID, predecessorSeq+1, ack.Sequence, blockio.ErrCollision)
	}

	out.saveLastWrittenMsg(wMsg, msg.Block, predecessorSeq+1, msgID)
	return nil
}

func (out *Output) analyzeVicinity(ctx context.Context, conn *connection, centers ...uint64) (map[uint64]messages.NatsMessage, error) {
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

	info, infoErr := conn.stream.GetInfo(ctx)
	if infoErr != nil {
		return nil, fmt.Errorf("unable to fetch stream info: %w (%w)", infoErr, ErrConnectionProblem)
	}
	out.logger.Infof("analysis: firstSeq=%d, lastSeq=%d", info.State.FirstSeq, info.State.LastSeq)

	res := make(map[uint64]messages.NatsMessage)

	for _, seq := range elements {
		if seq < info.State.FirstSeq {
			out.logger.Infof("analysis: seq=%d fell out from stream", seq)
			res[seq] = nil
			continue
		}
		if seq > info.State.LastSeq {
			out.logger.Infof("analysis: seq=%d is not yet in stream", seq)
			continue
		}
		msg, err := conn.stream.Get(ctx, seq)
		if err != nil {
			info, infoErr = conn.stream.GetInfo(ctx)
			if infoErr != nil {
				return nil, fmt.Errorf("unable to fetch stream info: %w (%w)", infoErr, ErrConnectionProblem)
			}
			if seq < info.State.FirstSeq {
				out.logger.Infof("analysis: seq=%d fell out from stream", seq)
				res[seq] = nil
				continue
			}
			if seq > info.State.LastSeq {
				out.logger.Infof("analysis: seq=%d is not yet in stream", seq)
				continue
			}
			return nil, fmt.Errorf("unable to fetch msg on seq=%d: %w (%w)", seq, err, ErrConnectionProblem)
		}
		out.logger.Infof("analysis: seq=%d has msgid='%s'", seq, msg.GetHeader().Get(jetstream.MsgIDHeader))
		res[seq] = msg
	}

	return res, nil
}

func (out *Output) saveLastWrittenMsg(msg *nats.Msg, block blocks.Block, seq uint64, msgid string) {
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

	// CAS-based atomic maximum
	for {
		cur := out.lastWrittenMsg.Load()
		if cur != nil && cur.Msg.GetSequence() > bmsg.Msg.GetSequence() {
			break
		}
		if out.lastWrittenMsg.CompareAndSwap(cur, bmsg) {
			break
		}
	}
}
