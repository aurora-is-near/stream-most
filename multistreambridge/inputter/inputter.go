package inputter

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/formats/headers"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/multistreambridge/blockreader"
	"github.com/aurora-is-near/stream-most/multistreambridge/headtracker"
	"github.com/aurora-is-near/stream-most/multistreambridge/jobrestarter"
	"github.com/aurora-is-near/stream-most/multistreambridge/outputter"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/streamconnector"
	"github.com/aurora-is-near/stream-most/transport"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

const maxLookupIterations = 100
const goldenWindowWidth = 100
const maxTimeOutsideGoldenWindow = time.Minute
const loggingInterval = time.Second * 5

// Static assertion
var _ jobrestarter.Job = (*Inputter)(nil)

type Inputter struct {
	logger *logrus.Entry
	cfg    *Config
	out    *outputter.Outputter
}

func NewInputter(cfg *Config, out *outputter.Outputter) (*Inputter, error) {
	in := &Inputter{
		logger: logrus.
			WithField("component", "inputter").
			WithField("tag", cfg.LogTag),
		cfg: cfg,
		out: out,
	}
	return in, nil
}

func (in *Inputter) Run(ctx context.Context) {
	in.logger.Infof("waiting for outputter head...")
	head := in.waitOutputterHead(ctx)
	if head == nil {
		in.logger.Infof("interrupted, stopping")
		return
	}

	in.logger.Infof("connecting to stream...")
	sc, err := streamconnector.Connect(&streamconnector.Config{
		Nats: &transport.NATSConfig{
			ServerURL: strings.Join(in.cfg.NatsEndpoints, ","),
			Creds:     in.cfg.NatsCredsPath,
			Options:   transport.RecommendedNatsOptions(),
			LogTag:    in.cfg.LogTag,
		},
		Stream: &stream.Config{
			Name:        in.cfg.StreamName,
			RequestWait: time.Second * 10,
			WriteWait:   time.Second * 10,
			LogTag:      in.cfg.LogTag,
		},
	})
	if err != nil {
		in.logger.Errorf("can't connect to stream: %v", err)
		return
	}
	defer sc.Disconnect()

	in.logger.Infof("waiting for outputter head again...")
	head = in.waitOutputterHead(ctx)
	if head == nil {
		in.logger.Infof("interrupted, stopping")
		return
	}

	targetHeight := in.calcTargetHeight(head)

	in.logger.Infof("targetHeight=%d, performing lookup...", targetHeight)
	startSeq, err := in.lookupTargetHeight(ctx, sc.Stream(), targetHeight)
	if err != nil {
		in.logger.Errorf("unable to find msg with target height: %v", err)
		return
	}

	var highestKnownSeq atomic.Uint64
	highestKnownSeq.Store(startSeq)

	var lastGoldenWindowHit atomic.Int64
	lastGoldenWindowHit.Store(time.Now().UnixNano())

	updateGoldenWindowHit := func(height uint64, targetHeight uint64, seq uint64) {
		if height > targetHeight {
			return
		}
		if min(targetHeight-height, highestKnownSeq.Load()-seq) > goldenWindowWidth {
			return
		}
		lastGoldenWindowHit.Store(time.Now().UnixNano())
	}

	var corruptedBlocksCnt, highBlocksCnt, hashMismatchCnt atomic.Uint64
	var skipsSinceLastLog, msgsHandledSinceLastLog, writesSinceLastLog atomic.Uint64
	var lastProcessedSeq, lastProcessedHeight atomic.Uint64
	var lastSkippedSeq, lastSkippedHeight atomic.Uint64

	in.logger.Infof("starting block reader from seq=%d", startSeq)
	reader, err := blockreader.StartBlockReader(ctx, &blockreader.Options{
		Stream:   sc.Stream(),
		StartSeq: startSeq,
		LogTag:   in.cfg.LogTag,
		HandleNewKnownSeqCb: func(ctx context.Context, seq uint64) (err error) {
			highestKnownSeq.Store(seq)
			return nil
		},
		FilterCb: func(ctx context.Context, msg messages.NatsMessage) (skip bool, err error) {
			if !in.cfg.TrustHeaders {
				return false, nil
			}

			head := in.out.GetHeadInfo()
			if head == nil {
				return false, nil
			}
			targetHeight := in.calcTargetHeight(head)

			hdr := msg.GetHeader().Get(jetstream.MsgIDHeader)
			if len(hdr) == 0 {
				return false, nil
			}
			msgid, err := headers.ParseMsgID(hdr)
			if err != nil {
				return false, nil
			}
			if msgid.GetHeight() >= targetHeight {
				return false, nil
			}

			updateGoldenWindowHit(msgid.GetHeight(), targetHeight, msg.GetSequence())
			skipsSinceLastLog.Add(1)
			lastSkippedSeq.Store(msg.GetSequence())
			lastSkippedHeight.Store(msgid.GetHeight())
			return true, nil
		},
		BlockCb: func(ctx context.Context, blockMsg *messages.BlockMessage) (err error) {
			defer func() {
				msgsHandledSinceLastLog.Add(1)
				lastProcessedSeq.Store(blockMsg.Msg.GetSequence())
				lastProcessedHeight.Store(blockMsg.Block.GetHeight())
			}()

			for {
				head := in.waitOutputterHead(ctx)
				if head == nil {
					return nil
				}

				status, head := in.out.TryWrite(ctx, blockMsg.Block, blockMsg.Msg.GetData())
				switch status {
				case outputter.OutputterUnavailable:
					continue
				case outputter.OK:
					writesSinceLastLog.Add(1)
					lastGoldenWindowHit.Store(time.Now().UnixNano())
					return nil
				case outputter.Interrupted:
					return nil
				case outputter.LowBlock:
					targetHeight := in.calcTargetHeight(head)
					updateGoldenWindowHit(blockMsg.Block.GetHeight(), targetHeight, blockMsg.Msg.GetSequence())
					return nil
				case outputter.HighBlock:
					highBlocksCnt.Add(1)
					return nil
				case outputter.HashMismatch:
					hashMismatchCnt.Add(1)
					return nil
				default:
					return fmt.Errorf("got unknown response status from outputter: %v", status)
				}
			}
		},
		CorruptedBlockCb: func(ctx context.Context, msg messages.NatsMessage, decodingError error) (err error) {
			msgsHandledSinceLastLog.Add(1)
			corruptedBlocksCnt.Add(1)
			lastProcessedSeq.Store(msg.GetSequence())
			return nil
		},
		FinishCb: func(err error, isInterrupted bool) {
			if isInterrupted {
				in.logger.Infof("reader stopping: %v", err)
			} else {
				in.logger.Errorf("reader stopping with error: %v", err)
			}
		},
	})
	if err != nil {
		in.logger.Errorf("unable to start block reader: %v", err)
		return
	}
	defer func() {
		reader.Lifecycle().InterruptAndWait(context.Background(), fmt.Errorf("inputter is stopping"))
	}()

	ticker := time.NewTicker(time.Second / 2)
	defer ticker.Stop()

	lastLogTime := time.Now()

	for {
		select {
		case <-reader.Lifecycle().Ctx().Done():
			return
		case <-ctx.Done():
			in.logger.Infof("interrupted")
			return
		default:
		}

		lastGoldenWindowHitTime := time.Unix(0, lastGoldenWindowHit.Load())
		timeOutsideGoldenWindow := time.Since(lastGoldenWindowHitTime)
		if timeOutsideGoldenWindow > maxTimeOutsideGoldenWindow {
			in.logger.Warnf("too much time spent far from head (%s), will readjust", timeOutsideGoldenWindow.String())
			return
		}

		if t := time.Since(lastLogTime); t > loggingInterval {
			in.logger.Infof(
				"last processed: (seq=%d, height=%d), last skip: (seq=%d, height=%d), tipSeq=%d",
				lastProcessedSeq.Load(), lastProcessedHeight.Load(),
				lastSkippedSeq.Load(), lastSkippedHeight.Load(),
				highestKnownSeq.Load(),
			)
			in.logger.Infof(
				"corrupted:%d, high:%d, hashMismatches:%d, outside golden window for: %s",
				corruptedBlocksCnt.Load(),
				highBlocksCnt.Load(),
				hashMismatchCnt.Load(),
				timeOutsideGoldenWindow.String(),
			)
			tSec := t.Seconds()
			in.logger.Infof(
				"msgs/sec:%0.2f, skips/sec:%0.2f, writes/sec:%0.2f",
				float64(msgsHandledSinceLastLog.Swap(0))/tSec,
				float64(skipsSinceLastLog.Swap(0))/tSec,
				float64(writesSinceLastLog.Swap(0))/tSec,
			)
			lastLogTime = time.Now()
		}

		select {
		case <-reader.Lifecycle().Ctx().Done():
			return
		case <-ctx.Done():
			in.logger.Infof("interrupted")
			return
		case <-ticker.C:
		}
	}
}

func (in *Inputter) calcTargetHeight(head *headtracker.HeadInfo) uint64 {
	if head.HasBlock() {
		return head.BlockOrNone().GetHeight() + 1
	}
	return in.out.GetStartHeight()
}

func (in *Inputter) waitOutputterHead(ctx context.Context) *headtracker.HeadInfo {
	if head := in.out.GetHeadInfo(); head != nil {
		return head
	}

	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if head := in.out.GetHeadInfo(); head != nil {
			return head
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (in *Inputter) lookupTargetHeight(ctx context.Context, s *stream.Stream, targetHeight uint64) (uint64, error) {
	in.logger.Infof("fetching stream info...")
	streamInfo, err := s.GetInfo(ctx)
	if err != nil {
		return 0, fmt.Errorf("unable to get stream info: %w", err)
	}

	state := streamInfo.State
	in.logger.Infof("firstSeq=%d, lastSeq=%d, minSeq=%d", state.FirstSeq, state.LastSeq, in.cfg.MinSeq)

	if state.Msgs == 0 || state.LastSeq == 0 || state.FirstSeq > state.LastSeq {
		return 0, fmt.Errorf("stream is empty, nothing to read")
	}

	if in.cfg.MinSeq > state.LastSeq {
		return 0, fmt.Errorf("stream is behind minseq, nothing to read")
	}

	minSeq := max(in.cfg.MinSeq, state.FirstSeq)
	curSeq := state.LastSeq

	for i := 0; i < maxLookupIterations; i++ {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("interrupted")
		default:
		}

		in.logger.Infof("checking height of msg on seq=%d...", curSeq)
		msg, err := s.Get(ctx, curSeq)
		if err != nil {
			return 0, fmt.Errorf("unable to fetch stream msg on seq=%d: %w", curSeq, err)
		}

		blockMsg, err := formats.Active().ParseMsg(msg)
		if err != nil {
			in.logger.Warnf("unable to parse block from msg on seq=%d: %v", curSeq, err)
			jump := min(curSeq-minSeq, 1000)
			if jump == 0 {
				return 0, fmt.Errorf("reached lower bound, cannot jump any lower")
			}
			curSeq -= jump
			continue
		}

		curHeight := blockMsg.Block.GetHeight()
		in.logger.Infof("lookup: seq=%d, height=%d", curSeq, curHeight)

		if curHeight <= targetHeight {
			in.logger.Infof("will start from block height=%d on seq=%d", curHeight, curSeq)
			return curSeq, nil
		}

		jump := min(curSeq-minSeq, curHeight-targetHeight)
		if jump == 0 {
			return 0, fmt.Errorf("reached lower bound, cannot jump any lower")
		}
	}

	return 0, fmt.Errorf("reached max lookup iterations (%d), can't find the target height", maxLookupIterations)
}
