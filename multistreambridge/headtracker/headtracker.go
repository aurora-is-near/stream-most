package headtracker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/multistreambridge/blockreader"
	"github.com/aurora-is-near/stream-most/multistreambridge/lifecycle"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
)

type HeadTracker struct {
	logger      *logrus.Entry
	lifecycle   *lifecycle.Lifecycle
	stream      *stream.Stream
	logTag      string
	headInfo    atomic.Pointer[HeadInfo]
	blockReader *blockreader.BlockReader
}

func StartHeadTracker(ctx context.Context, s *stream.Stream, logTag string) (*HeadTracker, error) {
	ht := &HeadTracker{
		logger: logrus.
			WithField("component", "headtracker").
			WithField("stream", s.Name()).
			WithField("tag", logTag),
		lifecycle: lifecycle.NewLifecycle(ctx, fmt.Errorf("head-tracker interrupted")),
		stream:    s,
		logTag:    logTag,
	}

	if err := ht.fetchHead(); err != nil {
		ht.lifecycle.MarkDone()
		return nil, fmt.Errorf("can't fetch head: %w", err)
	}

	if err := ht.startHeadReader(); err != nil {
		ht.lifecycle.MarkDone()
		return nil, fmt.Errorf("can't start block-reader: %w", err)
	}

	ht.logger.Infof("started")

	go ht.run()

	return ht, nil
}

func (ht *HeadTracker) Lifecycle() lifecycle.External {
	return ht.lifecycle
}

func (ht *HeadTracker) GetHeadInfo() *HeadInfo {
	return ht.headInfo.Load()
}

func (ht *HeadTracker) UpdateHeadInfo(sequence uint64, blockOrNone blocks.Block) (updated bool) {
	new := &HeadInfo{
		sequence:    sequence,
		blockOrNone: blockOrNone,
	}
	for {
		old := ht.headInfo.Load()
		if old == nil || old.sequence >= sequence {
			return false
		}
		if ht.headInfo.CompareAndSwap(old, new) {
			return true
		}
	}
}

func (ht *HeadTracker) run() {
	defer ht.lifecycle.MarkDone()

	defer func() {
		ht.blockReader.Lifecycle().InterruptAndWait(context.Background(), fmt.Errorf("head-tracker is stopping"))
	}()

	defer func() {
		ht.logger.Infof("stopping: %v", ht.lifecycle.StoppingReason())
	}()

	defer ht.headInfo.Store(nil)

	logTicker := time.NewTicker(time.Second * 5)
	defer logTicker.Stop()

	for !ht.lifecycle.StopInitiated() {
		select {
		case <-ht.lifecycle.Ctx().Done():
			return
		case <-logTicker.C:
			if head := ht.GetHeadInfo(); head != nil {
				if head.blockOrNone != nil {
					ht.logger.Infof("current seq=%d, current height=%d", head.sequence, head.blockOrNone.GetHeight())
				} else {
					ht.logger.Infof("current seq=%d, no head", head.sequence)
				}
			}
		}
	}
}

func (ht *HeadTracker) fetchHead() error {
	ht.logger.Infof("getting stream info...")
	streamInfo, err := ht.stream.GetInfo(ht.lifecycle.Ctx())
	if err != nil {
		return fmt.Errorf("can't get stream info: %w", err)
	}

	if streamInfo.State.Msgs == 0 || streamInfo.State.LastSeq == 0 || streamInfo.State.LastSeq < streamInfo.State.FirstSeq {
		ht.logger.Infof("stream is empty, head sequence: %d", streamInfo.State.LastSeq)
		ht.headInfo.Store(&HeadInfo{
			sequence:    streamInfo.State.LastSeq,
			blockOrNone: nil,
		})
		return nil
	}

	ht.logger.Infof("fetching head block...")
	msg, err := ht.stream.Get(ht.lifecycle.Ctx(), streamInfo.State.LastSeq)
	if err != nil {
		return fmt.Errorf("can't fetch head block at seq %d: %w", streamInfo.State.LastSeq, err)
	}

	block, err := formats.Active().ParseBlock(msg.GetData())
	if err != nil {
		return fmt.Errorf("can't parse head block at seq %d: %w", streamInfo.State.LastSeq, err)
	}

	ht.logger.Infof("head sequence: %d, head height: %d", streamInfo.State.LastSeq, block.GetHeight())
	ht.headInfo.Store(&HeadInfo{
		sequence:    streamInfo.State.LastSeq,
		blockOrNone: block,
	})

	return nil
}

func (ht *HeadTracker) startHeadReader() error {
	ht.logger.Infof("starting block-reader...")

	var err error
	ht.blockReader, err = blockreader.StartBlockReader(ht.lifecycle.Ctx(), &blockreader.Options{
		Stream:   ht.stream,
		StartSeq: ht.headInfo.Load().sequence + 1,
		LogTag:   ht.logTag,
		FilterCb: func(ctx context.Context, msg messages.NatsMessage) (skip bool, err error) {
			head := ht.headInfo.Load()
			if head == nil {
				return true, fmt.Errorf("head-tracker is stopping")
			}
			return msg.GetSequence() <= head.sequence, nil
		},
		BlockCb: func(ctx context.Context, blockMsg *messages.BlockMessage) (err error) {
			ht.UpdateHeadInfo(blockMsg.Msg.GetSequence(), blockMsg.Block)
			return nil
		},
		CorruptedBlockCb: func(ctx context.Context, msg messages.NatsMessage, decodingError error) (err error) {
			ht.headInfo.Store(nil)
			errMsg := fmt.Errorf("detected corrupted block at seq=%d: %w", msg.GetSequence(), decodingError)
			ht.lifecycle.SendStop(errMsg, false)
			ht.logger.Errorf("%v, stopping...", errMsg)
			return fmt.Errorf("head-tracker is stopping")
		},
		FinishCb: func(err error, isInterrupted bool) {
			ht.headInfo.Store(nil)

			if isInterrupted {
				return
			}

			errMsg := fmt.Errorf("got block-reader error: %w", err)
			ht.lifecycle.SendStop(errMsg, false)
			ht.logger.Errorf("%v, stopping...", errMsg)
		},
	})

	return err
}
