package blockreader

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/multistreambridge/lifecycle"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

type BlockReader struct {
	options       *Options
	logger        *logrus.Entry
	lifecycle     *lifecycle.Lifecycle
	decodingQueue chan *DecodingJob
	publishQueue  chan *DecodingJob
	workersWg     sync.WaitGroup
	reader        *reader.Reader
}

type DecodingJob struct {
	msg           messages.NatsMessage
	blockMessage  *messages.BlockMessage
	decodingError error
	done          chan struct{}
}

func StartBlockReader(ctx context.Context, options *Options) (*BlockReader, error) {
	optionsCopy := *options
	b := &BlockReader{
		options: &optionsCopy,
		logger: logrus.
			WithField("component", "blockreader").
			WithField("stream", options.Stream.Name()).
			WithField("tag", options.LogTag),
		lifecycle:     lifecycle.NewLifecycle(ctx, fmt.Errorf("block-reader interrupted")),
		decodingQueue: make(chan *DecodingJob, 128),
		publishQueue:  make(chan *DecodingJob, 128),
	}

	if b.options.HandleNewKnownSeqCb == nil {
		b.options.HandleNewKnownSeqCb = func(ctx context.Context, seq uint64) (err error) {
			return nil
		}
	}
	if b.options.FilterCb == nil {
		b.options.FilterCb = func(ctx context.Context, msg messages.NatsMessage) (skip bool, err error) {
			return false, nil
		}
	}
	if b.options.BlockCb == nil {
		b.options.BlockCb = func(ctx context.Context, blockMsg *messages.BlockMessage) (err error) {
			return nil
		}
	}
	if b.options.CorruptedBlockCb == nil {
		b.options.CorruptedBlockCb = func(ctx context.Context, msg messages.NatsMessage, decodingError error) (err error) {
			return nil
		}
	}
	if b.options.FinishCb == nil {
		b.options.FinishCb = func(err error, isInterrupted bool) {}
	}

	b.workersWg.Add(1)
	go b.runPublisher()

	maxprocs := runtime.GOMAXPROCS(0)
	b.workersWg.Add(maxprocs)
	for range maxprocs {
		go b.runDecoder()
	}

	go b.run()

	return b, nil
}

func (b *BlockReader) Lifecycle() lifecycle.External {
	return b.lifecycle
}

func (b *BlockReader) initiateStop(reason error, markAsInterruption bool) {
	if b.lifecycle.SendStop(reason, markAsInterruption) {
		if b.lifecycle.InterruptedExternally() {
			b.logger.Infof("stopping because of interruption: %v", reason)
		} else if b.lifecycle.StoppingReason() != nil {
			b.logger.Errorf("stopping because of error: %v", reason)
		} else {
			b.logger.Infof("finishing")
		}
	}
}

func (b *BlockReader) run() {
	defer b.lifecycle.MarkDone()
	defer func() {
		b.options.FinishCb(b.lifecycle.StoppingReason(), b.lifecycle.InterruptedExternally())
	}()
	defer b.workersWg.Wait()

	b.logger.Infof("starting stream reader...")

	var err error
	b.reader, err = reader.Start(
		b.options.Stream,
		&reader.Config{
			Consumer: jetstream.OrderedConsumerConfig{
				DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:   b.options.StartSeq,
				ReplayPolicy:  jetstream.ReplayInstantPolicy,
			},
			PullOpts: []jetstream.PullConsumeOpt{
				jetstream.PullMaxBytes(32 * 1024 * 1024), // 32MB buffer
			},
			StrictStart: true,
			MaxSilence:  time.Second * 5,
			LogTag:      b.options.LogTag,
		},
		reader.NewCbReceiver().WithHandleFinishCb(func(err error) {
			b.logger.Infof("stream reader finished: %v", err)
			close(b.publishQueue)
		}).WithHandleMsgCb(func(ctx context.Context, msg messages.NatsMessage) error {
			skip, err := b.options.FilterCb(ctx, msg)
			if err != nil {
				b.initiateStop(
					fmt.Errorf("interrupted via filter callback on msg at seq %d: %w", msg.GetSequence(), err),
					true,
				)
				return fmt.Errorf("block-reader is stopping")
			}
			if skip {
				return nil
			}

			job := &DecodingJob{
				msg:  msg,
				done: make(chan struct{}),
			}

			select {
			case <-b.lifecycle.Ctx().Done():
				return nil
			case b.decodingQueue <- job:
			}

			select {
			case <-b.lifecycle.Ctx().Done():
				return nil
			case b.publishQueue <- job:
			}

			return nil
		}).WithHandleNewKnownSeqCb(func(ctx context.Context, seq uint64) error {
			if err := b.options.HandleNewKnownSeqCb(ctx, seq); err != nil {
				b.initiateStop(
					fmt.Errorf("interrupted via new-known-seq callback at seq %d: %w", seq, err),
					true,
				)
				return fmt.Errorf("block-reader is stopping")
			}
			return nil
		}),
	)
	if err != nil {
		b.initiateStop(fmt.Errorf("unable to start reader: %w", err), false)
		return
	}
	defer func() {
		b.reader.Stop(fmt.Errorf("block-reader is stopping"), true)
	}()

	b.logger.Infof("stream reader started")

	<-b.lifecycle.Ctx().Done()
}

func (b *BlockReader) runDecoder() {
	defer b.workersWg.Done()

	for {
		if b.lifecycle.StopInitiated() {
			return
		}

		select {
		case <-b.lifecycle.Ctx().Done():
			return
		case job := <-b.decodingQueue:
			job.blockMessage, job.decodingError = formats.Active().ParseMsg(job.msg)
			close(job.done)
		}
	}
}

func (b *BlockReader) runPublisher() {
	defer b.workersWg.Done()

	for {
		if b.lifecycle.StopInitiated() {
			return
		}

		select {
		case <-b.lifecycle.Ctx().Done():
			return
		case job, ok := <-b.publishQueue:
			if !ok {
				b.initiateStop(fmt.Errorf("got stream reader error: %w", b.reader.Error()), false)
				return
			}

			select {
			case <-b.lifecycle.Ctx().Done():
				return
			case <-job.done:
			}

			if job.decodingError != nil {
				if err := b.options.CorruptedBlockCb(b.lifecycle.Ctx(), job.msg, job.decodingError); err != nil {
					b.initiateStop(
						fmt.Errorf("interrupted via receiver on corrupted block at seq %d: %w", job.msg.GetSequence(), err),
						true,
					)
					return
				}
				continue
			}

			if err := b.options.BlockCb(b.lifecycle.Ctx(), job.blockMessage); err != nil {
				b.initiateStop(
					fmt.Errorf("interrupted via receiver on block at seq %d: %w", job.msg.GetSequence(), err),
					true,
				)
				return
			}
		}
	}
}
