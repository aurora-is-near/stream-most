package blockreader

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/multistreambridge/lifecycle"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

var ErrInterrupted = errors.New("blockreader interrupted")

type BlockReader struct {
	options       *Options
	logger        *logrus.Entry
	lifecycle     *lifecycle.Lifecycle
	decodingQueue chan *DecodingJob
	publishQueue  chan *DecodingJob
	workersWg     sync.WaitGroup
	finishOnce    sync.Once
	finalError    error
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
		lifecycle:     lifecycle.NewLifecycle(ctx),
		decodingQueue: make(chan *DecodingJob, 128),
		publishQueue:  make(chan *DecodingJob, 128),
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
		b.options.FinishCb = func(err error) {}
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

func (b *BlockReader) Error() error {
	if b.lifecycle.StopSent() {
		b.finish(ErrInterrupted) // fallback if block-reader was stopped via parent context
		return b.finalError
	}
	return nil
}

// Pass `util.FinishedContext()` context to not wait for stop
func (b *BlockReader) Stop(ctx context.Context, err error) bool {
	if err != nil {
		b.finish(fmt.Errorf("%w: %w", ErrInterrupted, err))
	} else {
		b.finish(ErrInterrupted)
	}
	return b.lifecycle.Stop(ctx)
}

func (b *BlockReader) DoneCtx() context.Context {
	return b.lifecycle.DoneCtx()
}

func (b *BlockReader) finish(err error) {
	b.finishOnce.Do(func() {
		b.finalError = err
		b.lifecycle.Stop(util.FinishedContext())
		if err != nil {
			if errors.Is(err, ErrInterrupted) {
				b.logger.Infof("finished because of interruption: %v", err)
			} else {
				b.logger.Errorf("finished with error: %v", err)
			}
		} else {
			b.logger.Infof("finished normally")
		}
	})
}

func (b *BlockReader) run() {
	defer b.lifecycle.MarkDone()
	defer func() {
		b.options.FinishCb(b.Error())
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
				return fmt.Errorf("%w: interrupted via receiver at msg on seq %d: %w", ErrInterrupted, msg.GetSequence(), err)
			}
			if skip {
				return nil
			}

			job := &DecodingJob{
				msg:  msg,
				done: make(chan struct{}),
			}

			select {
			case <-b.lifecycle.StopCtx().Done():
				return nil
			case b.decodingQueue <- job:
			}

			select {
			case <-b.lifecycle.StopCtx().Done():
				return nil
			case b.publishQueue <- job:
			}

			return nil
		}),
	)
	if err != nil {
		b.finish(fmt.Errorf("unable to start reader: %w", err))
		return
	}
	defer func() {
		b.reader.Stop(b.Error(), true)
	}()

	b.logger.Infof("stream reader started")

	<-b.lifecycle.StopCtx().Done()
}

func (b *BlockReader) runDecoder() {
	defer b.workersWg.Done()

	for {
		if b.lifecycle.StopSent() {
			return
		}

		select {
		case <-b.lifecycle.StopCtx().Done():
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
		if b.lifecycle.StopSent() {
			return
		}

		select {
		case <-b.lifecycle.StopCtx().Done():
			return
		case job, ok := <-b.publishQueue:
			if !ok {
				if errors.Is(b.reader.Error(), ErrInterrupted) {
					b.finish(fmt.Errorf("got interrupted via stream reader: %w", b.reader.Error()))
				} else {
					b.finish(fmt.Errorf("got stream reader error: %w", b.reader.Error()))
				}
				return
			}

			select {
			case <-b.lifecycle.StopCtx().Done():
				return
			case <-job.done:
			}

			if job.decodingError != nil {
				if err := b.options.CorruptedBlockCb(b.lifecycle.StopCtx(), job.msg, job.decodingError); err != nil {
					b.finish(fmt.Errorf(
						"%w: interrupted via receiver on corrupted block at seq %d: %w",
						ErrInterrupted, job.msg.GetSequence(), err,
					))
					return
				}
				continue
			}

			if err := b.options.BlockCb(b.lifecycle.StopCtx(), job.blockMessage); err != nil {
				b.finish(fmt.Errorf(
					"%w: interrupted via receiver on block at seq %d: %w",
					ErrInterrupted, job.msg.GetSequence(), err,
				))
				return
			}
		}
	}
}
