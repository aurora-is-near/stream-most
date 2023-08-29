package reader

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader/monitoring"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

type IReader[T any] interface {
	Output() <-chan *DecodedMsg[T]
	Error() error
	Stop()
	IsFake() bool
}

type Reader[T any] struct {
	*logrus.Entry

	opts     *Options
	input    stream.Interface
	decodeFn func(msg messages.NatsMessage) (T, error)

	output        chan *DecodedMsg[T]
	decodingQueue chan *DecodedMsg[T]

	ctx     context.Context
	cancel  func()
	err     error
	errOnce sync.Once
	wg      sync.WaitGroup

	decodeCtx    context.Context
	decodeCancel func()
}

func Start[T any](ctx context.Context, input stream.Interface, opts *Options, decodeFn func(msg messages.NatsMessage) (T, error)) (IReader[T], error) {
	if input.IsFake() {
		return createFake(ctx, input, opts, decodeFn)
	}

	opts = opts.WithDefaults()

	r := &Reader[T]{
		Entry: logrus.New().
			WithField("component", "reader").
			WithField("stream", input.Name()).
			WithField("nats", input.Options().Nats.LogTag),

		opts:          opts,
		input:         input,
		decodeFn:      decodeFn,
		output:        make(chan *DecodedMsg[T], opts.OutputBufferSize),
		decodingQueue: make(chan *DecodedMsg[T], opts.DecodingQueueSize),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.decodeCtx, r.decodeCancel = context.WithCancel(context.Background())

	r.wg.Add(int(r.opts.MaxDecoders))
	for i := 0; i < int(r.opts.MaxDecoders); i++ {
		go r.runDecoder()
	}

	r.wg.Add(1)
	go r.run()

	return r, nil
}

func (r *Reader[T]) Output() <-chan *DecodedMsg[T] {
	return r.output
}

func (r *Reader[T]) Error() error {
	select {
	case <-r.ctx.Done():
		return r.err
	default:
		return nil
	}
}

func (r *Reader[T]) Stop() {
	r.cancel()
	r.decodeCancel()
	r.wg.Wait()
}

func (r *Reader[T]) IsFake() bool {
	return false
}

func (r *Reader[T]) finishWithError(format string, args ...any) {
	select {
	case <-r.ctx.Done():
		// Ignore error if ctx is already canceled
		return
	default:
	}
	r.errOnce.Do(func() {
		r.err = fmt.Errorf(format, args...)
		r.Errorf("finished with error: %v", r.err)
	})
	r.cancel()
}

func (r *Reader[T]) run() {
	defer r.wg.Done()
	defer close(r.output)
	defer close(r.decodingQueue)
	defer r.cancel()

	consumer, err := r.input.Stream().OrderedConsumer(r.ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: r.opts.FilterSubjects,
		OptStartSeq:    r.opts.StartSeq,
	})
	if err != nil {
		r.finishWithError("unable to create ordered consumer: %w", err)
		return
	}

	var lastConsumedSeq atomic.Uint64
	var lastFiredSeq atomic.Uint64

	consume, err := consumer.Consume(
		func(msg jetstream.Msg) {
			select {
			case <-r.ctx.Done():
				return
			default:
			}

			meta, err := msg.Metadata()
			if err != nil {
				r.finishWithError("unable to parse message metadata: %w", err)
				return
			}

			if monitoring.LastReadSequence != nil {
				monitoring.LastReadSequence.Set(float64(meta.Sequence.Stream))
			}

			if lastConsumedSeq.Load() == 0 {
				if r.opts.StrictStart && len(r.opts.FilterSubjects) == 0 {
					if meta.Sequence.Stream != r.opts.StartSeq {
						r.finishWithError("unexpected sequence: %d, expected: %d", meta.Sequence.Stream, r.opts.StartSeq)
						return
					}
				} else {
					if meta.Sequence.Stream < r.opts.StartSeq {
						r.finishWithError("unexpected sequence: %d, expected anything greater than or equal to %d", meta.Sequence.Stream, r.opts.StartSeq)
						return
					}
				}
			} else {
				if len(r.opts.FilterSubjects) == 0 {
					if meta.Sequence.Stream != lastConsumedSeq.Load()+1 {
						r.finishWithError("unexpected sequence: %d, expected: %d", meta.Sequence.Stream, lastConsumedSeq.Load()+1)
						return
					}
				} else {
					if meta.Sequence.Stream <= lastConsumedSeq.Load() {
						r.finishWithError("unexpected sequence: %d, expected anything greater than %d", meta.Sequence.Stream, lastConsumedSeq.Load())
						return
					}
				}
			}
			lastConsumedSeq.Store(meta.Sequence.Stream)

			if r.opts.EndSeq > 0 && meta.Sequence.Stream >= r.opts.EndSeq {
				r.Info("end sequence reached, finishing")
				r.cancel()
				return
			}

			dMsg := &DecodedMsg[T]{
				msg: &messages.StreamMessage{
					Msg:  msg,
					Meta: meta,
				},
				done: make(chan struct{}),
			}

			select {
			case <-r.ctx.Done():
				return
			case r.decodingQueue <- dMsg:
			}

			select {
			case <-r.ctx.Done():
				return
			case r.output <- dMsg:
			}

			lastFiredSeq.Store(meta.Sequence.Stream)

			if r.opts.EndSeq > 0 && meta.Sequence.Stream+1 >= r.opts.EndSeq {
				r.Info("last sequence reached, finishing")
				r.cancel()
				return
			}
		},
	)
	if err != nil {
		r.finishWithError("unable to start consuming: %w", err)
		return
	}
	defer consume.Stop()

	silenceCheckThrottler := time.NewTicker(time.Second / 20)
	defer silenceCheckThrottler.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		select {
		case <-r.ctx.Done():
			return
		case <-silenceCheckThrottler.C:
		}

		noSilence, err := r.ensureNoSilence(&lastConsumedSeq, &lastFiredSeq)
		if err != nil {
			r.finishWithError("silence check failed: %w", err)
			return
		}
		if !noSilence {
			r.finishWithError("detected consumer silence :/")
			return
		}
	}
}

func (r *Reader[T]) ensureNoSilence(lastConsumedSeq, lastFiredSeq *atomic.Uint64) (bool, error) {
	silenceCandidateSeq := lastConsumedSeq.Load()

	if lastFiredSeq.Load() < silenceCandidateSeq {
		return true, nil
	}

	if !util.CtxSleep(r.ctx, r.opts.MaxSilence) {
		return true, nil
	}

	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return true, nil
	}

	nextAvailableSeq := lastConsumedSeq.Load()

	if len(r.opts.FilterSubjects) == 0 {
		info, err := r.input.GetInfo(r.ctx)
		if err != nil {
			return false, fmt.Errorf("unable to get stream info: %w", err)
		}
		if info.State.LastSeq > nextAvailableSeq {
			nextAvailableSeq = info.State.LastSeq
		}
	}

	for _, subj := range r.opts.FilterSubjects {
		if lastConsumedSeq.Load() > silenceCandidateSeq {
			return true, nil
		}
		if nextAvailableSeq > silenceCandidateSeq {
			break
		}
		msg, err := r.input.GetLastMsgForSubject(r.ctx, subj)
		if err != nil {
			return false, fmt.Errorf("unable to get last stream message for subject '%s': %w", subj, err)
		}
		if msg.GetSequence() > nextAvailableSeq {
			nextAvailableSeq = msg.GetSequence()
		}
	}

	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return true, nil
	}

	if nextAvailableSeq <= silenceCandidateSeq {
		return true, nil
	}

	if !util.CtxSleep(r.ctx, r.opts.MaxSilence) {
		return true, nil
	}

	return lastConsumedSeq.Load() > silenceCandidateSeq, nil
}

func (r *Reader[T]) runDecoder() {
	defer r.wg.Done()

	for {
		select {
		case <-r.decodeCtx.Done():
			return
		default:
		}

		select {
		case <-r.decodeCtx.Done():
			return
		case msg, ok := <-r.decodingQueue:
			if !ok {
				return
			}
			msg.value, msg.decodingErr = r.decodeFn(msg.msg)
			close(msg.done)
		}
	}
}
