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
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

type IReader interface {
	Output() <-chan messages.NatsMessage
	Error() error
	Stop()
	IsFake() bool
}

type Reader struct {
	*logrus.Entry

	opts     *Options
	input    stream.Interface
	subjects []string
	startSeq uint64
	endSeq   uint64

	output chan messages.NatsMessage

	ctx     context.Context
	cancel  func()
	err     error
	errOnce sync.Once
	wg      sync.WaitGroup
}

func Start(ctx context.Context, opts *Options, input stream.Interface, subjects []string, startSeq uint64, endSeq uint64) (IReader, error) {
	if input.IsFake() {
		return createFake(ctx, opts, input, subjects, startSeq, endSeq)
	}

	r := &Reader{
		Entry: logrus.New().
			WithField("component", "reader").
			WithField("stream", input.Name()).
			WithField("log_tag", input.Options().Nats.LogTag),

		opts:     opts.WithDefaults(),
		input:    input,
		subjects: subjects,
		startSeq: startSeq,
		endSeq:   endSeq,
		output:   make(chan messages.NatsMessage, opts.BufferSize),
	}
	r.ctx, r.cancel = context.WithCancel(ctx)

	if len(r.subjects) == 1 && r.subjects[0] == ">" {
		r.subjects = nil
	}

	if r.startSeq < 1 {
		r.startSeq = 1
	}

	r.wg.Add(1)
	go r.run()

	return r, nil
}

func (r *Reader) Output() <-chan messages.NatsMessage {
	return r.output
}

func (r *Reader) Error() error {
	select {
	case <-r.ctx.Done():
		return r.err
	default:
		return nil
	}
}

func (r *Reader) Stop() {
	r.cancel()
	r.wg.Wait()
}

func (r *Reader) IsFake() bool {
	return false
}

func (r *Reader) finishWithError(format string, args ...any) {
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

func (r *Reader) run() {
	defer r.wg.Done()
	defer close(r.output)
	defer r.cancel()

	consumer, err := r.input.Stream().OrderedConsumer(r.ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: r.subjects,
		OptStartSeq:    r.startSeq,
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
				if r.opts.StrictStart && len(r.subjects) == 0 {
					if meta.Sequence.Stream != r.startSeq {
						r.finishWithError("unexpected sequence: %d, expected: %d", meta.Sequence.Stream, r.startSeq)
						return
					}
				} else {
					if meta.Sequence.Stream < r.startSeq {
						r.finishWithError("unexpected sequence: %d, expected anything greater than or equal to %d", meta.Sequence.Stream, r.startSeq)
						return
					}
				}
			} else {
				if len(r.subjects) == 0 {
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

			if r.endSeq > 0 && meta.Sequence.Stream >= r.endSeq {
				r.Info("end sequence reached, finishing")
				r.cancel()
				return
			}

			select {
			case <-r.ctx.Done():
				return
			case r.output <- &messages.StreamMessage{Msg: msg, Meta: meta}:
			}

			lastFiredSeq.Store(meta.Sequence.Stream)

			if r.endSeq > 0 && meta.Sequence.Stream+1 >= r.endSeq {
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

func (r *Reader) ensureNoSilence(lastConsumedSeq, lastFiredSeq *atomic.Uint64) (bool, error) {
	silenceCandidateSeq := lastConsumedSeq.Load()

	if lastFiredSeq.Load() < silenceCandidateSeq {
		return true, nil
	}

	if !r.tryWait(time.Millisecond * time.Duration(r.opts.MaxSilenceMs)) {
		return true, nil
	}

	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return true, nil
	}

	nextAvailableSeq := lastConsumedSeq.Load()

	if len(r.subjects) == 0 {
		info, err := r.input.GetInfo(r.ctx)
		if err != nil {
			return false, fmt.Errorf("unable to get stream info: %w", err)
		}
		if info.State.LastSeq > nextAvailableSeq {
			nextAvailableSeq = info.State.LastSeq
		}
	}

	for _, subj := range r.subjects {
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

	if !r.tryWait(time.Millisecond * time.Duration(r.opts.MaxSilenceMs)) {
		return true, nil
	}

	return lastConsumedSeq.Load() > silenceCandidateSeq, nil
}

func (r *Reader) tryWait(d time.Duration) bool {
	t := time.NewTimer(d)
	select {
	case <-r.ctx.Done():
		if !t.Stop() {
			<-t.C
		}
		return false
	case <-t.C:
		return true
	}
}
