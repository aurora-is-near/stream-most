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

type Receiver interface {
	HandleMsg(ctx context.Context, msg messages.NatsMessage)
	HandleFinish(err error)
}

type IReader interface {
	Error() error
	Stop(wait bool)
	IsFake() bool
}

type Reader struct {
	*logrus.Entry

	opts     *Options
	input    stream.Interface
	receiver Receiver

	ctx        context.Context
	cancel     func()
	err        error
	finishOnce sync.Once
	wg         sync.WaitGroup
}

func Start(input stream.Interface, opts *Options, receiver Receiver) (IReader, error) {
	if input.IsFake() {
		return createFake(input, opts, receiver)
	}

	opts = opts.WithDefaults()

	r := &Reader{
		Entry: logrus.New().
			WithField("streamreader", input.Name()).
			WithField("nats", input.Options().Nats.LogTag),

		opts:     opts,
		input:    input,
		receiver: receiver,
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.wg.Add(1)
	go r.run()

	return r, nil
}

func (r *Reader) Error() error {
	select {
	case <-r.ctx.Done():
		return r.err
	default:
		return nil
	}
}

func (r *Reader) Stop(wait bool) {
	r.finish(nil)
	if wait {
		r.wg.Wait()
	}
}

func (r *Reader) IsFake() bool {
	return false
}

func (r *Reader) finish(err error) {
	r.finishOnce.Do(func() {
		r.err = err
		r.cancel()
		if err != nil {
			r.Errorf("finished with error: %v", r.err)
		} else {
			r.Infof("finished normally")
		}
	})
}

func (r *Reader) run() {
	defer r.wg.Done()
	defer r.receiver.HandleFinish(r.err)

	consumer, err := r.input.Stream().OrderedConsumer(r.ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: r.opts.FilterSubjects,
		OptStartSeq:    r.opts.StartSeq,
	})
	if err != nil {
		r.finish(fmt.Errorf("unable to create ordered consumer: %w", err))
		return
	}

	var lastConsumedSeq, lastFiredSeq atomic.Uint64

	consume, err := consumer.Consume(
		func(msg jetstream.Msg) {
			select {
			case <-r.ctx.Done():
				return
			default:
			}

			meta, err := msg.Metadata()
			if err != nil {
				r.finish(fmt.Errorf("unable to parse message metadata: %w", err))
				return
			}

			if monitoring.LastReadSequence != nil {
				monitoring.LastReadSequence.Set(float64(meta.Sequence.Stream))
			}

			if lastConsumedSeq.Load() == 0 {
				if r.opts.StrictStart && len(r.opts.FilterSubjects) == 0 {
					if meta.Sequence.Stream != r.opts.StartSeq {
						r.finish(fmt.Errorf("unexpected sequence: %d, expected: %d", meta.Sequence.Stream, r.opts.StartSeq))
						return
					}
				} else {
					if meta.Sequence.Stream < r.opts.StartSeq {
						r.finish(fmt.Errorf("unexpected sequence: %d, expected anything greater than or equal to %d", meta.Sequence.Stream, r.opts.StartSeq))
						return
					}
				}
			} else {
				if len(r.opts.FilterSubjects) == 0 {
					if meta.Sequence.Stream != lastConsumedSeq.Load()+1 {
						r.finish(fmt.Errorf("unexpected sequence: %d, expected: %d", meta.Sequence.Stream, lastConsumedSeq.Load()+1))
						return
					}
				} else {
					if meta.Sequence.Stream <= lastConsumedSeq.Load() {
						r.finish(fmt.Errorf("unexpected sequence: %d, expected anything greater than %d", meta.Sequence.Stream, lastConsumedSeq.Load()))
						return
					}
				}
			}
			lastConsumedSeq.Store(meta.Sequence.Stream)

			if r.opts.EndSeq > 0 && meta.Sequence.Stream >= r.opts.EndSeq {
				r.Info("end sequence reached, finishing")
				r.finish(nil)
				return
			}

			r.receiver.HandleMsg(r.ctx, &messages.StreamMessage{Msg: msg, Meta: meta})

			lastFiredSeq.Store(meta.Sequence.Stream)

			if r.opts.EndSeq > 0 && meta.Sequence.Stream+1 >= r.opts.EndSeq {
				r.Info("last sequence reached, finishing")
				r.finish(nil)
				return
			}
		},
	)
	if err != nil {
		r.finish(fmt.Errorf("unable to start consuming: %w", err))
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

		r.ensureNoSilence(&lastConsumedSeq, &lastFiredSeq)
	}
}

func (r *Reader) ensureNoSilence(lastConsumedSeq, lastFiredSeq *atomic.Uint64) {
	// Let's consider that reader might be stuck on this sequence
	silenceCandidateSeq := lastConsumedSeq.Load()

	// If it wasn't fired yet - it's not stuck, it's in process of firing
	if lastFiredSeq.Load() < silenceCandidateSeq {
		return
	}

	// Let's wait to ensure it doesn't change
	if !util.CtxSleep(r.ctx, r.opts.MaxSilence) {
		return
	}

	// If it has changed - it's not stuck
	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return
	}

	// Getting info
	info, err := r.input.GetInfo(r.ctx)
	if err != nil {
		r.finish(fmt.Errorf("unable to get stream info: %w", err))
		return
	}

	// Is there anything to read potentially?
	if r.opts.EndSeq > 0 {
		minPotentialNextSeq := lastFiredSeq.Load() + 1
		if info.State.FirstSeq > minPotentialNextSeq {
			minPotentialNextSeq = info.State.FirstSeq
		}
		if r.opts.StartSeq > minPotentialNextSeq {
			minPotentialNextSeq = r.opts.StartSeq
		}
		if minPotentialNextSeq >= r.opts.EndSeq {
			r.Infof("min potential next seq (%d) >= endSeq (%d), finishing", minPotentialNextSeq, r.opts.EndSeq)
			r.finish(nil)
			return
		}
	}

	// OK, but is there anything to read right now?

	// In particular, is there anything in stream right now?
	if s := info.State; s.Msgs == 0 || s.LastSeq == 0 || s.FirstSeq > s.LastSeq {
		return
	}

	// Is there anything to read after the cursor?
	provenAvailableSeq := lastConsumedSeq.Load()

	// For read-all-subjects mode we simply check stream last seq
	if len(r.opts.FilterSubjects) == 0 && info.State.LastSeq > provenAvailableSeq {
		provenAvailableSeq = info.State.LastSeq
	}

	// For custom subjects we check last msg per subject
	for _, subj := range r.opts.FilterSubjects {
		// If cursor suddenly changed - all ok, return
		if lastConsumedSeq.Load() > silenceCandidateSeq {
			return
		}
		// If we already have some proven continuation, let's not waste time
		if provenAvailableSeq > silenceCandidateSeq {
			break
		}
		msg, err := r.input.GetLastMsgForSubject(r.ctx, subj)
		if err != nil {
			r.finish(fmt.Errorf("unable to get last stream message for subject '%s': %w", subj, err))
			return
		}
		if msg.GetSequence() > provenAvailableSeq {
			provenAvailableSeq = msg.GetSequence()
		}
	}

	// If cursor suddenly changed - all ok, return
	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return
	}

	// If there's no proof of continuation - it's ok, stream itself is silent, return
	if provenAvailableSeq <= silenceCandidateSeq {
		return
	}

	// Let's wait that we don't get new message for some time even though it's proven to exist
	if !util.CtxSleep(r.ctx, r.opts.MaxSilence) {
		return
	}

	// If new sequence is finally consumed - OK, good
	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return
	}

	// Silence confirmed...
	r.finish(fmt.Errorf("detected consumer silence :/"))
}
