package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/util"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

var ErrInterrupted = fmt.Errorf("interrupted")

type Reader struct {
	logger *logrus.Entry

	cfg      *Config
	input    *stream.Stream
	receiver Receiver

	lastKnownSeq atomic.Uint64

	ctx        context.Context
	cancel     func()
	err        error
	finishOnce sync.Once
	wg         sync.WaitGroup
}

func Start(input *stream.Stream, cfg *Config, receiver Receiver) (*Reader, error) {
	cfg = cfg.WithDefaults()

	r := &Reader{
		logger: logrus.
			WithField("component", "streamreader").
			WithField("stream", input.Name()).
			WithField("tag", cfg.LogTag),

		cfg:      cfg,
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

func (r *Reader) Stop(err error, wait bool) {
	if err != nil {
		r.finish(fmt.Errorf("%w: %w", ErrInterrupted, err))
	} else {
		r.finish(ErrInterrupted)
	}
	if wait {
		r.wg.Wait()
	}
}

func (r *Reader) finish(err error) {
	r.finishOnce.Do(func() {
		r.err = err
		r.cancel()
		if err != nil {
			if errors.Is(err, ErrInterrupted) {
				r.logger.Infof("finished because of interruption: %v", r.err)
			} else {
				r.logger.Errorf("finished with error: %v", r.err)
			}
		} else {
			r.logger.Infof("finished normally")
		}
	})
}

func (r *Reader) run() {
	defer r.wg.Done()
	defer func() {
		r.receiver.HandleFinish(r.err)
	}()

	consumer, err := r.input.Stream().OrderedConsumer(r.ctx, r.cfg.Consumer)
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

			r.updateLastKnownSeq(meta.Sequence.Stream + meta.NumPending)

			if lastConsumedSeq.Load() == 0 {
				if r.cfg.StrictStart && len(r.cfg.Consumer.FilterSubjects) == 0 && r.cfg.Consumer.OptStartSeq > 0 {
					if meta.Sequence.Stream != r.cfg.Consumer.OptStartSeq {
						r.finish(fmt.Errorf("unexpected sequence: %d, expected: %d", meta.Sequence.Stream, r.cfg.Consumer.OptStartSeq))
						return
					}
				} else {
					if meta.Sequence.Stream < r.cfg.Consumer.OptStartSeq {
						r.finish(fmt.Errorf("unexpected sequence: %d, expected anything greater than or equal to %d", meta.Sequence.Stream, r.cfg.Consumer.OptStartSeq))
						return
					}
				}
			} else {
				if len(r.cfg.Consumer.FilterSubjects) == 0 {
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

			if r.cfg.EndSeq > 0 && meta.Sequence.Stream >= r.cfg.EndSeq {
				r.logger.Info("end sequence reached, finishing")
				r.finish(nil)
				return
			}

			if r.cfg.EndTime != nil && !meta.Timestamp.Before(*r.cfg.EndTime) {
				r.logger.Info("end time reached, finishing")
				r.finish(nil)
				return
			}

			if err := r.receiver.HandleMsg(r.ctx, &messages.StreamMessage{Msg: msg, Meta: meta}); err != nil {
				r.logger.Info("stopped by receiver, finishing")
				r.finish(fmt.Errorf("%w: %w", ErrInterrupted, err))
				return
			}

			lastFiredSeq.Store(meta.Sequence.Stream)

			if r.cfg.EndSeq > 0 && meta.Sequence.Stream+1 >= r.cfg.EndSeq {
				r.logger.Info("last sequence reached, finishing")
				r.finish(nil)
				return
			}
		},
		r.cfg.PullOpts...,
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
	if !util.CtxSleep(r.ctx, r.cfg.MaxSilence) {
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
	r.updateLastKnownSeq(info.State.LastSeq)

	// Is there anything to read potentially?
	if r.cfg.EndSeq > 0 {
		minPotentialNextSeq := lastFiredSeq.Load() + 1
		if info.State.FirstSeq > minPotentialNextSeq {
			minPotentialNextSeq = info.State.FirstSeq
		}
		if r.cfg.Consumer.OptStartSeq > minPotentialNextSeq {
			minPotentialNextSeq = r.cfg.Consumer.OptStartSeq
		}
		if minPotentialNextSeq >= r.cfg.EndSeq {
			r.logger.Infof("min potential next seq (%d) >= endSeq (%d), finishing", minPotentialNextSeq, r.cfg.EndSeq)
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
	if len(r.cfg.Consumer.FilterSubjects) == 0 {
		// For read-all-subjects mode we simply check last message of stream

		// If no messages after StartSeq - it's fine, nothing to read yet
		if info.State.LastSeq < r.cfg.Consumer.OptStartSeq {
			return
		}
		// If no messages after StartTime - it's fine, nothing to read yet
		if r.cfg.Consumer.OptStartTime != nil && info.State.LastTime.Before(*r.cfg.Consumer.OptStartTime) {
			return
		}
		// If last message is already behind - it's fine, nothing to read yet
		if info.State.LastSeq <= silenceCandidateSeq {
			return
		}

		// OK, there's a proof of continuation (there's something more to read)

	} else {
		// For custom subjects we check last msg per subject

		provenContinuation := false

		for _, subj := range r.cfg.Consumer.FilterSubjects {
			// If cursor suddenly changed - all ok, return
			if lastConsumedSeq.Load() > silenceCandidateSeq {
				return
			}
			msg, err := r.input.GetLastMsgForSubject(r.ctx, subj)
			if err != nil {
				if errors.Is(err, jetstream.ErrMsgNotFound) {
					continue
				}
				r.finish(fmt.Errorf("unable to get last stream message for subject '%s': %w", subj, err))
				return
			}
			r.updateLastKnownSeq(msg.GetSequence())

			// If message is before StartSeq - it's fine, it's not a proven continuation
			if msg.GetSequence() < r.cfg.Consumer.OptStartSeq {
				continue
			}
			// If no messages after StartTime - it's fine, it's not a proven continuation
			if r.cfg.Consumer.OptStartTime != nil && msg.GetTimestamp().Before(*r.cfg.Consumer.OptStartTime) {
				continue
			}
			// If this message is already behind - it's fine, it's not a proven continuation
			if msg.GetSequence() <= silenceCandidateSeq {
				return
			}

			// OK, there's a proof of continuation (there's something more to read)
			provenContinuation = true
			break
		}

		// If no proof of continuation - it's fine, nothing to read
		if !provenContinuation {
			return
		}
	}

	// If cursor suddenly changed - all ok, return
	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return
	}

	// Let's wait that we don't get new message for some time even though it's proven to exist
	if !util.CtxSleep(r.ctx, r.cfg.MaxSilence) {
		return
	}

	// If new sequence is finally consumed - OK, good
	if lastConsumedSeq.Load() > silenceCandidateSeq {
		return
	}

	// Silence confirmed...
	r.finish(fmt.Errorf("detected consumer silence :/"))
}

func (r *Reader) updateLastKnownSeq(seq uint64) {
	// CAS-based atomic maximum
	for {
		prevValue := r.lastKnownSeq.Load()
		if prevValue >= seq {
			return
		}
		if r.lastKnownSeq.CompareAndSwap(prevValue, seq) {
			break
		}
	}
	if err := r.receiver.HandleNewKnownSeq(r.ctx, r.lastKnownSeq.Load()); err != nil {
		r.logger.Info("stopped by receiver, finishing")
		r.finish(fmt.Errorf("%w: %w", ErrInterrupted, err))
	}
}
