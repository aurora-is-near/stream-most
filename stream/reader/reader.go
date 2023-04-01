package reader

import (
	"errors"
	"fmt"
	"github.com/aurora-is-near/stream-most/stream"
	"log"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
)

type IReader interface {
	Output() <-chan *Output
	Stop()
	IsFake() bool
}

type Output struct {
	Msg      *nats.Msg
	Metadata *nats.MsgMetadata
	Error    error
}

type Reader struct {
	opts     *Options
	stream   stream.Interface
	startSeq uint64
	endSeq   uint64

	stop    chan bool
	stopped chan bool
	output  chan *Output
	sub     *nats.Subscription
}

func Start(opts *Options, input stream.Interface, startSeq uint64, endSeq uint64) (IReader, error) {
	if input.IsFake() {
		return createFake(opts, input, startSeq, endSeq)
	}

	if startSeq < 1 {
		startSeq = 1
	}
	r := &Reader{
		opts:     opts,
		stream:   input,
		startSeq: startSeq,
		endSeq:   endSeq,
		stop:     make(chan bool),
		stopped:  make(chan bool),
		output:   make(chan *Output, opts.BufferSize),
	}

	log.Printf("Stream Reader [%v]: making sure that previous consumer is deleted...", input.Options().Nats.LogTag)
	err := input.Js().DeleteConsumer(input.Options().Stream, opts.Durable)
	if err != nil && err != nats.ErrConsumerNotFound {
		log.Printf("Stream Reader [%v]: can't delete previous consumer: %v", input.Options().Nats.LogTag, err)
	}

	log.Printf("Stream Reader [%v]: subscribing...", input.Options().Nats.LogTag)
	r.sub, err = input.Js().PullSubscribe(
		input.Options().Subject,
		opts.Durable,
		nats.BindStream(input.Options().Stream),
		//nats.OrderedConsumer(),
		nats.StartSequence(startSeq),
		nats.InactiveThreshold(time.Second*time.Duration(opts.InactiveThresholdSeconds)),
	)
	if err != nil {
		log.Printf("Stream Reader [%v]: unable to subscribe: %v", input.Options().Nats.LogTag, err)
		return nil, err
	}

	log.Printf("Stream Reader [%v]: subscribed", input.Options().Nats.LogTag)

	log.Printf("Stream Reader [%v]: running...", input.Options().Nats.LogTag)
	go r.run()

	return r, nil
}

func (r *Reader) IsFake() bool {
	return false
}

func (r *Reader) Output() <-chan *Output {
	return r.output
}

func (r *Reader) Stop() {
	log.Printf("Stream Reader [%v]: stopping...", r.stream.Options().Nats.LogTag)
	for {
		select {
		case r.stop <- true:
		case <-r.stopped:
			return
		}
	}
}

func (r *Reader) run() {
	if r.endSeq > 0 && r.startSeq >= r.endSeq {
		r.finish("finished (r.startSeq >= r.endSeq)", nil)
		return
	}

	curSeq := r.startSeq - 1
	lastSeq, err := r.getLastSeq()
	if err != nil {
		r.finish("unable to fetch LastSeq", err)
		return
	}

	requestTicker := time.NewTicker(time.Duration(float64(time.Second) / r.opts.MaxRps))
	defer requestTicker.Stop()

	fetchWait := nats.MaxWait(time.Millisecond * time.Duration(r.opts.FetchTimeoutMs))

	first := true
	consecutiveWrongSeqCount := 0
	for {
		// Prioritized stop check
		select {
		case <-r.stop:
			r.finish("stopped", nil)
			return
		default:
		}

		select {
		case <-r.stop:
			r.finish("stopped", nil)
			return
		case <-requestTicker.C:
			batchSize := r.countBatchSize(curSeq, lastSeq)
			if batchSize < r.opts.MaxRequestBatchSize {
				lastSeq, err = r.getLastSeq()
				if err != nil {
					r.finish("unable to fetch LastSeq", err)
					return
				}
				batchSize = r.countBatchSize(curSeq, lastSeq)
			}

			messages, err := r.sub.Fetch(int(batchSize), fetchWait)
			if err != nil {
				if curSeq >= lastSeq {
					continue
				}
				r.finish(fmt.Sprintf("unable to fetch messages (batchSize=%d)", batchSize), err)
				return
			}

			result := make([]*Output, 0, len(messages))
			for _, msg := range messages {
				if err := msg.Ack(); err != nil {
					log.Printf("Stream Reader [%v]: can't ack message: %v", r.stream.Options().Nats.LogTag, err)
				}
				meta, err := msg.Metadata()
				if err != nil {
					r.finish("unable to parse message metadata", err)
					return
				}
				result = append(result, &Output{
					Msg:      msg,
					Metadata: meta,
				})
			}

			if r.opts.SortBatch {
				sort.Slice(result, func(i, j int) bool {
					return result[i].Metadata.Sequence.Stream < result[j].Metadata.Sequence.Stream
				})
			}

			for _, res := range result {
				if (!first || r.opts.StrictStart) && res.Metadata.Sequence.Stream != curSeq+1 {
					log.Printf(
						"Stream Reader [%v]: wrong sequence detected: %v, expected %v",
						r.stream.Options().Nats.LogTag,
						res.Metadata.Sequence.Stream,
						curSeq+1,
					)
					consecutiveWrongSeqCount++
					if consecutiveWrongSeqCount >= int(r.opts.WrongSeqToleranceWindow) {
						r.finish("error", errors.New("WrongSeqToleranceWindow exceeded"))
						return
					}
					continue
				}
				curSeq = res.Metadata.Sequence.Stream
				consecutiveWrongSeqCount = 0
				first = false

				if r.endSeq > 0 && res.Metadata.Sequence.Stream >= r.endSeq {
					r.finish("finished", nil)
					return
				}

				// Prioritized stop check
				select {
				case <-r.stop:
					r.finish("stopped", nil)
					return
				default:
				}

				select {
				case <-r.stop:
					r.finish("stopped", nil)
					return
				case r.output <- res:
				}

				if r.endSeq > 0 && res.Metadata.Sequence.Stream == r.endSeq-1 {
					r.finish("finished", nil)
					return
				}
			}
		}
	}
}

func (r *Reader) countBatchSize(curSeq uint64, lastSeq uint64) uint {
	border := lastSeq
	if r.endSeq != 0 && r.endSeq-1 < border {
		border = r.endSeq - 1
	}
	if curSeq >= border {
		return 1
	}
	residue := border - curSeq
	if residue > uint64(r.opts.MaxRequestBatchSize) {
		return r.opts.MaxRequestBatchSize
	}
	return uint(residue)
}

func (r *Reader) getLastSeq() (uint64, error) {
	info, _, err := r.stream.GetInfo(time.Second * time.Duration(r.opts.LastSeqUpdateIntervalSeconds))
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (r *Reader) finish(logMsg string, err error) {
	log.Printf("Stream Reader [%v]: %v: %v", r.stream.Options().Nats.LogTag, logMsg, err)
	if err != nil {
		out := &Output{
			Error: err,
		}
		select {
		case <-r.stop:
		case r.output <- out:
		}
	}
	close(r.output)
	r.sub.Unsubscribe()
	close(r.stopped)
}
