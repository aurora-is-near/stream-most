package autoreader

import (
	"context"
	"sync"
	"time"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/sirupsen/logrus"
)

type AutoReader[T any] struct {
	*logrus.Entry

	streamOpts      *stream.Options
	readerOpts      *reader.Options
	ReconnectWaitMs uint

	decodeFn func(msg messages.NatsMessage) (T, error)
	output   chan *reader.DecodedMsg[T]
	stop     chan struct{}
	wg       sync.WaitGroup
}

func NewAutoReader[T any](streamOptions *stream.Options, readerOpts *reader.Options, decodeFn func(msg messages.NatsMessage) (T, error)) *AutoReader[T] {
	ar := &AutoReader[T]{
		Entry: logrus.New().
			WithField("component", "autoreader").
			WithField("stream", streamOptions.Stream),

		streamOpts: streamOptions,
		readerOpts: readerOpts,
		decodeFn:   decodeFn,
	}

	return ar
}

func (ar *AutoReader[T]) Start(startSeq uint64, endSeq uint64) {
	ar.output = make(chan *reader.DecodedMsg[T])
	ar.stop = make(chan struct{})
	ar.wg.Add(1)
	go ar.run(startSeq, endSeq)
}

func (ar *AutoReader[T]) Output() <-chan *reader.DecodedMsg[T] {
	return ar.output
}

func (ar *AutoReader[T]) Stop() {
	close(ar.stop)
	ar.wg.Wait()
}

func (ar *AutoReader[T]) newStream() (stream.Interface, error) {
	if ar.streamOpts.ShouldFake {
		if ar.streamOpts.FakeStream != nil {
			return ar.streamOpts.FakeStream, nil
		}

		return defaultFakeStreamOpener(ar.streamOpts), nil
	}
	return stream.Connect(ar.streamOpts)
}

func (ar *AutoReader[T]) run(nextSeq uint64, endSeq uint64) {
	defer ar.wg.Done()
	defer close(ar.output)

	var err error
	var s stream.Interface
	var r reader.IReader[T]

	disconnect := func() {
		if r != nil {
			ar.Info("stopping reader...")
			r.Stop()
			r = nil
			ar.Info("reader stopped")
		}
		if s != nil {
			ar.Info("disconnecting from stream...")
			_ = s.Disconnect()
			s = nil
			ar.Info("disconnected from stream")
		}
	}
	defer disconnect()

	connectionProblem := false
	for {
		if endSeq > 0 && nextSeq >= endSeq {
			ar.Info("reached last sequence, finishing")
			return
		}

		select {
		case <-ar.stop:
			return
		default:
		}

		if connectionProblem {
			disconnect()
			ar.Infof("waiting for %vms before reconnection...", ar.ReconnectWaitMs)
			timer := time.NewTimer(time.Millisecond * time.Duration(ar.ReconnectWaitMs))
			select {
			case <-ar.stop:
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			connectionProblem = false
		}

		if s == nil || r == nil {
			disconnect()

			ar.Infof("connecting to stream...")
			s, err = ar.newStream()
			if err != nil {
				ar.Errorf("unable to connect to stream: %v", err)
				connectionProblem = true
				continue
			}

			select {
			case <-ar.stop:
				return
			default:
			}

			ar.Infof("starting reader from seq %d...", nextSeq)
			r, err = reader.Start(context.Background(), s, ar.readerOpts.WithStartSeq(nextSeq), ar.decodeFn)
			if err != nil {
				ar.Errorf("unable to start reader: %v", err)
				connectionProblem = true
				continue
			}
		}

		select {
		case <-ar.stop:
			return
		default:
		}

		select {
		case <-ar.stop:
			return
		case out, ok := <-r.Output():
			if !ok {
				if r.Error() != nil {
					ar.Errorf("got reader error, will reconnect: %v", err)
					connectionProblem = true
					continue
				}
				ar.Info("reached last sequence, finishing...")
				return
			}
			select {
			case <-ar.stop:
				return
			case <-out.WaitDecoding():
			}
			select {
			case <-ar.stop:
				return
			case ar.output <- out:
			}
			nextSeq = out.Msg().GetSequence() + 1
		}
	}
}
