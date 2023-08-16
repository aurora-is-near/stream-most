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

type AutoReader struct {
	*logrus.Entry

	streamOpts      *stream.Options
	readerOpts      *reader.Options
	ReconnectWaitMs uint

	output chan messages.NatsMessage
	stop   chan struct{}
	wg     sync.WaitGroup
}

func NewAutoReader(streamOptions *stream.Options, readerOpts *reader.Options) *AutoReader {
	ar := &AutoReader{
		Entry: logrus.New().
			WithField("component", "autoreader").
			WithField("stream", streamOptions.Stream),

		streamOpts: streamOptions,
		readerOpts: readerOpts,
	}

	return ar
}

func (ar *AutoReader) Start(startSeq uint64, endSeq uint64) {
	ar.output = make(chan messages.NatsMessage)
	ar.stop = make(chan struct{})
	ar.wg.Add(1)
	go ar.run(startSeq, endSeq)
}

func (ar *AutoReader) Output() <-chan messages.NatsMessage {
	return ar.output
}

func (ar *AutoReader) Stop() {
	close(ar.stop)
	ar.wg.Wait()
}

func (ar *AutoReader) newStream() (stream.Interface, error) {
	if ar.streamOpts.ShouldFake {
		if ar.streamOpts.FakeStream != nil {
			return ar.streamOpts.FakeStream, nil
		}

		return defaultFakeStreamOpener(ar.streamOpts), nil
	}
	return stream.Connect(ar.streamOpts)
}

func (ar *AutoReader) run(nextSeq uint64, endSeq uint64) {
	defer ar.wg.Done()
	defer close(ar.output)

	var err error
	var s stream.Interface
	var r reader.IReader

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
			r, err = reader.Start(context.Background(), ar.readerOpts, s, nil, nextSeq, endSeq)
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
			case ar.output <- out:
			}
			nextSeq = out.GetSequence() + 1
		}
	}
}
