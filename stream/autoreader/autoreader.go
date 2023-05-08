package autoreader

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/sirupsen/logrus"
	"log"
	"sync"
	"time"
)

type AutoReader struct {
	*logrus.Entry

	streamOpts      *stream.Options
	readerOpts      *reader.Options
	ReconnectWaitMs uint

	output chan *reader.Output
	stop   chan struct{}
	wg     sync.WaitGroup
}

func NewAutoReader(startSeq uint64, endSeq uint64, readerOpts *reader.Options, streamOptions *stream.Options) *AutoReader {
	ar := &AutoReader{
		readerOpts: readerOpts,
		Entry: logrus.New().
			WithField("component", "autoreader").
			WithField("stream", streamOptions.Stream),
		streamOpts: streamOptions,
	}

	return ar
}

func (ar *AutoReader) Start(startSeq uint64) {
	ar.output = make(chan *reader.Output)
	ar.stop = make(chan struct{})
	ar.wg.Add(1)
	go ar.run(startSeq)
}

func (ar *AutoReader) Output() chan *reader.Output {
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

func (ar *AutoReader) run(nextSeq uint64) {
	defer ar.wg.Done()

	var err error
	var s stream.Interface
	var r reader.IReader

	disconnect := func() {
		if r != nil {
			r.Stop()
			r = nil
		}
		if s != nil {
			_ = s.Disconnect()
			s = nil
		}
	}
	defer disconnect()

	connectionProblem := false
	for {
		select {
		case <-ar.stop:
			return
		default:
		}

		if connectionProblem {
			disconnect()
			log.Printf("Waiting for %vms before reconnection...", ar.ReconnectWaitMs)
			timer := time.NewTimer(time.Millisecond * time.Duration(ar.ReconnectWaitMs))
			select {
			case <-ar.stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			connectionProblem = false
		}

		if s == nil || r == nil {
			disconnect()
			s, err = ar.newStream()
			if err != nil {
				log.Printf("Can't connect stream: %v", err)
				connectionProblem = true
				continue
			}

			select {
			case <-ar.stop:
				return
			default:
			}

			r, err = reader.Start(ar.readerOpts, s, nextSeq, 0)
			if err != nil {
				log.Printf("Can't start reader: %v", err)
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
				log.Printf("readerOpts was stopped for some reason")
				connectionProblem = true
				continue
			}
			if out.Error != nil {
				log.Printf("readerOpts error: %v", out.Error)
				connectionProblem = true
				continue
			}
			if out.Metadata.Sequence.Stream != nextSeq {
				log.Printf("Got wrong seq from reader. Expected: %v, found: %v", nextSeq, out.Metadata.Sequence.Stream)
				connectionProblem = true
				continue
			}
			select {
			case <-ar.stop:
				return
			case ar.output <- out:
			}
			nextSeq++
		}
	}
}
