package streaminput

import (
	"context"
	"sync"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/service/blockio"
	"github.com/aurora-is-near/stream-most/service/streamseek"
	"github.com/aurora-is-near/stream-most/stream"
)

type seekSettings struct {
	seekBlockAfter blocks.Block
	seekSeq        uint64
}

type session struct {
	seekOpts *seekSettings
	nextSeq  uint64

	ch        chan blockio.Msg
	closeOnce sync.Once
	closed    chan struct{}
	err       error

	outdated chan struct{}
}

func newSession(seekOpts *seekSettings, bufferSize uint) *session {
	return &session{
		seekOpts: seekOpts,
		ch:       make(chan blockio.Msg, bufferSize),
		closed:   make(chan struct{}),
		outdated: make(chan struct{}),
	}
}

func (s *session) close(err error) *session {
	s.closeOnce.Do(func() {
		s.err = err
		close(s.closed)
		close(s.ch)
	})
	return s
}

func (s *session) getError() error {
	if s.isClosed() {
		return s.err
	}
	return nil
}

func (s *session) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *session) runSeek(inputStream *stream.Stream, startSeq uint64, endSeq uint64) (_done <-chan error, _cancel func(reset bool)) {
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		var err error
		if s.seekOpts.seekBlockAfter != nil {
			s.nextSeq, err = streamseek.SeekBlock(ctx, inputStream, s.seekOpts.seekBlockAfter, startSeq, endSeq)
		} else {
			s.nextSeq, err = streamseek.SeekSeq(ctx, inputStream, s.seekOpts.seekSeq, startSeq, endSeq)
		}
		done <- err
		close(done)
	}()

	return done, func(reset bool) {
		cancel()
		if reset {
			<-done
			s.nextSeq = 0
		}
	}
}
