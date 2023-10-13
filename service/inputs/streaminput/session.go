package streaminput

import (
	"sync"

	"github.com/aurora-is-near/stream-most/service/blockio"
)

// Static assertion
var _ blockio.InputSession = (*session)(nil)

type session struct {
	seekOpts *seekOptions
	nextSeq  uint64

	ch chan blockio.Msg

	err       error
	finalized chan struct{}
	acquired  bool
	mtx       sync.Mutex

	outdated chan struct{}
}

func newSession(seekOpts *seekOptions, bufferSize uint) *session {
	return &session{
		seekOpts:  seekOpts,
		ch:        make(chan blockio.Msg, bufferSize),
		finalized: make(chan struct{}),
		outdated:  make(chan struct{}),
	}
}

func (s *session) finalize(err error) *session {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	select {
	case <-s.finalized:
		return s
	default:
	}

	s.err = err
	close(s.finalized)

	if !s.acquired {
		close(s.ch)
	}

	return s
}

func (s *session) acquire() bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	select {
	case <-s.finalized:
		return false
	default:
	}

	s.acquired = true
	return true
}

func (s *session) release() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if !s.acquired {
		return
	}
	s.acquired = false

	select {
	case <-s.finalized:
		close(s.ch)
	default:
	}
}

func (s *session) Msgs() <-chan blockio.Msg {
	return s.ch
}

func (s *session) Error() error {
	select {
	case <-s.finalized:
		return s.err
	default:
		return nil
	}
}
