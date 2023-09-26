package streamoutput

import (
	"sync"

	"github.com/aurora-is-near/stream-most/stream"
)

const maxQuota = 100

type connection struct {
	stream         *stream.Stream
	subjectPattern string

	quota   chan struct{}
	err     error
	hasErr  chan struct{}
	errOnce sync.Once
}

func newConnection(s *stream.Stream, subjectPattern string) *connection {
	c := &connection{
		stream:         s,
		subjectPattern: subjectPattern,
		quota:          make(chan struct{}, maxQuota),
		hasErr:         make(chan struct{}),
	}
	for i := 0; i < maxQuota; i++ {
		c.quota <- struct{}{}
	}
	return c
}

func (c *connection) setError(err error) *connection {
	c.errOnce.Do(func() {
		c.err = err
		close(c.hasErr)
	})
	return c
}

func (c *connection) killQuota() *connection {
	for i := 0; i < maxQuota; i++ {
		<-c.quota
	}
	return c
}

func (c *connection) obtainQuota() error {
	select {
	case <-c.hasErr:
		return c.err
	default:
	}

	select {
	case <-c.hasErr:
		return c.err
	case <-c.quota:
		return nil
	}
}

func (c *connection) returnQuota() *connection {
	c.quota <- struct{}{}
	return c
}
