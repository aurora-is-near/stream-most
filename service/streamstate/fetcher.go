package streamstate

import (
	"context"
	"time"

	"github.com/aurora-is-near/stream-most/stream"
)

type Fetcher struct {
	input     *stream.Stream
	interval  time.Duration
	stopOnErr bool
	cb        func(*State)

	ctx     context.Context
	cancel  func()
	stopped chan struct{}
}

func StartFetcher(input *stream.Stream, interval time.Duration, stopOnErr bool, cb func(*State)) *Fetcher {
	f := &Fetcher{
		input:     input,
		interval:  interval,
		stopOnErr: stopOnErr,
		cb:        cb,
		stopped:   make(chan struct{}),
	}
	f.ctx, f.cancel = context.WithCancel(context.Background())

	go f.run()

	return f
}

func (f *Fetcher) Stop(wait bool) {
	f.cancel()
	if wait {
		<-f.stopped
	}
}

func (f *Fetcher) run() {
	defer close(f.stopped)

	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	if !f.fetch() {
		return
	}

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			if !f.fetch() {
				return
			}
		}
	}
}

func (f *Fetcher) fetch() bool {
	s := Fetch(f.ctx, f.input)

	select {
	case <-f.ctx.Done():
		return false
	default:
	}

	f.cb(s)

	if s.Err != nil && f.stopOnErr {
		return false
	}

	return true
}
