package stream

import "github.com/aurora-is-near/stream-most/transport"

type Options struct {
	Nats          *transport.Options
	Stream        string
	RequestWaitMs uint
	WriteWaitMs   uint

	ShouldFake bool
	FakeStream Interface
}

func (opts Options) WithDefaults() *Options {
	if opts.RequestWaitMs == 0 {
		opts.RequestWaitMs = 5000
	}
	if opts.WriteWaitMs == 0 {
		opts.WriteWaitMs = 10000
	}
	return &opts
}
