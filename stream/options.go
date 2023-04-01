package stream

import "github.com/aurora-is-near/stream-most/nats"

type Options struct {
	Nats          *nats.Options
	Stream        string
	Subject       string `json:",omitempty"`
	RequestWaitMs uint

	ShouldFake bool
	FakeStream Interface
}

func (opts Options) WithDefaults() *Options {
	if opts.RequestWaitMs == 0 {
		opts.RequestWaitMs = 5000
	}
	return &opts
}
