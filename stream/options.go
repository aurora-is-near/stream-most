package stream

import (
	"time"

	"github.com/aurora-is-near/stream-most/transport"
)

type Options struct {
	Nats        *transport.NATSConfig
	Stream      string
	RequestWait time.Duration
	WriteWait   time.Duration

	ShouldFake bool
	FakeStream Interface
}

func (opts Options) WithDefaults() *Options {
	if opts.RequestWait == 0 {
		opts.RequestWait = time.Second * 10
	}
	if opts.WriteWait == 0 {
		opts.WriteWait = time.Second * 10
	}
	return &opts
}
