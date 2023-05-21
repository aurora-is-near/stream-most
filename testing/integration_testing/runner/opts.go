package runner

import (
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"time"
)

type Option func(*Runner)

func WithBridgeOptions(o *bridge.Options) Option {
	return func(r *Runner) {
		r.bridgeOptions = o
	}
}

func WithDeadline(deadline time.Time) Option {
	return func(r *Runner) {
		r.deadline = deadline
	}
}

func WithInputStream(s stream.Interface) Option {
	return func(r *Runner) {
		r.inputStream = s
	}
}

func WithOutputStream(s stream.Interface) Option {
	return func(r *Runner) {
		r.outputStream = s
	}
}

func WithDriver(d drivers.Driver) Option {
	return func(r *Runner) {
		r.driver = d
	}
}

func HasToFinish() Option {
	return func(r *Runner) {
		r.hasToFinish = true
	}
}
