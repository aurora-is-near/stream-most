package runner

import (
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"time"
)

type Option func(*Runner)

// WithWritesLimit max writes for the runner to perform before manual stop
// Is a soft limit, exact number of writes may be exceeded
func WithWritesLimit(n uint64) Option {
	return func(r *Runner) {
		r.maxWrites = n
	}
}

func WithReaderOptions(o *reader.Options) Option {
	return func(r *Runner) {
		r.readerOptions = o
	}
}

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

func WithValidatorOptions(fromSequence, toSequence uint64) Option {
	return func(r *Runner) {
		r.validatorFrom = fromSequence
		r.validatorTo = toSequence
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
