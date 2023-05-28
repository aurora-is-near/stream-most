package runner

import (
	"context"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/service/validator"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/sirupsen/logrus"
	"time"
)

type Mode string

const (
	Bridge    Mode = "bridge"
	Validator Mode = "validator"
)

type Runner struct {
	mode        Mode
	hasToFinish bool
	deadline    time.Time

	inputStream   stream.Interface
	outputStream  stream.Interface
	driver        drivers.Driver
	bridgeOptions *bridge.Options
	readerOptions *reader.Options
	maxWrites     uint64
	writes        uint64
	validatorTo   uint64
	validatorFrom uint64
}

func (r *Runner) Run() error {
	switch r.mode {
	case Bridge:
		return r.runBridge()
	case Validator:
		return r.runValidator()
	default:
		panic("Unknown mode")
	}

	return nil
}

func (r *Runner) runBridge() error {
	b := bridge.NewBridge(
		r.bridgeOptions,
		r.driver,
		r.inputStream,
		r.outputStream,
		block_writer.Options{}.WithDefaults(),
		reader.Options{}.WithDefaults(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	if !r.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, r.deadline)
		defer cancel()
	}

	b.On(observer.Write, func(_ any) {
		if r.maxWrites > 0 && r.writes >= r.maxWrites {
			logrus.Info("Runner is cancelling context...")
			cancel()
		}
		r.writes++
	})

	return b.Run(ctx)
}

func (r *Runner) runValidator() error {
	v := validator.NewValidator(r.inputStream, r.readerOptions, r.validatorFrom, r.validatorTo)
	ctx := context.Background()
	if !r.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, r.deadline)
		defer cancel()
	}

	return v.Run(ctx)
}

func NewRunner(mode Mode, options ...Option) *Runner {
	r := &Runner{
		mode: mode,
	}
	for _, option := range options {
		option(r)
	}
	return r
}
