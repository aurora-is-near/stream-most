package runner

import (
	"context"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/bridge"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
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
}

func (r *Runner) Run() error {
	switch r.mode {
	case Bridge:
		return r.runBridge()
	case Validator:
		r.runValidator()
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

	ctx := context.Background()
	if !r.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, r.deadline)
		defer cancel()
	}

	return b.Run(ctx)
}

func (r *Runner) runValidator() {
	panic("Validator is not supported yet")
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
