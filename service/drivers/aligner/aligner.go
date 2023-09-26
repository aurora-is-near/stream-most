package aligner

import (
	"errors"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/drivers"
	"github.com/aurora-is-near/stream-most/service/verifier"
	"github.com/aurora-is-near/stream-most/support/tolerance"
)

type Config struct {
	ReorderBufferSize    uint
	LowBlocksTolerance   int
	WrongBlocksTolerance int
	NoWriteTolerance     int
}

type aligner struct {
	config   *Config
	verifier verifier.Verifier

	reorderBuffer []*messages.BlockMessage

	lowBlocksTolerance   *tolerance.Tolerance
	wrongBlocksTolerance *tolerance.Tolerance
	noWriteTolerance     *tolerance.Tolerance
}

func NewAligner(config *Config, verifier verifier.Verifier) drivers.Driver {
	return &aligner{
		config:               config,
		verifier:             verifier,
		lowBlocksTolerance:   tolerance.NewTolerance(config.LowBlocksTolerance),
		wrongBlocksTolerance: tolerance.NewTolerance(config.WrongBlocksTolerance),
		noWriteTolerance:     tolerance.NewTolerance(config.NoWriteTolerance),
	}
}

func (a *aligner) Next(tip, msg *messages.BlockMessage, decodingError error) (*messages.BlockMessage, error) {
	if decodingError != nil {
		if !a.wrongBlocksTolerance.Tolerate(1) {
			return nil, fmt.Errorf("wrong blocks tolerance exceeded (%w)", drivers.ErrToleranceExceeded)
		}
		if !a.noWriteTolerance.Tolerate(1) {
			return nil, fmt.Errorf("no write tolerance exceeded (%w)", drivers.ErrToleranceExceeded)
		}
		return a.nextReordered(tip)
	}
	if msg == nil {
		return a.nextReordered(tip)
	}

	err := a.verifier.CanAppend(tip, msg)
	switch {

	case err == nil:
		a.lowBlocksTolerance.Reset()
		a.wrongBlocksTolerance.Reset()
		a.noWriteTolerance.Reset()
		return msg, nil

	case errors.Is(err, verifier.ErrCompletelyIrrelevant):
		if !a.noWriteTolerance.Tolerate(1) {
			return nil, fmt.Errorf("no write tolerance exceeded (%w)", drivers.ErrToleranceExceeded)
		}
		return a.nextReordered(tip)

	case errors.Is(err, verifier.ErrAlreadyIrrelevant):
		a.wrongBlocksTolerance.Reset()
		if !a.lowBlocksTolerance.Tolerate(1) {
			return nil, fmt.Errorf("low blocks tolerance exceeded (%w)", drivers.ErrToleranceExceeded)
		}
		if !a.noWriteTolerance.Tolerate(1) {
			return nil, fmt.Errorf("no write tolerance exceeded (%w)", drivers.ErrToleranceExceeded)
		}
		a.stashMsg(msg)
		return a.nextReordered(tip)

	case errors.Is(err, verifier.ErrMayBeRelevantLater):
		a.lowBlocksTolerance.Reset()
		if !a.wrongBlocksTolerance.Tolerate(1) {
			return nil, fmt.Errorf("wrong blocks tolerance exceeded (%w)", drivers.ErrToleranceExceeded)
		}
		if !a.noWriteTolerance.Tolerate(1) {
			return nil, fmt.Errorf("no write tolerance exceeded (%w)", drivers.ErrToleranceExceeded)
		}
		a.stashMsg(msg)
		return a.nextReordered(tip)

	default:
		return nil, fmt.Errorf("unknown verifier error: %w", err)
	}
}

func (a *aligner) stashMsg(msg *messages.BlockMessage) {
	if a.config.ReorderBufferSize == 0 {
		return
	}
	if len(a.reorderBuffer) == int(a.config.ReorderBufferSize) {
		highest := 0
		for i := 1; i < len(a.reorderBuffer); i++ {
			if blocks.Less(a.reorderBuffer[highest].Block, a.reorderBuffer[i].Block) {
				highest = i
			}
		}
		a.reorderBuffer[highest] = msg
	} else {
		a.reorderBuffer = append(a.reorderBuffer, msg)
	}
}

func (a *aligner) nextReordered(tip *messages.BlockMessage) (*messages.BlockMessage, error) {
	selected := -1

	i := 0
	for i < len(a.reorderBuffer) {
		err := a.verifier.CanAppend(tip, a.reorderBuffer[i])
		if err == nil {
			if selected < 0 || blocks.Less(a.reorderBuffer[i].Block, a.reorderBuffer[selected].Block) {
				selected = i
			}
			i++
			continue
		}
		if errors.Is(err, verifier.ErrMayBeRelevantLater) {
			i++
			continue
		}
		a.reorderBuffer[i] = a.reorderBuffer[len(a.reorderBuffer)-1]
		a.reorderBuffer = a.reorderBuffer[:len(a.reorderBuffer)-1]
	}

	if selected < 0 {
		return nil, drivers.ErrNoNext
	}

	msg := a.reorderBuffer[selected]
	a.reorderBuffer[selected] = a.reorderBuffer[len(a.reorderBuffer)-1]
	a.reorderBuffer = a.reorderBuffer[:len(a.reorderBuffer)-1]

	return msg, nil
}
