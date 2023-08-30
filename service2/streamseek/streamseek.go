package streamseek

import (
	"context"
	"errors"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
)

var ErrEmptyRange = errors.New("empty range")

func Seek(ctx context.Context, input stream.Interface, target blocks.Block, startSeq, endSeq uint64) (uint64, error) {
	logger := logrus.WithField("component", "streamseek")

	logger.Infof("Seeking on stream %s (startSeq=%d, endSeq=%d)", input.Name(), startSeq, endSeq)
	logger.Infof("Looking for earliest block that is greater than '%s'", blocks.ConstructMsgID(target))
	logger.Infof("(if there's no such in range - the latest one will be returned)")

	info, err := input.GetInfo(ctx)
	if err != nil {
		logger.Infof("Unable to get stream info: %v", err)
		return 0, fmt.Errorf("unable to get stream info: %w", err)
	}

	state := info.State
	logger.Infof("Got stream info: firstSeq=%d, lastSeq=%d, msgs=%d", state.FirstSeq, state.LastSeq, state.Msgs)

	if state.Msgs == 0 || state.LastSeq == 0 || state.FirstSeq > state.LastSeq {
		logger.Warningf("There's currently no messages in this stream")
		return 0, ErrEmptyRange
	}

	if endSeq > 0 && startSeq >= endSeq {
		logger.Warningf("Weird configuration (startSeq >= endSeq)")
		return 0, ErrEmptyRange
	}

	if endSeq > 0 && state.FirstSeq >= endSeq {
		logger.Warningf("Nothing to seek (firstSeq >= endSeq)")
		return 0, ErrEmptyRange
	}

	if startSeq > state.LastSeq {
		logger.Warningf("Nothing to seek yet (startSeq > lastSeq)")
		return 0, ErrEmptyRange
	}

	leftBound := state.FirstSeq
	if leftBound < startSeq {
		leftBound = startSeq
	}

	rightBound := state.LastSeq + 1
	if endSeq > 0 && rightBound > endSeq {
		rightBound = endSeq
	}

	logger.Infof("Will perform binsearch on sequence range [%d; %d)", leftBound, rightBound)

	l, r := leftBound, rightBound
	for l+1 < r {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		m := (l + 1) / 2
		logger.Infof("Checking element on seq=%d", m)

		msg, err := input.Get(ctx, m)
		if err != nil {
			logger.Errorf("Unable to get msg on seq=%d: %v", m, err)
			return 0, fmt.Errorf("unable to get msg on seq=%d: %w", m, err)
		}

		msgBlock, err := formats.Active().ParseMsg(msg)
		if err != nil {
			logger.Errorf("Unable to get msg on seq=%d: (%v), will consider this message as greater than needed", m, err)
			r = m
			continue
		}

		if blocks.Less(target, msgBlock.Block) {
			logger.Infof(
				"Block on seq (%d) is greater than target ('%s' > '%s')",
				m,
				blocks.ConstructMsgID(msgBlock.Block),
				blocks.ConstructMsgID(target),
			)
			r = m
		} else {
			logger.Infof(
				"Block on seq (%d) is lower or equal to target ('%s' <= '%s')",
				m,
				blocks.ConstructMsgID(msgBlock.Block),
				blocks.ConstructMsgID(target),
			)
			l = m
		}
	}

	// Now l contains latest block that is lower or equal to target
	if l+1 < rightBound {
		// So when possible, we move it to next one (earliest that is greater than target)
		l++
	}

	logger.Infof("Result seq=%d", l)
	return l, nil
}
