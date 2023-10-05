package streamseek

import (
	"context"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
)

/*
	Seeks earliest block that is greater (or equal) than provided block (by using binsearch).
	If none of existing blocks match this condition, next (not yet available) sequence will be selected.
	If there's no (but will be in theory) available blocks, next (not yet available) sequence will be selected.
	If there's no (and will never be any) available blocks in provided range - ErrEmptyRange will be returned.
*/

func SeekBlock(ctx context.Context, input *stream.Stream, target blocks.Block, startSeq, endSeq uint64, onlyGreater bool) (uint64, error) {
	logger := logrus.WithField("component", "streamseek").WithField("stream", input.Name())

	logger.Infof("Seeking on stream %s (startSeq=%d, endSeq=%d)", input.Name(), startSeq, endSeq)

	if onlyGreater {
		logger.Infof("Looking for earliest block that is greater than '%s'", blocks.ConstructMsgID(target))
	} else {
		logger.Infof("Looking for earliest block that is greater or equal to '%s'", blocks.ConstructMsgID(target))
	}

	if endSeq > 0 && startSeq >= endSeq {
		logger.Warnf("Weird configuration (startSeq (%d) >= endSeq (%d))", startSeq, endSeq)
		return 0, fmt.Errorf("%w: weird configuration (startSeq (%d) >= endSeq (%d))", ErrEmptyRange, startSeq, endSeq)
	}

	info, err := input.GetInfo(ctx)
	if err != nil {
		logger.Errorf("Unable to get stream info: %v", err)
		return 0, fmt.Errorf("unable to get stream info: %w", err)
	}

	state := info.State
	logger.Infof("Got stream info: firstSeq=%d, lastSeq=%d, msgs=%d", state.FirstSeq, state.LastSeq, state.Msgs)

	if endSeq > 0 && state.FirstSeq >= endSeq {
		logger.Warnf("Nothing to seek (firstSeq (%d) >= endSeq (%d))", state.FirstSeq, endSeq)
		return 0, fmt.Errorf("%w: nothing to seek (firstSeq (%d) >= endSeq (%d))", ErrEmptyRange, state.FirstSeq, endSeq)
	}

	firstLegitSeq := uint64(1)
	if firstLegitSeq < state.FirstSeq {
		firstLegitSeq = state.FirstSeq
	}
	if firstLegitSeq < startSeq {
		firstLegitSeq = startSeq
	}

	if firstLegitSeq > state.LastSeq {
		logger.Warnf("Nothing to seek yet (firstLegitSeq (%d) > lastSeq (%d)), selecting firstLegitSeq", firstLegitSeq, state.LastSeq)
		return firstLegitSeq, nil
	}

	lowerBound := firstLegitSeq - 1

	upperBound := state.LastSeq + 1
	if endSeq > 0 && upperBound > endSeq {
		upperBound = endSeq
	}

	logger.Infof("Performing binsearch on sequence range (%d;%d)...", lowerBound, upperBound)

	l, r := lowerBound, upperBound
	for l+1 < r {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		m := (l + r) / 2

		msg, err := input.Get(ctx, m)
		if err != nil {
			info, infoErr := input.GetInfo(ctx)
			if infoErr != nil {
				logger.Errorf("Unable to get stream info: %v", infoErr)
				return 0, fmt.Errorf("unable to get stream info: %w", infoErr)
			}
			if info.State.FirstSeq > m {
				logger.Infof("Msg on seq=%d fell out of stream, will consider it as lower than needed", m)
				l = m
				continue
			}
			logger.Errorf("Unable to get msg on seq=%d: %v", m, err)
			return 0, fmt.Errorf("unable to get msg on seq=%d: %w", m, err)
		}

		msgBlock, err := formats.Active().ParseMsg(msg)
		if err != nil {
			logger.Errorf("Unable to parse block on seq=%d (%v), will consider this message as greater than needed", m, err)
			r = m
			continue
		}

		matches := blocks.Less(target, msgBlock.Block)
		if !onlyGreater {
			matches = matches || blocks.Equal(target, msgBlock.Block)
		}

		if matches {
			logger.Infof("Block on seq %d with msgid '%s' matches the condition", m, blocks.ConstructMsgID(msgBlock.Block))
			r = m
		} else {
			logger.Infof("Block on seq %d with msgid '%s' doesn't match the condition", m, blocks.ConstructMsgID(msgBlock.Block))
			l = m
		}
	}

	if endSeq > 0 && r >= endSeq {
		logger.Warnf("All elements with seq < endSeq (%d) don't match the condition", endSeq)
		return 0, fmt.Errorf("%w: all elements with seq < endSeq (%d) don't match the condition", ErrEmptyRange, endSeq)
	}

	if r > state.LastSeq {
		logger.Warnf("All existing elements don't match the condition, returning next (not yet available) seq %d", r)
		return r, nil
	}

	logger.Infof("Result seq=%d", r)
	return r, nil
}
