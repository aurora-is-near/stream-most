package streamseek

import (
	"context"
	"fmt"

	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
)

/*
	Seeks earliest sequence that is equal or greater to provided.
	If such sequence is out of range - ErrEmpty range will be returned.
	If such sequence is not available yet - it will still be returned.
*/

func SeekSeq(ctx context.Context, input stream.Interface, target uint64, startSeq, endSeq uint64) (uint64, error) {
	logger := logrus.WithField("component", "streamseek").WithField("stream", input.Name())

	logger.Infof("Seeking on stream %s (startSeq=%d, endSeq=%d)", input.Name(), startSeq, endSeq)
	logger.Infof("Looking for earliest sequence that is greater or equal to %d", target)

	if endSeq > 0 && startSeq >= endSeq {
		logger.Warnf("Weird configuration (startSeq (%d) >= endSeq (%d))", startSeq, endSeq)
		return 0, fmt.Errorf("%w: weird configuration (startSeq (%d) >= endSeq (%d))", ErrEmptyRange, startSeq, endSeq)
	}

	if endSeq > 0 && target >= endSeq {
		logger.Warnf("Nothing to seek (target (%d) >= endSeq (%d))", target, endSeq)
		return 0, fmt.Errorf("%w: nothing to seek (target (%d) >= endSeq (%d))", ErrEmptyRange, target, endSeq)
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
	if firstLegitSeq < target {
		firstLegitSeq = target
	}

	if firstLegitSeq > state.LastSeq {
		logger.Warnf("Nothing to seek yet (firstLegitSeq (%d) > lastSeq (%d)), selecting firstLegitSeq", firstLegitSeq, state.LastSeq)
	}

	logger.Infof("Result seq=%d", firstLegitSeq)
	return firstLegitSeq, nil
}
