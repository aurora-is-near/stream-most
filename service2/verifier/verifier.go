package verifier

import (
	"errors"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

var (
	ErrCompletelyIrrelevant = errors.New("completely irrelevant")
	ErrFilteredShard        = fmt.Errorf("filtered shard (%w)", ErrCompletelyIrrelevant)
	ErrUnknownBlockType     = fmt.Errorf("unknown block type (%w)", ErrCompletelyIrrelevant)

	ErrAlreadyIrrelevant = errors.New("already irrelevant")
	ErrLowHeight         = fmt.Errorf("low height (%w)", ErrAlreadyIrrelevant)
	ErrReannouncement    = fmt.Errorf("reannouncement (%w)", ErrAlreadyIrrelevant)
	ErrLowShard          = fmt.Errorf("low shard (%w)", ErrAlreadyIrrelevant)
	ErrUnwantedShard     = fmt.Errorf("unwanted shard (%w)", ErrAlreadyIrrelevant)

	ErrMayBeRelevantLater      = errors.New("may be relevant later")
	ErrHeightGap               = fmt.Errorf("height gap (%w)", ErrMayBeRelevantLater)
	ErrIncompletePreviousBlock = fmt.Errorf("incomplete previous block (%w)", ErrMayBeRelevantLater)
	ErrUnannouncedBlock        = fmt.Errorf("unannounced block (%w)", ErrMayBeRelevantLater)
	ErrHashMismatch            = fmt.Errorf("hash mismatch (%w)", ErrMayBeRelevantLater)
	ErrShardGap                = fmt.Errorf("shard gap (%w)", ErrMayBeRelevantLater)
)

type Verifier interface {
	CanAppend(last, next *messages.BlockMessage) error
}
