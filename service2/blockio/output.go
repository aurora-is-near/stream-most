package blockio

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Output interface {
	/*
		Returns stream state.

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
	*/
	State() (State, error)

	/*
		Tries to append message safely (using expected-predecessor checks).
		predecessorMsgID can be left empty (means no predecessor msgid enforced).

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
			- ErrCollision
			- ErrCanceled
			- ErrWrongPredecessor
			- ErrRemovedPredecessor
			- ErrRemovedPosition
	*/
	AppendSafely(ctx context.Context, predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage) error

	/*
		Gracefully stops component, terminates all associated goroutines, releases connections etc.
	*/
	Stop(wait bool)
}
