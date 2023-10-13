package blockio

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Output interface {
	StateProvider

	/*
		Tries to write message immediately after provided predecessor.
		If it's not feasible - no write happens, and reason is returned.
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
	ProtectedWrite(ctx context.Context, predecessorSeq uint64, predecessorMsgID string, msg *messages.BlockMessage) error

	/*
		Gracefully stops component, terminates all associated goroutines, releases connections etc.
	*/
	Stop(wait bool)
}
