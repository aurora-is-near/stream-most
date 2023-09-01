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
		Tries to append message safely (using expected-tip checks).

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
			- ErrCollision
	*/
	AppendSafely(ctx context.Context, tipSeq uint64, tip, msg *messages.BlockMessage) error
}
