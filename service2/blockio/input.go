package blockio

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
)

type Input interface {
	/*
		Returns stream state.

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
	*/
	State() (State, error)

	/*
		Returns messages from current reading session. It is only closed when reading session has ended.

		If new reading session was requested - next call to Blocks() would return another channel.
		In other words, each channel is associated with some reading session.
	*/
	Blocks() <-chan Msg

	/*
		Returns error if there's any.
		Error is only available when current reading session is closed.

		Error classes:
			- ErrCompletelyUnavailable
	*/
	Error() error

	/*
		Asynchronously initiates new reading session that will start from seeking earliest block that is
		greater than provided block (by using binsearch).
		If all available blocks are lower or equal to provided, next (not yet available) sequence will be selected.

		Immediately resets output and error, immediately returns.
	*/
	SeekNextBlock(block blocks.Block)

	/*
		Asynchronously initiates new reading session that will start from seeking earliest sequence
		that is greater or equal to provided.

		Immediately resets output and error, immediately returns.
	*/
	SeekSeq(seq uint64)
}
