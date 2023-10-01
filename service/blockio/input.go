package blockio

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
)

type Input interface {
	StateProvider

	/*
		Returns current reading session.
		Every seek-method immediately closes current session, places new one and returns.
	*/
	CurrentSession() InputSession

	/*
		Asynchronously initiates new reading session that will start from seeking earliest block that is
		greater than provided block (by using binsearch).
		If all available blocks are lower or equal to provided, next (not yet available) sequence will be selected.
	*/
	SeekNextBlock(block blocks.Block, startSeq uint64, endSeq uint64) InputSession

	/*
		Asynchronously initiates new reading session that will start from seeking earliest sequence
		that is greater or equal to provided.
	*/
	SeekSeq(seq uint64, startSeq uint64, endSeq uint64) InputSession

	/*
		Gracefully stops component, terminates all associated goroutines, releases connections etc.
	*/
	Stop(wait bool)
}

type InputSession interface {
	/*
		Returns messages from current reading session. It is only closed when reading session has ended.
		It's guaranteed that Error() will return the correct closing reason after channel is closed.
	*/
	Msgs() <-chan Msg

	/*
		Returns session closing reason.

		Error classes:
			- ErrReseeked
			- nil (if it wasn't closed yet, or end is reached)
			- ErrCompletelyUnavailable
	*/
	Error() error
}
