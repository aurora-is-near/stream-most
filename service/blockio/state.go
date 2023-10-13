package blockio

type StateProvider interface {
	/*
		Returns sequence of last known deleted element.

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
	*/
	LastKnownDeletedSeq() (uint64, error)

	/*
		Returns sequence of last known element.

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
	*/
	LastKnownSeq() (uint64, error)

	/*
		Returns last known message or nil if there's none (or it was deleted).
		Also returns sequence of this message - which is useful for identifying tip position when msg is nil.

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
	*/
	LastKnownMessage() (Msg, uint64, error)
}
