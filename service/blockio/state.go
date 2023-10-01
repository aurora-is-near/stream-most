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
		Returns last known message or nil if there's none.

		Error classes:
			- ErrTemporarilyUnavailable
			- ErrCompletelyUnavailable
	*/
	LastKnownMessage() (Msg, error)
}
