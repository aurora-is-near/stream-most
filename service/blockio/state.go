package blockio

type State interface {
	/*
		Returns first present sequence
	*/
	FirstSeq() uint64

	/*
		Returns last present sequence
	*/
	LastSeq() uint64

	/*
		Returns last known tip or nil if there's none.
		Nil is also returned when stream messages are thrown out so fast that it's impossible
		to catch them with tip-fetch.
	*/
	Tip() Msg
}

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
