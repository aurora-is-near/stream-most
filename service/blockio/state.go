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
