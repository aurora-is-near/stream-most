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
		Returns last known tip or nil if there's none
	*/
	Tip() Msg
}
