package outputter

type ResponseStatus uint

const (
	OutputterUnavailable ResponseStatus = 0
	OK                   ResponseStatus = 1
	Maybe                ResponseStatus = 2
	LowBlock             ResponseStatus = 3
	HighBlock            ResponseStatus = 4
	HashMismatch         ResponseStatus = 5
	Interrupted          ResponseStatus = 6
)
