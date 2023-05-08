package jitter

type Options struct {
	DelayChance float64

	// Maximal and minimal delay that can occur to one message
	// (MinDelay only happens if we hit our DelayChance)
	MaxDelay uint64
	MinDelay uint64

	DropoutChance float64
}

func (o *Options) Validated() *Options {
	if o.DropoutChance < 0 || o.DropoutChance > 1 {
		panic("dropout chance must be between 0 and 1")
	}

	return o
}
