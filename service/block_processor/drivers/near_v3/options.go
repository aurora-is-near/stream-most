package near_v3

type Options struct {
	// StuckTolerance is the number of messages we can receive without writing out any blocks,
	// before we consider ourselves stuck and exit.
	StuckTolerance uint64

	// StuckRecovery is whether driver should attempt to recover from a stuck state.
	// Recovery is done by manually searching for the block (or it's missing shards) in the input stream
	StuckRecovery bool
	// StuckRecoveryWindowSize is the number of messages to both left and right of the last written block
	// to search for the missing block
	StuckRecoveryWindowSize uint64

	// LastWrittenBlockHash is the hash of the last block fully written on the output stream.
	// If this is nil, driver will start with the first complete block on the input stream.
	// If not nil, driver will start with the first block after the one with this hash.
	LastWrittenBlockHash *string

	// BlocksCacheSize is the number of messages required to pass
	// until we consider some unused blocks/shards as stale and remove them from memory.
	BlocksCacheSize uint64
}

func (o *Options) WithDefaults() *Options {
	o.StuckTolerance = 100
	return o
}

func (o *Options) Validated() *Options {
	if o.StuckTolerance < 5 {
		panic("StuckTolerance must be at least 5")
	}
	if o.BlocksCacheSize < 10 {
		panic("BlocksCacheSize must be at least 10")
	}
	return o
}
