package reader

type Options struct {
	MaxRps                       float64
	BufferSize                   uint
	MaxRequestBatchSize          uint
	SubscribeAckWaitMs           uint
	InactiveThresholdSeconds     uint
	FetchTimeoutMs               uint
	SortBatch                    bool
	LastSeqUpdateIntervalSeconds uint
	Durable                      string
	StrictStart                  bool
	WrongSeqToleranceWindow      uint
}

func (opts Options) WithDefaults() *Options {
	if opts.MaxRps < 0.001 {
		opts.MaxRps = 0.001
	}
	if opts.MaxRequestBatchSize == 0 {
		opts.MaxRequestBatchSize = 100
	}
	if opts.SubscribeAckWaitMs == 0 {
		opts.SubscribeAckWaitMs = 5000
	}
	if opts.InactiveThresholdSeconds == 0 {
		opts.InactiveThresholdSeconds = 300
	}
	if opts.FetchTimeoutMs == 0 {
		opts.FetchTimeoutMs = 10000
	}
	if opts.LastSeqUpdateIntervalSeconds == 0 {
		opts.LastSeqUpdateIntervalSeconds = 5
	}
	return &opts
}
