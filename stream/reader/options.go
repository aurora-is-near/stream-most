package reader

type Options struct {
	BufferSize   uint
	StrictStart  bool
	MaxSilenceMs uint
}

func (opts Options) WithDefaults() *Options {
	if opts.MaxSilenceMs == 0 {
		opts.MaxSilenceMs = 5000
	}
	return &opts
}
