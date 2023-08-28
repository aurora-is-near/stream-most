package reader

import (
	"runtime"
	"time"
)

type Options struct {
	FilterSubjects    []string
	StartSeq          uint64
	EndSeq            uint64
	StrictStart       bool
	MaxSilence        time.Duration
	MaxDecoders       uint
	OutputBufferSize  uint
	DecodingQueueSize uint
}

func (opts Options) WithDefaults() *Options {
	if len(opts.FilterSubjects) == 1 && opts.FilterSubjects[0] == ">" {
		opts.FilterSubjects = nil
	}
	if opts.StartSeq == 0 {
		opts.StartSeq = 1
	}
	if opts.MaxSilence == 0 {
		opts.MaxSilence = time.Second * 5
	}

	maxprocs := uint(runtime.GOMAXPROCS(0))
	if opts.MaxDecoders == 0 {
		opts.MaxDecoders = maxprocs
	}
	if opts.MaxDecoders > maxprocs {
		opts.MaxDecoders = maxprocs
	}

	if opts.OutputBufferSize == 0 {
		opts.OutputBufferSize = 1024
	}
	if opts.DecodingQueueSize == 0 {
		opts.DecodingQueueSize = 1024
	}

	return &opts
}

func (opts Options) WithStartSeq(startSeq uint64) *Options {
	opts.StartSeq = startSeq
	return &opts
}

func (opts Options) WithEndSeq(endSeq uint64) *Options {
	opts.EndSeq = endSeq
	return &opts
}
