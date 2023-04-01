package reader

import "github.com/aurora-is-near/stream-most/stream"

var defaultFakeProvider func(
	opts *Options,
	input stream.Interface,
	startSeq uint64,
	endSeq uint64,
) (IReader, error)

func UseFake(provider func(
	opts *Options,
	input stream.Interface,
	startSeq uint64,
	endSeq uint64,
) (IReader, error)) {
	defaultFakeProvider = provider
}

func createFake(
	opts *Options,
	input stream.Interface,
	startSeq uint64,
	endSeq uint64,
) (IReader, error) {
	if defaultFakeProvider == nil {
		panic("No default fake for reader.IReader was selected. Please call fakes.UseDefaultOnes()")
	}
	return defaultFakeProvider(opts, input, startSeq, endSeq)
}
