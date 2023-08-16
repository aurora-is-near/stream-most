package reader

import (
	"context"

	"github.com/aurora-is-near/stream-most/stream"
)

var defaultFakeProvider func(
	ctx context.Context, opts *Options, input stream.Interface, subjects []string, startSeq uint64, endSeq uint64,
) (IReader, error)

func UseFake(provider func(
	ctx context.Context, opts *Options, input stream.Interface, subjects []string, startSeq uint64, endSeq uint64,
) (IReader, error)) {
	defaultFakeProvider = provider
}

func createFake(
	ctx context.Context, opts *Options, input stream.Interface, subjects []string, startSeq uint64, endSeq uint64,
) (IReader, error) {
	if defaultFakeProvider == nil {
		panic("No default fake for reader.IReader was selected. Please call fakes.UseDefaultOnes()")
	}
	return defaultFakeProvider(ctx, opts, input, subjects, startSeq, endSeq)
}
