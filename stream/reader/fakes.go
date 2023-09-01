package reader

import (
	"github.com/aurora-is-near/stream-most/stream"
)

type fakeProvider func(input stream.Interface, opts *Options, receiver Receiver) (IReader, error)

var defaultFakeProvider fakeProvider

func UseFake(provider fakeProvider) {
	defaultFakeProvider = provider
}

func createFake(input stream.Interface, opts *Options, receiver Receiver) (IReader, error) {
	if defaultFakeProvider == nil {
		panic("No default reader fake provider of type was selected. Please call fakes.UseDefaultOnes()")
	}
	return defaultFakeProvider(input, opts, receiver)
}
