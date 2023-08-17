package reader

import (
	"context"
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
)

type fakeProvider[T any] func(
	ctx context.Context, input stream.Interface, opts *Options, decodeFn func(msg messages.NatsMessage) (T, error),
) (IReader[T], error)

var defaultFakeProviders = make(map[string]any)

func UseFake[T any](provider fakeProvider[T]) {
	defaultFakeProviders[fmt.Sprintf("%T", provider)] = provider
}

func createFake[T any](
	ctx context.Context, input stream.Interface, opts *Options, decodeFn func(msg messages.NatsMessage) (T, error),
) (IReader[T], error) {

	var provider fakeProvider[T]
	genericProvider, ok := defaultFakeProviders[fmt.Sprintf("%T", provider)]
	if !ok {
		panic(fmt.Sprintf(
			"No default reader fake provider of type (%T) was selected. Please call fakes.UseDefaultOnes()",
			provider,
		))
	}
	provider = genericProvider.(fakeProvider[T])

	return provider(ctx, input, opts, decodeFn)
}

// var defaultFakeProvider func(
// 	ctx context.Context, input stream.Interface, opts *Options, decodeFn func(msg messages.NatsMessage) (any, error),
// ) (IReader[any], error)

// func UseFake(provider func(
// 	ctx context.Context, input stream.Interface, opts *Options, decodeFn func(msg messages.NatsMessage) (any, error),
// ) (IReader[any], error)) {
// 	defaultFakeProvider = provider
// }

// func createFake(
// 	ctx context.Context, input stream.Interface, opts *Options, decodeFn func(msg messages.NatsMessage) (any, error),
// ) (IReader[any], error) {
// 	if defaultFakeProvider == nil {
// 		panic("No default fake for reader.IReader was selected. Please call fakes.UseDefaultOnes()")
// 	}
// 	reflect.TypeOf()
// 	return defaultFakeProvider(ctx, input, opts, decodeFn)
// }
