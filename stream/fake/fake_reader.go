package fake

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

// Reader is a simple fake for reader.IReader, that only works in pair with fake.Stream
type Reader[T any] struct {
	opts     *reader.Options
	input    stream.Interface
	decodeFn func(msg messages.NatsMessage) (T, error)
	output   chan *reader.DecodedMsg[T]
}

func StartReader[T any](ctx context.Context, input stream.Interface, opts *reader.Options, decodeFn func(msg messages.NatsMessage) (T, error)) (reader.IReader[T], error) {
	opts = opts.WithDefaults()
	r := &Reader[T]{
		opts:     opts,
		input:    input,
		decodeFn: decodeFn,
	}
	r.fill()
	return r, nil
}

func (r *Reader[T]) IsFake() bool {
	return true
}

func (r *Reader[T]) Error() error {
	return nil
}

func (r *Reader[T]) Output() <-chan *reader.DecodedMsg[T] {
	return r.output
}

func (r *Reader[T]) Stop() {}

func (r *Reader[T]) fill() {
	r.output = make(chan *reader.DecodedMsg[T], len(r.input.(*Stream).GetArray()))
	for _, item := range r.input.(*Stream).GetArray() {
		if item.Msg.GetSequence() < r.opts.StartSeq {
			continue
		}
		if r.opts.EndSeq > 0 && item.Msg.GetSequence() >= r.opts.EndSeq {
			break
		}
		value, decErr := r.decodeFn(item.Msg)
		r.output <- reader.NewPredecodedMsg[T](item.Msg, value, decErr)
	}
	close(r.output)
}
