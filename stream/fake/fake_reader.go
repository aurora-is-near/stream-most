package fake

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

// Reader is a simple fake for reader.IReader, that only works in pair with fake.Stream
type Reader struct {
	opts     *reader.Options
	input    stream.Interface
	startSeq uint64
	endSeq   uint64
	output   chan messages.NatsMessage
	closed   bool
}

func StartReader(ctx context.Context, opts *reader.Options, input stream.Interface, subjects []string, startSeq uint64, endSeq uint64) (reader.IReader, error) {
	r := &Reader{
		opts:     opts,
		input:    input,
		startSeq: startSeq,
		endSeq:   endSeq,
	}
	r.fill()
	return r, nil
}

func (r *Reader) IsFake() bool {
	return true
}

func (r *Reader) Error() error {
	return nil
}

func (r *Reader) Output() <-chan messages.NatsMessage {
	return r.output
}

func (r *Reader) Stop() {
	if !r.closed {
		r.closed = true
	}
}

func (r *Reader) fill() {
	r.output = make(chan messages.NatsMessage, len(r.input.(*Stream).GetArray()))
	for _, item := range r.input.(*Stream).GetArray() {
		if item.GetSequence() < r.startSeq {
			continue
		}
		if r.endSeq > 0 && item.GetSequence() >= r.endSeq {
			break
		}
		r.output <- item.GetNatsMessage()
	}
	close(r.output)
}
