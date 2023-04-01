package fake

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

// Reader is a simple fake for reader.IReader, that only works in pair with fake.Stream
type Reader struct {
	opts     *reader.Options
	input    stream.Interface
	startSeq uint64
	endSeq   uint64
	output   chan *reader.Output
	closed   bool
}

func StartReader(opts *reader.Options, input stream.Interface, startSeq uint64, endSeq uint64) (reader.IReader, error) {
	return &Reader{
		opts:     opts,
		input:    input,
		startSeq: startSeq,
		endSeq:   endSeq,
	}, nil
}

func (r *Reader) IsFake() bool {
	return true
}

func (r *Reader) Output() <-chan *reader.Output {
	output := make(chan *reader.Output, len(r.input.(*Stream).GetArray()))
	r.output = output
	r.run()
	return output
}

func (r *Reader) Stop() {
	if !r.closed {
		r.closed = true
	}
}

func (r *Reader) run() {
	from := r.startSeq
	to := r.endSeq

	array := r.input.(*Stream).GetArray()
	if to == 0 && len(array) > 0 {
		to = array[len(array)-1].GetSequence()
	}

	for _, item := range array {
		if item.GetSequence() >= from && item.GetSequence() <= to {
			r.output <- &reader.Output{
				Msg:      item.GetMsg(),
				Metadata: item.GetMetadata(),
				Error:    nil,
			}
		}
	}
	close(r.output)
}
