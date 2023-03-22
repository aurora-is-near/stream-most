package nats_block_processor

import "github.com/aurora-is-near/stream-most/stream"

func NewProcessorWithReader(input <-chan *stream.ReaderOutput) *Processor {
	in := make(chan ProcessorInput, 1024)

	f := NewProcessor(in)
	go func() {
		for k := range input {
			in <- ProcessorInput{
				Msg:      k.Msg,
				Metadata: k.Metadata,
			}
		}
		close(in)
	}()

	return f
}
