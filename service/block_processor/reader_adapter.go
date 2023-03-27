package block_processor

import (
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/adapters"
)

func NewProcessorWithReader(input <-chan *stream.ReaderOutput, driver drivers.Driver) *Processor {
	in := adapters.ReaderOutputToNatsMessages(input)
	f := NewProcessor(in, driver)
	return f
}
