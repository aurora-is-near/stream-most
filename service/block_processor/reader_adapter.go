package block_processor

import (
	"context"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/stream/adapters"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

func NewProcessorWithReader(ctx context.Context, input <-chan *reader.Output, driver drivers.Driver, parseTolerance uint64) (*Processor, chan error) {
	in, errors := adapters.ReaderOutputToNatsMessages(ctx, input, parseTolerance)
	f := NewProcessor(in, driver)
	return f, errors
}
