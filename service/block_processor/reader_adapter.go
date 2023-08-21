package block_processor

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/stream/adapters"
	"github.com/aurora-is-near/stream-most/stream/reader"
)

func NewProcessorWithReader(ctx context.Context, reader reader.IReader[*messages.BlockMessage], driver drivers.Driver, parseTolerance uint) (*Processor, chan error) {
	in, errors := adapters.ReaderToBlockMessage(ctx, reader, parseTolerance)
	f := NewProcessor(in, driver)
	return f, errors
}
