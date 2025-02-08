package blockreader

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream"
)

type Options struct {
	Stream              *stream.Stream
	StartSeq            uint64
	StrictStart         bool
	LogTag              string
	HandleNewKnownSeqCb func(ctx context.Context, seq uint64) (err error)
	FilterCb            func(ctx context.Context, msg messages.NatsMessage) (skip bool, err error)
	BlockCb             func(ctx context.Context, blockMsg *messages.BlockMessage) (err error)
	CorruptedBlockCb    func(ctx context.Context, msg messages.NatsMessage, decodingError error) (err error)
	FinishCb            func(err error, isInterrupted bool)
}
