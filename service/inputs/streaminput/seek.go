package streaminput

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/service/streamseek"
	"github.com/aurora-is-near/stream-most/stream"
)

type seekOptions struct {
	seekBlock             blocks.Block
	seekOnlyGreaterBlocks bool
	seekSeq               uint64

	startSeq uint64
	endSeq   uint64
}

type seekResult struct {
	seq uint64
	err error
}

func (so *seekOptions) seek(ctx context.Context, s *stream.Stream) <-chan seekResult {
	resCh := make(chan seekResult, 1)

	go func() {
		var res seekResult

		if so.seekBlock != nil {
			res.seq, res.err = streamseek.SeekBlock(ctx, s, so.seekBlock, so.startSeq, so.endSeq, so.seekOnlyGreaterBlocks)
		} else {
			res.seq, res.err = streamseek.SeekSeq(ctx, s, so.seekSeq, so.startSeq, so.endSeq)
		}

		resCh <- res
		close(resCh)
	}()

	return resCh
}
