package bridge

import (
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
)

type Config struct {
	MaxReseeks        int
	ReseekDelay       time.Duration
	ReorderBufferSize uint

	InputStartSeq uint64
	InputEndSeq   uint64

	AnchorOutputSeq uint64
	AnchorBlock     blocks.Block
	PreAnchorBlock  blocks.Block
	AnchorInputSeq  uint64

	LastBlock blocks.Block
	EndBlock  blocks.Block

	CorruptedBlocksTolerance int64
	LowBlocksTolerance       int64
	HighBlocksTolerance      int64
	WrongBlocksTolerance     int64
	NoWriteTolerance         int64
}
