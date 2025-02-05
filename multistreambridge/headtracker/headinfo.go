package headtracker

import "github.com/aurora-is-near/stream-most/domain/blocks"

type HeadInfo struct {
	sequence    uint64
	blockOrNone blocks.Block
}

func (h *HeadInfo) Sequence() uint64 {
	return h.sequence
}

func (h *HeadInfo) HasBlock() bool {
	return h.blockOrNone != nil
}

func (h *HeadInfo) BlockOrNone() blocks.Block {
	return h.blockOrNone
}
