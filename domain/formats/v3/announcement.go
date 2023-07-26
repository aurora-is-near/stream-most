package v3

import (
	nearblock "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
)

type NearBlockAnnouncement struct {
	BlockHeaderView *nearblock.BlockHeaderView
}

func (a NearBlockAnnouncement) GetHash() string {
	return b2s(a.BlockHeaderView.Header.H256Hash)
}

func (a NearBlockAnnouncement) GetPrevHash() string {
	return b2s(a.BlockHeaderView.Header.H256PrevHash)
}

func (a NearBlockAnnouncement) GetHeight() uint64 {
	return a.BlockHeaderView.Header.Height
}

func (a NearBlockAnnouncement) GetShardMask() []bool {
	return a.BlockHeaderView.Header.ChunkMask
}
