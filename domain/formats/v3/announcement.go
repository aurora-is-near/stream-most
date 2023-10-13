package v3

import (
	nearblock "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
	"github.com/aurora-is-near/stream-most/domain/blocks"
)

type NearBlockAnnouncement struct {
	BlockHeaderView *nearblock.BlockHeaderView

	hash     cachedString
	prevHash cachedString
}

func (a *NearBlockAnnouncement) GetHash() string {
	return a.hash.get(a.BlockHeaderView.Header.H256Hash)
}

func (a *NearBlockAnnouncement) GetPrevHash() string {
	return a.prevHash.get(a.BlockHeaderView.Header.H256PrevHash)
}

func (a *NearBlockAnnouncement) GetHeight() uint64 {
	return a.BlockHeaderView.Header.Height
}

func (a *NearBlockAnnouncement) GetBlockType() blocks.BlockType {
	return blocks.Announcement
}

func (a *NearBlockAnnouncement) GetShardMask() []bool {
	return a.BlockHeaderView.Header.ChunkMask
}

func (a *NearBlockAnnouncement) GetShardID() uint64 {
	return 0
}
