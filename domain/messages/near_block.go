package messages

import (
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/blocks"
)

// Hm looks like this struct is never used
type NearBlock struct {
	Parent *borealisproto.Message_NearBlockHeader
	Block  blocks.NearBlock
}

// And this is just the same as NewBlockAnnouncementV3?
func NewNearBlock(parent *borealisproto.Message_NearBlockHeader) *BlockAnnouncement {
	header := parent.NearBlockHeader.GetHeader()
	return &BlockAnnouncement{
		Parent: parent,
		Block: blocks.NearBlock{
			Hash:     string(header.H256Hash),
			PrevHash: string(header.H256PrevHash),
			Height:   header.Height,
		},
		ParticipatingShardsMap: parent.NearBlockHeader.Header.ChunkMask,
	}
}
