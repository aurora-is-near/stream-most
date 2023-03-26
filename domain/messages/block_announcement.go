package messages

import (
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/blocks"
)

// BlockAnnouncement is a message that contains announcement of a new near block,
// contains block header and info about amount of shards participating.
type BlockAnnouncement struct {
	Parent *borealisproto.Message_NearBlockHeader
	Block  blocks.NearBlock

	// If ParticipatingShardsMap[i-1] is true, then shard with id i is participating in this block.
	ParticipatingShardsMap []bool
}

func NewBlockAnnouncement(parent *borealisproto.Message_NearBlockHeader) *BlockAnnouncement {
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
