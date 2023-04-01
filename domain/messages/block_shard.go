package messages

import (
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/blocks"
)

// BlockShard is a message that contains part of a near block that was previously announced.
// Structurally it's just a wrapper around borealisproto.Message_NearBlockShard
type BlockShard struct {
	Parent  *borealisproto.Message_NearBlockShard
	ShardID uint8
	Block   blocks.NearBlock
}

func NewBlockShard(parent *borealisproto.Message_NearBlockShard) *BlockShard {
	header := parent.NearBlockShard.GetHeader()
	return &BlockShard{
		Parent:  parent,
		ShardID: uint8(parent.NearBlockShard.ShardId),
		Block: blocks.NearBlock{
			Hash:     string(header.Header.H256Hash),
			PrevHash: string(header.Header.H256PrevHash),
			Height:   header.Header.Height,
		},
	}
}
