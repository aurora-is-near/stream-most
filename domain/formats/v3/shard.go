package v3

import (
	nearblock "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
	"github.com/aurora-is-near/stream-most/domain/blocks"
)

type NearBlockShard struct {
	BlockShard *nearblock.BlockShard

	hash     cachedString
	prevHash cachedString
}

func (s *NearBlockShard) GetHash() string {
	return s.hash.get(s.BlockShard.Header.Header.H256Hash)
}

func (s *NearBlockShard) GetPrevHash() string {
	return s.prevHash.get(s.BlockShard.Header.Header.H256PrevHash)
}

func (s *NearBlockShard) GetHeight() uint64 {
	return s.BlockShard.Header.Header.Height
}

func (a *NearBlockShard) GetBlockType() blocks.BlockType {
	return blocks.Shard
}

func (a *NearBlockShard) GetShardMask() []bool {
	return a.BlockShard.Header.Header.ChunkMask
}

func (s *NearBlockShard) GetShardID() uint64 {
	return s.BlockShard.ShardId
}
