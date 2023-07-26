package v3

import (
	nearblock "github.com/aurora-is-near/borealis-prototypes/go/payloads/near_block"
)

type NearBlockShard struct {
	BlockShard *nearblock.BlockShard
}

func (s NearBlockShard) GetHash() string {
	return b2s(s.BlockShard.Header.Header.H256Hash)
}

func (s NearBlockShard) GetPrevHash() string {
	return b2s(s.BlockShard.Header.Header.H256PrevHash)
}

func (s NearBlockShard) GetHeight() uint64 {
	return s.BlockShard.Header.Header.Height
}

func (s NearBlockShard) GetShardID() uint64 {
	return s.BlockShard.ShardId
}
