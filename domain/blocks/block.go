package blocks

import "fmt"

type Block interface {
	GetHash() string
	GetPrevHash() string
	GetHeight() uint64

	// v3+
	GetBlockType() BlockType
	GetShardMask() []bool
	GetShardID() uint64
}

// Mostly needed for tests etc
type AbstractBlock struct {
	Hash      string
	PrevHash  string
	Height    uint64
	BlockType BlockType
	ShardMask []bool
	ShardID   uint64
}

func (b *AbstractBlock) GetHash() string {
	return b.Hash
}

func (b *AbstractBlock) GetPrevHash() string {
	return b.PrevHash
}

func (b *AbstractBlock) GetHeight() uint64 {
	return b.Height
}

func (b *AbstractBlock) GetBlockType() BlockType {
	return b.BlockType
}

func (b *AbstractBlock) GetShardMask() []bool {
	return b.ShardMask
}

func (b *AbstractBlock) GetShardID() uint64 {
	return b.ShardID
}

func (b *AbstractBlock) String() string {
	return fmt.Sprintf("%s:%d.%d", b.BlockType.String(), b.Height, b.ShardID)
}
