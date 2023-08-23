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

func (t *AbstractBlock) GetHash() string {
	return t.Hash
}

func (t *AbstractBlock) GetPrevHash() string {
	return t.PrevHash
}

func (t *AbstractBlock) GetHeight() uint64 {
	return t.Height
}

func (t *AbstractBlock) GetBlockType() BlockType {
	return t.BlockType
}

func (t *AbstractBlock) GetShardMask() []bool {
	return t.ShardMask
}

func (t *AbstractBlock) GetShardID() uint64 {
	return t.ShardID
}

func (t *AbstractBlock) String() string {
	return fmt.Sprintf("%s:%d.%d", t.BlockType.String(), t.Height, t.ShardID)
}
