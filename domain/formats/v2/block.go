package v2

import "github.com/aurora-is-near/stream-most/domain/blocks"

type Block struct {
	// CBOR annotations are needed for parsing of v2 aurora block
	Hash     string `cbor:"hash"`
	PrevHash string `cbor:"parent_hash"`
	Height   uint64 `cbor:"height"`
}

func (b *Block) GetHash() string {
	return b.Hash
}

func (b *Block) GetPrevHash() string {
	return b.PrevHash
}

func (b *Block) GetHeight() uint64 {
	return b.Height
}

func (b *Block) GetBlockType() blocks.BlockType {
	return blocks.Announcement
}

func (b *Block) GetShardMask() []bool {
	return nil
}

func (b *Block) GetShardID() uint64 {
	return 0
}
