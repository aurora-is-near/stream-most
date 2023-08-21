package blocks

type Block interface {
	GetHash() string
	GetPrevHash() string
	GetHeight() uint64

	// v3+
	GetBlockType() BlockType
	GetShardMask() []bool
	GetShardID() uint64
}
