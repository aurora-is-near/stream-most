package blocks

type Block interface {
	GetHash() string
	GetPrevHash() string
	GetHeight() uint64
}

type BlockAnnouncement interface {
	Block
	GetShardMask() []bool
}

type BlockShard interface {
	Block
	GetShardID() uint64
}
