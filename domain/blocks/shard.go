package blocks

type BlockShard interface {
	Block
	GetShardID() uint64
}

type AbstractBlockShard struct {
	AbstractBlock
	ShardID uint64
}

func (a *AbstractBlockShard) GetShardID() uint64 {
	return a.ShardID
}
