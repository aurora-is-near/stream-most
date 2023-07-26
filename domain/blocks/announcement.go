package blocks

type BlockAnnouncement interface {
	Block
	GetShardMask() []bool
}

type AbstractBlockAnnouncement struct {
	AbstractBlock
	ShardMask []bool
}

func (a *AbstractBlockAnnouncement) GetShardMask() []bool {
	return a.ShardMask
}

type LegacyBlockAnnouncement struct {
	*AbstractBlock
}

func (a LegacyBlockAnnouncement) GetShardMask() []bool {
	return nil
}
