package blocks

type LegacyBlockAnnouncement struct {
	*AbstractBlock
}

func (a LegacyBlockAnnouncement) GetShardMask() []bool {
	return nil
}
