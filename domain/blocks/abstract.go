package blocks

type AbstractBlock struct {
	Hash     string
	PrevHash string
	Height   uint64
	Sequence uint64
}

func (b *AbstractBlock) WithSequence(sequence uint64) *AbstractBlock {
	b.Sequence = sequence
	return b
}

// ToAbstractBlock TODO: move to proper file
func (ab *AuroraBlock) ToAbstractBlock() *AbstractBlock {
	return &AbstractBlock{
		Hash:     ab.Hash,
		PrevHash: ab.ParentHash,
		Height:   ab.Height,
	}
}

// ToAbstractBlock TODO: move to proper file
func (ab *NearBlock) ToAbstractBlock() *AbstractBlock {
	return &AbstractBlock{
		Hash:     ab.Hash,
		PrevHash: ab.PrevHash,
		Height:   ab.Height,
	}
}
