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
