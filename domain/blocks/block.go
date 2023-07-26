package blocks

type Block interface {
	GetHash() string
	GetPrevHash() string
	GetHeight() uint64
}

type AbstractBlock struct {
	// CBOR annotations are needed for parsing of v2 aurora block
	Hash     string `cbor:"hash"`
	PrevHash string `cbor:"parent_hash"`
	Height   uint64 `cbor:"height"`
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
