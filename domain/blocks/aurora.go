package blocks

// AuroraBlock is a default block from Aurora without content
type AuroraBlock struct {
	Hash       string `cbor:"hash" json:"hash"`
	ParentHash string `cbor:"parent_hash" json:"parent_hash"`
	Height     uint64 `cbor:"height" json:"height"`
}

func DecodeAuroraBlock(data []byte) (*AuroraBlock, error) {
	return DecodeBorealisPayload[AuroraBlock](data)
}

func (ab *AuroraBlock) ToAbstractBlock() *AbstractBlock {
	return &AbstractBlock{
		Hash:     ab.Hash,
		PrevHash: ab.ParentHash,
		Height:   ab.Height,
	}
}
