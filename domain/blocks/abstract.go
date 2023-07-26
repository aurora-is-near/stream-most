package blocks

/*
	CBOR annotations are needed for parsing of v2 aurora block
*/

type AbstractBlock struct {
	Hash     string `cbor:"hash"`
	PrevHash string `cbor:"parent_hash"`
	Height   uint64 `cbor:"height"`
}
