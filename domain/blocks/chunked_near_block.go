package blocks

type NearBlockChunk struct {
	Hash    string
	ChunkID uint8
}

type ChunkedNearBlock struct {
	Hash     string
	PrevHash string
	Height   uint64
	Sequence uint64

	Chunks []NearBlockChunk
}
