package block_guard

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"sync"
)

// TODO: finish

// BlockGuard is a service that accumulates messages and sends them out in a proper order
type BlockGuard struct {
	// TODO: change mutex to proper maps
	*sync.Mutex

	// Map from block hash to map of chunk's shard id to chunk message.
	accumulatedShardChunks map[string]map[uint8]messages.BlockShard

	knownBlockAnnouncements map[string]messages.BlockAnnouncement

	// If we received a shard block chunk without a prior header, we store its block hash here.
	// When header will come in, we'll pick up all the chunks from accumulatedShardChunks belonging to it.
	headerlessBlockChunks map[string]struct{}

	output chan interface{}
}

// Start starts the service and returns a channel that will receive all the processed messages.
// Only messages.BlockAnnouncement and messages.BlockShard can be in the output stream
func (m *BlockGuard) Start() <-chan interface{} {
	m.output = make(chan interface{})
	return m.output
}

func (m *BlockGuard) Stop() {
	close(m.output)
}

func (m *BlockGuard) ProcessBlockAnnouncement(message messages.BlockAnnouncement) error {
	m.Lock()
	defer m.Unlock()

	if m.isBlockKnown(message.Block.Hash) {
		return nil
	}

	m.acquaintBlock(message)
	m.output <- message

	return nil
}

func (m *BlockGuard) ProcessBlockShard(message messages.BlockShard) error {
	m.Lock()
	defer m.Unlock()

	m.storeBlockShardChunk(message)

	if m.isBlockComplete(message.Block.Hash) {
		m.sendBlockToOutput(message.Block.Hash)
	}

	return nil // TODO: let client code know about our fails
}

func (m *BlockGuard) isBlockKnown(hash string) bool {
	_, ok := m.knownBlockAnnouncements[hash]
	return ok
}

func (m *BlockGuard) acquaintBlock(message messages.BlockAnnouncement) {
	blockHash := message.Block.Hash
	if m.isBlockKnown(blockHash) {
		return
	}

	m.knownBlockAnnouncements[blockHash] = message
}

func (m *BlockGuard) storeBlockShardChunk(message messages.BlockShard) {
	blockHash := message.Block.Hash

	if m.accumulatedShardChunks[blockHash] == nil {
		m.accumulatedShardChunks[blockHash] = make(map[uint8]messages.BlockShard)
	}
	m.accumulatedShardChunks[blockHash][message.ShardID] = message
}

func (m *BlockGuard) isBlockComplete(hash string) bool {
	if _, known := m.knownBlockAnnouncements[hash]; !known {
		return false
	}

	for k := range m.knownBlockAnnouncements[hash].ParticipatingShardsMap {
		if _, exists := m.accumulatedShardChunks[hash][uint8(k)+1]; !exists {
			return false
		}
	}

	return true
}

func (m *BlockGuard) sendBlockToOutput(hash string) {
	announce := m.knownBlockAnnouncements[hash]
	block := blocks.ChunkedNearBlock{
		Hash:     announce.Block.Hash,
		PrevHash: announce.Block.PrevHash,
		Height:   announce.Block.Height,
		Sequence: 0,
		Chunks:   []blocks.NearBlockChunk{},
	}

	msgs := m.accumulatedShardChunks[hash]
	for _, message := range msgs {
		block.Chunks = append(block.Chunks, blocks.NearBlockChunk{
			Hash:    message.Block.Hash,
			ChunkID: message.ShardID,
		})
	}

	m.output <- block
}

func NewBlockGuard() *BlockGuard {
	return &BlockGuard{
		Mutex:                   &sync.Mutex{},
		accumulatedShardChunks:  make(map[string]map[uint8]messages.BlockShard),
		knownBlockAnnouncements: make(map[string]messages.BlockAnnouncement),
		headerlessBlockChunks:   make(map[string]struct{}),
	}
}
