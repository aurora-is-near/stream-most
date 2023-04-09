package near_v3

import (
	"errors"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/sirupsen/logrus"
)

// NearV3 TODO: remove old blocks from memory
type NearV3 struct {
	opts *Options

	blocks               map[string]*storedBlock
	blocksByPreviousHash map[string]*storedBlock

	blocksExpirationQueue []*storedBlock

	lastWrittenBlockHash *string

	input  chan messages.AbstractNatsMessage
	output chan messages.AbstractNatsMessage

	observer *observer.Observer

	messagesSinceLastWrite uint64
	clock                  uint64

	err error
}

func (n *NearV3) Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage) {
	n.input = input
	n.output = output
}

func (n *NearV3) BindObserver(observer *observer.Observer) {
	n.observer = observer
}

func (n *NearV3) Run() {
	defer close(n.output)
	for msg := range n.input {
		n.clock += 1

		if msg.IsAnnouncement() {
			n.processAnnouncement(msg)
		}
		if msg.IsShard() {
			n.processShard(msg)
		}

		err := n.popReadyBlocks()
		if err != nil {
			logrus.Error(err)
		}

		n.clearCache()

		n.messagesSinceLastWrite += 1
		if n.messagesSinceLastWrite > n.opts.StuckTolerance {
			leave := n.stuck()
			if leave {
				break
			}
		}
	}
}

func (n *NearV3) FinishError() error {
	return n.err
}

func (n *NearV3) pop(block *storedBlock) {
	n.lastWrittenBlockHash = &block.announcement.GetBlock().Hash
	delete(n.blocks, block.announcement.GetBlock().Hash)
	delete(n.blocksByPreviousHash, block.announcement.GetBlock().PrevHash)
	block.writeTo(n.output)

	n.messagesSinceLastWrite = 0
}

func (n *NearV3) popReadyBlocks() error {
	if n.lastWrittenBlockHash == nil {
		// We haven't returned any blocks yet,
		// so we need to choose the one with the lowest height
		var minBlock *storedBlock
		for _, block := range n.blocks {
			if !block.isComplete() {
				continue
			}
			if minBlock == nil {
				minBlock = block
			}
			if block.getAbstractBlock().Height < minBlock.getAbstractBlock().Height {
				minBlock = block
			}
		}

		if minBlock == nil {
			// No blocks to return :(
			return nil
		}

		n.pop(minBlock)
	}

	for block := n.blocksByPreviousHash[*n.lastWrittenBlockHash]; block != nil; block = n.blocksByPreviousHash[*n.lastWrittenBlockHash] {
		if !block.isComplete() {
			return nil
		}
		n.pop(block)
	}

	return nil
}

func (n *NearV3) newBlockFrom(message messages.AbstractNatsMessage) *storedBlock {
	hash := message.GetBlock().Hash

	block := newStoredBlock(n.clock + n.opts.BlocksCacheSize)
	if message.IsAnnouncement() {
		block.addAnnouncement(message)
	} else if message.IsShard() {
		block.stashShard(message)
	}

	n.blocks[hash] = block
	n.blocksByPreviousHash[block.getAbstractBlock().PrevHash] = block
	n.blocksExpirationQueue = append(n.blocksExpirationQueue, block)
	n.prolongCache(block)

	return block
}

func (n *NearV3) prolongCache(block *storedBlock) {
	block.expiresAt = n.clock + n.opts.BlocksCacheSize
}

func (n *NearV3) processAnnouncement(message messages.AbstractNatsMessage) {
	hash := message.GetAnnouncement().Block.Hash

	if block, exists := n.blocks[hash]; !exists {
		n.newBlockFrom(message)
	} else if block.missingAnnouncement() {
		block.addAnnouncement(message)
		n.prolongCache(block)
	} else {
		n.observer.Emit(observer.BlockAnnouncementDuplicate, message)
	}
}

func (n *NearV3) processShard(shard messages.AbstractNatsMessage) {
	if block, exists := n.blocks[shard.GetBlock().Hash]; !exists {
		n.newBlockFrom(shard)
	} else if block.missingAnnouncement() {
		block.stashShard(shard)
		n.prolongCache(block)
	} else {
		block.addShard(shard)
		n.prolongCache(block)
	}
}

func (n *NearV3) stuck() (leave bool) {
	logrus.Error("Stuck!")

	if !n.opts.StuckRecovery {
		n.err = errors.New("stuck")
		return true
	}
	recovered := n.recovery()
	if !recovered {
		n.err = errors.New("stuck")
		return true
	}

	return true
}

func (n *NearV3) recovery() bool {
	logrus.Error("Attempting to recover from stuck")
	return false
}

func (n *NearV3) clearCache() {
	for len(n.blocksExpirationQueue) > 0 {
		block := n.blocksExpirationQueue[0]

		if n.clock > block.expiresAt {
			delete(n.blocks, block.getAbstractBlock().Hash)
			delete(n.blocksByPreviousHash, block.getAbstractBlock().PrevHash)
			n.blocksExpirationQueue = n.blocksExpirationQueue[1:]
		} else {
			break
		}
	}
}

func NewNearV3(opts *Options) *NearV3 {
	return &NearV3{
		opts:                 opts,
		lastWrittenBlockHash: opts.LastWrittenBlockHash,
		blocks:               map[string]*storedBlock{},
		blocksByPreviousHash: map[string]*storedBlock{},
	}
}
