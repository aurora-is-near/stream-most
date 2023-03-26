package near_v3

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Seeker interface {
	// SeekShards must return all shards between sequences from and to in the input stream
	// If forBlock is not nil, it must only return shards found for the given block
	SeekShards(from, to uint64, forBlock *string) ([]messages.AbstractNatsMessage, error)
}

// NearV3NoSorting this driver is the same as NearV3, but it does not sort the shards.
type NearV3NoSorting struct {
	seeker         Seeker
	seekLeftParam  uint64
	seekRightParam uint64

	input  chan messages.AbstractNatsMessage
	output chan messages.AbstractNatsMessage

	// Map from block hash to accumulated nats messages in case shard arrives before header
	shardsStash map[string][]messages.AbstractNatsMessage

	currentAnnouncement           *messages.BlockAnnouncement
	currentAnnouncementSequence   uint64
	shardsCompleteForCurrentBlock map[uint8]bool
}

func (n *NearV3NoSorting) Run() {
	for msg := range n.input {
		if msg.IsAnnouncement() {
			if !n.isPreviousBlockComplete() {
				// If we receive new announcement before previous block is complete,
				// we need to find its shards somewhere
				n.rescueBlock()
			}

			n.processAnnouncement(msg.GetAnnouncement())
			n.output <- msg
		}

		if msg.IsShard() {
			if n.isPreviousBlockComplete() {
				n.stashShard(msg)
			} else {
				n.processShard(msg)
				n.output <- msg
			}
		}
	}
}

func (n *NearV3NoSorting) Bind(input chan messages.AbstractNatsMessage, output chan messages.AbstractNatsMessage) {
	n.input = input
	n.output = output
}

func (n *NearV3NoSorting) rescueBlock() {
	shards, err := n.seeker.SeekShards(
		n.currentAnnouncementSequence-n.seekLeftParam,
		n.currentAnnouncementSequence+n.seekRightParam,
		&n.currentAnnouncement.Block.Hash,
	)
	if err != nil {
		panic(err)
	}

	for _, shard := range shards {
		if !n.shardsCompleteForCurrentBlock[shard.GetShard().ShardID] {
			n.processShard(shard)
			n.output <- shard
		}
	}
}

func (n *NearV3NoSorting) isPreviousBlockComplete() bool {
	if n.currentAnnouncement == nil {
		return true
	}

	return len(n.shardsCompleteForCurrentBlock) == len(n.currentAnnouncement.ParticipatingShardsMap)
}

func (n *NearV3NoSorting) processAnnouncement(announcement *messages.BlockAnnouncement) {
	if n.currentAnnouncement.Block.Height > announcement.Block.Height {
		panic("We have already processed a block with a higher height!")
	}
	if n.currentAnnouncement.Block.Hash != announcement.Block.PrevHash {
		panic("PrevHash of the new announcement's block doesn't match current block's hash!")
	}

	n.currentAnnouncement = announcement
	n.shardsCompleteForCurrentBlock = map[uint8]bool{}

	for _, shard := range n.shardsStash[announcement.Block.Hash] {
		n.processShard(shard)
		n.output <- shard
	}
}

func (n *NearV3NoSorting) stashShard(msg messages.AbstractNatsMessage) {
	n.shardsStash[msg.GetShard().Block.Hash] = append(n.shardsStash[msg.GetShard().Block.Hash], msg)
}

func (n *NearV3NoSorting) processShard(msg messages.AbstractNatsMessage) {
	n.shardsCompleteForCurrentBlock[msg.GetShard().ShardID] = true
}

func NewNearV3NoSorting(seeker Seeker) *NearV3NoSorting {
	return &NearV3NoSorting{
		seeker:         seeker,
		seekLeftParam:  10,
		seekRightParam: 10,
	}
}
