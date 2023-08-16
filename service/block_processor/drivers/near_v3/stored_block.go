package near_v3

import (
	"sort"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/sirupsen/logrus"
)

type storedBlock struct {
	expiresAt uint64

	announcement messages.BlockMessage
	shards       []messages.BlockMessage
	shardsStash  []messages.BlockMessage

	shardsRequired []bool
}

func (b *storedBlock) missingAnnouncement() bool {
	return b.announcement == nil
}

func (b *storedBlock) addAnnouncement(announcement messages.BlockMessage) {
	b.announcement = announcement
	b.shardsRequired = announcement.GetAnnouncement().GetShardMask()

	for i := range b.shardsStash {
		b.addShard(b.shardsStash[i])
	}
}

func (b *storedBlock) addShard(shard messages.BlockMessage) (isNew bool) {
	if b.missingAnnouncement() {
		b.stashShard(shard)
		return true
	}

	shardId := shard.GetShard().GetShardID()

	if len(b.shardsRequired) <= int(shardId) {
		logrus.Error("Shard ID is out of range for the given block")
		return false
	}

	if b.shardsRequired[shard.GetShard().GetShardID()] {
		b.shards = append(b.shards, shard)
		b.shardsRequired[shard.GetShard().GetShardID()] = false
		return true
	}

	logrus.Warn("Undesired shard received for the given block")
	return false
}

func (b *storedBlock) stashShard(shard messages.BlockMessage) {
	b.shardsStash = append(b.shardsStash, shard)
}

func (b *storedBlock) shardsSorted() []messages.BlockMessage {
	sort.Slice(b.shards, func(i, j int) bool {
		return b.shards[i].GetShard().GetShardID() < b.shards[j].GetShard().GetShardID()
	})

	return b.shards
}

func (b *storedBlock) isComplete() bool {
	shardsCompleted := true
	for _, v := range b.shardsRequired {
		if v {
			shardsCompleted = false
		}
	}
	return b.announcement != nil && shardsCompleted
}

func (b *storedBlock) writeTo(output chan messages.BlockMessage) {
	output <- b.announcement
	for _, shard := range b.shardsSorted() {
		output <- shard
	}
}

func (b *storedBlock) getAbstractBlock() blocks.Block {
	if !b.missingAnnouncement() {
		return b.announcement.GetBlock()
	} else if len(b.shardsStash) != 0 {
		return b.shardsStash[0].GetBlock()
	}

	return nil
}

func newStoredBlock(expiresAt uint64) *storedBlock {
	return &storedBlock{
		expiresAt: expiresAt,
		shards:    make([]messages.BlockMessage, 0),
	}
}
