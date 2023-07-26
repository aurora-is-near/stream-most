package messages

import "github.com/aurora-is-near/stream-most/domain/blocks"

type TypedMessage struct {
	blocks.Block
}

func (t *TypedMessage) GetBlock() blocks.Block {
	return t.Block
}

func (t *TypedMessage) GetAnnouncement() blocks.BlockAnnouncement {
	return t.Block.(blocks.BlockAnnouncement)
}

func (t *TypedMessage) GetShard() blocks.BlockShard {
	return t.Block.(blocks.BlockShard)
}

func (t *TypedMessage) GetType() MessageType {
	if _, ok := t.Block.(blocks.BlockAnnouncement); ok {
		return Announcement
	}
	if _, ok := t.Block.(blocks.BlockShard); ok {
		return Shard
	}
	return Unknown
}
