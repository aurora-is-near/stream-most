package messages

import "github.com/aurora-is-near/stream-most/domain/blocks"

type AbstractBlockMessage struct {
	blocks.Block
	NatsMessage
}

func (m *AbstractBlockMessage) GetBlock() blocks.Block {
	return m.Block
}

func (m *AbstractBlockMessage) GetNatsMessage() NatsMessage {
	return m.NatsMessage
}

func (m *AbstractBlockMessage) GetAnnouncement() blocks.BlockAnnouncement {
	return m.Block.(blocks.BlockAnnouncement)
}

func (m *AbstractBlockMessage) GetShard() blocks.BlockShard {
	return m.Block.(blocks.BlockShard)
}

func (m *AbstractBlockMessage) GetType() MessageType {
	if _, ok := m.Block.(blocks.BlockAnnouncement); ok {
		return Announcement
	}
	if _, ok := m.Block.(blocks.BlockShard); ok {
		return Shard
	}
	return Unknown
}
