package messages

import (
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/nats-io/nats.go"
)

type AbstractNatsMessage interface {
	GetType() MessageType
	GetSequence() uint64
	GetAnnouncement() *BlockAnnouncement
	GetShard() *BlockShard
	GetMsg() *nats.Msg
	GetMetadata() *nats.MsgMetadata
	GetBlock() *blocks.AbstractBlock
	IsAnnouncement() bool
	IsShard() bool
}

type NatsMessage struct {
	Msg      *nats.Msg
	Metadata *nats.MsgMetadata

	// One of:
	Announcement *BlockAnnouncement
	Shard        *BlockShard
}

// Why not pointer-receiver methods (all of them below)?
func (f NatsMessage) GetSequence() uint64 {
	return f.Metadata.Sequence.Stream
}

func (f NatsMessage) GetType() MessageType {
	if f.Announcement != nil {
		return Announcement
	}
	if f.Shard != nil {
		return Shard
	}
	panic("Invalid message")
}

func (f NatsMessage) IsAnnouncement() bool {
	return f.Announcement != nil
}

func (f NatsMessage) IsShard() bool {
	return f.Shard != nil
}

func (f NatsMessage) GetAnnouncement() *BlockAnnouncement {
	return f.Announcement
}

func (f NatsMessage) GetShard() *BlockShard {
	return f.Shard
}

func (f NatsMessage) GetMsg() *nats.Msg {
	return f.Msg
}

func (f NatsMessage) GetMetadata() *nats.MsgMetadata {
	return f.Metadata
}

func (f NatsMessage) GetBlock() *blocks.AbstractBlock {
	if f.Announcement != nil {
		return f.GetAnnouncement().Block.ToAbstractBlock()
	}
	if f.Shard != nil {
		return f.GetShard().Block.ToAbstractBlock()
	}
	panic("Invalid message")
}
