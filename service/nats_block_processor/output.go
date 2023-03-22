package nats_block_processor

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/nats-io/nats.go"
)

type ProcessorOutput struct {
	Msg      *nats.Msg
	Metadata *nats.MsgMetadata

	// One of:
	Announcement *messages.BlockAnnouncement
	Shard        *messages.BlockShard
}

func (f *ProcessorOutput) IsAnnouncement() bool {
	return f.Announcement != nil
}

func (f *ProcessorOutput) IsShard() bool {
	return f.Shard != nil
}
