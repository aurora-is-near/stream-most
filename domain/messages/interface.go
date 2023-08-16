package messages

import (
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/nats-io/nats.go"
)

type NatsMessage interface {
	GetData() []byte
	GetHeader() nats.Header
	GetSubject() string
	GetSequence() uint64
	GetTimestamp() time.Time
}

type BlockMessage interface {
	blocks.Block
	NatsMessage

	GetBlock() blocks.Block
	GetNatsMessage() NatsMessage

	GetAnnouncement() blocks.BlockAnnouncement
	GetShard() blocks.BlockShard
	GetType() MessageType
}
