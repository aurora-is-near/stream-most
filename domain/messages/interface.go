package messages

import (
	"time"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/nats-io/nats.go"
)

type Message interface {
	blocks.Block

	GetBlock() blocks.Block
	GetAnnouncement() blocks.BlockAnnouncement
	GetShard() blocks.BlockShard
	GetType() MessageType

	GetData() []byte
	GetHeader() nats.Header
	GetSubject() string
	GetSequence() uint64
	GetNumPending() uint64
	GetTimestamp() time.Time
	GetStream() string
	GetConsumer() string
}
