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

type AbstractMessage struct {
	TypedMessage
	Msg  *nats.Msg
	Meta *nats.MsgMetadata
}

func (m *AbstractMessage) GetData() []byte {
	return m.Msg.Data
}

func (m *AbstractMessage) GetHeader() nats.Header {
	return m.Msg.Header
}

func (m *AbstractMessage) GetSubject() string {
	return m.Msg.Subject
}

func (m *AbstractMessage) GetSequence() uint64 {
	return m.Meta.Sequence.Stream
}

func (m *AbstractMessage) GetNumPending() uint64 {
	return m.Meta.NumPending
}

func (m *AbstractMessage) GetTimestamp() time.Time {
	return m.Meta.Timestamp
}

func (m *AbstractMessage) GetStream() string {
	return m.Meta.Stream
}

func (m *AbstractMessage) GetConsumer() string {
	return m.Meta.Consumer
}

type AbstractRawMessage struct {
	TypedMessage
	RawMsg *nats.RawStreamMsg
}

func (m *AbstractRawMessage) GetData() []byte {
	return m.RawMsg.Data
}

func (m *AbstractRawMessage) GetHeader() nats.Header {
	return m.RawMsg.Header
}

func (m *AbstractRawMessage) GetSubject() string {
	return m.RawMsg.Subject
}

func (m *AbstractRawMessage) GetSequence() uint64 {
	return m.RawMsg.Sequence
}

func (m *AbstractRawMessage) GetNumPending() uint64 {
	return 0
}

func (m *AbstractRawMessage) GetTimestamp() time.Time {
	return m.RawMsg.Time
}

func (m *AbstractRawMessage) GetStream() string {
	return ""
}

func (m *AbstractRawMessage) GetConsumer() string {
	return ""
}
