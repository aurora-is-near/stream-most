package messages

import (
	"time"

	"github.com/nats-io/nats.go"
)

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
