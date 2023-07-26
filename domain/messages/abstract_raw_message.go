package messages

import (
	"time"

	"github.com/nats-io/nats.go"
)

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
