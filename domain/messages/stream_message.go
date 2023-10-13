package messages

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type StreamMessage struct {
	Msg  jetstream.Msg
	Meta *jetstream.MsgMetadata
}

func (m *StreamMessage) GetData() []byte {
	return m.Msg.Data()
}

func (m *StreamMessage) GetHeader() nats.Header {
	return m.Msg.Headers()
}

func (m *StreamMessage) GetSubject() string {
	return m.Msg.Subject()
}

func (m *StreamMessage) GetSequence() uint64 {
	return m.Meta.Sequence.Stream
}

func (m *StreamMessage) GetTimestamp() time.Time {
	return m.Meta.Timestamp
}
