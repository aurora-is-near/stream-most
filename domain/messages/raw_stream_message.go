package messages

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type RawStreamMessage struct {
	RawStreamMsg *jetstream.RawStreamMsg
}

func (m RawStreamMessage) GetData() []byte {
	return m.RawStreamMsg.Data
}

func (m RawStreamMessage) GetHeader() nats.Header {
	return m.RawStreamMsg.Header
}

func (m RawStreamMessage) GetSubject() string {
	return m.RawStreamMsg.Subject
}

func (m RawStreamMessage) GetSequence() uint64 {
	return m.RawStreamMsg.Sequence
}

func (m RawStreamMessage) GetTimestamp() time.Time {
	return m.RawStreamMsg.Time
}
