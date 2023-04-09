package observer

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"strings"
)

type WrappedMessage struct {
	Message messages.AbstractNatsMessage
	Wraps   []error
}

func (m *WrappedMessage) String() string {
	builder := strings.Builder{}
	for i, wrap := range m.Wraps {
		builder.WriteString(wrap.Error())
		if i != 0 {
			builder.WriteString(", ")
		}
	}

	return builder.String()
}

func (m *WrappedMessage) Wrap(wrap error) *WrappedMessage {
	return &WrappedMessage{Wraps: append(m.Wraps, wrap), Message: m.Message}
}

func WrapMessage(message messages.AbstractNatsMessage, err error) *WrappedMessage {
	return &WrappedMessage{Wraps: []error{err}, Message: message}
}
