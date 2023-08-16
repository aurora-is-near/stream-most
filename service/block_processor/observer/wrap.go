package observer

import (
	"strings"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type WrappedMessage struct {
	Message messages.BlockMessage
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
	// TODO: check if slice is reused safely
	return &WrappedMessage{Wraps: append(m.Wraps, wrap), Message: m.Message}
}

func WrapMessage(message messages.BlockMessage, err error) *WrappedMessage {
	return &WrappedMessage{Wraps: []error{err}, Message: message}
}
