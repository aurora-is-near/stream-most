package storage

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Interface interface {
	// Write should be sequentially called on sequential messages,
	Write(message messages.AbstractNatsMessage) error
}
