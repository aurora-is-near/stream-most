package drivers

import (
	"errors"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

var (
	ErrNoNext            = errors.New("no next message")
	ErrToleranceExceeded = errors.New("tolerance exceeded")
)

type Driver interface {
	Next(tip, msg *messages.BlockMessage, decodingError error) (*messages.BlockMessage, error)
}
