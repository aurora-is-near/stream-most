package outputter

import (
	"errors"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

func isWrongExpectedLastSequence(err error) bool {
	var jsErr jetstream.JetStreamError
	if !errors.As(err, &jsErr) || jsErr.APIError() == nil {
		return false
	}
	errCode := server.ErrorIdentifier(jsErr.APIError().ErrorCode)
	return errCode == server.JSStreamWrongLastSequenceErrF
}
