package streamoutput

import (
	"errors"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

var serviceHeaders = map[string]struct{}{
	jetstream.MsgIDHeader:               {},
	jetstream.ExpectedStreamHeader:      {},
	jetstream.ExpectedLastSeqHeader:     {},
	jetstream.ExpectedLastSubjSeqHeader: {},
	jetstream.ExpectedLastMsgIDHeader:   {},
	jetstream.MsgRollup:                 {},
}

func isFailedExpectErr(err error) bool {
	var jsErr jetstream.JetStreamError
	if !errors.As(err, &jsErr) || jsErr.APIError() == nil {
		return false
	}
	errCode := server.ErrorIdentifier(jsErr.APIError().ErrorCode)
	return errCode == server.JSStreamWrongLastMsgIDErrF || errCode == server.JSStreamWrongLastSequenceErrF
}
