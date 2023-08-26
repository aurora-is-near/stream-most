package blockwriter

import "github.com/nats-io/nats.go/jetstream"

var serviceHeaders = map[string]struct{}{
	jetstream.MsgIDHeader:               {},
	jetstream.ExpectedStreamHeader:      {},
	jetstream.ExpectedLastSeqHeader:     {},
	jetstream.ExpectedLastSubjSeqHeader: {},
	jetstream.ExpectedLastMsgIDHeader:   {},
	jetstream.MsgRollup:                 {},
}
