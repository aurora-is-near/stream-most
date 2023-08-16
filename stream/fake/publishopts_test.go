package fake

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestApplyPublishOpts(t *testing.T) {
	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.MsgIDHeader: {"rgerfge"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithMsgID("rgerfge"),
		),
	)

	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.ExpectedLastMsgIDHeader: {"bnasdasd"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithExpectLastMsgID("bnasdasd"),
		),
	)

	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.ExpectedStreamHeader: {"34r23r"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithExpectStream("34r23r"),
		),
	)

	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.ExpectedLastSeqHeader: {"228"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithExpectLastSequence(228),
		),
	)

	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.ExpectedLastSubjSeqHeader: {"5005"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithExpectLastSequencePerSubject(5005),
		),
	)

	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.MsgIDHeader:               {"rgerfge"},
			jetstream.ExpectedLastMsgIDHeader:   {"bnasdasd"},
			jetstream.ExpectedStreamHeader:      {"34r23r"},
			jetstream.ExpectedLastSeqHeader:     {"228"},
			jetstream.ExpectedLastSubjSeqHeader: {"5005"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithMsgID("rgerfge"),
			jetstream.WithExpectLastMsgID("bnasdasd"),
			jetstream.WithExpectStream("34r23r"),
			jetstream.WithExpectLastSequence(228),
			jetstream.WithExpectLastSequencePerSubject(5005),
		),
	)

	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.MsgIDHeader:               {"rgerfge"},
			jetstream.ExpectedStreamHeader:      {"34r23r"},
			jetstream.ExpectedLastSubjSeqHeader: {"5005"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithMsgID("rgerfge"),
			jetstream.WithExpectStream("34r23r"),
			jetstream.WithExpectLastSequencePerSubject(5005),
		),
	)

	assert.EqualValues(
		t,
		map[string][]string{
			jetstream.ExpectedLastMsgIDHeader: {"bnasdasd"},
			jetstream.ExpectedLastSeqHeader:   {"228"},
		},
		applyPublishOpts(
			make(nats.Header),
			jetstream.WithExpectLastMsgID("bnasdasd"),
			jetstream.WithExpectLastSequence(228),
		),
	)
}
