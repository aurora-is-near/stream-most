package fake

import (
	"context"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/stretchr/testify/require"
)

func TestFakeReader(t *testing.T) {
	reader.UseFake(StartReader[struct{}])

	fakeInput := &Stream{}
	fakeInput.Add(
		u.Announcement(1, []bool{true, true, true}, 1, "hash", "prev_hash"),
		u.Announcement(2, []bool{true, true, true}, 2, "hash", "prev_hash"),
		u.Announcement(3, []bool{true, true, true}, 3, "hash", "prev_hash"),
		u.Announcement(4, []bool{true, true, true}, 4, "hash", "prev_hash"),
		u.Announcement(5, []bool{true, true, true}, 5, "hash", "prev_hash"),
	)

	reader, err := reader.Start(context.Background(), fakeInput, &reader.Options{StartSeq: 2, EndSeq: 4},
		func(msg messages.NatsMessage) (struct{}, error) {
			return struct{}{}, nil
		},
	)
	if err != nil {
		panic(err)
	}

	resultSequences := []uint64{}
	for msg := range reader.Output() {
		resultSequences = append(resultSequences, msg.Msg().GetSequence())
	}

	require.Equal(
		t,
		[]uint64{2, 3},
		resultSequences,
		"Expected result sequences",
	)
}
