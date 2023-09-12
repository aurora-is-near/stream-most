package fake

import (
	"testing"
	"time"

	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/stretchr/testify/require"
)

func TestFakeReader(t *testing.T) {
	reader.UseFake(StartReader)

	fakeInput := &Stream{}
	fakeInput.Add(
		u.Announcement(1, 1, "hash", "prev_hash", []bool{true, true, true}),
		u.Announcement(2, 2, "hash", "prev_hash", []bool{true, true, true}),
		u.Announcement(3, 3, "hash", "prev_hash", []bool{true, true, true}),
		u.Announcement(4, 4, "hash", "prev_hash", []bool{true, true, true}),
		u.Announcement(5, 5, "hash", "prev_hash", []bool{true, true, true}),
	)

	receiver := NewFakeReceiver(len(fakeInput.GetArray()))

	r, err := StartReader(fakeInput, &reader.Options{StartSeq: 2, EndSeq: 5}, receiver)
	require.NoError(t, err)
	defer r.Stop(true)

	result, err := receiver.GetAll(10, time.Second)
	require.NoError(t, err)

	resultSeq := []uint64{}
	for _, msg := range result {
		resultSeq = append(resultSeq, msg.GetSequence())
	}

	require.Equal(
		t,
		[]uint64{2, 3, 4},
		resultSeq,
		"Expected result sequences",
	)
}
