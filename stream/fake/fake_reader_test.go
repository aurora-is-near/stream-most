package fake

import (
	"testing"

	reader2 "github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
)

func TestFakeReader(t *testing.T) {
	reader2.UseFake(StartReader)

	fakeInput := &Stream{}
	fakeInput.Add(
		u.BTN(1, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "hash", "prev_hash")),
		u.BTN(2, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 2, "hash", "prev_hash")),
		u.BTN(3, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 3, "hash", "prev_hash")),
		u.BTN(4, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 4, "hash", "prev_hash")),
		u.BTN(5, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 5, "hash", "prev_hash")),
	)

	reader, err := reader2.Start(&reader2.Options{}, fakeInput, 2, 3)
	if err != nil {
		panic(err)
	}

	expectedSequences := []uint64{2, 3}
	for x := range reader.Output() {
		if x.Metadata.Sequence.Stream != expectedSequences[0] {
			panic("wrong sequence")
		}
		expectedSequences = expectedSequences[1:]
	}
}
