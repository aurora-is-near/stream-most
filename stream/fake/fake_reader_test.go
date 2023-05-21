package fake

import (
	reader2 "github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
	"testing"
)

func TestFakeReader(t *testing.T) {
	fakeInput := &Stream{}
	fakeInput.Add(
		u.ATN(1, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "hash", "prev_hash")),
		u.ATN(2, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 2, "hash", "prev_hash")),
		u.ATN(3, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 3, "hash", "prev_hash")),
		u.ATN(4, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 4, "hash", "prev_hash")),
		u.ATN(5, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 5, "hash", "prev_hash")),
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
