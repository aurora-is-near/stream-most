package stream

import (
	"github.com/aurora-is-near/stream-most/support"
	"testing"
)

func TestFakeReader(t *testing.T) {
	fakeInput := &FakeNearV3Stream{}
	fakeInput.Add(
		support.ATN(1, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "hash", "prev_hash")),
		support.ATN(2, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 2, "hash", "prev_hash")),
		support.ATN(3, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 3, "hash", "prev_hash")),
		support.ATN(4, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 4, "hash", "prev_hash")),
		support.ATN(5, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 5, "hash", "prev_hash")),
	)

	reader, err := StartReader(&ReaderOpts{}, fakeInput, 2, 3)
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
