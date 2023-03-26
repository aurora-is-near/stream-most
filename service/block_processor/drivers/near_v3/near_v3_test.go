package near_v3

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/support"
	"testing"
)

func TestNearV3(t *testing.T) {
	fakeInput := &stream.FakeNearV3Stream{}
	fakeOutput := &stream.FakeNearV3Stream{}

	fakeInput.Add(
		support.ATN(1, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "hash", "prev_hash")),
		support.STN(2, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "hash", "prev_hash", 1)),
		support.STN(3, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "hash", "prev_hash", 2)),
		support.STN(4, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "hash", "prev_hash", 3)),
		support.ATN(5, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "kekek", "prev_hash")),
		support.STN(6, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "kekek", "prev_hash", 1)),
		support.STN(7, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "kekek", "prev_hash", 2)),
		support.STN(8, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "kekek", "prev_hash", 3)),
	)

	fakeInput.Display()

	driver := NewNearV3NoSorting(
		stream_seek.NewStreamSeek(fakeInput),
	)

	input := make(chan messages.AbstractNatsMessage)
	output := make(chan messages.AbstractNatsMessage)

	driver.Bind(input, output)
}
