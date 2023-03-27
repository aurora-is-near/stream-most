package bridge

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/support"
	"testing"
)

func TestBridge(t *testing.T) {
	inputStream := stream.NewFakeNearV3Stream()
	outputStream := stream.NewFakeNearV3Stream().WithDeduplication()

	inputStream.Add(
		support.ATN(1, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "AAA", "000")),
		support.STN(2, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 1)),
		support.STN(3, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
		support.STN(4, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
		support.STN(5, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 3)),
		support.ATN(6, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 2, "BBB", "AAA")),
		support.STN(7, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 1)),
		support.STN(8, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 2)),
		support.STN(9, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 3)),
		support.ATN(10, support.NewSimpleBlockAnnouncement([]bool{}, 3, "CCC", "BBB")),
		support.ATN(11, support.NewSimpleBlockAnnouncement([]bool{}, 8, "DDD", "CCC")),
	)

	bridge := NewBridge(
		&stream.Opts{ShouldFake: true, FakeStream: inputStream},
		&stream.Opts{ShouldFake: true, FakeStream: outputStream},
		&stream.ReaderOpts{},
		1, 0,
	)

	err := bridge.Run()
	if err != nil {
		panic(err)
	}

	outputStream.DisplayWithHeaders()
}
