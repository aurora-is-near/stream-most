package bridge

import (
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/u"
	"testing"
)

func TestBridge(t *testing.T) {
	inputStream := stream.NewFakeNearV3Stream()
	outputStream := stream.NewFakeNearV3Stream()

	inputStream.Add(
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 1),
		u.Shard(3, 1, "AAA", "000", 2),
		u.Shard(4, 1, "AAA", "000", 2),
		u.Shard(5, 1, "AAA", "000", 3),

		u.Announcement(6, []bool{true, true, true}, 2, "BBB", "AAA"),
		u.Shard(7, 2, "BBB", "AAA", 1),
		u.Shard(8, 2, "BBB", "AAA", 2),
		u.Shard(9, 2, "BBB", "AAA", 3),

		u.Announcement(10, []bool{}, 3, "CCC", "BBB"),
		u.Announcement(11, []bool{}, 4, "DDD", "CCC"),
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

	outputStream.DisplayRows() // or DisplayWithHeaders()
}
