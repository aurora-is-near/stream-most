package near_v3

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/support"
	"github.com/sirupsen/logrus"
	"testing"
)

func TestNearV3_Basic(t *testing.T) {
	fakeInput := &stream.FakeNearV3Stream{}
	fakeOutput := &stream.FakeNearV3Stream{}

	fakeInput.Add(
		support.ATN(1, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "AAA", "000")),
		support.STN(2, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 1)),
		support.STN(3, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
		support.STN(4, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 3)),
		support.STN(5, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 1)),
		support.STN(6, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 2)),
		support.STN(7, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 3)),
		support.ATN(8, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 2, "BBB", "AAA")),
	)

	fakeInput.Display()

	reader, err := stream.StartReader(&stream.ReaderOpts{}, fakeInput, 0, 0)
	if err != nil {
		t.Error(err)
	}

	driver := NewNearV3NoSorting(
		stream_seek.NewStreamSeek(fakeInput),
	)

	pr := block_processor.NewProcessorWithReader(reader.Output(), driver)
	for x := range pr.Run() {
		fakeOutput.Add(x.(messages.NatsMessage))
	}

	fakeOutput.Display()
}

func TestNearV3_Rescue(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	fakeInput := &stream.FakeNearV3Stream{}
	fakeOutput := &stream.FakeNearV3Stream{}

	fakeInput.Add(
		support.STN(1, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 1)),
		support.STN(2, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
		support.ATN(3, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "AAA", "000")),
		support.STN(4, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 3)),
		support.STN(5, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 1)),
		support.STN(6, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 2)),
		support.STN(7, support.NewSimpleBlockShard([]bool{true, true, true}, 2, "BBB", "AAA", 3)),
		support.ATN(8, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 2, "BBB", "AAA")),
	)

	fakeInput.Display()

	reader, err := stream.StartReader(&stream.ReaderOpts{}, fakeInput, 3, 0)
	if err != nil {
		t.Error(err)
	}

	driver := NewNearV3NoSorting(
		stream_seek.NewStreamSeek(fakeInput),
	)

	pr := block_processor.NewProcessorWithReader(reader.Output(), driver)
	for x := range pr.Run() {
		fakeOutput.Add(x.(messages.NatsMessage))
	}

	fakeOutput.Display()
}
