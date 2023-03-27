package near_v3

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/stream/adapters"
	"github.com/aurora-is-near/stream-most/u"
	"testing"
)

func TestNearV3_Basic(t *testing.T) {
	fakeInput := &stream.FakeNearV3Stream{}
	fakeOutput := &stream.FakeNearV3Stream{}

	fakeInput.Add(
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 1),
		u.Shard(3, 1, "AAA", "000", 2),
		u.Shard(4, 1, "AAA", "000", 3),
		u.Shard(5, 2, "BBB", "AAA", 1),
		u.Shard(6, 2, "BBB", "AAA", 2),
		u.Shard(7, 2, "BBB", "AAA", 3),
		u.Announcement(8, []bool{true, true, true}, 2, "BBB", "AAA"),
	)

	fakeInput.Display()

	reader, err := stream.StartReader(&stream.ReaderOpts{}, fakeInput, 0, 0)
	if err != nil {
		t.Error(err)
	}

	driver := NewNearV3NoSorting(
		stream_seek.NewStreamSeek(fakeInput),
	)

	input := adapters.ReaderOutputToNatsMessages(reader.Output())
	output := make(chan messages.AbstractNatsMessage, 100)
	driver.Bind(input, output)

	go func() {
		driver.Run()
		close(output)
	}()

	for x := range output {
		fakeOutput.Add(x.(messages.NatsMessage))
	}

	fakeOutput.Display()
}

func TestNearV3_Rescue(t *testing.T) {
	// logrus.SetLevel(logrus.DebugLevel)
	fakeInput := &stream.FakeNearV3Stream{}
	fakeOutput := &stream.FakeNearV3Stream{}

	fakeInput.Add(
		u.Shard(1, 1, "AAA", "000", 1),
		u.Shard(2, 1, "AAA", "000", 2),
		u.Announcement(3, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(4, 1, "AAA", "000", 3),
		u.Shard(5, 2, "BBB", "AAA", 1),
		u.Shard(6, 2, "BBB", "AAA", 2),
		u.Shard(7, 2, "BBB", "AAA", 3),
		u.Announcement(8, []bool{true, true, true}, 2, "BBB", "AAA"),
	)

	fakeInput.Display()

	reader, err := stream.StartReader(&stream.ReaderOpts{}, fakeInput, 3, 0)
	if err != nil {
		t.Error(err)
	}

	driver := NewNearV3NoSorting(
		stream_seek.NewStreamSeek(fakeInput),
	)

	input := adapters.ReaderOutputToNatsMessages(reader.Output())
	output := make(chan messages.AbstractNatsMessage, 100)
	driver.Bind(input, output)

	go func() {
		driver.Run()
		close(output)
	}()

	for x := range output {
		fakeOutput.Add(x.(messages.NatsMessage))
	}

	fakeOutput.Display()
}
