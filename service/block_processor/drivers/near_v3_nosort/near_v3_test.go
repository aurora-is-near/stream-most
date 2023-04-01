package near_v3_nosort

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/aurora-is-near/stream-most/service/fakes"
	"github.com/aurora-is-near/stream-most/service/stream_seek"
	"github.com/aurora-is-near/stream-most/stream/adapters"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/u"
	"testing"
)

func TestNearV3_Basic(t *testing.T) {
	fakeInput := fake.NewStream()
	fakeOutput := fake.NewStream()

	fakeInput.Add(
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 1),
		u.Shard(3, 1, "AAA", "000", 2),
		u.Shard(4, 1, "AAA", "000", 3),
		u.Shard(5, 5, "BBB", "AAA", 1),
		u.Shard(6, 5, "BBB", "AAA", 2),
		u.Shard(7, 5, "BBB", "AAA", 3),
		u.Announcement(8, []bool{true, true, true}, 2, "BBB", "AAA"),
	)

	fakeInput.Display()

	rdr, err := reader.Start(&reader.Options{}, fakeInput, 0, 0)
	if err != nil {
		t.Error(err)
	}

	driver := NewNearV3NoSorting(
		stream_seek.NewStreamSeek(fakeInput),
	)

	input := adapters.ReaderOutputToNatsMessages(rdr.Output())
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
	fakes.UseDefaultOnes()

	fakeInput := &fake.Stream{}
	fakeOutput := &fake.Stream{}

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

	rdr, err := reader.Start(&reader.Options{}, fakeInput, 3, 0)
	if err != nil {
		t.Error(err)
	}

	driver := NewNearV3NoSorting(
		stream_seek.NewStreamSeek(fakeInput),
	)

	input := adapters.ReaderOutputToNatsMessages(rdr.Output())
	output := make(chan messages.AbstractNatsMessage, 100)
	driver.Bind(input, output)
	driver.BindObserver(observer.NewObserver())

	go func() {
		driver.Run()
		close(output)
	}()

	for x := range output {
		fakeOutput.Add(x.(messages.NatsMessage))
	}

	fakeOutput.DisplayRows()
}
