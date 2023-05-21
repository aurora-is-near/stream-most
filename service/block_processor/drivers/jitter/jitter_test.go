package jitter

import (
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/fakes"
	"github.com/aurora-is-near/stream-most/stream/adapters"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
	"testing"
)

func TestJitter_NoDropout(t *testing.T) {
	fakes.UseDefaultOnes()

	fakeInput := fake.NewStream()
	fakeOutput := fake.NewStream()

	fakeInput.Add(
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 0),
		u.Shard(3, 1, "AAA", "000", 1),
		u.Shard(4, 1, "AAA", "000", 2),
		u.Shard(5, 5, "BBB", "AAA", 0),
		u.Shard(6, 5, "BBB", "AAA", 1),
		u.Shard(7, 5, "BBB", "AAA", 2),
		u.Announcement(8, []bool{true, true, true}, 2, "BBB", "AAA"),
	)

	fakeInput.Display()

	rdr, err := reader.Start(&reader.Options{}, fakeInput, 0, 0)
	if err != nil {
		t.Error(err)
	}

	driver := NewJitter(&Options{
		DelayChance:   0.4,
		MaxDelay:      6,
		MinDelay:      2,
		DropoutChance: 0.0,
	})

	input, _ := adapters.ReaderOutputToNatsMessages(rdr.Output(), 10)
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
