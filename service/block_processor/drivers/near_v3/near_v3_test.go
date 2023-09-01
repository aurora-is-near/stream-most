package near_v3

/*
import (
	"context"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/fakes"
	"github.com/aurora-is-near/stream-most/stream/adapters"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
)

func TestNearV3_Basic(t *testing.T) {
	fakes.UseDefaultOnes()
	formats.UseFormat(formats.NearV3)

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

	rdr, err := reader.Start(context.Background(), fakeInput, &reader.Options{}, formats.DefaultMsgParser)
	if err != nil {
		t.Fatal(err)
	}

	driver := NewNearV3(&Options{
		StuckTolerance:          10,
		StuckRecovery:           false,
		StuckRecoveryWindowSize: 0,
		LastWrittenBlockHash:    nil,
		BlocksCacheSize:         10,
	})

	input, _ := adapters.ReaderToBlockMessage(context.Background(), rdr, 10)
	output := make(chan *messages.BlockMessage, 100)
	driver.Bind(input, output)

	go func() {
		driver.Run()
	}()

	for x := range output {
		fakeOutput.Add(x)
	}

	err = driver.FinishError()
	if err != nil {
		t.Fatal(err)
	}

	fakeOutput.Display()
}

func TestNearV3_Stuck(t *testing.T) {
	fakes.UseDefaultOnes()
	formats.UseFormat(formats.NearV3)

	fakeInput := fake.NewStream()
	fakeOutput := fake.NewStream()

	fakeInput.Add(
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 0),
		u.Shard(3, 1, "AAA", "000", 1),
		u.Shard(4, 1, "AAA", "000", 2),
		u.Shard(5, 5, "BBB", "AAA", 0),
		u.Shard(7, 5, "BBB", "AAA", 2),
		u.Announcement(8, []bool{true, true, true}, 2, "BBB", "AAA"),
		u.Announcement(9, []bool{true, true, true}, 3, "CCC", "BBB"),
		u.Announcement(10, []bool{true, true, true}, 4, "DDD", "CCC"),
		u.Announcement(11, []bool{true, true, true}, 5, "EEE", "DDD"),
		u.Announcement(12, []bool{true, true, true}, 6, "FFF", "EEE"),
		u.Announcement(13, []bool{true, true, true}, 7, "GGG", "FFF"),
	)

	fakeInput.Display()

	rdr, err := reader.Start(context.Background(), fakeInput, &reader.Options{}, formats.DefaultMsgParser)
	if err != nil {
		t.Fatal(err)
	}

	driver := NewNearV3(&Options{
		StuckTolerance:          5,
		StuckRecovery:           false,
		StuckRecoveryWindowSize: 0,
		LastWrittenBlockHash:    nil,
		BlocksCacheSize:         10,
	})

	input, _ := adapters.ReaderToBlockMessage(context.Background(), rdr, 10)
	output := make(chan *messages.BlockMessage, 100)
	driver.Bind(input, output)

	go func() {
		driver.Run()
	}()

	for x := range output {
		fakeOutput.Add(x)
	}

	fakeOutput.Display()

	err = driver.FinishError()
	if err == nil {
		t.Fatal("error is nil, expected stuck")
	}
	if err.Error() != "stuck" {
		t.Error("error is not stuck")
	}
}
*/
