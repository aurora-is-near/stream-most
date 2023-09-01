package bridge

/*
import (
	"context"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/near_v3"
	"github.com/aurora-is-near/stream-most/service/block_writer"
	"github.com/aurora-is-near/stream-most/service/fakes"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
)

func TestBridge(t *testing.T) {
	// Use default fakes for reader and streams
	fakes.UseDefaultOnes()
	formats.UseFormat(formats.NearV3)

	inputStream := fake.NewStream()
	outputStream := fake.NewStream() //.WithDeduplication()

	inputStream.Add(
		u.Announcement(1, []bool{true, true, true}, 0, "XXX", "000"),
		u.Announcement(2, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(3, 1, "AAA", "000", 0),
		u.Shard(4, 1, "AAA", "000", 1),
		u.Shard(5, 1, "AAA", "000", 1),
		u.Shard(6, 1, "AAA", "000", 2),

		u.Announcement(7, []bool{true, true, true}, 2, "BBB", "AAA"),
		u.Shard(8, 2, "BBB", "AAA", 2),
		u.Shard(9, 2, "BBB", "AAA", 1),
		u.Shard(10, 2, "BBB", "AAA", 0),

		u.Announcement(11, []bool{}, 3, "CCC", "BBB"),
		u.Announcement(12, []bool{}, 4, "DDD", "CCC"),
	)

	driver := near_v3.NewNearV3((&near_v3.Options{
		StuckTolerance:          5,
		StuckRecovery:           true,
		StuckRecoveryWindowSize: 10,
		LastWrittenBlockHash:    nil,
		BlocksCacheSize:         10,
	}).Validated())

	bridge := NewBridge(
		&Options{},
		driver,
		inputStream,
		outputStream,
		&block_writer.Options{},
		&reader.Options{},
	)

	err := bridge.Run(context.TODO())
	if err != nil {
		panic(err)
	}

	outputStream.DisplayWithHeaders()

	outputStream.ExpectExactly(t,
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 0),
		u.Shard(3, 1, "AAA", "000", 1),
		u.Shard(4, 1, "AAA", "000", 2),

		u.Announcement(5, []bool{true, true, true}, 2, "BBB", "AAA"),
		u.Shard(6, 2, "BBB", "AAA", 0),
		u.Shard(7, 2, "BBB", "AAA", 1),
		u.Shard(8, 2, "BBB", "AAA", 2),

		u.Announcement(9, []bool{}, 3, "CCC", "BBB"),
		u.Announcement(10, []bool{}, 4, "DDD", "CCC"),
	)
}
*/
