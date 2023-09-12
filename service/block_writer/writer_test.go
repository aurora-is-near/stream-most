package block_writer

import (
	"context"
	"testing"

	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/testing/u"
)

func TestWriter(t *testing.T) {
	formats.UseFormat(formats.NearV3)

	outputStream := fake.NewStream()
	peeker := stream_peek.NewStreamPeek(outputStream)

	writer := NewWriter(NewOptions().WithDefaults().Validated(), outputStream, peeker)

	msgs := []*messages.BlockMessage{
		u.Announcement(1, 1, "AAA", "000", []bool{true, true, true}),
		u.Shard(2, 1, 1, "AAA", "000", []bool{true, true, true}),
		u.Shard(3, 1, 2, "AAA", "000", []bool{true, true, true}),
		u.Shard(4, 1, 2, "AAA", "000", []bool{true, true, true}),
		u.Announcement(5, 2, "BBB", "AAA", []bool{true, true, true}),
		u.Announcement(6, 3, "CCC", "BBB", []bool{true, true, true}),
		u.Announcement(7, 4, "DDD", "CCC", []bool{true, true, true}),
	}

	for _, m := range msgs {
		err := writer.Write(context.Background(), m)
		if err != nil {
			panic(err)
		}
	}

	outputStream.Display()
}
