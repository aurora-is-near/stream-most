package block_writer

import (
	"context"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/u"
	"testing"
)

func TestWriter(t *testing.T) {
	outputStream := fake.NewStream()
	peeker := stream_peek.NewStreamPeek(outputStream)

	writer := NewWriter(NewOptions().WithDefaults().Validated(), outputStream, peeker)

	msgs := []messages.AbstractNatsMessage{
		u.Announcement(1, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(2, 1, "AAA", "000", 1),
		u.Shard(3, 1, "AAA", "000", 2),
		u.Shard(4, 1, "AAA", "000", 2),
		u.Announcement(5, []bool{true, true, true}, 2, "BBB", "AAA"),
		u.Announcement(6, []bool{true, true, true}, 3, "CCC", "BBB"),
		u.Announcement(7, []bool{true, true, true}, 4, "DDD", "CCC"),
	}

	for _, m := range msgs {
		err := writer.Write(context.Background(), m)
		if err != nil {
			panic(err)
		}
	}

	outputStream.Display()
}
