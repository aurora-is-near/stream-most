package block_writer

import (
	"context"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/u"
	"testing"
)

func TestWriter(t *testing.T) {
	outputStream := &stream.FakeNearV3Stream{}
	peeker := stream_peek.NewStreamPeek(outputStream)

	writer := NewWriter(NewOptions().WithDefaults().Validated(), outputStream, peeker)

	msgs := []messages.AbstractNatsMessage{
		u.ATN(1, u.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "AAA", "000")),
		u.STN(2, u.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 1)),
		u.STN(3, u.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
		u.STN(4, u.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 3)),
		u.ATN(5, u.NewSimpleBlockAnnouncement([]bool{}, 2, "BBB", "AAA")),
		u.ATN(6, u.NewSimpleBlockAnnouncement([]bool{}, 3, "CCC", "BBB")),
		u.ATN(7, u.NewSimpleBlockAnnouncement([]bool{}, 8, "DDD", "CCC")),
	}

	for _, m := range msgs {
		err := writer.Write(context.Background(), m)
		if err != nil {
			panic(err)
		}
	}

	outputStream.Display()
}
