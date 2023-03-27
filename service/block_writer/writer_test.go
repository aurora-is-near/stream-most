package block_writer

import (
	"context"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/stream_peek"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/aurora-is-near/stream-most/support"
	"testing"
)

func TestWriter(t *testing.T) {
	outputStream := &stream.FakeNearV3Stream{}
	peeker := stream_peek.NewStreamPeek(outputStream)

	writer := NewWriter(NewOptions().WithDefaults().Validated(), outputStream, peeker)

	msgs := []messages.AbstractNatsMessage{
		support.ATN(1, support.NewSimpleBlockAnnouncement([]bool{true, true, true}, 1, "AAA", "000")),
		support.STN(2, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 1)),
		support.STN(3, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 2)),
		support.STN(4, support.NewSimpleBlockShard([]bool{true, true, true}, 1, "AAA", "000", 3)),
		support.ATN(5, support.NewSimpleBlockAnnouncement([]bool{}, 2, "BBB", "AAA")),
		support.ATN(6, support.NewSimpleBlockAnnouncement([]bool{}, 3, "CCC", "BBB")),
		support.ATN(7, support.NewSimpleBlockAnnouncement([]bool{}, 8, "DDD", "CCC")),
	}

	for _, m := range msgs {
		err := writer.Write(context.Background(), m)
		if err != nil {
			panic(err)
		}
	}

	outputStream.Display()
}
