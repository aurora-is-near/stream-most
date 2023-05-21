package block_processor

import (
	"context"
	"github.com/aurora-is-near/stream-most/domain/formats"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers/near_v3"
	"github.com/aurora-is-near/stream-most/service/block_processor/observer"
	"github.com/aurora-is-near/stream-most/service/fakes"
	"github.com/aurora-is-near/stream-most/stream/adapters"
	"github.com/aurora-is-near/stream-most/stream/fake"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/aurora-is-near/stream-most/testing/u"
	"github.com/sirupsen/logrus"
	"testing"
)

func TestBlockProcessor(t *testing.T) {
	fakes.UseDefaultOnes()
	formats.UseFormat(formats.NearV3)

	input := fake.NewStream()
	input.Add(
		u.Shard(1, 1, "AAA", "AAA", 3),
		u.Announcement(2, []bool{true, true, true}, 1, "AAA", "000"),
		u.Shard(3, 1, "AAA", "AAA", 1),
		u.Shard(4, 1, "AAA", "AAA", 2),

		u.Announcement(5, []bool{true, true, true}, 2, "BBB", "AAA"),
	)

	reader, err := reader.Start(&reader.Options{}, input, 2, 0)
	if err != nil {
		return
	}

	inputStream, _ := adapters.ReaderOutputToNatsMessages(reader.Output(), 10)

	processor := NewProcessor(inputStream, near_v3.NewNearV3((&near_v3.Options{
		StuckTolerance:          10,
		StuckRecovery:           false,
		StuckRecoveryWindowSize: 0,
		LastWrittenBlockHash:    nil,
		BlocksCacheSize:         100,
	}).WithDefaults().Validated()))
	processor.On(observer.RescueNeeded, func(currentAnnouncement interface{}) {
		logrus.Warnf(
			"We had a need for a rescue operation on the block %s",
			currentAnnouncement.(*messages.BlockAnnouncement).Block.Hash,
		)
	})

	output := processor.Run(context.TODO())
	outputStream := fake.NewStream()
	for msg := range output {
		outputStream.Add(msg.(messages.NatsMessage))
	}

	outputStream.DisplayRows()
}
