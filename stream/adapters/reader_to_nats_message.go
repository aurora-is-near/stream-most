package adapters

import (
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/stream/reader"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func ReaderOutputToNatsMessages(input <-chan *reader.Output) chan messages.AbstractNatsMessage {
	in := make(chan messages.AbstractNatsMessage, 1024)
	go func() {
		for k := range input {
			if k.Error != nil {
				logrus.Errorf("Reader adapter: %v", k.Error)
				continue
			}
			message, err := v3.ProtoDecode(k.Msg.Data)
			if err != nil {
				logrus.Error(errors.Wrap(err, "failed to decode message: "))
				continue
			}

			if message.Payload == nil {
				logrus.Error("Message without a payload!")
				continue
			}

			switch msgT := message.Payload.(type) {
			case *borealisproto.Message_NearBlockHeader:
				in <- messages.NatsMessage{
					Msg:          k.Msg,
					Metadata:     k.Metadata,
					Announcement: messages.NewBlockAnnouncementV3(msgT),
				}
			case *borealisproto.Message_NearBlockShard:
				in <- messages.NatsMessage{
					Msg:      k.Msg,
					Metadata: k.Metadata,
					Shard:    messages.NewBlockShard(msgT),
				}
			}
		}
		close(in)
	}()

	return in
}
