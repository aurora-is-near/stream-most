package block_processor

import (
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/aurora-is-near/stream-most/domain/new_format"
	"github.com/aurora-is-near/stream-most/service/block_processor/drivers"
	"github.com/aurora-is-near/stream-most/stream"
	"github.com/sirupsen/logrus"
)

func NewProcessorWithReader(input <-chan *stream.ReaderOutput, driver drivers.Driver) *Processor {
	in := make(chan messages.AbstractNatsMessage, 1024)

	f := NewProcessor(in, driver)
	go func() {
		for k := range input {
			message, err := new_format.ProtoDecode(k.Msg.Data)
			if err != nil {
				logrus.Error(err)
			}

			switch msgT := message.Payload.(type) {
			case *borealisproto.Message_NearBlockHeader:
				in <- messages.NatsMessage{
					Msg:          k.Msg,
					Metadata:     k.Metadata,
					Announcement: messages.NewBlockAnnouncement(msgT),
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

	return f
}
