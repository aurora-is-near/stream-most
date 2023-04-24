package formats

import (
	borealisproto "github.com/aurora-is-near/borealis-prototypes/go"
	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats/v2_aurora"
	"github.com/aurora-is-near/stream-most/domain/formats/v2_near"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type Facade struct {
	format FormatType
}

func (f *Facade) UseFormat(format FormatType) {
	f.format = format
}

func (f *Facade) ParseAbstractBlock(data []byte) (*blocks.AbstractBlock, error) {
	switch f.format {
	case NearV2:
		return v2_near.ParseNearBlock(data, map[string][]string{})
	case AuroraV2:
		return v2_aurora.ParseAuroraBlock(data, map[string][]string{})
	case NearV3:
		message, err := v3.ProtoToMessage(data)
		if err != nil {
			return nil, err
		}

		switch m := message.(type) {
		case *messages.BlockAnnouncement:
			return &blocks.AbstractBlock{
				Hash:     m.Block.Hash,
				PrevHash: m.Block.PrevHash,
				Height:   m.Block.Height,
			}, nil
		case *messages.BlockShard:
			return &blocks.AbstractBlock{
				Hash:     m.Block.Hash,
				PrevHash: m.Block.PrevHash,
				Height:   m.Block.Height,
			}, nil
		}
	}
	return nil, errors.New("unknown format")
}

func (f *Facade) ParseRawMsg(msg *nats.RawStreamMsg) (messages.AbstractNatsMessage, error) {
	switch f.format {
	case NearV2:
		parsed, err := v2_near.ParseNearBlock(msg.Data, msg.Header)
		if err != nil {
			return nil, err
		}

		return messages.NatsMessage{
			// Msg and Metadata are same for all cases.
			// Consider factoring them out outside of switch-case?
			Msg: &nats.Msg{               
				Subject: msg.Subject,
				Header:  msg.Header,
				Data:    msg.Data,
			},
			Metadata: &nats.MsgMetadata{
				Sequence: nats.SequencePair{
					Stream: msg.Sequence,
				},
			},
			Announcement: messages.NewBlockAnnouncementV2(parsed),
			Shard:        nil,
		}, nil
	case AuroraV2:
		parsed, err := v2_aurora.ParseAuroraBlock(msg.Data, msg.Header)
		if err != nil {
			return nil, err
		}

		return messages.NatsMessage{
			Msg: &nats.Msg{
				Subject: msg.Subject,
				Header:  msg.Header,
				Data:    msg.Data,
			},
			Metadata: &nats.MsgMetadata{
				Sequence: nats.SequencePair{
					Stream: msg.Sequence,
				},
			},
			Announcement: messages.NewBlockAnnouncementV2(parsed),
			Shard:        nil,
		}, nil
	case NearV3:
		message, err := v3.ProtoToMessage(msg.Data)
		if err != nil {
			return nil, err
		}

		switch m := message.(type) {
		case *messages.BlockAnnouncement:
			return messages.NatsMessage{
				Msg: &nats.Msg{
					Subject: msg.Subject,
					Header:  msg.Header,
					Data:    msg.Data,
				},
				Metadata: &nats.MsgMetadata{
					Sequence: nats.SequencePair{
						Stream: msg.Sequence,
					},
				},
				Announcement: m,
			}, nil
		case *messages.BlockShard:
			return messages.NatsMessage{
				Msg: &nats.Msg{
					Subject: msg.Subject,
					Header:  msg.Header,
					Data:    msg.Data,
				},
				Metadata: &nats.MsgMetadata{
					Sequence: nats.SequencePair{
						Stream: msg.Sequence,
					},
				},
				Shard: m,
			}, nil
		default:
			return nil, errors.New("unknown message type for NearV3")
		}
	default:
		panic("Unknown format specified for facade")
	}
}

func (f *Facade) Parse(msg *nats.Msg) (messages.AbstractNatsMessage, error) {
	metadata, _ := msg.Metadata()
	return f.ParseWithMetadata(msg, metadata)
}

func (f *Facade) ParseWithMetadata(msg *nats.Msg, metadata *nats.MsgMetadata) (messages.AbstractNatsMessage, error) {
	header := msg.Header

	switch f.format {
	case NearV2:
		parsed, err := v2_near.ParseNearBlock(msg.Data, header)
		if err != nil {
			return nil, err
		}

		return messages.NatsMessage{
			Msg:          msg,
			Metadata:     metadata,
			Announcement: messages.NewBlockAnnouncementV2(parsed),
			Shard:        nil,
		}, nil
	case AuroraV2:
		parsed, err := v2_aurora.ParseAuroraBlock(msg.Data, header)
		if err != nil {
			return nil, err
		}

		return messages.NatsMessage{
			Msg:          msg,
			Metadata:     metadata,
			Announcement: messages.NewBlockAnnouncementV2(parsed),
			Shard:        nil,
		}, nil
	case NearV3:
		message, err := v3.ProtoDecode(msg.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode message: ")
		}

		if message.Payload == nil {
			return nil, errors.New("message without a payload")
		}

		switch msgT := message.Payload.(type) {
		case *borealisproto.Message_NearBlockHeader:
			return messages.NatsMessage{
				Msg:          msg,
				Metadata:     metadata,
				Announcement: messages.NewBlockAnnouncementV3(msgT),
			}, nil
		case *borealisproto.Message_NearBlockShard:
			return messages.NatsMessage{
				Msg:      msg,
				Metadata: metadata,
				Shard:    messages.NewBlockShard(msgT),
			}, nil
		}
		return nil, errors.New("unknown message type for NearV3")
	default:
		panic("Unknown format specified for facade")
	}
}

func NewFacade() *Facade {
	return &Facade{}
}
