package formats

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats/headers"
	v2 "github.com/aurora-is-near/stream-most/domain/formats/v2"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/nats-io/nats.go/jetstream"
)

type Facade struct {
	format FormatType
}

func (f *Facade) UseFormat(format FormatType) {
	f.format = format
}

func (f *Facade) GetFormat() FormatType {
	return f.format
}

func (f *Facade) ParseBlock(data []byte) (blocks.Block, error) {
	switch f.format {
	case HeadersOnly:
		return nil, fmt.Errorf("can't parse block from message data, selected format is headers-only")
	case NearV2:
		return v2.DecodeNearBlock(data)
	case AuroraV2:
		return v2.DecodeAuroraBlock(data)
	case NearV3:
		return v3.DecodeProtoBlock(data)
	default:
		return nil, fmt.Errorf("unknown format: %v", f.format)
	}
}

func (f *Facade) ParseMsgID(msgID string) (blocks.Block, error) {
	return headers.ParseMsgID(msgID)
}

func (f *Facade) ParseMsg(msg messages.NatsMessage) (*messages.BlockMessage, error) {
	var err error
	var block blocks.Block

	switch f.format {
	case HeadersOnly:
		block, err = f.ParseMsgID(msg.GetHeader().Get(jetstream.MsgIDHeader))
	default:
		block, err = f.ParseBlock(msg.GetData())
	}

	if err != nil {
		return nil, fmt.Errorf("unable to parse block: %w", err)
	}

	return &messages.BlockMessage{
		Block: block,
		Msg:   msg,
	}, nil
}

func NewFacade() *Facade {
	return &Facade{}
}
