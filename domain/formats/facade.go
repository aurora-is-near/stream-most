package formats

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats/v2_aurora"
	"github.com/aurora-is-near/stream-most/domain/formats/v2_near"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
	"github.com/nats-io/nats.go"
)

type Facade struct {
	format FormatType
}

func (f *Facade) UseFormat(format FormatType) {
	f.format = format
}

func (f *Facade) ParseBlock(data []byte) (blocks.Block, error) {
	switch f.format {
	case AuroraV2:
		return v2_aurora.DecodeAuroraBlock(data)
	case NearV2:
		return v2_near.DecodeNearBlock(data)
	case NearV3:
		return v3.DecodeProtoBlock(data)
	default:
		return nil, fmt.Errorf("unknown format: %v", f.format)
	}
}

func (f *Facade) ParseMsg(msg *nats.Msg, meta *nats.MsgMetadata) (messages.Message, error) {
	block, err := f.ParseBlock(msg.Data)
	if err != nil {
		return nil, err
	}

	return &messages.AbstractMessage{
		TypedMessage: messages.TypedMessage{Block: block},
		Msg:          msg,
		Meta:         meta,
	}, nil
}

func (f *Facade) ParseRawMsg(msg *nats.RawStreamMsg) (messages.Message, error) {
	block, err := f.ParseBlock(msg.Data)
	if err != nil {
		return nil, err
	}

	return &messages.AbstractRawMessage{
		TypedMessage: messages.TypedMessage{Block: block},
		RawMsg:       msg,
	}, nil
}

func NewFacade() *Facade {
	return &Facade{}
}
