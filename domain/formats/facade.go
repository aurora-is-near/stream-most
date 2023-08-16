package formats

import (
	"fmt"

	"github.com/aurora-is-near/stream-most/domain/blocks"
	"github.com/aurora-is-near/stream-most/domain/formats/v2_aurora"
	"github.com/aurora-is-near/stream-most/domain/formats/v2_near"
	v3 "github.com/aurora-is-near/stream-most/domain/formats/v3"
	"github.com/aurora-is-near/stream-most/domain/messages"
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

func (f *Facade) ParseMsg(msg messages.NatsMessage) (messages.BlockMessage, error) {
	block, err := f.ParseBlock(msg.GetData())
	if err != nil {
		return nil, fmt.Errorf("unable to parse block from message data: %w", err)
	}

	return &messages.AbstractBlockMessage{
		Block:       block,
		NatsMessage: msg,
	}, nil
}

func NewFacade() *Facade {
	return &Facade{}
}
