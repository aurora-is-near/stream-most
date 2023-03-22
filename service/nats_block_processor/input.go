package nats_block_processor

import "github.com/nats-io/nats.go"

type ProcessorInput struct {
	Msg      *nats.Msg
	Metadata *nats.MsgMetadata
}
