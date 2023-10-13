package blockio

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Msg interface {
	/*
		Returns original message
	*/
	Get() messages.NatsMessage

	/*
		Waits until block is decoded and returns it (or decoding error).
	*/
	GetDecoded(ctx context.Context) (*messages.BlockMessage, error)
}
