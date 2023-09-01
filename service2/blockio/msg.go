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
		Waits until block is available and returns it.
		Primary purpose is asynchronous decoding.

		Error classes:
			- ErrCantDecode
	*/
	GetDecoded(ctx context.Context) (*messages.BlockMessage, error)
}
