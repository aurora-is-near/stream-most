package reader

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Receiver interface {
	HandleMsg(ctx context.Context, msg messages.NatsMessage)
	HandleFinish(err error)       // It's guaranteed to be the very last callback which is only called once
	HandleNewKnownSeq(seq uint64) // Might be called concurrently
}

type CbReceiver struct {
	HandleMsgCb         func(ctx context.Context, msg messages.NatsMessage)
	HandleFinishCb      func(err error)
	HandleNewKnownSeqCb func(seq uint64)
}

func NewCbReceiver() *CbReceiver {
	return &CbReceiver{
		HandleMsgCb:         func(ctx context.Context, msg messages.NatsMessage) {},
		HandleFinishCb:      func(err error) {},
		HandleNewKnownSeqCb: func(seq uint64) {},
	}
}

func (cr *CbReceiver) WithHandleMsgCb(cb func(ctx context.Context, msg messages.NatsMessage)) *CbReceiver {
	cr.HandleMsgCb = cb
	return cr
}

func (cr *CbReceiver) WithHandleFinishCb(cb func(err error)) *CbReceiver {
	cr.HandleFinishCb = cb
	return cr
}

func (cr *CbReceiver) WithHandleNewKnownSeqCb(cb func(seq uint64)) *CbReceiver {
	cr.HandleNewKnownSeqCb = cb
	return cr
}
