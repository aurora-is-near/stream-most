package reader

import (
	"context"

	"github.com/aurora-is-near/stream-most/domain/messages"
)

type Receiver interface {
	HandleMsg(ctx context.Context, msg messages.NatsMessage)
	HandleFinish(err error)
}

type CbReceiver struct {
	HandleMsgCb    func(ctx context.Context, msg messages.NatsMessage)
	HandleFinishCb func(err error)
}

func NewCbReceiver() *CbReceiver {
	return &CbReceiver{
		HandleMsgCb:    func(ctx context.Context, msg messages.NatsMessage) {},
		HandleFinishCb: func(err error) {},
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
